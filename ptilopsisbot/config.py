import json
import re
import tomllib
from importlib.metadata import entry_points
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


class FilePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name_pattern: str = r"run-\d{8}-\d{6}-\d{6}\.zip"
    time_format: str | None = "run-%Y%m%d-%H%M%S-%f.zip"
    validation: Literal["zip", "bytes"] = "zip"

    @model_validator(mode="after")
    def validate_pattern(self) -> "FilePolicy":
        re.compile(self.name_pattern)
        return self


class PluginCommand(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    command: list[str] = Field(min_length=1, max_length=50)
    timeout_seconds: int = Field(default=60, ge=1, le=3600)

    @model_validator(mode="after")
    def validate_command(self) -> "PluginCommand":
        if any(not value or "\0" in value for value in self.command):
            raise ValueError("插件命令参数不能为空或包含 NUL")
        if not Path(self.command[0]).is_absolute():
            raise ValueError("插件可执行文件必须是绝对路径")
        return self


class PluginMaintenance(PluginCommand):
    interval_seconds: int = Field(default=30, ge=1, le=3600)


class PluginConfig(PluginCommand):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,79}$")
    version: str = Field(min_length=1, max_length=80)
    required_for_cleanup: bool = False
    maintenance: PluginMaintenance | None = None


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    group_id: int = Field(gt=0)
    data_dir: Path = Path("data")
    access_token: SecretStr = SecretStr("")
    port: int = Field(default=8080, ge=1, le=65535)
    auto_delete: bool = True
    send_receipts: bool = True
    milestone_interval: int | None = Field(default=None, gt=0)
    delete_grace_hours: float = Field(default=24, ge=0, allow_inf_nan=False)
    history_delete_grace_hours: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    max_file_mib: int = Field(default=256, gt=0)
    min_free_gib: float = Field(default=2, ge=0, allow_inf_nan=False)
    download_concurrency: int = Field(default=3, ge=1, le=6)
    download_max_attempts: int = Field(default=3, ge=1, le=10)
    cleanup_concurrency: int = Field(default=3, ge=1, le=3)
    group_storage_limit_gib: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    file_policy: FilePolicy = FilePolicy()
    plugins: list[PluginConfig] = Field(default_factory=list)
    plugin_configs: list[Path] = Field(default_factory=list)
    disabled_plugins: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_plugins(self) -> "Settings":
        if len({plugin.id for plugin in self.plugins}) != len(self.plugins):
            raise ValueError("插件 ID 重复")
        return self


def load_settings(path: Path) -> Settings:
    with path.open("rb") as source:
        settings = Settings.model_validate(tomllib.load(source))
    plugins = list(settings.plugins)
    for reference in settings.plugin_configs:
        source_path = (path.resolve().parent / reference).resolve()
        if source_path.stat().st_size > 65536:
            raise ValueError("插件配置超过 64 KiB")
        plugins.append(
            PluginConfig.model_validate(json.loads(source_path.read_text(encoding="utf-8")))
        )
    settings = Settings.model_validate({**settings.model_dump(), "plugins": plugins})
    if not settings.access_token.get_secret_value().strip():
        raise ValueError("请在配置中填写 access_token，并在 NapCat 的反向 WS 中使用同一个值")
    data_dir = (path.resolve().parent / settings.data_dir).resolve()
    disabled = set(settings.disabled_plugins)
    configured = {plugin.id for plugin in plugins}
    discovered: set[str] = set()
    for entry in sorted(entry_points(group="ptilopsisbot.plugins"), key=lambda item: item.name):
        if entry.name in configured or entry.name in disabled:
            continue
        if entry.name in discovered:
            raise ValueError(f"多个已安装包提供同一个插件：{entry.name}")
        discovered.add(entry.name)
        try:
            # Providers are locally installed code, never discovered from uploads.
            value = entry.load()({"protocol_version": 1, "data_dir": str(data_dir)})
            plugin = PluginConfig.model_validate(value)
            if plugin.id != entry.name:
                raise ValueError("插件 ID 与安装声明不一致")
        except Exception as error:
            raise ValueError(f"已安装插件 {entry.name} 无法加载：{error}") from error
        plugins.append(plugin)
    return Settings.model_validate(
        {
            **settings.model_dump(),
            "data_dir": data_dir,
            "plugins": [plugin for plugin in plugins if plugin.id not in disabled],
        }
    )
