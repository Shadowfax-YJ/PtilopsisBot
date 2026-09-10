import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, SecretStr


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


def load_settings(path: Path) -> Settings:
    with path.open("rb") as source:
        settings = Settings.model_validate(tomllib.load(source))
    if not settings.access_token.get_secret_value().strip():
        raise ValueError("请在配置中填写 access_token，并在 NapCat 的反向 WS 中使用同一个值")
    return settings.model_copy(
        update={"data_dir": (path.resolve().parent / settings.data_dir).resolve()}
    )
