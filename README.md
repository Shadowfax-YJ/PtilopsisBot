# QQ 对局数据小助手

将群里的 `run-YYYYMMDD-HHMMSS-NNNNNN.zip` 下载到本机，检查完整性，再清理对应群文件腾出空间。默认群号为 **1095012141**，只主动扫描根目录。

## 安装与启动（Windows / Python 3.11+）

在项目目录执行：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
Copy-Item config.example.toml config.toml
.\.venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
```

把生成的随机值填进 `config.toml` 的 `access_token`。本机若已有配置，保留它即可。配置和数据目录不会提交到 Git。

1. 按 [LLBot 官方安装说明](https://luckylillia.com/guide/choice_install)安装并登录机器人 QQ，确保它已加入目标群且有群文件删除权限。
2. 在 LLBot 中启用 **OneBot 11 WebSocket 客户端 / 反向 WebSocket**，URL 为 `ws://127.0.0.1:8080/onebot/v11/`，token 与本机配置一致。连接方式见 [NoneBot OneBot 文档](https://onebot.adapters.nonebot.dev/docs/guide/setup/)。只需这个连接，无需另开 LLBot HTTP API。
3. 运行机器人：

```powershell
.\.venv\Scripts\python.exe -X utf8 -m databot run
```

修改配置后重启。默认 `auto_delete=true`，每个包保存成功满 24 小时后可清理；将 `delete_grace_hours` 改为 `0` 可在保存和检查通过后立即释放群空间。启动补扫会收集根目录里已有的符合条件的包。

## 平时怎么用

机器人常驻时，在另一个终端执行：

```powershell
.\.venv\Scripts\python.exe -X utf8 -m databot status
.\.venv\Scripts\python.exe -X utf8 -m databot scan
.\.venv\Scripts\python.exe -X utf8 -m databot retry 12
.\.venv\Scripts\python.exe -X utf8 -m databot report 2026-09-05
.\.venv\Scripts\python.exe -X utf8 -m databot report 2026-09-05 --preview
```

`status` 查看连接和最近 50 条上传记录；`scan` 补扫并检查到期清理；`retry` 使用上传记录 ID 重试；`report` 发回目标群，`--preview` 只在本机查看。命令通过同一个本机端口和 token 操作正在运行的机器人，不会启动第二个下载进程。正常 Ctrl+C 停止机器人即可暂停采集和清理。

收到事件即登记下载；连接、重连及每 30 分钟补扫根目录，LLBot 的列表接口自行翻页。下载失败每隔 5 分钟重试，合计最多 3 次；检查不通过或耗尽次数后看错误原因，处理完再 `retry`。每轮处理和远程操作前检查 QQ 在线状态，掉线暂停，不消耗单个文件的重试次数。

每天北京时间 00:05 发前一天日报，分别统计上传次数和按完整 ZIP 哈希去重的新增唯一包数。相同包的新增贡献归首次收集成功的上传记录；迟到完成和离线漏发可手动重发。当前只检查 ZIP 可读性，待真实游戏样本到位后补必需文件与字段检查。

## 保存和清理规则

- 文件保存为 `data/archive/<上传日期>/<上传者QQ>/<记录ID>.zip`；每次上传独立保留，原名和 SHA-256 在 `data/collector.sqlite3` 中。
- 删除只针对当前群的确切源文件 ID，必须已收集成功、经过宽限期，且本地文件大小和重新计算的 SHA-256 都正确。失败包、本地文件缺失或损坏时保留群文件；删除不影响本地文件和贡献记录。
- 本地空间低于 2 GiB、账号离线或数据库不可写时暂停下载与清理。修复后自动继续；日志位于 `data/collector.log`，最多保留约 20 MiB。
- 本地文件不自动清理。需要备份时停止机器人，复制整个 `data` 目录；仅有本地一份副本时，磁盘损坏仍可能丢失数据。

## 开机运行

用 Windows 任务计划程序新建一个开机或登录触发的任务，程序为 `powershell.exe`，参数为 `-NoProfile -ExecutionPolicy Bypass -File "D:\Projects\databot\start.ps1"`。设置“如果任务已在运行，不启动新实例”，并关闭默认的运行时长限制。LLBot 也需要启动并保持 QQ 登录。

## 验证与当前进度

```powershell
.\.venv\Scripts\python.exe -m mypy
.\.venv\Scripts\python.exe -m ruff check databot tests
.\.venv\Scripts\python.exe -X utf8 -m pytest -q
```

自动测试覆盖真实 SQLite、临时归档目录、HTTP 流式下载和 OneBot 反向 WebSocket；其中包含 100 MiB ZIP 的本机端到端收集、清理与日报测试。测试使用模拟的 QQ 文件列表，不连接实际 QQ 群。

当前本机尚无 LLBot，也没有真实游戏 ZIP 样本。代码和离线验证不能替代真实群验收：安装连接后，使用专门测试上传确认群文件被移除、本地 ZIP 可读、日报仍保留贡献；同时检查实际根目录列表是否完整。验收完成前，[地图](.scratch/qq-group-run-collector/map.md)中的真实环境事项保持待验收。

接入端已由用户确认改为 LLBot：其[目录列表](https://github.com/LLOneBot/LuckyLilliaBot/blob/main/src/onebot11/action/go-cqhttp/GetGroupRootFiles.ts)、[上传事件](https://github.com/LLOneBot/LuckyLilliaBot/blob/main/src/onebot11/entities.ts)和[删除接口](https://github.com/LLOneBot/LuckyLilliaBot/blob/main/src/onebot11/action/go-cqhttp/DeleteGroupFile.ts)使用源文件 UUID，符合本项目的去重和延迟清理方式。当前 NapCat 的文件 ID 是内存缓存引用，事件与列表引用不同且重启失效，因此本实现不支持直接替换为 NapCat。
