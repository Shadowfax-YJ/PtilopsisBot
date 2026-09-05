# PtilopsisBot

一个采用《明日方舟》干员白面鸮说话风格的 QQ 群对局数据小助手。将 `run-YYYYMMDD-HHMMSS-NNNNNN.zip` 下载到本机，检查完整性，再清理对应群文件腾出空间。默认群号为 **1095012141**，只主动扫描根目录。

## 安装与启动（Windows / Python 3.11+）

NapCat 和小助手都放在那台 24 小时开机的 Windows 电脑上。两者通过本机连接，开发电脑可以关机，无需公网 IP 或端口映射。示例目录为 `D:\Projects\PtilopsisBot`。

已有安装目录可以保留原名，配置和 `data` 不需要迁移。更新代码后重新运行 `.\.venv\Scripts\python.exe -m pip install -e .`，使用 `start.ps1` 或新的 `python -m ptilopsisbot run` 启动；旧的 `python -m databot` 已改名。本机管理接口前缀同步改为 `/ptilopsisbot`。

新电脑安装 Python 3.12 和 GitHub CLI，登录有私有仓库权限的 GitHub 账号后执行（已有项目时用 `git pull` 更新）：

```powershell
gh repo clone Shadowfax-YJ/PtilopsisBot D:\Projects\PtilopsisBot
cd D:\Projects\PtilopsisBot
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
if (-not (Test-Path config.toml)) { Copy-Item config.example.toml config.toml }
.\.venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
```

首次配置时，把生成的随机值填进 `config.toml` 的 `access_token`。已有配置保留原值即可。配置和数据目录不会提交到 Git：换电脑需单独带上 `config.toml`；已经收集过数据时先停止旧进程，再完整复制 `data` 目录。新电脑重新建立 `.venv`，不要复制旧虚拟环境；数据目录换位置时同步修改配置里的 `data_dir`。

1. 从 [NapCat 官方发布页](https://github.com/NapNeko/NapCatQQ/releases)下载 `NapCat.Shell.Windows.OneKey.zip`，解压后运行 `NapCatInstaller.exe`，完成后进入生成的 `NapCat.XXXX.Shell` 目录运行 `napcat.bat`。见 [Windows 一键版安装说明](https://napneko.github.io/guide/boot/Shell)。
2. 打开 NapCat 启动日志给出的 WebUI 地址（默认端口 6099），使用日志里的随机 Token 登录，按界面提示修改 WebUI 密码、扫码登录 QQ。账号需要加入群 **1095012141**；删除其他成员的群文件需要群管理权限。NapCat 无需申请启动授权，WebUI 密码在本机生成。见 [官方配置说明](https://napneko.github.io/config/basic)。
3. 在 NapCat 的网络配置中新建并启用 **WebSocket 客户端 / 反向 WebSocket**：URL 为 `ws://127.0.0.1:8080/onebot/v11/`，消息格式选 `array`，Token 填 `config.toml` 中 `access_token` 引号内的值。这个连接密钥与 WebUI 登录密码用途不同。只需这个 OneBot 11 连接，无需另开 HTTP API。
4. 运行机器人：

```powershell
.\.venv\Scripts\python.exe -X utf8 -m ptilopsisbot run
```

修改配置后重启。默认 `auto_delete=true`，每个包保存成功满 24 小时后可清理；将 `delete_grace_hours` 改为 `0` 可在保存和检查通过后立即释放群空间。启动补扫会收集根目录里已有的符合条件的包。

另开终端运行下方 `status` 命令，看到 `"online": true` 后上传真实游戏 ZIP 进行验收。正常运行只启动一份小助手。

## 平时怎么用

机器人常驻时，在另一个终端执行：

```powershell
.\.venv\Scripts\python.exe -X utf8 -m ptilopsisbot status
.\.venv\Scripts\python.exe -X utf8 -m ptilopsisbot scan
.\.venv\Scripts\python.exe -X utf8 -m ptilopsisbot retry 12
.\.venv\Scripts\python.exe -X utf8 -m ptilopsisbot report 2026-09-05
.\.venv\Scripts\python.exe -X utf8 -m ptilopsisbot report 2026-09-05 --preview
```

`status` 查看连接和最近 50 条上传记录；`scan` 请求补扫并检查到期清理，返回 `queued` 后由后台执行；`retry` 使用上传记录 ID 重试；`report` 发回目标群，`--preview` 只在本机查看。命令通过同一个本机端口和 token 操作正在运行的机器人，不会启动第二个下载进程。正常 Ctrl+C 停止机器人即可暂停采集和清理。

上传通知触发根目录补扫；连接、重连及每 30 分钟也会补扫。所有扫描、下载和清理由同一个后台循环顺序执行。单次请求最多 1000 项，达到上限会提示日志，不承诺完整枚举更大的根目录；包需直接上传到根目录，不收集子目录。下载失败每隔 5 分钟重试，合计最多 3 次；检查不通过或耗尽次数后看错误原因，处理完再 `retry`。每轮处理和远程操作前检查 QQ 在线状态，掉线暂停，不消耗单个文件的重试次数。

每天北京时间 00:05 发前一天日报，分别统计上传次数和按完整 ZIP 哈希去重的新增唯一包数。相同包的新增贡献归首次收集成功的上传记录；迟到完成和离线漏发可手动重发。当前只检查 ZIP 可读性，待真实游戏样本到位后补必需文件与字段检查。

## 群内回复风格

修改或新增群内回复时，沿用以下约定。参考游戏内中文[语音记录](https://prts.wiki/w/%E7%99%BD%E9%9D%A2%E9%B8%AE/%E8%AF%AD%E9%9F%B3%E8%AE%B0%E5%BD%95)与[角色档案](https://prts.wiki/w/%E7%99%BD%E9%9D%A2%E9%B8%AE)的公开转录；文案为适配本助手的原创表达。

- 白面鸮是有情感的医疗干员与数据维护员。保持冷静、礼貌和含蓄的关心，以短句说明对象、状态和结果；自称可以使用“白面鸮”。
- 用少量“检索”“数据汇总”“校验”等与实际工作对应的词。句子保持自然、信息完整，偶尔停顿即可。
- 数字、失败原因和处理建议准确优先。“数据汇总完成”只说明统计完成，不能把仍待处理的包说成已归档，也不能把 ZIP 可读说成游戏内容有效。
- 休眠与故障措辞只用于真实对应状态；收集、删除、错误和统计通知不插入睡眠、初始化数据库或权限变化的玩笑。
- 使用“您”或成员昵称，不默认所有群成员都是博士。表达克制，不加入卖萌口癖、网络谐音梗或擅自补写人物关系与经历。

群内回复有两处：每条对局包首次下载、校验并成功归档后发送收包回复；次日发送日报。两处各有 8 套文案，每次从对应模板池随机选择，允许偶尔重复。模板位于 [ptilopsisbot/messages.py](ptilopsisbot/messages.py)，数字、文件名和校验范围保持一致；没有接入闲聊模型。

重复通知、补扫、重启或对同一份已收集数据手动重试不会重复发送收包回复。另一次可区分的上传会收到自己的回复，包括内容重复的 ZIP。归档失败或检查不通过时不发成功回复。发送失败只记日志，不影响收集与清理，不建立通知重试队列；日报可以手动补发。

示例：

```text
白面鸮已保存这份对局包。相关数据已登记。
上传者：小张（456）。
文件：run-20260905-120000-000001.zip
校验范围：仅检查 ZIP 可读性，未验证游戏内容。
```

```text
白面鸮已完成本次数据汇总。请查阅。
统计日期：2026-09-05。
小张（456）：上传 2，新增唯一包 1。
处理状态：待处理 0，检查不通过 0，处理失败 0。
校验范围：仅检查 ZIP 可读性，未验证游戏内容。
```

## 保存和清理规则

- 文件保存为 `data/archive/<上传日期>/<上传者QQ>/<记录ID>.zip`；每次上传独立保留，原名和 SHA-256 在 `data/collector.sqlite3` 中。
- NapCat 的 `file_id` 是临时缓存引用，不能跨重启保存使用。本助手按群号、busid、上传者、原名、大小、QQ 上传时间生成登记键；通知只触发扫描，登记采用根目录的上传时间，重复通知或重启补扫不会重复计数。同名但上传时间不同的包分别登记。同一秒的上传信息完全相同时无法可靠区分，跳过冲突文件并提示日志，需要人工处理。
- 删除必须已收集成功、经过宽限期，且本地文件大小和重新计算的 SHA-256 都正确。随后重新枚举根目录定位唯一候选，用新 ID 获取远端内容并再次计算 SHA-256，与归档一致后才用该 ID 删除。**每个成功清理的包通常下载两次**，第二次只计算哈希，不另存副本；失败包、本地损坏、候选冲突或内容变化时保留群文件。
- QQ 删除结果必须明确为成功才记 `deleted_at`。列表里暂时找不到文件（可能已移动或超出扫描范围）时保留记录和错误，下次补扫再试；不将“未找到”当作已清理。删除不影响本地文件和贡献记录。
- 本地空间低于 2 GiB、账号离线或数据库不可写时暂停下载与清理。修复后自动继续；日志位于 `data/collector.log`，最多保留约 20 MiB。
- 本地文件不自动清理。需要备份时停止机器人，复制整个 `data` 目录；仅有本地一份副本时，磁盘损坏仍可能丢失数据。

## 开机运行

关闭 Windows 接通电源时的自动睡眠。用任务计划程序新建一个登录触发的任务，程序为 `powershell.exe`，参数为 `-NoProfile -ExecutionPolicy Bypass -File "D:\Projects\PtilopsisBot\start.ps1"`。设置“如果任务已在运行，不启动新实例”，并关闭默认的运行时长限制。NapCat 也要设置启动并保持 QQ 登录；先分别手动跑通，再配置自启动。

## 验证与当前进度

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m mypy
.\.venv\Scripts\python.exe -m ruff check ptilopsisbot tests
.\.venv\Scripts\python.exe -X utf8 -m pytest -q
```

自动测试覆盖真实 SQLite、临时归档目录、HTTP 流式下载和 OneBot 反向 WebSocket；其中包含 100 MiB ZIP 的本机端到端收集、清理与日报测试。测试使用模拟的 QQ 文件列表，不连接实际 QQ 群。

当前尚未完成真实 NapCat 连接，也没有真实游戏 ZIP 样本。代码和离线验证不能替代真实群验收：安装连接后，使用专门测试上传确认群文件被移除、本地 ZIP 可读、日报仍保留贡献；同时检查实际根目录列表，并重启 NapCat 后验证到期删源。验收完成前，[地图](.scratch/qq-group-run-collector/map.md)中的真实环境事项保持待验收。

接入依据：[NapCat 临时 ID 缓存](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-common/src/file-uuid.ts)、[根目录字段映射](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/helper/data.ts)、[删除接口](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/action/go-cqhttp/DeleteGroupFile.ts)。当前只支持 NapCat，不保留双接入端或通用兼容层。
