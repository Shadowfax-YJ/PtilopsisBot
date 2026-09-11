# PtilopsisBot

通用文件收集插件已支持持久化投递、重试和可选清理回执，配置见 [插件接入方案](docs/plugins.md)，维护入口为 [extend-collector-plugins](.agents/skills/extend-collector-plugins/SKILL.md)。领域验证和分析由外部业务插件提供。

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

收集 BlackFlow 并自动后处理时，同一次安装环境使用 `pip install -e '.[blackflow]'`，会带上 analysis 业务插件与 OCR。照常启动机器人即自动处理并发布到 `data/blackflow/published`（相对实际 data_dir）；夸克备份选择该目录。基础安装供其他收集场景使用，不引入 OCR。依赖已固定到配套 analysis 提交；功能分支尚未合并时请使用包含本功能的机器人分支。无需另外安装运行器、启动工作进程或为后处理配置 Windows 任务。

首次配置时，把生成的随机值填进 `config.toml` 的 `access_token`。已有配置保留原值即可。配置和数据目录不会提交到 Git：换电脑需单独带上 `config.toml`；已经收集过数据时先停止旧进程，再完整复制 `data` 目录。新电脑重新建立 `.venv`，不要复制旧虚拟环境；数据目录换位置时同步修改配置里的 `data_dir`。

1. Windows x64 使用 [NapCat 官方发布页](https://github.com/NapNeko/NapCatQQ/releases)中的 `NapCat.Shell.Windows.Node.zip`，完整解压到独立目录（例如 `D:\Bots\NapCat`）。[v4.18.19](https://github.com/NapNeko/NapCatQQ/releases/download/v4.18.19/NapCat.Shell.Windows.Node.zip) 自带 Node.js 和大部分 QQ 运行文件，但有启动参数错误及缺少 DLL 的问题，需先完成下文的启动修复，再运行 `napcat-fixed.bat`。
2. 打开 NapCat 启动日志给出的 WebUI 地址（默认端口 6099），使用日志里的随机 Token 登录，按界面提示修改 WebUI 密码、扫码登录 QQ。账号需要加入群 **1095012141**；删除其他成员的群文件需要群管理权限。NapCat 无需申请启动授权，WebUI 密码在本机生成。见 [官方配置说明](https://napneko.github.io/config/basic)。
3. 在 NapCat 的网络配置中新建并启用 **WebSocket 客户端 / 反向 WebSocket**：URL 为 `ws://127.0.0.1:8080/onebot/v11/`，消息格式选 `array`，Token 填 `config.toml` 中 `access_token` 引号内的值。这个连接密钥与 WebUI 登录密码用途不同。只需这个 OneBot 11 连接，无需另开 HTTP API。
4. 运行机器人：

```powershell
.\.venv\Scripts\python.exe -X utf8 -m ptilopsisbot run
```

修改配置后重启。默认 `auto_delete=true`，每个包保存成功满 24 小时后可清理；将 `delete_grace_hours` 改为 `0` 可在保存和检查通过后立即释放群空间。启动补扫会收集根目录里已有的符合条件的包。

如果之前下载了 `NapCat.Shell.Windows.OneKey.zip`，运行安装器时出现“下载QQ失败 / HTTP 404”，关闭安装器并按第 1 步换用 Node 包。2026-09-05 核对时，一键包内置的 QQ 下载地址返回 404；这是上游已有的[安装器问题](https://github.com/NapNeko/NapCatQQ/issues/1973)，此时尚未进入 PtilopsisBot 的连接配置阶段。

**v4.18.19 Node 包启动修复：**该版本给 Node 工作进程传入 `--no-sandbox`，导致退出码 9，见[上游问题](https://github.com/NapNeko/NapCatQQ/issues/2040)。在 NapCat 目录中新建 `napcat-fixed.bat`，写入：

```bat
@echo off
setlocal
cd /d "%~dp0"
set "NAPCAT_DISABLE_MULTI_PROCESS=1"
"%~dp0node.exe" "%~dp0index.js"
pause
```

本次下载的 Node 包内置 QQ `9.9.32-50969`，还缺少 `crypto.dll` 和 `ssl.dll`，会使 `wrapper.node` 加载失败。用 7-Zip 解压[腾讯同版本 QQ 安装包](https://qqdl.gtimg.cn/qqfile/QQNT/9.9.32/beta/a33ab721/QQ9.9.32.50969_x64.exe)，将其中与 `wrapper.node` 同目录的这两个 DLL 复制到 NapCat 的 `node.exe` 所在目录；使用其他内置 QQ 版本时不要混用这些 DLL。补齐后运行 `napcat-fixed.bat`。此方案已在本机验证 WebUI 正常响应并生成登录二维码。单进程模式下需要重启时，关闭窗口后重新运行该脚本。

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

`status` 查看连接和最近 50 条上传记录，`active_downloads`、`active_cleanup` 分别列出正在下载和清理的记录 ID；`scan` 请求补扫并检查到期清理，返回 `queued` 后由后台执行；`retry` 使用上传记录 ID 重试，正在下载或清理的记录需等本次操作结束；`report` 发回目标群，`--preview` 只在本机查看。命令通过同一个本机端口和 token 操作正在运行的机器人，不会启动第二个下载进程。正常 Ctrl+C 停止即可暂停采集和清理；正在进行的本地文件检查会先结束并关闭文件。被中断的下载不消耗失败重试次数，重启后从头下载。

上传通知触发根目录补扫；连接、重连及每 30 分钟也会补扫。扫描、下载和清理使用独立的异步任务，ZIP 检查和本地哈希计算在线程中执行。单次请求最多 1000 项，达到上限会提示日志，不承诺完整枚举更大的根目录；包需直接上传到根目录，不收集子目录。下载失败每隔 5 分钟重试，合计最多 3 次；检查不通过或耗尽次数后看错误原因，处理完再 `retry`。每轮处理和远程操作前检查 QQ 在线状态，掉线暂停，不消耗单个文件的重试次数。

下载默认同时处理 3 个包，可在 `config.toml` 中设置 `download_concurrency = 3`（范围 1–6，修改后重启）。大于 1 时保留一个名额给本次连接收到文件消息、且与根目录唯一匹配的新包；其余名额也优先新包，没有新包时处理历史积压。默认因此最多同时下载 2 个历史包，并能立即接纳一个新包；设置为 1 时下载串行，仍优先尚未开始处理的新包。已经开始的下载不中途抢占，重连或重启后旧任务按历史积压处理。清理另有一个任务，不占上述下载名额，同一记录的回执处理结束后才可进入清理。

每天北京时间 00:05 发前一天日报，分别统计上传次数和按完整 ZIP 哈希去重的新增唯一包数。相同包的新增贡献归首次收集成功的上传记录；迟到完成和离线漏发可手动重发。当前只检查 ZIP 可读性，待真实游戏样本到位后补必需文件与字段检查。

下载和删源前的远端校验每 10 秒输出已完成比例、MiB 和平均 MiB/s；完成时记录耗时，ZIP 检查与本地哈希校验也分别记录耗时。没有程序限速，但每个成功清理的包通常需读取远端两次。并发可以减少新包排队等待，不保证总带宽随并发数成倍增加；收包回复仍在该包下载、检查和归档成功后发送。`queued` 且 `attempts>0` 也可能表示正在下载，可用 `active_downloads` 和阶段日志判断。

## 群内回复风格

修改或新增群内回复时，沿用以下约定。参考游戏内中文[语音记录](https://prts.wiki/w/%E7%99%BD%E9%9D%A2%E9%B8%AE/%E8%AF%AD%E9%9F%B3%E8%AE%B0%E5%BD%95)与[角色档案](https://prts.wiki/w/%E7%99%BD%E9%9D%A2%E9%B8%AE)的公开转录；文案为适配本助手的原创表达。

- 白面鸮是有情感的医疗干员与数据维护员。保持冷静、礼貌和含蓄的关心，以短句说明对象、状态和结果；自称可以使用“白面鸮”。
- 用少量“检索”“数据汇总”“校验”等与实际工作对应的词。句子保持自然、信息完整，偶尔停顿即可。
- 数字、失败原因和处理建议准确优先。“数据汇总完成”只说明统计完成，不能把仍待处理的包说成已归档，也不能把 ZIP 可读说成游戏内容有效。
- 休眠与故障措辞只用于真实对应状态；收集、删除、错误和统计通知不插入睡眠、初始化数据库或权限变化的玩笑。
- 使用“您”或成员昵称，不默认所有群成员都是博士。表达克制，不加入卖萌口癖、网络谐音梗或擅自补写人物关系与经历。

群内回复有两处：在线时收到的新对局包首次下载、校验并成功归档后，引用对应的文件聊天消息发送收包回复；次日发送日报。两处各有 8 套文案，每次从对应模板池随机选择，允许偶尔重复。模板位于 [ptilopsisbot/messages.py](ptilopsisbot/messages.py)，数字、文件名和校验范围保持一致；没有接入闲聊模型。

**首次进群、启动、重连和手动补扫发现的历史文件静默收集，不逐包回复，也不补发历史日报。**这些文件照常归档和统计，并按宽限期清理群文件，无需修改配置。上传日期缺失时采用下文的发现日期规则。

引用使用 NapCat 文件聊天消息的 `message_id`，不是文件通知里的临时 `file_id`。只使用本次连接期间收到的新文件消息，并要求上传者、文件名、大小与根目录文件唯一对应；根目录有有效上传时间时，还要求本次连接后上传、与消息时间相差不超过 60 秒。上传时间为 `0` 或缺失时，使用当前文件消息匹配，不再误判成 1970 年的历史文件。找不到对应消息、消息在归档后的回执检查结束后才到达，或存在歧义时保持静默；实际引用气泡的显示由 QQ 客户端决定。消息引用只在内存中保留最近 1000 条，断线或重启清空，不追补跨重启的收包回复。

重复通知、补扫、重启或对同一份已收集数据手动重试不会重复发送收包回复。另一次可区分的新上传也可以收到自己的回复，包括内容重复的 ZIP。归档失败或检查不通过时不发成功回复。发送失败只记日志，不影响收集与清理，不建立通知重试队列；日报可以手动补发。

成功发送后日志会写明“收包回复已发送”、源文件消息 ID 和回复消息 ID；静默跳过与发送失败也各有记录。QQ 的撤回通知中，`user_id` 是被撤回消息的发送者，`operator_id` 是操作者，可据此区分文件消息与机器人回复。

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

- 文件保存为 `data/archive/<统计日期>/<上传者QQ>/<记录ID>.zip`；每次上传独立保留，原名和 SHA-256 在 `data/collector.sqlite3` 中。统计时间优先使用 QQ 上传时间，缺失时使用可唯一匹配的文件消息时间，否则使用首次发现时间；`status` 中的 `time_source` 分别为 `qq`、`message`、`observed`。按发现日期统计的记录会在日报注明，不把文件名里的对局时间当成上传时间。
- 旧版误存为 `uploaded_at=0` 的记录在启动时自动修正：使用已保存的观察/归档时间，并移动原归档、更新数据库路径，保留记录 ID、哈希和删源状态。真实上传日期无法从 `0` 恢复，修正值标记为 `observed`。若遇到目标文件冲突或归档缺失，会保留现场并记错误，不覆盖已有文件。不要手动只改目录名，否则数据库路径会失效。
- NapCat 的 `file_id` 是临时缓存引用，不能跨重启保存使用。本助手按群号、busid、上传者、原名、大小、根目录原始上传时间生成登记键；推定的统计日期不会改变登记键，避免补扫或重启重复计数。同名但原始上传时间不同的包分别登记。同一秒的上传信息完全相同，或上传时间缺失且同名同大小同上传者时，无法可靠区分重新上传，需要使用不同文件名；同时出现的冲突候选会跳过并记日志。
- 删除必须已收集成功、经过宽限期，且本地文件大小和重新计算的 SHA-256 都正确。随后重新枚举根目录定位唯一候选，用新 ID 获取远端内容并再次计算 SHA-256，与归档一致后才用该 ID 删除。**每个成功清理的包通常下载两次**，第二次只计算哈希，不另存副本；失败包、本地损坏、候选冲突或内容变化时保留群文件。
- QQ 删除结果必须明确为成功才记 `deleted_at`。列表里暂时找不到文件（可能已移动或超出扫描范围）时保留记录和错误，下次补扫再试；不将“未找到”当作已清理。删除不影响本地文件和贡献记录。
- 实测删除群文件会联动撤回对应的文件聊天消息，出现“管理员撤回了一条消息”。PtilopsisBot 没有调用 `delete_msg`；NapCat v4.18.19 的[删除群文件接口](https://github.com/NapNeko/NapCatQQ/blob/v4.18.19/packages/napcat-onebot/action/go-cqhttp/DeleteGroupFile.ts)及其[QQ 内核调用](https://github.com/NapNeko/NapCatQQ/blob/v4.18.19/packages/napcat-core/apis/group.ts#L332)没有“保留聊天消息”参数，因此目前不能保证静默删文件、保留原文件气泡。延长宽限期只推迟清理，不保证取消撤回联动。
- 本地空间低于 2 GiB、账号离线或数据库不可写时暂停下载与清理。修复后自动继续；日志位于 `data/collector.log`，最多保留约 20 MiB。
- 本地文件不自动清理。需要备份时停止机器人，复制整个 `data` 目录；仅有本地一份副本时，磁盘损坏仍可能丢失数据。

## 可选：整台机器登录后自启动

这是原有机器人/NapCat 的开机方式，仅在需要自动启动整套收集服务时选用。正常手动启动机器人即可运行全部后台后处理。

关闭 Windows 接通电源时的自动睡眠。用任务计划程序新建一个登录触发的任务，程序为 `powershell.exe`，参数为 `-NoProfile -ExecutionPolicy Bypass -File "D:\Projects\PtilopsisBot\start.ps1"`。设置“如果任务已在运行，不启动新实例”，并关闭默认的运行时长限制。NapCat 也要设置启动并保持 QQ 登录；先分别手动跑通，再配置自启动。

## 验证与当前进度

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m mypy
.\.venv\Scripts\python.exe -m ruff check ptilopsisbot tests
.\.venv\Scripts\python.exe -X utf8 -m pytest -q
```

自动测试覆盖真实 SQLite、临时归档目录、HTTP 流式下载和 OneBot 反向 WebSocket；其中包含 100 MiB ZIP 的本机端到端收集、清理与日报测试，并检查两份历史下载阻塞期间新包仍可归档、回复和清理，随后历史包静默处理完毕。还覆盖下载名额上限、同一记录不重复领取、清理不阻塞下载和 Ctrl+C 的任务取消恢复。测试使用模拟的 QQ 文件列表，不连接实际 QQ 群。

2026-09-05 用户在测试群提供的真实日志已确认 NapCat 连接、约 100–125 MiB ZIP 归档与成功删源；同时暴露了根目录上传时间为 `0`、1970 日期与新包回执被跳过的问题。本次修复有对应离线回归测试，仍需在部署电脑更新后确认实际引用回复、日期修正和日报，并重启 NapCat 验证到期删源。尚未取得可供游戏内容检查的样本，ZIP 可读不等于游戏内容有效。

接入依据：[NapCat 临时 ID 缓存](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-common/src/file-uuid.ts)、[根目录字段映射](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/helper/data.ts)、[删除接口](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/action/go-cqhttp/DeleteGroupFile.ts)。当前只支持 NapCat，不保留双接入端或通用兼容层。
