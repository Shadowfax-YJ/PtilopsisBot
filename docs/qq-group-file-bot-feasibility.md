# QQ 群对局日志自动归档 Bot：可行性与框架选型

> 本文保留早期调研时的平台比较与架构建议，不是当前实现规格。实现审查发现 NapCat 的文件 ID 是临时缓存引用，用户已确认改用提供源文件 UUID 的 **LLBot**，原因和源码依据见 [README](../README.md)。为解决群文件空间不足，保存后自动清理群文件是首版核心功能。首版范围以[实现地图](../.scratch/qq-group-run-collector/map.md)为准；下文的旧选型结论、扩展能力、运维和备份建议不自动成为开发要求。

> 调研日期：2026-09-05（Asia/Shanghai）  
> 场景：普通 QQ 群每天产生几十个约 100 MB 的 `run-xxxxx.zip`，需要自动下载到本地、校验、按上传者/日期分类，并在日终统计贡献。  
> 资料范围：仅使用腾讯官方文档、OneBot 正式规范、项目官方文档及源码/测试。

## 结论先行

这个需求**可行**，但不应该实现成“一天结束时再扫描一次群文件并下载”。最佳结构是：

1. 常驻接收群文件消息；
2. 一旦出现符合 `run-*.zip` 的新文件，立即持久化任务并流式下载；
3. 下载成功后做 ZIP 与业务数据校验，保存元数据；
4. 日终任务只做聚合统计和发送报告；
5. 启动、重连与日终时再做可用范围内的对账。

首选是**QQ 开放平台官方机器人 + Python**。官方的 `GROUP_MESSAGE_CREATE`（群消息全量模式）会把群里的每条消息推给机器人，事件中有 `author`、`timestamp` 和 `attachments`；附件字段包含 `url`、`filename`、`size`，并明确 `content_type=file` 代表群文件。[腾讯：群消息（全量模式）](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/group_message_create.html)

官方路线的硬前提是机器人能够申请并开启“接收所有消息”。该事件使用 `GROUP_AND_C2C_EVENT (1 << 25)`，腾讯文档说明基础事件以外的特殊事件需要权限，传入无权限 Intent 会导致 WebSocket 报错并断开。[腾讯：事件订阅 Intents 与权限](https://bot.q.qq.com/wiki/develop/api-v2/dev-prepare/event-emit/payload.html)

官方 API **没有普通群文件目录枚举、子目录枚举或历史群文件补拉接口**。公开接口索引里有机器人向群上传富媒体的 `POST /v2/groups/{group_openid}/files`，但没有对应的 `GET /files`，事件索引也没有独立“群文件上传”事件；因此官方路线只能可靠抓取机器人在线/可恢复窗口内的新文件消息，不能在每天某个时刻扫描群文件夹补齐一整天。[腾讯：服务端接口总索引](https://bot.q.qq.com/wiki/develop/api-v2/autogen/)

如果实际开放平台账号拿不到全量群消息权限，有两个选择：

- 调整群规，要求上传文件时 `@机器人`，使用 `GROUP_AT_MESSAGE_CREATE`；这个事件有相同的附件结构，但不再是无感收集。[腾讯：群 @ 机器人消息](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/group_at_message_create.html)
- 接受协议与账号风险，采用 NapCatQQ 或 LLBot/LLOneBot 的 OneBot 11 接入。它们能监听上传事件、枚举群文件和获取下载直链，但都不是腾讯开放平台官方接口。

## 规模与容量判断

每天 `N` 个、每个约 100 MB，则新增量约为 `N × 100 MB/天`。例如：

- 30 局/天：约 3 GB/天、90 GB/月、1.1 TB/年；
- 50 局/天：约 5 GB/天、150 GB/月、1.8 TB/年。

10 GB 群文件空间在 30 局/天时约 3.3 天就会填满，在 50 局/天时约 2 天。因此下载应在收到事件后尽快开始，而不是日终才下载。官方事件文档没有承诺接收侧附件 URL 的有效期，所以也不应把 URL 当永久地址保存后延迟使用。

100 MB 本身不是官方文档所示的发送硬限制。机器人**向群发送**文件的软/硬限制均为 200 MB，且文档还列有“超过今天发送文件容量上限”的错误码，但没有公布每日容量具体值。[腾讯：发送群聊富媒体](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_groups_group_openid_files.post.html) 这只是发送侧限制，不能据此推导接收下载侧上限；接收事件文档没有公开下载大小上限。

## 路线一：QQ 开放平台官方机器人（推荐）

### 能力

官方机器人已明确支持 QQ 群场景。未认证机器人只能由管理员使用并添加到管理员担任群主的群；个人认证机器人可公开使用，公开进群数量上限为 500 个。[腾讯：QQ 机器人产品介绍与服务范围](https://bot.q.qq.com/wiki/bot_new_product-intro/)

在开启群消息全量模式后，文件消息给出的信息足够完成本需求：

- `author.member_openid`：群成员稳定标识，建议作为贡献者主键；
- `author.username`：昵称，只用于显示，不能作为唯一键；
- `timestamp`：上传时间；
- `group_openid`：群标识；
- `attachments[].filename`、`size`、`url`、`content_type`：文件筛选、容量核对和下载。

事件并不是名为 `file_upload` 的专用事件，而是带群文件附件的 `GROUP_MESSAGE_CREATE`。实现时筛选 `attachment.content_type == "file"` 且文件名匹配 `^run-[A-Za-z0-9_-]+\.zip$` 即可。

官方 WebSocket 适合本地单机常驻，不需要公网回调地址；Webhook 更适合已有公网 HTTPS 服务的稳定部署。WebSocket 文档说明短时间重连可补发中间遗漏的事件，并要求保存 `session_id`/`seq` 以 Resume，但这只是短时恢复，不是群文件历史 backfill。[腾讯：WebSocket 事件接收与 Resume](https://bot.q.qq.com/wiki/develop/api-v2/dev-prepare/event-emit/websocket.html)

群管理员可以在机器人资料页关闭或重新开启通知，官方分别会发 `GROUP_MSG_REJECT` 和 `GROUP_MSG_RECEIVE`；系统应把“通知被关闭”作为高优先级告警，而不是静默少收文件。[腾讯：群消息拒收事件](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/group_msg_reject.html)、[腾讯：群消息接收事件](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/group_msg_receive.html)

### 框架与语言建议

建议使用 Python 3.11+，原因不是吞吐瓶颈，而是文件校验、任务调度、SQLite 与运维实现成本低。事件接入可用腾讯 `tencent-connect` 组织维护的 [`qqbot-agent-sdk`](https://github.com/tencent-connect/qqbot-agent-sdk)：其官方仓库提供 WebSocket Gateway、心跳、自动重连、Resume、OpenAPI v2 与附件处理能力。

不过，当前 SDK 内置 `AttachmentDownloader` 的 `_fetch()` 使用普通 GET 后返回 `resp.content`，随后 `write_bytes`，即先把整个文件放入内存；默认下载超时也是 30 秒。这对于几十个 100 MB 文件不是合适的生产下载器，应只复用 SDK 的事件/Gateway 层，另写流式下载器。[腾讯 SDK：`attachment.py`](https://github.com/tencent-connect/qqbot-agent-sdk/blob/main/src/qqbot_agent_sdk/attachment.py)

推荐组件：

- 事件与会话：`qqbot-agent-sdk`；
- 下载：`httpx.AsyncClient.stream()` 或 `aiohttp`，64 KiB～1 MiB 分块写入 `.part`；
- 队列：单机先用 SQLite 持久化任务表 + `asyncio.Queue` 唤醒，不必一开始引入 Redis；
- 调度：APScheduler，仅负责日终报告、失败重试扫描和磁盘巡检；
- 数据库：SQLite（WAL），文件规模增长或多实例后再迁移 PostgreSQL；
- 校验：Python 标准库 `zipfile` + `hashlib.sha256`，业务日志解析器独立为 validator 模块。

### 官方路线的缺点

- 全量消息能力需要申请/开启，必须用真实机器人账号验证；
- 无法枚举普通 QQ 群文件夹，也无法补拉机器人加入前的文件；
- 机器人离线超过 WebSocket 可恢复窗口时，可能产生无法自动找回的缺口；
- 群管理员关闭通知后将停止收到文件消息；
- 官方没有承诺接收附件 URL 的 TTL，也没有公开接收下载大小上限。

因此上线前应先做 1～2 天的 PoC：申请权限、开启全量消息、由普通成员上传一个真实 100 MB ZIP，验证事件字段、下载速度、URL 行为、断线 Resume 和日终统计。若这一步通过，没必要承担非官方协议风险。

## OneBot 11 与 OneBot 12 到底提供了什么

OneBot 是机器人应用接口规范，不负责帮 QQ 账号登录；必须再选择 NapCat、LLBot 等具体实现。

### OneBot 11

OneBot 11 标准原生定义了 `notice_type=group_upload`：事件带 `group_id`、`user_id` 以及 `file.id/name/size/busid`，所以能够识别谁上传了什么。[OneBot 11：群文件上传事件](https://github.com/botuniverse/onebot-11/blob/master/event/notice.md#%E7%BE%A4%E6%96%87%E4%BB%B6%E4%B8%8A%E4%BC%A0)

但是，`get_group_root_files`、`get_group_files_by_folder`、`get_group_file_url` **不在 OneBot 11 核心公开 API 中**；它们是 go-cqhttp 扩展，后来被多数实现沿用。[OneBot 11：核心公开 API](https://github.com/botuniverse/onebot-11/blob/master/api/public.md)、[go-cqhttp：群文件扩展 API](https://github.com/Mrs4s/go-cqhttp/blob/master/docs/cqhttp.md#%E8%8E%B7%E5%8F%96%E7%BE%A4%E6%A0%B9%E7%9B%AE%E5%BD%95%E6%96%87%E4%BB%B6%E5%88%97%E8%A1%A8)

这意味着业务代码不能仅写“兼容 OneBot 11”就假定一定能下载群文件，必须对目标实现做能力探测，并锁定已验证版本。

### OneBot 12

OneBot 12 提供通用 `upload_file`、`get_file` 以及分片上传/获取文件接口，适合实现端和应用端交换文件。[OneBot 12：文件动作](https://github.com/botuniverse/onebot/blob/main/specs/interface/file/actions.md)

但它没有标准化“QQ 群文件系统目录枚举”。规范里的 `detail_type=qq.group_file_upload` 是说明扩展事件命名规则的示例，不是各实现都必须提供的标准事件。[OneBot 12：扩展事件规则](https://github.com/botuniverse/onebot/blob/main/specs/interface/rules.md#%E6%89%A9%E5%B1%95%E4%BA%8B%E4%BB%B6)

对本需求来说，选 OneBot 12 不会自动获得比 OneBot 11 更好的群文件能力；现实可用性主要取决于具体 QQ 实现，因此非官方备选仍以 OneBot 11 生态更直接。

## 非官方实现核实

### NapCatQQ

NapCat 官方将其定位为“基于 NTQQ 的协议端框架”，支持 OneBot 11。[NapCatQQ 官方仓库](https://github.com/NapNeko/NapCatQQ)

对本需求的当前源码支持如下：

- 上传事件：收到群文件元素后构造 `OB11GroupUploadNoticeEvent`，带上传者、文件名、大小、busid 和 NapCat 编码的文件 ID。[事件解析源码](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/api/group.ts)、[事件类型源码](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/event/notice/OB11GroupUploadNoticeEvent.ts)
- 根目录枚举：`get_group_root_files` 调用 NTQQ 群文件列表并返回文件与文件夹，列表项包含 `upload_time`、`uploader` 和 `uploader_name`。[根目录实现](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/action/go-cqhttp/GetGroupRootFiles.ts)、[字段映射](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/helper/data.ts)
- 子目录枚举：有 `get_group_files_by_folder`。[子目录实现](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/action/go-cqhttp/GetGroupFilesByFolder.ts)
- 下载 URL：`get_group_file_url` 解码 NapCat 文件 ID 后请求真实文件 UUID 的直链。[URL 实现](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/action/file/GetGroupFileUrl.ts)
- 官方 API 文档也列出了上述三个接口以及 `download_file`/流式接口。[NapCat API 文档](https://napneko.github.io/onebot/api)、[NapCat 文件处理指南](https://napneko.github.io/develop/file)

需要注意当前源码细节：根目录接口默认 `file_count=50`，从 `startIndex=0` 只请求一次，没有内部翻页；子目录实现返回 `folders: []`，因此不能依赖它发现更深层的嵌套目录。10 GB / 100 MB 理论上约能容纳 100 个文件，做对账时必须显式传足够大的 `file_count`，并在真实群上测试上限。如果所有 `run-*.zip` 都放根目录，这个限制容易规避。

NapCat 还有 WebSocket 流式下载动作，源码按块读取本地文件再发送 Base64 块。[NapCat：`DownloadFileStream`](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/action/stream/DownloadFileStream.ts) 但同机部署时更简单可靠的方式仍是先调用 `get_group_file_url`，让业务服务直接 HTTP 流式落盘，避免 100 MB 文件经过 JSON/Base64 WebSocket 多一层膨胀和转发。

### LLBot / LLOneBot（仓库名 LuckyLilliaBot）

当前官方仓库将项目称为 LLBot，提供 OneBot 11、Satori 和 Milky Server。[LLBot 官方仓库](https://github.com/LLOneBot/LuckyLilliaBot)

其源码对群文件场景的证据更完整：

- 群消息中出现 `fileElement` 时构造 `group_upload`，带 `peerUin`、`senderUin`、UUID、名称、大小与 busid。[事件转换源码](https://github.com/LLOneBot/LuckyLilliaBot/blob/main/src/onebot11/entities.ts#L102-L119)
- 项目有双账号集成测试：主账号上传文件，次账号等待并断言收到 `notice_type=group_upload`。[上传事件集成测试](https://github.com/LLOneBot/LuckyLilliaBot/blob/main/test/onebot11-api-test/tests/group/notice-events.test.ts)
- `get_group_root_files` 每次取 100 条并按 `nextIndex` 循环到结束，比 NapCat 当前单次列表实现更适合完整对账。[根目录实现](https://github.com/LLOneBot/LuckyLilliaBot/blob/main/src/onebot11/action/go-cqhttp/GetGroupRootFiles.ts)
- `get_group_files_by_folder` 同样翻页，但当前返回 `folders: []`，也不适合递归发现更深目录。[子目录实现](https://github.com/LLOneBot/LuckyLilliaBot/blob/main/src/onebot11/action/go-cqhttp/GetGroupFilesByFolder.ts)
- `get_group_file_url` 调用 NT 文件 API 返回直链。[URL 实现](https://github.com/LLOneBot/LuckyLilliaBot/blob/main/src/onebot11/action/go-cqhttp/GetGroupFileUrl.ts)

这些测试证明了事件与 API 路径存在，但项目的上传事件测试使用的是小型测试文件，并没有给出 100 MB 群文件的公开端到端承诺。因为下载实际走直链且 OneBot 只传 URL，应用侧合理使用流式 HTTP 时，100 MB 在架构上没有 JSON 消息体瓶颈；仍应以真实账号 PoC 为准。

### Lagrange.Core / Lagrange.OneBot

Lagrange 需要区分 V1 和当前 V2：

- 官方 V2 文档明确说明，LagrangeV2 不再支持 OneBot 11，V1 的 OneBot 11 实现也不再维护。[Lagrange V2 文档](https://lagrangedev.github.io/Lagrange.Doc/v2/)
- 当前 `Lagrange.Core` 本身仍公开 `GroupFSDownload`、`FetchGroupFSList` 等 C# API，可获取文件 URL和按目标目录枚举群文件。[当前 Core 接口源码](https://github.com/LagrangeDev/Lagrange.Core/blob/master/Lagrange.Core/Common/Interface/MessageExt.cs#L51-L61)
- 当前官方提供给机器人应用的 Web 服务是 `Lagrange.Milky`。其能力表标记 `get_group_file_download_url` 和 `get_group_files` 已实现，但 `group_file_upload` 事件仍未实现。[Lagrange.Milky 能力表](https://github.com/LagrangeDev/Lagrange.Core/blob/master/Lagrange.Milky/README.md)
- 历史 V1 Lagrange.OneBot 确实曾支持根/子目录列表和群文件 URL，但文档已属于不再维护的 V1。[Lagrange V1 扩展 API](https://lagrangedev.github.io/Lagrange.Doc/v1/Lagrange.OneBot/API/Extend/)

所以不建议为了本项目新上已停止维护的 Lagrange.OneBot V1。若团队强 C#、愿意直接集成 Core，可以基于当前 `Lagrange.Core` 自行做消息监听和群文件 API；但开发与维护成本明显高于官方 QQ SDK，且仍属于非官方协议路线。使用 Lagrange.Milky 时只能依赖轮询列表补拉，不能直接依赖群文件上传事件。

## 合规、封号与稳定性风险

官方机器人路线与腾讯开放平台的授权模型一致，合规与账号风险最低。非官方方案没有可引用的“安全频率”或“绝不封号”保证，不能给出封号概率。

腾讯发布的 QQ 软件许可及服务协议是判断边界的主要依据；腾讯政策页指向当前正式协议。[腾讯 Policies：QQ Agreement](https://www.tencent.com/policies/)、[QQ 软件许可及服务协议](https://rule.tencent.com/rule/preview/46a15f24-e42c-4cb6-a308-2347139b1201) 协议禁止未经许可对软件、运行时内存或交互数据进行挂接/访问，以及通过非腾讯授权的第三方软件、插件、系统登录或使用服务。NapCat、LLBot、Lagrange.Core 采用的 NTQQ 注入或协议实现路线均不能被视为腾讯开放平台授权 API，因此存在协议违约、风控、冻结/限制账号和随 QQ 更新失效的风险。

工程上的稳定性风险同样客观存在：

- QQ/NTQQ 更新后底层接口、字段或登录链路可能变化；
- 事件可能因进程崩溃、账号掉线、群通知关闭、WS 断连而漏收；
- 文件直链可能过期，或因网络、CDN、权限变化失效；
- 列表 API 在不同实现上的分页和子目录行为并不一致；
- 项目升级可能改变文件 ID 编码，数据库里只存实现私有 `file_id` 不足以构成永久恢复能力。

如果不得不使用非官方路线，应至少：

- 使用专门的机器人 QQ，不使用群主或高价值主账号；
- 不做群发、刷屏、批量加群等无关自动化；
- 锁定并灰度升级 NapCat/LLBot 与 QQ 版本；
- 本机监听 OneBot 端口，配置 access token，不暴露到公网；
- 保留事件原文、文件元数据和下载审计日志；
- 把群文件列表对账作为事件兜底，而不是只信事件；
- 明确告知群成员日志会被自动下载、保存多久、用于什么目的，并限制本地目录访问权限。

这些措施只能降低运维与数据风险，不能把非官方接入变成腾讯授权方式。

## 推荐落地架构

```text
QQ 事件源
  ├─ 首选：官方 GROUP_MESSAGE_CREATE（全量群消息）
  └─ 备选：OneBot 11 group_upload + 定期群文件列表对账
           │
           v
事件接收器 → SQLite jobs（先落库再确认处理） → 有界下载队列（建议并发 2～3）
                                                  │
                                                  v
                                  .part 临时文件 → 大小/SHA-256 → 原子改名
                                                  │
                                                  v
                               ZIP 安全校验 + 业务日志校验 → 归档目录
                                                  │
                                                  v
                                       SQLite files / validations
                                                  │
                              00:05 日终聚合 ───────┘ → 群内报告/本地报表
```

### 数据流与幂等

收到事件后第一步不是下载，而是把任务写入 SQLite。建议最少保存：

- `source`：`qq_official` / `onebot11`；
- `group_id`；
- `event_id` 或消息 ID；
- `source_file_id`（OneBot 时）；
- `uploader_id`（官方用 `member_openid`，非官方用 QQ UIN）；
- `uploader_display_name`；
- `uploaded_at`；
- `original_name`、`declared_size`；
- `status`、`attempts`、`last_error`；
- `sha256`、`actual_size`、`archive_path`、`validation_status`。

幂等键优先使用 `(source, group_id, event_id, attachment_index)`；若事件 ID 不稳定，再辅以 `(group_id, source_file_id)` 和 SHA-256 去重。不同成员上传相同内容时可以物理去重，但贡献统计是否都计数应由业务规则决定，建议同时报告“上传次数”和“唯一有效数据量”。

### 下载

- 使用有界并发，建议 2～3 个 100 MB 文件同时下载，避免把家用上行/下行和磁盘占满；
- 写 `incoming/<job-id>.part`，每个块同步更新已下载字节；
- 检查 HTTP 状态、`Content-Length`（若有）与事件声明大小；
- 成功完成后计算 SHA-256，再原子移动到归档位置；
- 失败采用指数退避，但 403/404 应立即重新获取 URL（OneBot）或标记需人工补传（官方 URL 已失效）；
- 不要把 100 MB 文件编码成 Base64 塞进数据库或普通 JSON API。

建议归档路径使用稳定 ID，避免昵称改名或 Windows 非法字符：

```text
archive/
  2026-09-05/
    <member_openid-or-uin>/
      run-xxxxx.zip
```

昵称保存在数据库和日报中，不直接作为唯一目录键。若同名文件重复，文件名后追加上传时间或内容哈希前 8 位。

### ZIP 与数据验证

最低限度验证：

1. 文件名匹配 `run-*.zip`，大小处于业务允许范围；
2. ZIP 中央目录可读，`ZipFile.testzip()` 的 CRC 全部通过；
3. 限制成员数量、单文件解压大小和总解压大小，拒绝路径穿越（`../`、绝对路径、盘符）与异常压缩比，防 ZIP bomb；
4. 只校验需要的日志文件，不把整个 ZIP 解压到共享目录；
5. 验证必需文件/字段、对局 ID、开始/结束时间、玩家数等业务约束；
6. 记录 validator 版本与错误原因，原 ZIP 保留在 quarantine 或按保留策略删除。

ZIP CRC 只能发现传输/压缩损坏，不能证明对局日志业务有效；业务有效性必须单独定义。

### 日终统计

建议在次日 `00:05` 统计前一自然日，避免 23:59 正在下载的任务产生边界竞态。每人至少输出：

- 上传总数；
- 下载成功数；
- 验证有效数/无效数；
- 重复数；
- 有效压缩包总大小；
- 尚在下载或需人工重传的文件名。

时间归属建议按事件的 `uploaded_at`（Asia/Shanghai）而不是下载完成时间。日报发送失败不应影响统计落库，并应可重复生成。

### 存储与保留策略

按 30～50 局/天估算，一块 2 TB 磁盘只能覆盖大约 1 年量级，且还要留 15%～20% 空闲空间。上线前明确：

- 原始 ZIP 保留多久；
- 验证失败文件保留多久；
- 是否迁移到 NAS/对象存储；
- 是否做第二份备份；
- 磁盘剩余空间低于阈值时停止下载还是告警；
- 群文件何时清理：当前首版已确定在本地保存完整、大小/哈希检查通过且满足配置宽限期后自动删除，以释放群空间；独立备份由使用者自行安排，具体规则见实现地图。

## 最终选型

### 推荐方案 A（应先验证）

**QQ 开放平台官方机器人 + `qqbot-agent-sdk`（只负责 Gateway）+ 自写 `httpx` 流式下载 + SQLite + APScheduler。**

适用条件：能开启 `GROUP_MESSAGE_CREATE` 全量群消息；接受无法扫描历史群文件，并能保证 Bot 常驻、Resume 与告警。

这是合规风险最低、组件最少、对 100 MB 文件也最自然的方案。

### 备选方案 B（全量消息权限不可得且必须无感收集）

**NapCatQQ + OneBot 11 + NoneBot2/Python + 自写流式下载器 + SQLite + APScheduler。**

选择 NapCat 是因为事件、根目录/子目录、文件 URL 都有当前官方源码与文档支持，生态接入直接；业务层用 NoneBot2 的 OneBot V11 适配器接收 `GroupUploadNoticeEvent`，扩展 API 用 `bot.call_api(...)` 调用。NoneBot 官方文档也推荐 OneBot V11 使用反向 WebSocket，并明确适配器负责事件接收和 API 调用。[NoneBot OneBot 连接配置](https://onebot.adapters.nonebot.dev/docs/guide/setup/)、[NoneBot `GroupUploadNoticeEvent`](https://onebot.adapters.nonebot.dev/docs/api/v11/event/)

若“每次必须完整枚举根目录、不想自己绕过 NapCat 单次列表限制”比生态优先级更高，可将 LLBot 作为同一业务层下的替代实现；其当前根目录源码有明确翻页和上传事件集成测试。无论选哪个，都应在部署时做能力探测，不把 go-cqhttp 扩展当作 OneBot 11 必然能力。

### 不推荐

- **Lagrange.OneBot V1**：官方已停止维护；
- **为此需求优先上 OneBot 12**：没有标准化 QQ 群文件目录能力，当前收益不明显；
- **纯日终扫描**：官方机器人做不到历史目录枚举，非官方方案也会承受 URL 失效、列表差异和空间挤满风险；
- **未确认本地文件完整就删除群文件**：自动清理属于首版核心功能，但必须先完成本地保存和大小/哈希检查。

## 建议的上线顺序

1. 用官方开放平台申请测试机器人并加入目标群；
2. 验证是否能开启全量群消息和收到普通成员上传的 `content_type=file`；
3. 用真实 100 MB `run-*.zip` 做下载、断网、重连和重复事件测试；
4. 跑通下载、检查、本地保存、自动清理源群文件及日报；
5. 对照群文件检查收集和清理结果；不把观察一周作为启用清理的门槛；
6. 官方权限确实无法满足时，再决定是否接受 NapCat/LLBot 的协议与账号风险；
7. 一个月后根据实际增长量确定 NAS/对象存储和备份策略。
