# dsh-cot-compress

DeepSeek Harness（DSH）插件：把**已经结束的回合**里模型的推理（reasoning）整理成更短的要点，在之后的请求里代替原文发给模型，以节省上下文。

- 只改发出去的请求，**不改会话记录**。
- 当前回合内绝不改写，从下一轮开始才替换。
- 每段推理只决定一次（压缩版或原文），之后一直发同一个版本，所以提示词缓存在换版一次后保持稳定。
- 任何失败（超时、格式不合格、引文对不上等）都退回原文，不会让请求出错。

> English: a DSH plugin that compresses the reasoning of finished turns into short, quote-checked records and replays one fixed version per segment, so the request prefix stays cache-stable. Failures always fall back to the original text. MIT licensed.

版本：**0.6.0**

## 它怎么工作

1. 某一步发出工具调用、或一个回合结束时，插件在后台处理这一轮里 ≥ 5000 字符的推理段。
2. 长段按空行切成约 8000 字符的块（相邻块有重叠），每块单独请求一次模型，提取结构化记录：类别、对象、推理状态（例如已决定 / 已排除 / 待验证）、内容和理由。
3. 每条记录必须引用原文里能逐字找到的句子作证据，程序逐条校验；不合格的条目可丢弃少量（默认最多 max(2, 15%)），超过就整块保留原文。
4. 合格的块合并成这一段的替换文本；替换后的估算 token 必须明显少于原文，否则保留原文。
5. 整理结果的语言跟随原文（英文推理用英文整理，中文用中文），可在配置里固定。

## 需要什么

- DSH ≥ 0.2.2-rc.1，Node `^22.19.0` 或 `>=24.0.0`。
- 压缩请求使用当前会话的同一个模型和路由（默认 `reasoning_effort: low`），**会额外消耗 token / 额度**。长推理段压缩一次可能需要 1–4 分钟。
- 路由必须允许改写历史推理文本。DeepSeek 的 `openai-completions` 路由可以；Anthropic 风格带 thinking 签名的路由改写后签名会失配，不要使用。

## 安装

1. 下载安装包：[`dist/dsh-cot-compress-0.6.0.tgz`](dist/dsh-cot-compress-0.6.0.tgz)。
2. 在 DSH 里用插件管理器安装（需要 Full 权限的 agent 会话，或插件管理器界面）：

   ```text
   plugin_manager  action=install_bundle  target="<下载位置>/dsh-cot-compress-0.6.0.tgz"
   ```

   成功时返回 `"application":"applied"`。**请用 .tgz 安装，不要用目录**（目录会被装成 `link:`，配置校验会失效）。
3. 桌面版一般会热加载；`headless` / `tui` 等常驻宿主可能需要重启后生效。
4. Windows 上偶尔出现 `EPERM … rename package.json`，原样重试一次即可。

卸载：

```text
plugin_manager  action=remove_bundle  target="dsh-cot-compress"
```

## 使用

- 装好后默认**只压缩主智能体会话**。桌面版输入框工具栏会出现「压缩范围」开关：只压主智能体 / 只压子智能体 / 全部 / 关闭。关闭只停止新的压缩，已经替换的段保持不变。
- 想让某个会话完全不压缩：在该会话的**第一条消息**里写上 `cotab-q3`。
- 桌面版里还有一个「推理压缩」页面，可以查看每段的原文与整理结果、统计和节省量。

## 配置

默认**不写日志、不持久化**（安装包里不带任何文件路径）。不持久化时，宿主重启后，之前没记下来的推理段会按原文发送，只损失压缩率，不会出错。

需要日志或持久化时，在你自己 profile 的 `cordis.patch.yml` 里加一段覆盖（**会整体替换这一行的配置，不做合并**，要改的字段都写上）：

```yaml
- id: dsh-cot-compress
  config:
    logFile: 'D:/your/path/cot-compress.jsonl'      # 实验日志，空 = 不写
    storeFile: 'D:/your/path/cot-compress-store.jsonl'  # 每段的决定，空 = 只在内存
```

常用字段：

| 字段 | 默认 | 说明 |
|---|---|---|
| `minChars` | `5000` | 短于此长度的推理段不压缩 |
| `reasoningEffort` | `low` | 压缩请求的推理强度；空串 = 用路由默认 |
| `outputLanguage` | `auto` | 整理语言：`auto` 跟随原文、`zh`、`en` |
| `timeoutMs` | `300000` | 单段压缩总时限（毫秒），超时保留原文 |
| `maxWaitMs` | `60000` | 下一轮第一次请求最多等后台压缩多久；到时没完成的段固定为原文 |
| `chunkMinChars` / `chunkChars` / `overlapChars` | `12000` / `8000` / `1500` | 切块：超过多长才切、每块目标长度、相邻块重叠 |
| `chunkConcurrency` | `6` | 同一段同时请求的块数 |
| `itemDropMin` / `itemDropFraction` | `2` / `0.15` | 每块允许丢弃的不合格条目上限 = max(下限, 条目数 × 比例) |
| `maxTokenRatio` | `0.9` | 替换文本估算 token 必须 ≤ 原文的这个比例；`0` 关闭此检查 |
| `minRatio` | `0.02` | 替换文本短于原文这个比例视为失真，保留原文 |
| `applyToAll` | `true` | `false` 时只有第一条消息含 `compressMarker` 的会话才压缩 |
| `compressMarker` / `fullMarker` | `cotab-k7` / `cotab-q3` | 会话级开启 / 关闭标记 |
| `logFile` / `storeFile` / `stateFile` | `""` | 日志、决定存储、压缩范围的保存路径；`stateFile` 为空时放在 `storeFile` 同目录 |

日志只记录推理文本、token 估算和用时，**不含请求头、API key 等任何密钥**。

## 已知限制（请先读）

- **整理会丢细节。** 我们的评测里，整理结果偶尔会漏掉被放弃的备选方案、方案成立的前提条件，或把“待验证”写得像“已决定”。插件只保证引文能在原文里找到，不能保证语义完整。对推理细节要求很高的任务，请把范围切到“关闭”，或在会话第一条消息加 `cotab-q3`。
- **每段只决定一次。** 同一段原文的决定是永久的；以后改进提示词也不会重压已决定的段。
- **换版时缓存失效一次。** 上一轮推理在下一轮第一次请求时换成压缩版，从那一处起的缓存失效一次，之后稳定。
- **超长段可能超时。** 十几万字的推理段在默认时限下可能大量超时，超时部分保留原文。
- **回合中途重启宿主**会丢掉进行中的压缩；已写入 `storeFile` 的决定会保留。
- 插件只在宿主报告回合结束后才替换；宿主回合事件不可用时会自动进入“只记录、不改写”的安全模式。

## 贡献者

- **Clawd**

## 许可证

[MIT](LICENSE)
