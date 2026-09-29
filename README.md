# logengine — 本地多行日志采集处理引擎

第一版聚焦一条可靠的本地采集链路：

- **输入来源**：`pkg/tail` 增量读取本地文件，只从上次偏移量之后读，绝不周期性整文件重读。
- **解析链**：`pkg/parser` 先做多行切分（起始行正则 + 续行），再按顺序跑可插拔的 `Parser` 链；新解析方式只需实现接口并追加到链尾。
- **输出确认**：`Output.Publish` 返回成功（下游已确认）后，位点才原子落盘（临时文件 + fsync + rename）。因此是至少一次语义：崩溃在确认前会重放，崩溃在确认后、提交位点前最多重放一条，**不会跳过也不会重复发布已确认记录**。

## 关键设计

- **文件轮换**：用 `dev+inode` 识别文件切换。轮换时先把旧 inode 尾部可能残留的字节读完，再交付边界事件并从新文件偏移 0 开始，新文件绝不会被当成旧文件的后半段。
- **inode 复用防护**：位点中保存已确认前缀的 SHA-256 摘要与长度（最多 256 字节）。重启恢复时要求 inode 相同 **且** 该前缀仍逐字节存在，防止轮换后 inode 复用导致从错误偏移续读。
- **截断**：同 inode 且当前大小小于读偏移即判定截断，seek 回 0，新生成重新读取。
- **半条多行内容**：切分器为每个生成（generation）维护暂存。轮换/截断边界处把暂存记录以真实结束位点刷出（轮换）或随旧内容丢弃（截断），不会跨文件拼接；长时间无换行的尾部记录由 `FlushAfter` 超时刷出，标记 `incomplete`。
- **超限记录**：多行记录超过 `MaxBytes` 后继续吃进的续行不再占内存，但位点照常推进到真实结尾，记录标记为坏记录并给出明确原因（`multiline record exceeds N bytes`）。
- **坏记录与脱敏**：任何解析器拒绝或报错都成为坏记录，保留 `source/gen/start/end` 精确位置；样例经 `parser.Redact` 脱敏（`password`、`token`、`api_key`、`secret`、`authorization` 等的 JSON、`key=value`、`key: value`、`Authorization: Bearer/Basic` 形式），样例截断到 512 字节。好记录中的敏感原文同样会被脱敏。

## 目录

- `pkg/position` — 位点存储（偏移量、dev/inode、前缀摘要），原子写入
- `pkg/tail` — 增量尾读、轮换/截断/出现检测、生成编号
- `pkg/parser` — 多行切分、解析链、JSON 解析器、脱敏
- `pkg/output` — 输出接口
- `pkg/engine` — 编排：尾读 → 切分 → 解析链 → 输出确认 → 提交位点
- `cmd/logcollector` — 命令行示例

## 运行

```sh
go test ./...
go run ./cmd/logcollector -positions ./pos.json /var/log/app.log
```

## 测试覆盖

`go test ./...`（含 `-race`）覆盖：基本多行读取与位置连续性、不重复发布、敏感信息脱敏、
轮换先排空旧 inode 再从新文件 0 读、截断回 0、重启后已确认记录不重放、超限记录的明确处理，
以及轮换 → 截断 → 重启三者交错的端到端场景。
