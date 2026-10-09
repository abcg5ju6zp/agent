# 可恢复的缓存提交（Resumable Cache Commit）- 产品需求文档

## Overview
- **Summary**：为作业结束时的 cache save 建立"可恢复提交"语义：上传中断或代理重启后，不完整内容可被识别并继续或安全放弃；只有完整内容能被后续 restore 使用；重复 save/restore 结果一致。
- **Purpose**：消除"半成品缓存被当成可恢复结果"以及"一次失败反复污染后续命中"两类风险，使缓存在上传失败、代理重启、对象缺失、格式陈旧等可观察场景下都能自愈或安全降级。
- **Target Users**：在 Buildkite 作业中使用 cache save / cache restore（含 cache exec）的用户与运维人员。

## Goals
- 未完成的上传（中断的 S3 multipart upload、本地临时文件、NSC 非 LIVE 对象）可被识别：能够继续（复用已上传部分）或被安全放弃（abort/清理），且不会无限累积。
- 只有"上传完成 + 服务端 commit 成功"的条目才可被 retrieve 命中；restore 在触碰目标路径前完成摘要与格式校验。
- 一次失败不污染后续命中：被证实损坏的 blob 会导致条目失效，且同进程内后续 save 不会因"仅尺寸匹配"而再次信任它。
- 重复 save 与重复 restore 产生一致（幂等）结果。
- 覆盖五类可观察行为：成功、部分上传失败、代理重启、缓存缺失、旧缓存格式。

## Non-Goals
- 不改变服务端注册表（DynamoDB）的数据模型与 API 形状；不新增服务端接口。
- 不实现跨进程/跨代理的"坏 blob"全局黑名单（跨代理场景仍由既有"restore 不刷新其 retention → 自然过期"机制兜底）。
- 不实现内容定义分块（content-defined chunking，blobs 数组多元素）；仍保持单归档单 blob。
- 不替代 S3 桶生命周期策略（`AbortIncompleteMultipartUpload`）；代理侧恢复是附加防线，桶策略仍被推荐。
- 不改变"restore 非事务性清理"与 fail-open/fail-on-error 的既有语义。
- 不修改 `.buildkite/cache.yml` 的配置格式。

## Background & Context
现有实现（`internal/cache/`、`api/cache.go`、`clicommand/cache_*.go`）已经具备：

- Save 三段式：`CacheEntryCreate`（每次生成新 `upload_id` 的临时行）→ 以归档 sha256 摘要为对象名上传 blob → `CacheEntryCommit`（服务端无条件覆盖写）。未 commit 的条目不会被 retrieve 返回。
- Restore 闸门：下载 → `verifyBlobDigest`（触碰目标路径之前）→ `archive.Validate`（清单 manifest v2）→ 清理目标 → 解压 → `CacheEntryConfirm`。
- 缺失 blob（`ErrBlobNotFound`）、摘要不符（`ErrDigestMismatch`）、无法识别格式（`archive.ErrUnrecognizedFormat`）均降级为干净 miss，并按 retrieve 得到的 `upload_id` 条件失效条目。
- AWS SDK v2 `manager.Uploader` 在**进程内**上传失败时默认调用 `AbortMultipartUpload`（`LeavePartsOnError=false`）。

经代码核实仍存在的缺口：

1. **进程死亡（kill -9 / 重启 / 掉电）时 abort 无法执行**：S3 上残留进行中的 multipart upload 与已上传分片；新 save 无法发现也无法恢复它，只会并行新建 MPU，造成分片永久泄漏（除非桶配置生命周期规则）且无法"续传"。
2. **优雅取消时 SDK 的 abort 复用已取消的 ctx**：`multiuploader.fail()` 用请求 ctx 发起 abort，ctx 已取消则 abort 失败，同样可能残留 MPU。
3. **本地文件存储的崩溃残留**：`LocalFileBlob` 的 `.zstash-upload-*`、`.zstash-meta-*` 临时文件在崩溃后永久残留（不影响正确性，但持续累积）。
4. **`blobAlreadyStored` 仅按尺寸信任既有 blob**：restore 发现摘要不符 → 失效条目 → 同进程随后的 save 若见到同尺寸 blob 会跳过上传并再次 commit 坏 blob，形成"一次失败反复污染"的同进程窗口。
5. **结构残缺条目**：retrieve 命中但 `blobs` 为空或摘要值为空时，`downloadCache` 返回普通错误（"no blobs to download"），表现为歧义性硬错误，而非与旧格式一致的"干净 miss + 失效"。

NSC 侧：对象只有服务端定稿后才为 LIVE，`Stat` 已要求 LIVE，非 LIVE 对象天然不可恢复命中，预计无需改动，仅补测试固化。

## Functional Requirements
- **FR-1（不完整上传可发现）**：save 上传阶段必须能够发现目标对象上残留的不完整上传（S3 进行中的 MPU、本地临时文件），且发现动作失败不得阻断正常保存。
- **FR-2（继续或放弃 S3 MPU）**：发现同对象键的残留 MPU 时，优先续传：列举已上传分片，仅上传缺失分片后 CompleteMultipartUpload；续传不可行时安全 abort 后重新上传。两种路径产出的最终对象一致。
- **FR-3（放弃本地残留）**：本地文件存储在初始化或上传前清理超过年龄阈值的崩溃临时文件（数据/元数据临时文件），避免删除正在进行的他进程上传。
- **FR-4（失败不污染：可疑 blob 记录）**：restore 观察到摘要不符或归档校验失败时，将该摘要标记为"可疑"；同一 client 进程内随后的 save 不得仅凭尺寸跳过该 blob 的上传（--force 本就始终上传）。
- **FR-5（同进程尽力 abort）**：save 的上传返回错误时，若错误暴露了 MPU 标识，使用独立的短超时 context 尽力 abort（不依赖已取消的调用 ctx）；失败不改变返回错误，恢复交给下一次 save。
- **FR-6（结构残缺按旧格式处理）**：retrieve 命中但 blob 列表为空/摘要为空时，降级为干净 miss 并条件失效条目，与无法识别格式行为一致。
- **FR-7（幂等：重复 save）**：已 commit 的地址重复 save → peek 命中 → 不产生新对象；重启后重复 save → 续传/去重后提交内容一致的条目，不产生重复条目。
- **FR-8（幂等：重复 restore）**：完整条目重复 restore 稳定命中且内容一致；缺失/损坏条目重复 restore 稳定为 miss；retention 刷新维持既有节流（S3 12h、NSC ensure_minimum）。
- **FR-9（可观察输出）**：各场景在日志/进度回调中有可区分的结果文案（命中、miss 及原因、续传/放弃、失效是否实际发生），不把 no-op 报告为已失效。

## Non-Functional Requirements
- **NFR-1（向后兼容）**：不改变既有 API 请求/响应结构与配置文件格式；旧条目（空 upload_id、旧版 sidecar、manifest v1）可被安全处理而不 panic。
- **NFR-2（失败安全）**：恢复、清理、失效、确认等辅助路径均为 best-effort：任何失败只记录日志，不得让 save 因恢复逻辑而失败，也不得让 restore 因确认/刷新而失败。
- **NFR-3（可测试性）**：恢复逻辑通过窄接口（S3 API 子集）与 HTTP 协议级假服务器可在单元/集成测试中确定性复现；时间阈值用包级变量便于测试。
- **NFR-4（性能不退化）**：正常成功路径除一次既有的 MPU 发现调用外不增加额外往返；续传在部分上传后应显著减少重传字节。
- **NFR-5（代码风格）**：遵循 gofumpt -extra、显式错误包装、context-aware 函数与仓库既有结构。

## Constraints
- **Technical**：Go 单一 CLI；store 层为 `Blob` 接口 + S3/NSC/LocalFile 三实现；能力扩展通过可选接口（type assertion）而非修改 `Blob` 签名。
- **Business**：功能处于受限发布阶段，帮助文案已有 "in development" 提示；不改变 fail-open 默认行为。
- **Dependencies**：AWS SDK v2（s3 client、已在用的 manager）、roko 重试、nsc CLI；不引入新依赖。

## Assumptions
- 同一对象键（摘要值）上同时存在的残留 MPU 至多需要"续传或中止"处理；S3 允许同键多个 MPU 并存，处理以 ListMultipartUploads 返回者为准。
- 本地文件存储被多进程共享属罕见场景，年龄阈值（默认约 1 小时，可测试覆盖）足以区分崩溃残留与在途上传。
- 代理重启后内存状态全部丢失属预期；恢复依赖对象存储侧的可发现状态，不依赖本地内存。
- e2e 测试依赖组织云资源，本地以单元/集成测试为主验证手段。

## Acceptance Criteria

### AC-1：成功路径端到端可用
- **Type**: `rule`
- **Given**：缓存配置有效、目标路径存在、存储可用
- **When**：执行一次 save 后在新作业中执行 restore
- **Then**：save 完成 create→upload→commit，restore 命中并还原全部文件，内容与保存时一致
- **Pass Condition**：集成测试断言 `CacheEntryCreated=true`、条目已 committed、restore `CacheRestored=true` 且逐文件内容一致
- **Evidence**：`internal/cache` 集成测试 + e2e `TestCacheBasicSaveRestore`

### AC-2：部分上传失败不产生可恢复条目
- **Type**: `rule`
- **Given**：上传过程中注入确定性失败（如分片上传返回错误）
- **When**：save 返回错误后立即 restore，再重新 save + restore
- **Then**：失败 save 无 commit、retrieve 不可命中（干净 miss）；残留 MPU 被 SDK abort 或由下一次 save 识别并中止/续传；重存后 restore 命中且内容一致
- **Pass Condition**：失败后无 committed 条目；restore 为 miss；恢复后再 restore 命中且内容一致
- **Evidence**：store 层与 cache 包测试（假 S3 HTTP 服务器注入分片失败）

### AC-3：代理重启后可识别并继续/放弃不完整上传
- **Type**: `rule`
- **Given**：create 成功且部分分片已上传后模拟进程死亡（存在残留 MPU；本地存储存在临时文件）
- **When**：新进程重新执行 save，随后 restore
- **Then**：save 发现残留 MPU 并续传（已上传分片被复用，仅缺失分片重传）完成提交，或中止后重传并提交；本地超龄临时文件被清理；restore 稳定命中
- **Pass Condition**：测试断言残留 MPU 被发现；续传路径下未重传已存在分片、Complete 成功；或 abort+重传成功；条目仅一个且 committed；临时文件被清除
- **Evidence**：`S3Blob` 恢复逻辑测试 + `LocalFileBlob` 临时文件清理测试

### AC-4：恢复尝试失败不阻断保存
- **Type**: `rule`
- **Given**：MPU 发现/列举/续传接口返回错误或无权限
- **When**：执行 save
- **Then**：回退为普通（全新）上传并正常 commit，日志记录恢复失败
- **Pass Condition**：恢复接口报错时 save 仍成功 commit，且不产生重复/残缺条目
- **Evidence**：store 层测试注入 List/Resume 错误

### AC-5：缓存缺失为稳定干净 miss 并可自愈
- **Type**: `rule`
- **Given**：（a）条目从不存在；（b）条目存在但 blob 缺失；（c）条目存在但 blobs 为空/摘要为空
- **When**：执行 restore（b/c 后再 save + restore）
- **Then**：三种情况均为退出成功的干净 miss（目标路径不被修改）；b/c 尽力条件失效条目；随后 save 重新上传，最终 restore 命中
- **Pass Condition**：restore 无错误返回、`CacheRestored=false`、目标路径内容不变；失效后重存再 restore 命中
- **Evidence**：cache 包集成测试 + e2e `TestCacheMissingBlobReupload`

### AC-6：旧缓存格式被识别、不被还原、可自愈
- **Type**: `rule`
- **Given**：注册表条目指向（a）无 manifest/非 zip 内容或 manifest 版本非 v2 的归档；（b）空 upload_id 的旧条目；（c）旧版本/缺字段的本地 sidecar
- **When**：执行 restore（随后 save + restore）
- **Then**：restore 在触碰目标前判定为无法识别格式 → 干净 miss 并条件失效（空 upload_id 时为无条件失效）；重存后命中；读取旧 sidecar 不 panic
- **Pass Condition**：`CacheRestored=false`、目标未被修改、日志标明 unrecognized format；重存后命中；旧 sidecar 场景下载/还原正常
- **Evidence**：archive、cache、store 包测试

### AC-7：一次失败不污染同进程后续命中
- **Type**: `rule`
- **Given**：同一 client 进程内 restore 先观察到某摘要的 blob 摘要不符（或归档校验失败）
- **When**：随后对相同地址执行 save（存储中仍存在同尺寸 blob），再 restore
- **Then**：save 不跳过上传（覆盖坏 blob）并 commit；新 restore 摘要通过并命中
- **Pass Condition**：测试断言可疑摘要之后 save 发生实际上传、commit 成功、restore 命中
- **Evidence**：cache 包集成测试

### AC-8：重复 save 与重复 restore 结果一致（幂等）
- **Type**: `rule`
- **Given**：一个已 commit 的完整条目与一个不存在/损坏地址
- **When**：对同一地址连续两次 save，连续两次 restore
- **Then**：第二次 save 不产生新对象/新条目（peek no-op；重启场景续传/去重后内容一致）；完整条目两次 restore 均命中且内容一致，缺失/损坏地址两次均为 miss
- **Pass Condition**：对象与条目数量、内容、摘要、命中/miss 结果在重复操作间完全一致
- **Evidence**：cache 包集成测试

### AC-9：中断期间同进程尽力 abort
- **Type**: `rule`
- **Given**：上传失败错误暴露了 MPU 标识，调用 ctx 已被取消
- **When**：save 处理上传错误
- **Then**：使用独立短超时 context 发起 abort；abort 成败均不改变 save 返回的错误；若 abort 未成功，后续 save 仍能发现并处理残留 MPU
- **Pass Condition**：测试断言 abort 以非取消的独立 ctx 发出；整体错误语义不变；最终状态满足 AC-2/AC-3
- **Evidence**：save 错误路径测试

### AC-10：可观察行为分类清晰
- **Type**: `rubric`
- **Dimension**：日志/进度输出对五类场景（成功、部分失败、重启恢复、缺失、旧格式）的可区分性与准确性
- **Scale**: 1-5
- **Anchors**：1 = 场景无区分或误导（如把 no-op 说成已失效、把 miss 说成命中）；3 = 五类场景可区分但部分文案含糊；5 = 五类场景均有明确文案，含命中/miss 原因、续传或放弃、失效是否实际发生，且不泄露内部实现细节
- **Pass Threshold**: >= 4
- **Evidence**：测试断言的进度阶段/日志文案与人工走查

## Open Questions
- [ ] MPU 发现调用所需权限（s3:ListBucketMultipartUploads / s3:ListParts）是否需要在帮助文案中显式说明？（倾向：在 cache 命令帮助中补充一句权限说明）
- [ ] 本地临时文件年龄阈值默认值（暂定 1 小时）是否可接受？
