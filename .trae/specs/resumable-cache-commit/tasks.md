# 可恢复的缓存提交 - 实施计划

说明：任务按依赖排序；每条任务至少含一个 TR（`rule` 或 `rubric`）。实现文件预计落在
`internal/cache/store/`、`internal/cache/`、`clicommand/`，测试与实现同包。

## Issue I-1: abort 放弃路径无可观察信号 + e2e 硬 grep 续传文案（Review R1 / F1）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - 新增哨兵 `errStaleUploadAbandoned`（区别于 errNoResumableUpload）；零分片/坏布局分支经新 helper `abandonUpload` 处理：
    abort 成功输出 Info "aborted an incomplete multipart upload that could not be resumed, uploading again"，失败仍 Warn。
  - s3.go Upload 改为三分支分类：无残留/已放弃均静默继续，仅意外错误 Warn。
  - e2e fixture grep 放宽：同时接受 resumed/aborted 两种文案。
  - 新增 `TestS3AbandonedSentinel`（坏布局、零分片均返回 errStaleUploadAbandoned 且 upload aborted）。
  - A1：更新 restore.go 摘要不符路径的陈旧矛盾注释。
  - `go test -race ./internal/cache/...` 与 fixture 模板渲染均通过。

## Issue I-2: corrupt fixture 无法覆盖目标 blob（Review R1 / F2）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - `cache_corrupt_blob.yaml` 覆盖命令改为 `aws s3 cp garbage.txt "s3://bucket/$KEY" --region us-west-2`（URI 无 query，region 独立参数），
    静态走查确认覆盖目标 `$KEY` 本身。
  - fixture 模板渲染通过；`go vet -tags e2e` 通过。

## Task 1: S3Blob 残留 multipart upload 的发现、续传与中止
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - 新增 [resume.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/cache/store/resume.go)：`s3MPUAPI` 窄接口、`findMultipartUpload`（分页+精确键匹配）、`listUploadedParts`（分页）、
    `tryResumeUpload`（布局校验→仅传缺失分片→Complete；坏布局/零分片 abort；中途失败保留分片）、
    `AbortUpload`（实现可选接口 `MultipartAborter`）；`S3Blob.Upload` 先续传后回退 manager。
  - TR-1.1~1.4 全部通过：`TestS3ResumeUpload`（仅上传 part3、5B、对象一致）、`TestS3UploadNoMPU`（发现调用 1 次）、
    `TestS3ResumeFailuresFallback`（三类注入错误均回退成功）、`TestS3ResumeBadLayoutAborted`、`TestS3ResumeZeroPartsAborted`。
  - `go test -race ./internal/cache/store/` 通过。
- **Description**:
  - 在 `internal/cache/store/` 新增恢复逻辑（预计新文件 `resume.go`），为 S3 定义窄接口封装
    `ListMultipartUploads / ListParts / CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload`，
    使恢复路径可用假 S3 HTTP 服务器确定性测试。
  - `S3Blob.Upload` 在常规上传前先尝试恢复：按**精确 fullKey** 过滤残留 MPU；
    存在则 `ListParts` 读取已上传分片，仅从本地文件对应偏移上传缺失分片，
    排序后 `CompleteMultipartUpload`；恢复任一步失败则尽力 abort 并回退到现有 `manager.Uploader` 全新上传。
  - 成功路径只增加一次发现调用；发现接口报错/无权限时直接回退，不得阻断保存。
  - 复用既有 `TransferInfo` 指标（BytesTransferred 仅计实际传输字节、PartCount 等）。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4, AC-8
- **Test Requirements**:
  - `rule` TR-1.1：构造"create 后仅部分分片上传"的残留 MPU，调用 Upload 断言已存在分片不重传、
    缺失分片上传、Complete 成功，对象字节与源文件一致；证据：store 包测试输出
  - `rule` TR-1.2：无残留 MPU 时 Upload 行为与现状一致（manager 上传成功，仅一次发现调用）
  - `rule` TR-1.3：List/ListParts/续传分别注入错误时，Upload 仍通过回退路径成功，且最终对象完整
  - `rule` TR-1.4：同键残留 MPU 续传不可行（如分片尺寸不一致）时 abort 后重新上传成功，无重复最终对象
- **Notes**: S3 同键允许多个 MPU；以 ListMultipartUploads（prefix 过滤 + 客户端精确匹配）返回者为准。

## Task 2: LocalFileBlob 崩溃临时文件清理
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - [file.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/cache/store/file.go) 新增包级变量 `staleTempFileAge`（默认 1h）与 `tempFilePrefixes`，
    `sweepStaleTempFiles` 在 `NewLocalFileBlob` 初始化时清理超龄 `.zstash-upload-*`/`.zstash-meta-*`，全程 best-effort。
  - TR-2.1/2.2：`TestSweepStaleTempFiles` 断言超龄数据/元数据临时文件被删、未超龄保留、真实数据与 sidecar 不被触及。
  - TR-2.3：`TestSweepStaleTempFilesRemoveError`（只读父目录注入删除失败）断言存储仍成功初始化、文件保留。
  - `go test -race ./internal/cache/store/` 通过。
- **Description**:
  - 在 `NewLocalFileBlob` 初始化时（或首次上传前）扫描 root，清理超过年龄阈值的
    `.zstash-upload-*` 与 `.zstash-meta-*` 临时文件；阈值设为包级变量（默认约 1 小时）便于测试。
  - 清理为 best-effort：单个文件删除失败只记录日志，不影响初始化与正常上传。
- **Acceptance Criteria Addressed**: AC-3
- **Test Requirements**:
  - `rule` TR-2.1：放置超龄与未超龄临时文件各一，初始化后断言超龄文件被删、未超龄文件保留
  - `rule` TR-2.2：正常数据文件与 sidecar 不被清理逻辑触及
  - `rule` TR-2.3：清理中注入删除错误时 NewLocalFileBlob 仍成功返回可用存储

## Task 3: NSC 非 LIVE 对象不可恢复性的测试固化
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: None
- **Completion Evidence**:
  - 无生产代码改动；在 [nsc_test.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/cache/store/nsc_test.go) 的 `TestNscStore_Stat` 增加 "unfinished and non-live statuses return no size"：
    UPLOADING/PROCESSING/FAILED/ABORTED 均返回非 ErrBlobNotFound 错误且 size=0；结合既有 LIVE→size 与缺失→ErrBlobNotFound 用例，
    证明不完整 NSC 上传不可能被 restore 命中。
  - TR-3.1、TR-3.2 通过。
- **Description**:
  - 不改动生产代码：通过假 commandRunner 补测试，证明 `Stat` 对非 LIVE（UPLOADING/FAILED 等）
    状态返回错误、Download 对未定稿对象不可用，因此不完整 NSC 上传不可能被 restore 命中。
- **Acceptance Criteria Addressed**: AC-3, AC-5
- **Test Requirements**:
  - `rule` TR-3.1：describe 返回非 LIVE 状态时 Stat 返回错误（非 ErrBlobNotFound 也不返回尺寸）
  - `rule` TR-3.2：describe 报告不存在时 Stat 映射为 ErrBlobNotFound

## Task 4: 上传失败时同进程尽力 abort（独立 context）
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 1
- **Completion Evidence**:
  - 实现细化（同 AC 范围内的偏差）：abort 放在 store 层而非 save.go —— `S3Blob.Upload` 在 manager 返回错误后调用
    `bestEffortAbortForKey`（[resume.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/cache/store/resume.go)）：
    以 `context.WithoutCancel` + 10s 超时的独立 ctx 发现并中止残留 MPU。相比原计划的 MultiUploadFailure 判断，
    覆盖分片失败与 Complete 失败全部场景（分片失败时 SDK 不返回 MultiUploadFailure）。
  - TR-4.1：`TestS3FailedUploadBestEffortAbort/independent_abort_succeeds_after_SDK_abort_fails`（SDK abort 被拒→独立 abort 成功）
    与 `TestBestEffortAbortIndependentContext`（调用 ctx 已取消仍完成 abort）通过。
  - TR-4.2：`.../abort_failure_leaves_upload_error_unchanged`（两次 abort 均被拒）断言 Upload 错误仍为 AccessDenied、不掺 abort 错误。
  - TR-4.3：`TestS3FailedUploadResumedNextSave` 断言孤儿上传在第二次 Upload 被续传完成，仅重传 5MiB，对象字节一致。
  - 注意：假服务器需用 403（非重试）注入 abort 失败；500 会被 SDK 重试耗尽计数。
- **Description**:
  - store 层暴露可选接口（如 `MultipartAborter { AbortUpload(ctx, key, uploadID) error }`），由 S3Blob 实现。
  - `save.go` 上传返回错误后：若错误实现 SDK 的 `manager.MultiUploadFailure`（可暴露 MPU 标识），
    使用 `context.Background()` 派生的短超时 context 尽力 abort；abort 成败不改变 save 返回错误。
  - 调用 ctx 已取消时 abort 仍能发出（不依赖 wctx）。
- **Acceptance Criteria Addressed**: AC-9, AC-2
- **Test Requirements**:
  - `rule` TR-4.1：ctx 取消且上传失败时，断言 abort 以独立非取消 ctx、正确 key/uploadID 发出
  - `rule` TR-4.2：abort 自身失败时 save 返回的错误与未执行 abort 时一致
  - `rule` TR-4.3：abort 未成功的场景下，再次 save 仍能按 Task 1 逻辑发现并处理残留 MPU

## Task 5: 可疑 blob 记录与 save 跳过判定加强
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - [client.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/cache/client.go) 增加 `suspectBlobs`（互斥保护、懒初始化）+ `markBlobSuspect/isBlobSuspect`；
    [save.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/cache/save.go) 的 `blobAlreadyStored` 改为 client 方法，命中可疑集合即返回 false；
    [restore.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/cache/restore.go) 在摘要不符与归档校验失败两处标记可疑。
  - TR-5.1/5.4：`TestResumableSuspectBlob`（同尺寸垃圾 blob → miss 后 save 重传 → 再 restore 命中）。
  - TR-5.2：`TestResumableArchiveValidationSuspect`（跳过摘要校验触发 Validate 失败 → 标记可疑 → save 重传命中）。
  - TR-5.3：未标记场景跳过行为不变，既有测试与 `TestResumableIdempotency/repeated_saves` 佐证。
  - `go test -race ./internal/cache/...` 通过。
- **Description**:
  - `client` 增加带互斥锁的可疑摘要集合（按 digest value）；restore 在摘要不符
    （`ErrDigestMismatch`）与归档校验失败两条路径标记对应摘要。
  - `blobAlreadyStored` 除尺寸检查外，命中可疑集合时返回 false（触发实际上传覆盖坏 blob）。
  - 纯进程内状态，重启后为空（重启后恢复依赖对象存储侧），无需 API/配置变更。
- **Acceptance Criteria Addressed**: AC-7, AC-8
- **Test Requirements**:
  - `rule` TR-5.1：restore 摘要不符后同进程 save，即使存在同尺寸 blob 也发生实际上传并 commit 成功
  - `rule` TR-5.2：归档校验失败路径同样标记可疑并使后续 save 重传
  - `rule` TR-5.3：未标记摘要时跳过行为与现状一致（尺寸匹配仍跳过）
  - `rule` TR-5.4：重新上传后再 restore 摘要通过并命中，污染闭环消除

## Task 6: retrieve 命中但结构残缺条目按干净 miss 处理
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - 新增类型化错误 `errStaleEntryStructure`；`downloadCache` 对空 blobs/空摘要值返回该错误；
    Restore 新增处理分支：干净 miss + 条件失效，文案为 "stale entry structure"。
  - TR-6.1/6.2：`TestResumableStaleStructure`（空 blobs / 空摘要值 → miss 且条目被失效）；
    同步更新既有 `TestRunRestore_Diagnostics/hit_without_downloadable_blob`（原为歧义硬错误，现已改为干净 miss）。
  - TR-6.3：失效后 Save 重建、Restore 命中（同上测试）。
  - `go test -race ./internal/cache/...` 通过。
- **Description**:
  - `downloadCache` 对 `blobs` 为空或 blob 摘要值为空返回类型化错误（如 `errStaleEntryStructure`），
    而非普通字符串错误。
  - `Restore` 识别该错误：目标路径尚未触碰，按"无法识别/陈旧结构"降级 miss 并条件失效条目，
    复用既有 miss 结果组装与进度文案。
- **Acceptance Criteria Addressed**: AC-5, AC-6
- **Test Requirements**:
  - `rule` TR-6.1：retrieve 返回空 blobs 时 restore 无错误、`CacheRestored=false`、目标路径不变，
    并对条目发起条件失效
  - `rule` TR-6.2：retrieve 返回空摘要值时行为同上
  - `rule` TR-6.3：失效后重新 save + restore 正常命中

## Task 7: cache 包场景矩阵集成测试
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 4, Task 5, Task 6
- **Completion Evidence**:
  - 新增 [resumable_test.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/cache/resumable_test.go)（轻量 file:// + mock 矩阵）：
    `TestResumableSuccess`（TR-7.1）、`TestResumablePartialUploadFailure`（TR-7.2：只读存储注入上传失败→无 committed 条目→miss→重存命中，仅 1 次 commit）、
    `TestResumableSuspectBlob`/`TestResumableArchiveValidationSuspect`、
    `TestResumableMissing`（纯 miss、条目无 blob 自愈；TR-7.3）、
    `TestResumableStaleStructure`、`TestResumableOldFormats`（manifest v1、空 upload_id 无条件失效；TR-7.3）、
    `TestResumableIdempotency`（重复 save/重启去重/重复 restore/miss；TR-7.4）。
  - store 侧补充 `TestLocalFileBlobDownloadBadSidecar`（缺失/截断/非法 sidecar 容忍）。
  - TR-7.5：`go test -race ./internal/cache/...` 全部通过。
- **Description**:
  - 利用既有 `mockAPIClient` 与 file:// 存储（必要时加最小假 S3 HTTP 服务器）补齐端到端矩阵：
    成功往返、部分上传失败后 miss→重存自愈、模拟重启（残留 MPU/临时文件）后续传/放弃、
    三种缓存缺失、三种旧格式（非 zip/错误 manifest 版本、空 upload_id、旧 sidecar）、
    重复 save/restore 幂等。
  - 断言条目的 committed 状态、数量、摘要、命中/miss、目标文件内容在全流程中一致。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-5, AC-6, AC-8
- **Test Requirements**:
  - `rule` TR-7.1：成功场景 save→restore 逐文件内容一致且条目已 committed
  - `rule` TR-7.2：部分失败/重启场景断言无半成品条目可命中，恢复后仅一个 committed 条目且命中
  - `rule` TR-7.3：缺失与旧格式场景断言稳定 miss（含重复 restore）、失效、自愈命中
  - `rule` TR-7.4：重复 save 不产生新对象/条目，重复 restore 结果完全一致
  - `rule` TR-7.5：`go test ./internal/cache/... ./internal/cache/store/...` 全部通过（含 -race）

## Task 8: 可观察输出与帮助文案
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 1, Task 5, Task 6
- **Completion Evidence**:
  - store.TransferInfo 新增 `Resumed` 信号，由续传成功路径置位；save.go 在 Upload 返回 Resumed 时输出明确 Info 日志
    "resumed an incomplete multipart upload left by a previous save"。
  - miss 原因四类文案在场景测试中断言：missing blob、digest mismatch、archive failed verification、stale entry structure（均为不同字符串），命中路径另计。
  - cache_save 帮助补充中断续传/中止、可疑 blob 重传与 S3 权限（s3:ListBucketMultipartUploads、s3:ListParts）说明；
    cache_restore 帮助补充"无可用 blob → 干净 miss + 失效"。
  - TR-8.1 通过（resumable_test 各类 NotRestoredReason 断言）；TR-8.2 rubric 自评 5：五类场景文案明确、无 miss/命中误报、no-op 不报告为已失效。
  - `go tool gofumpt -extra` 无输出；`go build ./...` 通过。
- **Description**:
  - 为续传/放弃/可疑重传补充明确进度阶段与日志文案；miss 原因继续区分 missing blob、
    digest mismatch、archive failed verification、stale structure；no-op 不报告为已失效。
  - 在 `cache_save.go` / `cache_restore.go` 帮助文案中补充：MPU 发现/续传所需 S3 权限，
    以及中断恢复行为说明（文案保持英文，与现有帮助一致）。
- **Acceptance Criteria Addressed**: AC-10
- **Test Requirements**:
  - `rule` TR-8.1：测试断言五类场景的进度/日志文案可区分且无 miss/命中误报
  - `rubric` TR-8.2：可观察输出质量；scale 1-5；anchors 1=误导/无区分，3=可区分但含糊，
    5=五类场景明确含原因与续传/放弃/失效结果；threshold >= 4；证据：测试断言与人工走查

## Task 9: e2e 中断/重启恢复 fixture
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 1, Task 7
- **Completion Evidence**:
  - 新增 [cache_interrupted_resume.yaml](file:///Users/ding/Documents/swe/0910081/project-01/internal/e2e/fixtures/cache_interrupted_resume.yaml)：500MiB 随机内容 + `timeout 12` 中断保存，
    以 `aws s3api list-multipart-uploads` 轮询确认残留 MPU 存在（最多 3 次）；再次 save 要求日志含
    "resumed an incomplete multipart upload"；删除目标后 restore 并用 shasum 校验字节完全一致。
  - 新增 [cache_corrupt_blob.yaml](file:///Users/ding/Documents/swe/0910081/project-01/internal/e2e/fixtures/cache_corrupt_blob.yaml)：save 后以 garbage 覆盖其 blob，
    restore 必须为干净 miss 且目标不出现；失效后以新内容重存，最终 restore 命中 "healed contents"。
  - 在 [cache_test.go](file:///Users/ding/Documents/swe/0910081/project-01/internal/e2e/cache_test.go) 注册 `TestCacheInterruptedResume`、`TestCacheCorruptBlobHeal`，
    命名 slug 符合 testcache* 信任策略约束。
  - TR-9.2：`go vet -tags e2e ./internal/e2e/` 通过；两个 fixture 经 Go text/template（fixture/b64enc 函数）实际渲染成功（TR-9.1 静态走查：中断后无半成品命中、恢复命中内容一致）。
  - 注：真实云资源执行需在 CI 环境完成；主要可运行证据来自 Task 1/7 的协议级与集成测试。
- **Description**:
  - 新增 e2e fixture（与 `cache_missing_blob_reupload.yaml` 同风格）：
    （a）上传中途制造失败/杀进程后重启 agent，再 save 续传、restore 命中；
    （b）向对象写入非 zip/旧 manifest 内容，restore 为 miss + 失效，重存自愈。
  - 在 `internal/e2e/cache_test.go` 注册测试（遵循 test name slug 约束）。
  - 本地无法跑真实云资源时，以 Task 1/7 的协议级测试作为主要证据，并在 review 中注明。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-6
- **Test Requirements**:
  - `rule` TR-9.1：fixture 步骤与断言可由静态走查验证：中断后无半成品命中、恢复后命中且内容一致
  - `rule` TR-9.2：新测试遵循 e2e testcase 注册规范（命名 slug、queue、agent binary 变量）
