# 交接说明：层级目录采集的有界暂存优化

本文件只描述本轮交付的不变量与实测，不新增业务。冻结文件（`bench.py`、`workloads.py`、`checks.py`、`optimize_checks.py`、`samples/`、`tests/`）保持原字节不变；`catalogue/` 内的 parse/prepare 中间表示替换为磁盘暂存，公开接口与已持久化 v1 库均兼容。

## 一、两侧交接表示

资格侧（`catalogue/qualification.py`）：

- Python 中常量驻留：活动记录（仅 `source_order`、正在累加的 SHA256 和字节数）、活动栏目栈（每层 `[group_index, verdict]`）、以及一个输入指纹 hasher。
- 每次 `body` 事件把该块追加到本轮自有正文文件 `tempfile.mkstemp(prefix='catalogue-stage-body-')`，同时更新活动记录的摘要；`end_record` 把 `source_order, id, offset, size, sha256, approved, parent` 写入结构暂存 SQLite `tempfile.mkstemp(prefix='catalogue-stage-meta-')`。
- 栏目 ID 只以整数索引驻留，父链接写入 structure 暂存，因此栏目标签可重复且重复标签不合并；`begin_group` 每次都是独立出现。
- 整批 `end_batch` 之后才 `finalize()`：按索引顺序（父索引必小于子索引）计算每层 `effective = approved AND parent.effective`，写入 `group_effective`，随后提交结构暂存。祖先资格因此不依赖“最近栏目”，也不依赖审核出现的早晚。
- 资格交接面是 `Staging.accepted()`：一条一条 `yield StagedRecord(source_order, record_id, offset, size, sha256)`，从不返回列表、ID 集合或整棵树。`Staging.chunks(record)` 以 64KiB 为单位回读正文。

归档侧（`catalogue/archive.py`）：

- `publish()` 先做一遍**只读元数据**的 `prepare()`：从结构暂存累加 manifest、总字节数并发出 `body_staged(sequence)`，完全不触碰正文。
- 事务内再遍历 `staging.accepted()`，逐条写归档行：`size <= STAGE_CHUNK` 的正文直接作为单个参数绑定；更大的正文用 `zeroblob(size)` 预留、经 `sqlite3.Connection.blobopen` 的增量 blob 接口按 64KiB 回填。两条路径都不构造 `PreparedRecord` 列表或整批 bytes。
- `record_written(sequence)` 每插入一条触发；`before_commit(Receipt)` 在写入 `current_batch` 之后、`COMMIT` 之前触发；任一回调抛异常即回滚。

## 二、保持的语义（差分核验覆盖点）

- 接纳条件：记录自身审核为真且全部祖先最终审核为真；`source_order` 按所有 `begin_record` 递增（被否决记录占位）；归档 `sequence` 仅对接纳记录从 0 连续编号。
- 输入指纹：正文仅在 `end_record` 成帧 `['end_record', approved, length, sha256]`，重新分块不改变身份；被否决内容仍成帧；`begin_group` 重复标签各自成帧；`verdict` 在结构中的位置参与身份。
- manifest：`[sequence, source_order, id, byte_length, sha256]` 逐条成帧。
- 发布门槛：完整结构、全局记录 ID 唯一、全部审核、`end_batch` 后真实 EOF、来源 `close()` 成功——全部在 `archive.publish()` 之前完成；重投同样完整验证。
- 同批同指纹返回原回执且不改 `current`；同批不同指纹 `BatchConflict`；新批 `generation` 为最大代次 +1 并成为 `current`；旧批次仍可读。
- 全否决批正常发布零条。

## 三、失败与资源释放

- `ingest_batch()` 在来源异常或结构错误时先 `close()` 来源（异常保持原对象），再关闭并删除本轮暂存；不进入发布。
- 来源 `close()` 抛异常时该异常原样传播，暂存仍被删除，不发布。
- 发布阶段的暂存读、observer 回调、SQLite 写入或提交前异常都触发事务 `ROLLBACK`，旧批次、回执和指针不变；`finally` 中 `staging.close()` 仍会删除两个临时文件。
- 清理阶段自身的错误（含 `os.unlink` 被拒绝）被吞掉，绝不替换正在传播的原始异常。
- 成功、否决、冲突、重投与异常返回时都不会留下暂存文件；不删除调用者数据库或输入文件。
- 观察点：`streaming_checks.fault_probes()` 在 `Archive` 仍存活时断言临时目录为空、无回执、`current` 未变、旧批可读，并验证同 ID 重试成功。

## 四、实测（本机 python3.11.2 / SQLite 3.40.1）

`python3.11 -B bench.py full`（两类负载 × 64MiB/256MiB × 3 个新进程）：

| 负载 | 64MiB 三轮峰值 (B) | 256MiB 三轮峰值 (B) | 中位增量 (B) |
| --- | --- | --- | --- |
| late-veto | 109761 ×3 | 109761 ×3 | 0 |
| interleaved | 110865 ×3 | 110893 ×3 | 28 |

门槛 32MiB 与 4MiB 均远未触顶；四组回执摘要与 `optimize_checks.EXPECTED` 四组固定值逐项相符（`optimize_checks.py target` 通过）。

`python3.11 -B streaming_checks.py`：

- 64 个独立目录对照 `tests/reference.py`（随机树、重复标签、早晚审核、任意分块、空正文）。
- 暂存异常：`begin_group`/`begin_record`/`append_body`/`end_record`/`finalize` 以及暂存回读 `chunks` 各注入一次异常，全部保全旧成果并在返回时清空暂存。
- 16MiB 大正文（256KiB/记录）峰值 166099 B。
- 单个 64MiB 记录的临时核验：峰值约 1.06MB，归档正文逐字节与摘要一致。

`python3.11 -B compat_checks.py`：对初始提交（`git archive HEAD` 独立解包）做跨实现差分，28 个批次、33 条输出、互读双方 v1 数据库，结果完全一致。作为参照，初始实现在 16MiB interleaved 输入的峰值已达 29.5MiB，且随负载线性增长。

`python3.11 -B -m catalogue demo` 经真实 CLI 入口展示：嵌套栏目否决后仅保留应接纳记录、同内容不同 ID 各保留一次、空正文归档、不同分块重投得到相同回执、非法尾事件被隔离且 `current` 不变。

## 五、复现

```sh
python3.11 -B optimize_checks.py target     # 既有清单 verify
python3.11 -B streaming_checks.py           # 独立差分 + 暂存异常清理
python3.11 -B compat_checks.py              # 跨实现差分
python3.11 -B -m catalogue demo             # 真实入口审核归档演示
```

实测数值受机器与 Python 版本影响；门槛检查只断言上限与增量，不把本次数字写成固定成功结果。
