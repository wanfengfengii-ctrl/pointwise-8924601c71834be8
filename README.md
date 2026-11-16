# Offline Catalogue Archive

Python 3.11 标准库应用。读取离线栏目目录的分块事件，完整验证栏目结构和所有审核后，将合格正文连同回执一次提交至 SQLite。下游按原记录开始顺序读取原字节并校验摘要。所有数据库和输入路径均由调用者指定；示例和测试仅使用项目样例及自有临时目录。

## 运行

```sh
python3.11 -B checks.py
python3.11 -B -m catalogue demo
python3.11 -B bench.py full
```

checks.py 运行完整 tests 套件及两类 1MiB 独立进程测量。bench.py full 对末尾否决、深浅栏目交错两类输入各运行 64MiB、256MiB，每档三个新进程，输出逐轮峰值、业务摘要和四倍规模峰值增量。它是测量工具，没有把资源数值写成固定成功结果。测量窗口从创建事件迭代器之前持续至完整提交返回，包含来源消费、资格判断、正文物化与发布；之后重新读取并逐字节核对全部归档。当前实现保留完整目录树及整批待写记录，适合有限离线批次。tests/reference.py 是独立声明式树语义参照；workloads.py 冻结有限大负载及独立输出规格。

命令行同时提供：
```sh
python3.11 -B -m catalogue ingest ./archive.sqlite batch-1 samples/directory.jsonl --chunk-size 7
python3.11 -B -m catalogue receipt ./archive.sqlite batch-1
python3.11 -B -m catalogue list ./archive.sqlite batch-1
```
list 的正文为 base64；demo 使用自有临时库，展示嵌套否决、相同正文的不同记录、空正文、分块重投以及非法尾事件失败隔离。

## 公开 Python API

```python
from catalogue import Archive, open_source

with Archive("archive.sqlite") as archive:
    result = archive.ingest_batch("batch-1", open_source("samples/directory.jsonl", chunk_size=7))
    assert archive.receipt("batch-1") == result
    with archive.read_records("batch-1") as records:
        for record in records:
            print(record.id, record.body, record.sha256)
```

Archive(path, observer=None) 打开或创建 v1 SQLite 库；也支持 :memory: 用于有限临时业务。一个实例只供顺序写入；支持独立数据库连接读取，不承诺并行写入或回调重入。close 幂等，对象关闭后不再接收操作。底层连接由 Archive 拥有。模块函数 ingest_batch(archive, batch_id, events) 与实例方法等价。

open_source(path, chunk_size=65536) 返回单遍迭代器，在首次推进时打开 UTF-8 JSON Lines 文件；耗尽、close 或交给 ingest_batch 后均释放自有句柄。每行恰有 event 及以下字段：begin_group/begin_record 的 id；body 的 base64 data；end_record/verdict 的 approved；end_group/end_batch 无附加字段。额外字段、无效 JSON/base64 抛 StructureError，文件 I/O 异常原样传播。chunk_size 为严格正整数，非法配置立即抛 ValueError。JSONL 格式见 samples/directory.jsonl。

ingest_batch 接受 batch_id 和任意单遍事件迭代器，取得并负责关闭该迭代器。所有事件均为元组：
- ('begin_group', id) 打开栏目；栏目可嵌套，栏目标签可重复，每次打开为独立栏目。
- ('begin_record', id) 打开记录；记录不能嵌套，记录 ID 在本批唯一。
- ('body', bytes) 为当前记录附加原字节；任意分块，可为空。
- ('end_record', bool) 结束记录并给出自身审核。
- ('verdict', bool) 给当前栏目唯一的最终审核；可在其孩子之前、之间或之后出现，结束前必须出现且不得修改。
- ('end_group',) 结束当前栏目。
- ('end_batch',) 表明完整目录结束。仍须消费到来源 EOF，任何后续事件或来源异常都拒绝整批。

ID 是不含 NUL 的非空字符串；审核严格为 bool。允许空批次、顶层直属记录及多个顶层栏目。记录只有自身及全部祖先审核通过才接纳；不得按最近祖先或正文结束时已知的审核提前接纳。begin_record 的零起始位置为 source_order，拒绝记录也占位。归档 sequence 仅对接纳记录连续从零编号；相同正文不同 ID 和空正文各自保留。

receipt(batch_id) 返回不可变 Receipt 或不存在时 None。字段为 batch_id、input_sha256、generation、record_count、total_bytes、manifest_sha256。输入指纹的 v1 规范见下面；所有被否决内容仍参与身份。同批同指纹返回原回执且不改 current；同批不同指纹抛 BatchConflict，不覆盖已有数据。新批次 generation 为本库最大代次加一，并成为 current()；旧批次仍可按 ID 读取。原输入必须再次完整验证，重投也不能跳过非法尾部。

read_records(batch_id) 返回支持迭代、close 及上下文管理的 Records，每项 ArchivedRecord 含 sequence、source_order、id、body、sha256。打开时固定该已提交批次的读取事务；不存在抛 KeyError。每次读取核对正文 SHA256 和顺序，耗尽时核对记录数、字节数及回执清单。损坏抛 IntegrityError；数据库异常传播；异常、耗尽和 close 均关闭自有游标与连接。提前停止须用上下文或显式 close；提前 close 不宣称验证未读尾部。每项正文作为 bytes 返回，不提供正文切片接口。

## 身份和失败边界

input_sha256 以 b'catalogue-input-v1\x00' 为 SHA256 初始输入，逐项加入长度前缀规范帧。帧为 ensure_ascii=True、separators=(',', ':') 的 JSON ASCII 字节，之前加八字节大端长度。begin_group/begin_record/verdict/end_group/end_batch 直接将元组转为列表成帧。正文仅在 end_record 时成帧 ['end_record', approved, byte_length, body_sha256]，所以正文分块不影响身份；栏目审核事件在结构中的位置属于输入身份。begin_group 的重复标签不合并栏目。

manifest_sha256 以 b'catalogue-manifest-v1\x00' 初始化，对每条接纳记录将 [sequence, source_order, id, byte_length, sha256] 按相同方式成帧。存储采用 batches、records、current_batch 三表，schema user_version=1。成果、回执、代次及当前指针在一个 SQLite 事务中提交；未通过完整结构验证、来源 EOF 和来源关闭前不进入发布。失败不会删除任何旧批次；本轮回执、记录和指针一起回滚，随后允许使用同 ID 重试。没有跨进程续跑、联网抓取或增量目录接口。

observer(phase, detail) 是同步进度端口，可抛异常中止处理：body_staged 携带待写记录序号，record_written 携带已插入序号，before_commit 携带拟提交 Receipt；回调返回不代表已提交。回调异常原样传播。完整回归同时使用该端口、真实 SQLite 拒写触发器及独立连接检查事务可见性，覆盖来源中断、最终审核前异常、坏尾事件、正文物化失败、写入失败、提交前失败、重开重试及冲突重投。正文物化阶段只建立本轮内存对象，不创建持久暂存；释放库和读者均不删除调用者数据库。
