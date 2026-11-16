# 层级目录采集的内存优化契约

本轮保持已有采集功能。README.md、catalogue/、samples/directory.jsonl、tests/、checks.py、workloads.py 和 bench.py 已提供正确未优化实现、完整接口、来源样例、独立树模型及可运行测量；不需要重建来源、读者、回执或原子事务。本文件约定本轮优化及交付，README 中对整树和整批内存物化的描述是当前实现现状，不要求保留该内部表示。

## 共同结果与两项交付

A：优化资格判定与正文暂存的交接，使等待祖先审核的记录不再依赖整树驻留。
B：优化归档对该交接表示的消费，使完整接纳不再建立整批 PreparedRecord 正文列表。
端到端不得在任一组件驻留整批正文、完整目录树或全量记录 ID 集合。活动嵌套栈和有限缓冲可以保留；临时磁盘和 SQLite 均可使用，允许多遍重读自建暂存，不指定实现组织或扫描次数。

记录仅在自身及全部祖先最终审核通过时接纳；栏目标签可重复，每次 begin_group 都是独立出现。最终审核可在孩子之前、之间或之后出现，不能用当前已知审核或最近栏目代替全部祖先资格。顶层记录、空批次、空正文和任意 bytes 均沿用既有语义。保持原序、source_order、连续 sequence、ID、逐条摘要、完整输入指纹及回执不变；同内容不同 ID 不合并，被否决内容仍参与指纹，正文重新分块不改变身份。

例如两层栏目都叫 repeated：外层直属 keep 通过，内层 omit 通过但内层审核否决，随后外层审核通过，最后顶层 twin 与空正文 empty 通过。keep 与 twin 都含原字节 00 ff 41，仍各保留一次；结果依次为 keep、twin、empty，source_order 为 0、2、3，总字节数为 6。若外层最终否决，keep 和 omit 都不能接纳；在该否决输入中若省去 twin 与 empty，该批正常发布零条。优化不能把合法否决批误当成处理失败。

## 保持的接口和失败边界

README 中的 Archive、ingest_batch、open_source、回执、读取及 CLI 接口兼容；已持久化的 v1 库、历史批次及其回执继续可读。内部 parse、prepare 和中间表示可以调整。原 observer 的 phase/detail 含义保持，异常仍原样传播。

完整结构、全局记录 ID 唯一性、全部审核、end_batch 后真实 EOF 和来源 close 成功之前不得发布。重复批也须完整验证；同批同输入返回原回执且不改变 current，同批不同输入仍冲突拒绝。资格侧处理的记录出现和归档侧消费的正文必须始终对应，不能通过先发布再撤回补偿。

来源、暂存读写、observer 或数据库提交前异常均不得发布本轮成果、回执或指针；旧批次保全并允许同 ID 重试。成功、否决、重投、冲突和异常返回时均释放本轮自建暂存、句柄及游标，不删除调用方文件。无需并行采集、跨进程续跑或崩溃恢复。

## 冻结基准与核对入口

保持 bench.py、workloads.py、checks.py、optimize_checks.py、samples/ 和 tests/ 原字节不变。tests/reference.py 的独立树模型用于结果对照，不提供优化答案；不另附完整生产源码副本。

bench.py full 对 late-veto 和 interleaved 两类输入，各取 64MiB、256MiB 正文，各运行三个全新进程。每轮完整 ingest_batch 的 Python 分配峰值须不超过 32MiB；同类 256MiB 与 64MiB 三轮峰值中位数之差须不超过 4MiB。不能提前消费来源，或把资格、暂存、摘要、准备及发布移出原计量窗口；不得改变负载、计量或逐字节业务断言。读取验证沿用 worker 的计量后完整重读，输出正确性与计量相互独立。optimize_checks.py 中的四组固定回执摘要按 README 的规范帧及 workloads.expected 独立计算，baseline 和 target 均核对，不替代逐字节重读。这里只约定 Python 分配峰值，不把 SQLite 原生缓存冒充 Python 计量；把整批改存到 Python 计量不可见的内存介质仍不符合有界暂存要求。

新增 optimize_checks.py baseline 执行原完整套件、活动写者中的失败与暂存清理探针，再运行完整冻结基准并输出逐轮实际值，不检查优化门槛。optimize_checks.py target 执行同样的核验并检查上述两条内存门槛。该入口检查公共异常、重复栏目、二进制、空正文、原序、重开和存储失败；不能代替对你新增暂存边界的异常核验。

交付兼容优化、独立差分核验、在实际新增暂存读写处注入异常的资源释放核验、可重跑实测及简短交接不变量说明。独立差分须覆盖重复栏目标签、审核早晚、任意分块和拒绝正文的指纹；清理核验在操作返回后、归档对象仍存活时观察，不能只依赖进程退出。

现有 .pointwise-runtime.json 保持 version=1、kind=cli、setup 为 argv 数组列表、verify/demo 各为 argv 数组。交付时使 verify 调用 python3.11 -B optimize_checks.py target；demo 沿用 catalogue demo，经真实入口展示嵌套审核、同内容不同 ID、空正文、分块重投与非法尾部隔离。本轮不新增网站登录、格式、增量修订或跨批去重。
