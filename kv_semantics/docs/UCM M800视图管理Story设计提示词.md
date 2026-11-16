## 一、任务
为KVClient增加M800视图管理能力：
- M800盘框后端集群的控制面使用Ceph，视图通过KVClientAgent（Ceph agent动态库）对外提供（agent目录下代码)
- KVClient需在M800场景下委托该agent拉取多命名空间（kvnamespace）视图，接入UCM现有ViewServer刷新机制
- 在此之上叠加基于多kvns多节点的IO DHT打散：transport跨ns共享（全局池化）+默认二次哈希故障转移

## 二、设计前提
四个IO命令已携带‘KvNsId nsId’入参，本次设计不改造IO接口，仅按此前提实现per-ns快照选取

## 三、设计决策
### 3.1 视图模型：二级
- `kvnsId -> GlobalView`, 1:1。`ViewUniverse`存`unordered_map<KvnsId, GlobalView>` ,per-ns比对`viewEpoch`
- `GlobalView.asumap`为`map<NodeId, NodeInfo>,一个NodeId = 一个ASU节点、
- ```
  knNsId---1:1--->GlobalView
					  |_____asuMap:map<NodeId, NodeInfo>
  ```

### 3.2 新增模块
| 模块                   | 说明                                                                            |
| -------------------- | ----------------------------------------------------------------------------- |
| M800ViewServerConfig | 字段monHost/clientName/keyring/agentLib；static Build（attrs, config)解析+必填校验      |
| M800MasterViewServer | dlopen+init+get_all_ns_name+get_view_by_ns-->GlobalView适配层;而外启动轻量视图服务对外提供只读快照 |
| M800SlaveServer      | Worker: 不加载agent; 通过视图服务向Master拉取视图, 解析后返回;                                   |
| M800ViewCodec        | 视图文本序列化/反序列化,Master/Slave共用;复用ParseAsuInfo/TryParseAsuInfoKey                 |
| ViewUniverse         | 维护二级视图映射+PublishAll/GetView（nsId）/GetAllNsId/ReplaceAll                       |
| RefreshScheduler     | 独立线程，30s定时/IO失败/手动RefreshNow                                                  |
Master/Worker角色拆分(TP推理场景)
- 推理服务TP场景下多个进程下各跑一个KVClient,但只有Master直连agent;Worker通过访问Master获取视图.
- 角色来源(设计前提):view.role由上层Init时传入
- 两个实现偶读实现ViewServer::GetAllViews,因此RefreshScheduler/ViewUniverse/KvClientImpl完全不需要感知角色差异
Master关键逻辑
```
dlopen(view.ceph.agent_lib)
	->KVClientAgent.init(monHost, clientName, keying)
	->get_all_ns_name()
	->每个ns:
		get_view_by_ns(ns, up=true, in=true)
		 ->GlobalView{viewEpoch = agent.get_epoch(),
					  asuMap[osd_id] = NodeInfo{ endpoints = addrs }}
		 -> views[nsId]
	->更新lastFetched_
```
视图同步协议(Slave<-Master)
- 传输:TCP + 极简HTTP/1.1 `GET /v1/views?since_epoch=<lastEpoch>` 仓库内的standalone_metrics_backend.cc的`MetricsHttpServer`可直接借鉴的裸socket实力.
- 编码:纯文本行式格式,与`ConfigFileViewServer`的`key=value`风格一致,便于复用现有解析器:```
  viewEpoch=123
  ns=1
  asuIds=10,20
  asuInfo.10=....
  asuInfo.20=...
  ns=2
  asuIds=20,30
  asuInfo.20=...
  asuInfo.30=...
  `
  - 语义:Master比对自身epoch,未变返回304 Not Modified(空body,Slave返回空map);变化返回200+全量视图.
  - 一致性:Slave完全信任Master的`viewEpoch`,不自行推进;比较由ViewUniverse::PublishAll统一完成,Master是视图的唯一权威源

### 3.3 配置项示例
```
view.provider=m800
view.role=master               # 默认Master
view.listen_addr=0.0.0.0:9100  # 默认值
view.ceph.mon_host=[10.0.0.1:6789,10.0.0.2:6789] # master必填
view.ceph.client_name=admin
view.ceph.keyring=/etc/ceph/ceph.client.admin.keyring
view.ceph.agent_lib=/usr/lib/libkv_agent.so
view.refresh_interval_ms=30000
```
 ```
view.provider=m800
view.role=worker
view.master_addr=10.0.0.1:9100  # worker必填
view.refresh_interval_ms=30000
 ```
### 3.4 快照与transport
- `snapShot_`: `map<KvNsId, shared_ptr<ViewSnapshot>>`, per-ns原子替换；在途IO持`shared_ptr`拷贝保活旧快照
- `transportPool`: `unordered_map<NodeId, shared_ptr<AsuTransport>>`(mutex保护)，全节点transport的唯一持有方 
  - ViewSnapshot.transports 语义从“独占持有”改为“引用池中shared_ptr
  - BuildSnapShot通过GetOrCreateTransport（NodeId）服用或建连后入池
  - 同一NodeId跨ns只有一份连接，连接总数=去重NodeId数
  - 映射关系：`NodeId->transportPool_[NodeId](唯一持有)->被N个ns快照引用;引用计数=1(池)+引用该NodeId的ns快照数.物理连接数=去重NodeId数,而非各ns ASU数之和.
  - 某ns快照替换/删除只是放其持有的引用;只要还有其他ns引用,连接不关闭.引用计数降成1(仅池本身)时入`retiredTransports_`
-  被新快照淘汰且不被任何快照引用的`transport`->`retiredTransport_`,`Shutdown`时`Drain`后统一关闭
### 3.5 二次哈希故障转移（默认行为）
- `RingHashRouter`在`ring_`(主环Crc32IEEE)之外新增`secondaryRing_`(次环 FNV-1a) + `secondaryHash`,`Build`时同时建环
- `BuildTransportTasks`路由前遍历`snapshot->transports`调`CheckHealth()`收集`unavailableNodes`,再走双哈希路由.
- 语义:主归属不可用->次哈希找次归属->次也不可用则该entry失败(不三次充实,不回退主);主恢复后下一批IO自动回归一次哈希,不回迁已写入次归属的孤儿数据
### 3.6 ASU增减导致重打散的读写语义
- 环节点变更后,已有key的主归属可能改变
- 写总是发往新归属
- 读发往新归属;若该key的数据仍在旧归属节点上,不保证读成功,会返回错误
- 数据一致性由上层TTL/GC或重写收敛,KVClient不做数据迁移
## 四、设计约束
- `ViewServer::Getallviews`必须带默认实现,旧派生类不破.
- 旧配置路径(view.config_path/viewServiceAddrs)行为完全不变;view.provider未设置时走旧路径
- 视图拉取在后台线程,不阻塞前台IO
- 控制集群失联时保留旧`ViewUniverse`,不得清空任何ns的asuMap.
- Ceph agent通信使用keyring认证(view.ceph.client_name/view.ceph.keyring).
- IO命令已带nsId(前提,不改造)
- 旧快照由`shared_ptr`保活;旧transport由retiredTransport_延迟回收
- PublishAll全量对齐;ns增建环,ns删销毁环;空map视为拉取失败不删除.
- ASU增减后写成功,读不保证;不做数据迁移.
- DHT更新期间及BuildSnapshot失败时,新IO继续走旧快照,不中断
- 角色由上层在Init时传入(设计前提);仅Master dlopen agent, Worker不加载agent
- Master视图服务用TCP + 极简HTTP/1.1,不引入新第三方依赖;200 全量/304 未变更
- Worker在Master不可达时保留旧视图继续服务,恢复后下周期自动同步;Master侧viewEpoch必须单调不减

