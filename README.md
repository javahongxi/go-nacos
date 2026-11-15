# go-nacos

Nacos 的 Go 原生服务端实现，与官方 Nacos 2.x/3.x gRPC SDK 线级兼容，用于学习研究。

- 命名服务：临时实例内存态，活性 = 连接
- 配置中心：hashicorp/raft + bbolt 嵌入式存储
- 管理台：独立 HTTP 端口（默认 8080，对齐 Nacos 3.x），配置/服务/连接/raft 一览 + 配置发布/编辑/删除、实例下线、删空服务
- 测试客户端：[sweet](https://github.com/javahongxi/sweet)（手写 Nacos 协议客户端）

设计文档见 [doc/design.md](doc/design.md)。

## 运行

端口对齐 Java Nacos（主端口 8848 偏移）：gRPC 9848（+1000，SDK 与节点间内部 RPC 复用）、raft 7848（-1000）；控制台 8080（Nacos 3.x）。

```sh
./run.sh              # 单机
./run.sh cluster      # 本机 3 节点集群，每节点全部端口 +10
./run.sh cluster 5    # 5 节点
```

或手工启动：

```sh
go run ./cmd/gonacos -data-dir ./data
```

管理台：`http://127.0.0.1:8080`（`-admin-listen off` 可禁用；暂不鉴权，默认仅回环监听）。

## 测试

```sh
go test ./e2e -v
```

e2e 用 sweet 客户端驱动：config 生命周期 / CAS 冲突 / 重启持久化 / naming 订阅推送 / 断线重放 / 管理台读写。