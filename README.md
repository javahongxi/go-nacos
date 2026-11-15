# go-nacos

Nacos 的 Go 原生服务端实现，与官方 Nacos 2.x/3.x gRPC SDK 线级兼容（不做 1.x HTTP），目标是打造生产可用的 Go 版 Nacos。

- 配置中心：hashicorp/raft + bbolt 嵌入式存储
- 命名服务：临时实例内存态，活性 = 连接
- 观测台：独立 HTTP 端口（默认 8848），配置/服务/连接/raft 一览 + 配置发布/编辑/删除、实例下线、删空服务
- 测试客户端：[sweet](https://github.com/javahongxi/sweet)（手写 Nacos 协议客户端）

设计文档见 [doc/design.md](doc/design.md)。

## 运行

```sh
go run ./cmd/gonacos -listen 127.0.0.1:9848 -raft-listen 127.0.0.1:9849 -data-dir ./data
```

观测台：http://127.0.0.1:8848（`-admin-listen off` 可禁用；暂不鉴权，默认仅回环监听）。

## 测试

```sh
go test ./e2e -v
```

e2e 用 sweet 客户端驱动：config 生命周期 / CAS 冲突 / 重启持久化 / naming 订阅推送 / 断线重放 / 观测台读写。