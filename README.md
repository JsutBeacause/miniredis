# 🚀 Mini-Redis

[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey.svg)]()
[![Performance](https://img.shields.io/badge/QPS-100K%2B%20(Single)%20%7C%20580K%2B%20(Pipelined)-brightgreen.svg)]()

基于 Go 语言标准库构建的高性能轻量级内存缓存服务，全面兼容 Redis 核心通信协议（RESP 及普通文本协议）。单机普通压测可达 **100,000+ QPS**，流水线批处理模式突破 **580,000+ QPS**，稳定承载 2000+ 并发长连接。

---

## ✨ 核心特性

- **纯 Go 标准库实现**：零第三方依赖，仅依赖 `net`、`sync`、`bufio`、`time` 等标准库模块。
- **Redis 协议全兼容 (RESP)**：
  - 完美支持标准 RESP 协议（RESP Array、Bulk String 等格式），可无缝对接官方 `redis-cli` 与各大主流语言的 Redis 客户端驱动。
  - 兼顾兼容 Inline 文本协议，支持使用 `nc`、`telnet` 或普通 Socket 直接交互。
- **双重过期淘汰策略**：
  - **惰性删除（Lazy Deletion）**：在访问（`GET` / `TTL`）时动态判断是否过期，过期则同步删除并返回空，保证数据一致性。
  - **主动定期清理（Active Daemon）**：后台常驻定时扫描协程（Ticker），周期性清理遗留的冷数据，防止内存泄漏。
- **高并发安全模型**：
  - 基于 **Goroutine-per-Connection** 经典并发架构，高效调度海量 TCP 连接。
  - 采用 `sync.RWMutex` 读写分离锁保障内存状态并发安全。
- **企业级优雅停机（Graceful Shutdown）**：
  - 捕获系统退出信号（`SIGINT`、`SIGTERM`）。
  - 利用 `context.Context` 广播停机通知，结合 `sync.WaitGroup` 等待现有连接业务处理完毕后安全关机。

---

## 🏗️ 架构设计

```text
[ Redis Client (redis-cli / nc / SDK) ]
                  │
                  ▼ (TCP :6380)
┌──────────────────────────────────────────────────────────┐
│                   Mini-Redis Server                      │
│                                                          │
│  [ Connection Pool / Goroutine per Connection ]          │
│                         │                                │
│                         ▼                                │
│  [ RESP / Inline Protocol Parser (bufio.Reader) ]        │
│                         │                                │
│                         ▼                                │
│  [ Command Dispatcher (PING / SET / GET / DEL / TTL) ]   │
│                         │                                │
│                         ▼                                │
│  ┌────────────────────────────────────────────────────┐  │
│  │            Memory Store (sync.RWMutex)             │  │
│  │   map[string]cacheItem { value, expiresAt }        │  │
│  └────────────────────────────────────────────────────┘  │
│                         ▲                                │
│                         │                                │
│  [ Background Cleanup Daemon (Ticker: 2s) ]              │
└──────────────────────────────────────────────────────────┘
```

---

## ⚡ 性能压测结果（Benchmark）

压测工具采用 Redis 官方基准测试套件 `redis-benchmark`，在本地环境进行多梯度并发与吞吐极限压测。

### 1. 标准请求模式 (Pipeline = 1)
模拟常规应用客户端“一问一答”模式，测试各并发连接下的吞吐量及响应延迟：

| 并发连接数 (`-c`) | 请求总量 (`-n`) | PING QPS | SET QPS | GET QPS | P50 延迟 |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **50** | 100,000 | 107,758 req/s | 106,837 req/s | 107,296 req/s | **0.25 ms** |
| **100** | 100,000 | 105,152 req/s | 90,415 req/s | 104,384 req/s | **0.48 ms** |
| **500** | 100,000 | 107,526 req/s | 106,837 req/s | 83,542 req/s | **2.36 ms** |
| **1,000** | 100,000 | 100,300 req/s | 95,328 req/s | 98,522 req/s | **5.01 ms** |
| **2,000** | 100,000 | 103,199 req/s | 99,304 req/s | 102,354 req/s | **10.26 ms** |

> 💡 **结论**：在 50 至 2000 个高并发客户端持续压测下，QPS 稳定维持在 **10 万+**，P50 延迟控制在极低水平，展现出优异的并发稳定性。

### 2. 流水线批处理模式 (Pipeline = 16)
模拟高性能客户端通过 Pipeline 批量发送指令的极致性能：

```bash
redis-benchmark -p 6380 -t ping,set,get -c 50 -n 100000 -q -P 16
```

- 🟢 **PING**：**543,478 req/s**（~54 万 QPS）
- 🟢 **SET**：**564,971 req/s**（~56 万 QPS）
- 🟢 **GET**：**584,795 req/s**（~58 万 QPS）
- ⏱️ **P50 延迟**：仅 **0.97 ms**

---

## 📋 支持的命令列表

| 命令 | 语法 | 描述 | 返回值示例 |
| :--- | :--- | :--- | :--- |
| **`PING`** | `PING [msg]` | 连通性测试 | `+PONG` 或 `$4\r\ntest\r\n` |
| **`SET`** | `SET key value [EX seconds]` | 存入键值，支持设置生存时间（秒） | `+OK` |
| **`GET`** | `GET key` | 获取键值（过期自动删除并返回空） | `$<len>\r\nvalue\r\n` 或 `$-1\r\n` |
| **`DEL`** | `DEL key [key ...]` | 批量删除键 | `:<count>\r\n`（删除成功的个数） |
| **`TTL`** | `TTL key` | 查看剩余存活秒数 | `:<seconds>`（-1: 永不过期, -2: 不存在/已过期） |

---

## 🚀 快速上手

### 1. 运行服务
克隆仓库后，直接通过 Go 命令启动：

```bash
git clone https://github.com/your-username/miniredis.git
cd miniredis
go run miniredis.go
```

控制台将输出启动信息：
```text
==================================================
   🚀 Mini-Redis 缓存服务已成功启动！
   监听地址: 127.0.0.1:6380
==================================================
```

### 2. 客户端连接体验

#### 选项 A：使用官方 `redis-cli`
```bash
redis-cli -p 6380

127.0.0.1:6380> PING
PONG
127.0.0.1:6380> SET username "derek" EX 10
OK
127.0.0.1:6380> TTL username
(integer) 8
127.0.0.1:6380> GET username
"derek"
127.0.0.1:6380> DEL username
(integer) 1
```

#### 选项 B：使用 `nc` / `telnet`（普通文本交互）
```bash
nc 127.0.0.1 6380
PING
+PONG
SET token abc123
+OK
GET token
$6
abc123
```

### 3. 本地复现基准压力测试
```bash
# 1. 基础 100 并发压测
redis-benchmark -p 6380 -t ping,set,get -c 100 -n 100000 -q -P 1

# 2. 高吞吐 Pipeline 压测
redis-benchmark -p 6380 -t ping,set,get -c 50 -n 100000 -q -P 16
```

---

## 📄 开源许可证

本项目采用 [MIT License](LICENSE) 授权许可。
