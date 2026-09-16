# go-dirtmq
go-DirtMQ is a lightweigth message broker written entirely in *pure Go*.

## Custom Binary Protocol Layout

The broker implements a custom, *self-design binary framing protocol* over TCP. Structure of every packet is very simple:
| Version | Operation | TopicLength | PayloadLength | Topic | Payload |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 1 byte | 1 byte | 2 bytes | 4 bytes | N bytes | M bytes |

* ***Version*** (1 byte) - Identifies the binary protocol version signature used by the packet frame to guarantee backward compatibility.
* ***Operation*** (1 byte) - Defines the operation type token of the inbound/outbound packet (e.g. OpPublish, OpSubscribe, OpSend)

> **Data-Agnostic Design:** Because the protocol transmits raw binary memory streams (`[]byte`), the core engine is completely data-agnostic. It can route any serialized data format, including plain text, heavy JSON structures, Protocol Buffers (Protobuf), or raw media asset data blocks.

---

## Concurrency & Memory Core Optimizations

The system heavily utilizes Go's native synchronization primitives to guarantee processing speed and absolute memory safety under high multi-threaded load:

*   **Insulated Concurrency Barriers:** Employs explicit read/write mutex locks (`sync.RWMutex`) to guarantee thread-safe operations over the internal in-memory maps cache across thousands of parallel connections.
*   **Zero-Allocation Buffer Recycling (`sync.Pool`):** Integrates a global `sync.Pool` caching mechanism to reuse byte slices. This completely eliminates dynamic heap allocations during packet processing, protecting the server from Garbage Collector latency pauses.

---

## Local Cluster Orchestration

To compile the binaries and deploy the entire multi-node architecture (the broker alongside multiple automated subscriber instances) inside an isolated virtual network grid on your machine, simply execute:

```powershell
docker compose up --build
```

---

## 🗺️ Engineering Roadmap (Future Milestones)

*   [ ] Implement a Write-Ahead Log (WAL) for persistent disk storage.
*   [ ] Integrate a Graceful Shutdown channel loop to flush active socket connections safely.
*   [ ] Run execution profiling metrics to continuously optimize runtime memory usage and processing speed.
*   [ ] Develop an internal asynchronous packet buffer queue pool.