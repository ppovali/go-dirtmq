# go-dirtmq
go-DirtMQ is a lightweight message broker written entirely in *pure Go*.

## Custom Binary Protocol Layout

The broker implements a custom, *self-design binary framing protocol* over TCP. Structure of every packet is very simple:
| Version | Operation | TopicLength | PayloadLength | Topic | Payload |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 1 byte | 1 byte | 2 bytes | 4 bytes | N bytes | M bytes |

* ***Version*** (1 byte) - Identifies the binary protocol version signature used by the packet frame to guarantee backward compatibility.
* ***Operation*** (1 byte) - Defines the operation type token of the inbound/outbound packet (e.g. OpPublish, OpSubscribe, OpSend)

> **Data-Agnostic Design:** Because the protocol transmits raw binary memory streams (`[]byte`), the core engine is completely data-agnostic. It can route any serialized data format, including plain text, heavy JSON structures, Protocol Buffers (Protobuf), or raw media asset data blocks.

### Benchmarks

Benchmarks were run on a local development machine using Go benchmarks.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| SerializePacket | 27.3 | 24 | 1 |
| DecodePacket | 117.8 | 216 | 6 |

The benchmarks measure protocol serialization and deserialization for a single test packet and should not be interpreted as end-to-end broker throughput.

---

## Concurrency & Memory Core Optimizations

The system heavily utilizes Go's native synchronization primitives to guarantee processing speed and absolute memory safety under high multi-threaded load:

*   Employs explicit read/write mutex locks (`sync.RWMutex`) to guarantee thread-safe operations over the internal in-memory maps cache across thousands of parallel connections.
*   Uses `sync.Pool` to reduce temporary buffer allocations during packet processing.

---

## Local Cluster Orchestration

To compile the binaries and deploy the entire multi-node architecture (the broker alongside multiple automated subscriber instances) inside an isolated virtual network grid on your machine, simply execute:

```powershell
docker compose up --build
```

---

## Write-Ahead Log (WAL)

To prevent data loss from system crashes or restarts, `go-DirtMQ` implements an append-only **Write-Ahead Log (WAL)**. Hard disk storage is treated as the primary source of truth, while RAM acts as a fast lookup cache layer.

#### The Persistence Pipeline

When a message is published, the broker processes data in a durable, highly optimized sequence **before** updating memory or broadcasting to subscribers:

1. **Serialization:** Packs data into a strict binary network frame (`protocol.Packet`).
2. **Buffered Memory Write:** Appends raw binary bytes into an in-memory `bufio.Writer` buffer (configured at 64 KB). This step executes at RAM speeds, preventing the broker from being choked by individual disk I/O operations.
3. **Asynchronous Group Commit:** A dedicated background worker pulls accumulated data from the buffer every **2 milliseconds** (or sooner if the buffer fills up) and triggers a synchronized two-stage hardware flush:
   * `writer.Flush()` pushes data from Go user space to the OS Page Cache.
   * `file.Sync()` executes an explicit hardware `fsync` call to bypass all volatile caches and commit data directly to physical storage.
4. **Memory Update:** Once the disk write is confirmed, the broker updates its in-memory map cache safely under mutual exclusion (`sync.Mutex`).

#### State Recovery on Startup

When the broker reboots, it follows a strict sequence to automatically restore its state **before** opening network ports or accepting new connections:

1. **Exclusive Read Stream:** Opens a temporary read-only stream to parse `topics.log` from byte zero, completely isolated from the background sync worker.
2. **Sequential Decoding:** Decodes binary frames sequentially using the core network parser (`protocol.DecodePacket`) until it encounters an `io.EOF` signal.
3. **Cache Bootstrapping:** Loads recovered payloads directly into memory using a dedicated bootstrapper (`e.RestoreMessageCache`), rebuilding the exact in-memory map state without executing redundant disk operations.

---

## Engineering Insights: Optimizing WAL Performance

During the development of the Write-Ahead Log (WAL), a crucial architectural lesson was learned regarding disk I/O, performance optimization and benchmark calibration. 

### The Bottleneck: Individual Fsyncs
Originally, the WAL triggered an explicit hardware flush (`w.file.Sync()`) on *every single incoming message* to guarantee immediate durability. In benchmarks with a 4-byte payload, this achieved **~300,000 QPS**, but completely choked the hardware:
* The disk write throughput reached **~12 MB/s**.
* Because operating systems and physical drives write data in fixed block pages (usually 4 KB), writing small 4-byte messages sequentially caused massive **Write Amplification**. 
* The OS spent more CPU cycles on system call context switches (`syscall write`) and rewriting metadata than on handling message streams.
* Furthermore, the initial benchmark script operated **incorrectly**: it lacked adequate write-confirmation loops, caused severe *Thread Starvation*, and led to silent packet loss as overflowing OS network buffers forced the loopback pipeline to drop in-flight frames before they could reach the storage layer.

### The Solution: Lock-Free Pipeline & Smart Backpressure
By moving to a lock-free channel queue coupled with an adaptive micro-sleep retry backoff loop, the broker gained the ability to absorb massive packet floods in memory, processing incoming streams gracefully and maximizing core execution.

### High-Load Performance Benchmarks (Warmed Capacity)

To capture the definitive peak performance characteristics, the end-to-end stress test evaluated the system inside an isolated multi-stage Docker Compose network grid. The metrics below capture consistent **"warm runs"**:

| Metric | Micro-Packets (4 Bytes) | Heavy Data Streams (4 KB) |
| :--- | :---: | :---: |
| **Average Throughput (QPS)** | **~906,000 QPS** | **~54,500 QPS** |
| **Average Disk I/O Speed** | **~20.7 MB/s** | **~214.1 MB/s** |
| **Network Concurrency** | 100–500 Active Sockets | 40 Active Sockets |

*Benchmark Specifications: 4-byte streams evaluated at 100 concurrent threads with a 15μs adaptive connection sleep. 4KB payloads generated using cryptographically secure random bytes (`crypto/rand`) to eliminate OS compression cache falsification.*

---


## Engineering Roadmap (Future Milestones)

*   [x] Implement a Write-Ahead Log (WAL) for persistent disk storage.
*   [ ] Integrate a Graceful Shutdown channel loop to flush active socket connections safely.
*   [x] Develop an internal asynchronous packet buffer queue pool.