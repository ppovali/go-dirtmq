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

### The Persistence Pipeline
When a message is published, the broker processes data in a strict, durable sequence **before** updating memory or broadcasting to subscribers:
*   **Serialization:** Packs data into a strict binary network frame (`protocol.Packet`).
*   **Disk Commit:** Appends raw binary bytes to the end of a continuous log file (`topics.log`).
*   **Hardware Sync:** Invokes `w.file.Sync()` to bypass volatile OS caches and force an immediate physical hardware flash.
*   **Memory Update:** Updates the in-memory map cache safely once the disk write is confirmed.

### State Recovery on Startup
When the broker reboots, it automatically restores its state **before** opening network ports:
1.  Opens a temporary read-only stream to parse `topics.log` from byte zero.
2.  Decodes binary frames sequentially using the core network parser (`protocol.DecodePacket`) until it hits an `io.EOF` signal.
3.  Loads payloads directly into memory using a dedicated bootstrapper (`e.RestoreMessageCache`), rebuilding the exact in-memory map state without writing to the disk twice.

---

## Engineering Roadmap (Future Milestones)

*   [x] Implement a Write-Ahead Log (WAL) for persistent disk storage.
*   [ ] Integrate a Graceful Shutdown channel loop to flush active socket connections safely.
*   [x] Develop an internal asynchronous packet buffer queue pool.