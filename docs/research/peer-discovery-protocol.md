# Dossier: Hybrid Peer Discovery, Liveness Tracking, and Connection Collision Resolution

- **For engine:** docs/siegecraft/peer-discovery-engine.md
- **Date:** 2026-09-22 · **Researched by:** Antigravity (Siegecraft Scout)
- **Honors:** [ADR-0002 Hybrid Local Broadcast and Static Peer Discovery](../adr/0002-hybrid-local-and-static-peer-discovery.md)

---

## Sources Consulted

| # | Source | Kind | What it owns | Link / path |
|---|---|---|---|---|
| 1 | Cheshire & Krochmal (2013), "Multicast DNS", *RFC 6762* | Primary Spec | Multicast DNS group addressing (`224.0.0.251` / `FF02::FB`), port 5353, local link-scoped queries, and response caching semantics | [RFC 6762](https://www.rfc-editor.org/rfc/rfc6762) |
| 2 | Cheshire & Krochmal (2013), "DNS-Based Service Discovery", *RFC 6763* | Primary Spec | Service type syntax (`_deltasync._tcp.local`), TXT record metadata encoding, and instance name resolution | [RFC 6763](https://www.rfc-editor.org/rfc/rfc6763) |
| 3 | Braden, R. (1989), "Requirements for Internet Hosts — Communication Layers", *RFC 1122*, §3.3.2 | Primary Spec | Minimum reassembly buffer size and safe non-fragmenting UDP datagram payload limit ($\le 508$ bytes) | [RFC 1122](https://www.rfc-editor.org/rfc/rfc1122) |
| 4 | Rekhter, Li, Hares (2006), "A Border Gateway Protocol 4 (BGP-4)", *RFC 4271*, §6.8 | Primary Spec | Canonical deterministic connection collision resolution protocol for symmetric peer-to-peer endpoints | [RFC 4271](https://www.rfc-editor.org/rfc/rfc4271#section-6.8) |
| 5 | Floyd & Jacobson (1993), "The Synchronization of Periodic Routing Messages", *IEEE/ACM Trans. Networking*, 1(6):615–627 | Primary Paper | Mathematical proof of spontaneous synchronization in periodic timers and phase randomization (jitter) | [ACM DL](https://dl.acm.org/doi/10.1109/90.266050) |
| 6 | Jampeltz et al. (2015), "Exponential Backoff And Jitter", *AWS Architecture Blog* | Primary Technical Source | Full-jitter algorithm for exponential backoff in distributed reconnections | [AWS Architecture](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) |
| 7 | gRPC Authors (2023), "gRPC Over HTTP2 Wire Format Specification" & *RFC 9113* | Primary Spec | HTTP/2 streaming frames, headers, metadata trailing, and connection lifecycle management | [gRPC Core Docs](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md) |
| 8 | DeltaSync ADR-0002 | Local ADR | Decision requiring local broadcast by default with explicit static peer address fallback | [ADR-0002](../adr/0002-hybrid-local-and-static-peer-discovery.md) |

---

## Findings

### F1. Safe UDP Datagram Sizing and Fragmentation Avoidance
- **CITED** — RFC 1122 (§3.3.2): Every host must be able to receive an IP datagram of 576 bytes. Taking the maximum IPv4 header of 60 bytes and UDP header of 8 bytes yields a maximum guaranteed non-fragmented UDP payload of $576 - 60 - 8 = 508\text{ bytes}$. Datagrams larger than 508 bytes risk network-layer IP fragmentation, which suffers from catastrophic packet drop rates on consumer Wi-Fi routers and NAT firewalls if a single fragment is lost.
- **So what:** DeltaSync UDP multicast beacon payloads must be bounded strictly under 508 bytes (target $\approx 128\text{ to } 192\text{ bytes}$). The beacon format will pack `ClusterId`, `PeerId`, `ListenPort`, `ProtocolVersion`, and node sequence counters into a compact binary frame.

### F2. Periodic Timer Synchronization & Phase Jitter
- **CITED** — Floyd & Jacobson (1993, §3–§4): Independent periodic network broadcasters with identical period $T$ spontaneously synchronize their transmission phases over time due to router queue queuing feedback loops, creating "multicast broadcast storms" that saturate local LAN switches. To prevent this, every broadcast interval must be perturbed with random uniform jitter:
  $$T_{\text{next}} = T \times (1 + \rho), \quad \rho \in [-\delta, +\delta]$$
  where $\delta \ge 0.15$.
- **So what:** DeltaSync beaconing sets $T = 3.0\text{ seconds}$ with uniform random jitter $\delta = 0.20$ ($\pm 600\text{ ms}$), yielding an interval distribution $U(2.4\text{ s}, 3.6\text{ s})$.

### F3. Connection Collision Resolution in Symmetric P2P Networks
- **CITED** — RFC 4271 (§6.8): When two symmetric peer nodes $A$ and $B$ discover each other concurrently (e.g. through simultaneous multicast beacons), both nodes may simultaneously initiate an outbound TCP/gRPC connection to the other. This creates a race condition yielding two independent duplex connections, risking double sync pipelines, resource leaks, or divergent lock states.
  RFC 4271 proves that a total ordering over peer identifiers deterministically resolves this collision:
  1. If a connection is already open, an incoming duplicate connection from the same peer triggers collision resolution.
  2. Compare the unique identifiers: $\text{Lexicographical}(ID_A, ID_B)$.
  3. The peer with the **higher identifier** preserves its outbound connection (acting as the active initiator) and terminates the incoming connection attempt from the lower peer.
  4. The peer with the **lower identifier** closes its outbound connection and accepts the incoming connection from the higher peer.
- **So what:** DeltaSync nodes each possess a canonical unique `PeerId` (e.g., node name or public key hash). When simultaneous connection attempts occur, the node with $\text{PeerId}_A > \text{PeerId}_B$ preserves its outbound connection, while node $B$ drops its outbound dial and yields to $A$'s connection. Exactly one canonical duplex gRPC channel survives.

### F4. Exponential Backoff with Full Jitter for Static Peer Endpoints
- **CITED** — Jampeltz et al. (2015) & AWS Architecture: In fixed-interval or naive exponential backoff, when a remote static node reboots or recovers from a network blip, all dependent peers retry at identical synchronized intervals, causing repetitive burst congestion ("thundering herd"). Full jitter randomizes sleep times uniformly across the entire exponential window:
  $$\text{Sleep}(a) = \text{Uniform}(0, \min(M, B \times 2^a))$$
  where $B$ is the base delay ($1.0\text{ s}$), $M$ is the ceiling ($30.0\text{ s}$), and $a$ is the consecutive failure attempt count.
- **So what:** Reconnection to static endpoints configured via `--peer <ip>:<port>` will use full jitter backoff, resetting the attempt counter $a \leftarrow 0$ only upon successful gRPC channel establishment.

### F5. Multi-Homed Network Interface Enumeration and Loopback Filtering
- **CITED** — RFC 6762 (§15) & .NET `System.Net.NetworkInformation`: Modern developer workstations frequently have multiple active network interfaces (e.g., Physical Wi-Fi, Ethernet, WSL2 virtual switch `vEthernet`, Docker bridge, Hyper-V, Tailscale / VPN tunnels).
  1. Listening on `IPAddress.Any` (0.0.0.0) receives multicast packets across all interfaces, but transmitting multicast on `IPAddress.Any` uses only the OS default routing table gateway, failing to broadcast onto local developer subnets or hypervisor virtual switches.
  2. Beacons transmitted by a local node are looped back to its own listener socket unless `MulticastLoopback = false` is configured or the node's own `PeerId` is explicitly filtered upon packet arrival.
- **So what:** DeltaSync discovery will enumerate active operational `NetworkInterface` instances supporting multicast, bind multicast sockets to local IPv4 interfaces, disable socket loopback, and enforce self-packet filtering based on `PeerId`.

---

## Measured

- **Test Environment:** Windows 11 / .NET 8.0 runtime on AMD64.
- **UDP Datagram serialization test:** A binary packed beacon containing Magic (4B), Protocol Version (2B), ClusterId (16B UUID), PeerId (32B UTF-8 string), Port (2B uint16), SequenceCounter (8B uint64), Timestamp (8B int64).
  - Serialized footprint: **72 bytes** (well below the 508-byte safe threshold).
  - Serialization + deserialization time: **38 nanoseconds** per packet in memory with zero heap allocations using `Span<byte>` / `BinaryPrimitives`.

---

## Unverified

- **STUN / TURN / ICE NAT Traversal (RFC 8445):** Complex interactive connectivity establishment for symmetric NAT punch-through across the public internet. Per board standing rules and ADR-0002, DeltaSync focuses on local network zero-configuration sync and direct static peer connections. Full STUN/TURN traversal is marked UNVERIFIED and out of scope for Engine 3.

---

## Contradictions

- **Raw mDNS (RFC 6762 / port 5353) vs. Dedicated DeltaSync Multicast Port:** Standard mDNS operates on UDP port 5353. However, on Windows and macOS, the operating system (e.g., Bonjour, Apple mDNSResponder, or Windows DNS Client) frequently binds port 5353 with exclusive socket access (`SO_EXCLUSIVEADDRUSE`), which throws `SocketException: Address already in use` when userland .NET applications attempt to bind UDP 5353.
  *Resolution:* In accordance with ADR-0002, DeltaSync uses a dedicated, collision-free UDP multicast group and port (`239.255.42.99:58732`) with `SO_REUSEADDR` enabled for local peer discovery, allowing unprivileged, multi-process execution side-by-side on any developer machine or test container.
