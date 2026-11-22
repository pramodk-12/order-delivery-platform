# How to explain this project in interviews

This guide grows with every phase. You don't need to memorise APIs. Interviewers are checking that you understand **why** reactive exists, **when** to use it, and **what you would do** in a real system.

---

## 1. A formula for explaining any concept

Answer in four short steps, 30–45 seconds in total:

1. **Problem:** what goes wrong without it.
2. **Idea:** the core idea, in plain words.
3. **In my project:** where you used it. This is what makes the answer credible.
4. **Trade-off or gotcha:** when not to use it, or a mistake to avoid. This is what makes you sound senior.

**Example: "Why reactive / WebFlux?"**
> **Problem:** In Spring MVC each request holds a thread until it finishes, even while it's only waiting for the database. With 200 threads and slow downstream calls, request 201 waits in a queue while most threads sit idle.
> **Idea:** WebFlux doesn't hold a thread while waiting. A request starts on one of a few Netty event-loop threads, the I/O is started, and the thread moves on to other requests. When the data arrives, a callback continues the work.
> **In my project:** The product service streams the catalog and pushes live prices over SSE. Thousands of open connections cost a handful of threads.
> **Trade-off:** It's harder to debug, and every library in the chain must be non-blocking. One JDBC call on the event loop stalls everything. For CPU-heavy work or a blocking-only stack I'd stay on MVC, possibly with virtual threads.

---

## 2. The core concepts: what to say

You've done Exercises 1–5, which cover everything in this section.

**Mono and Flux** *(Ex01)*
> "They're Reactive Streams publishers: `Mono` for 0 or 1 item, `Flux` for 0..N. They're lazy. Building the chain only describes the work, and nothing runs until someone subscribes. In WebFlux the framework subscribes when it writes the response."
> *Gotcha:* `Mono.just(callDb())` calls the DB immediately, at assembly time. Use `Mono.fromCallable` or `Mono.defer` when the work must be lazy (for retries, for example).

**map vs flatMap** *(Ex02)*
> "`map` is a synchronous 1-to-1 transform, like converting an entity to a DTO. `flatMap` is for when the transform is itself asynchronous and returns a `Mono` or `Flux`, like calling another service per item. `flatMap` runs those calls concurrently, so results come back in completion order; `concatMap` runs them one at a time and keeps the order."
> *In my project:* `ProductService.update` uses `findAndModify` → `map(ProductResponse::from)`.

**Calling things in parallel** *(Ex03)*
> "`Mono.zip(a, b, c)` subscribes to all of them at once and combines the results, so total latency is the slowest call, not the sum."
> *In my project:* pagination runs the page query and the count query in parallel with `Mono.zip`.
> *Gotcha:* if any source is empty, zip completes empty, so give optional sources a `defaultIfEmpty`.

**Errors, retries, timeouts** *(Ex04)*
> "An error is a terminal signal that travels down the chain. I handle it by type: `onErrorResume` for a fallback, `onErrorMap` to translate technical errors into domain errors, `retryWhen(Retry.backoff(...))` for transient failures, and `timeout` so a slow dependency can't hold the request forever."
> *In my project:* `DuplicateKeyException` → `DuplicateSkuException` → 409. The change stream reconnects with `retryWhen(backoff)`.

**Threads and blocking code** *(Ex05)*
> "Reactor uses whatever thread you subscribe on unless you say otherwise. `subscribeOn` moves where the source runs; `publishOn` switches the thread for everything below it. Blocking calls such as JDBC or a legacy SDK go in `Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())` so they never block the event loop."
> *In my project:* nothing in the product service blocks. Phase 7 (DB2) is where this matters.

**Backpressure** *(Ex06; you can skip the exercise for now)*
> "The consumer tells the producer how much it can take (`request(n)`), so a fast producer can't flood a slow consumer."
> *In my project:* NDJSON streaming. A slow client's TCP connection slows the MongoDB cursor down.

---

## 3. Explaining the system design

### The 30-second pitch
> "It's an order and delivery platform built as microservices to practise reactive programming. Customers browse a catalog, place orders, pay, and track delivery live. I used Spring WebFlux for the I/O-heavy services and kept the payment service on Spring MVC on purpose, so I could compare the two. Services talk over REST for queries and Kafka for events, with a saga for the order-payment transaction. Each service owns its database: MongoDB for the catalog, MySQL with R2DBC for inventory and orders, PostgreSQL as a CQRS read model, and IBM DB2 for legacy customer data."

### The 2-minute walkthrough: follow one order
Use the [target architecture](../architecture/target-architecture.svg) and walk through **one order** from start to finish. A single concrete flow is much easier to follow than a list of boxes.

1. The client calls the **API Gateway**, which checks the JWT and rate limit, then routes the request. *(Phase 8)*
2. The customer browses the **Product Service**: reactive MongoDB, NDJSON streaming, and live prices over SSE. ✅ *built*
3. The customer places an order. The **Order Service** checks product, stock and customer **in parallel** with `WebClient` + `Mono.zip`, then asks the **Inventory Service** to reserve the stock. That's one all-or-nothing reactive transaction, safe under concurrency and idempotent by order ID (inventory ✅ *built*). It returns **202 Accepted** at once. *(Phase 4)*
4. It publishes `OrderCreated` to **Kafka**. The **Payment Service** charges the customer and publishes the result. On failure, the order service **compensates** by releasing the stock (a saga). *(Phase 5)*
5. Status events fan out to the **Notification** service (WebSocket), **Tracking** (SSE) and **Order Query** (a CQRS read model in PostgreSQL). *(Phase 6)*

### Design decisions (this table grows each phase)

| Decision | Why | Trade-off you should mention |
|---|---|---|
| Microservices, database per service | Independent deploys and scaling; each service picks the right store | Cross-service consistency needs events and sagas, not ACID transactions |
| WebFlux for I/O-bound services | Many concurrent requests and long-lived streams on few threads | Harder debugging; the whole stack must be non-blocking |
| MVC for the payment service | Comparison point; many real payment SDKs are blocking | Thread-per-request limits concurrency |
| MongoDB for the catalog *(Phase 2)* | Read-heavy, whole-document reads, flexible attributes | No joins; relationships have to be modelled carefully |
| NDJSON streaming *(Phase 2)* | Fast first byte, flat memory, end-to-end backpressure | Clients must parse line by line; it isn't a single JSON document |
| SSE from a change stream *(Phase 2)* | Push without polling; plain HTTP; automatic reconnect | One direction only; needs a replica set; heartbeats needed behind proxies |
| Atomic `findAndModify` *(Phase 2)* | No lost updates, one round trip | Business rules that need the old value must be written as conditional updates |
| R2DBC for relational data *(Phase 3)* | Non-blocking SQL, so threads never wait on MySQL | No JPA: no lazy loading, relationships or dirty checking; more hand-written SQL |
| Atomic conditional UPDATE against overselling *(Phase 3)* | Correct under any contention, no retries. Measured: 100 buyers / 50 units → exactly 50 sold, 0 retries | The rule has to fit in a WHERE clause; complex checks need pessimistic locking |
| Optimistic locking kept as a comparison *(Phase 3)* | No locks held; ideal when conflicts are rare | Under contention: 560–610 retries and 2–4× slower (measured over 5 runs) |
| Idempotency key (orderId + unique constraint) *(Phase 3)* | Safe client retries and Kafka redeliveries | Clients must send a stable ID; it's stored per order |
| Kafka for events *(Phase 5)* | Decoupling, retries, replay | Eventual consistency, duplicates, ordering only within a partition |
| CQRS read model in PostgreSQL *(Phase 6)* | Fast denormalised reads for order history | Reads lag writes slightly (eventual consistency) |

### Answering "why didn't you use X?"
Use this pattern: **"X would work. The trade-off is A. I chose Y because B. If C changed, I'd switch to X."**
> e.g. "WebSocket would work for live prices. The trade-off is extra complexity for two-way traffic we don't need. SSE is plain HTTP and reconnects by itself. If clients needed to send messages back, I'd use WebSocket, which is the plan for notifications."

---

## 4. Exercises 6–10: where they come back
You can leave these for now. Each one becomes much easier once you've seen it in a real service.

| Exercise | Comes back in | What you'll see |
|---|---|---|
| Ex06 Backpressure | Phase 2 ✅ (NDJSON), Phase 6 (Kafka consumer) | `limitRate`, concurrency limits on a real message stream |
| Ex07 Hot vs cold | Phase 2 ✅ (`share()` on the change stream), Phase 6 | `Sinks` for WebSocket fan-out |
| Ex08 Context | Phase 4 | Correlation ID passed through `WebClient` calls |
| Ex09 Testing | Every phase ✅ | `StepVerifier` on SSE in the Phase 2 integration test |
| Ex10 Blocking | Phase 7 | DB2 over JDBC on `boundedElastic`; BlockHound |

## Concepts you've now used in real code

| Concept | Where |
|---|---|
| `concatMap` vs `flatMap` (Ex02) | Phase 3: `concatMap` inside a transaction (one connection); `flatMap` to fire 100 concurrent buyers in the test |
| `retryWhen(Retry.backoff…)` with `filter` (Ex04) | Phase 3: retry only transient failures, each retry a fresh transaction |
| Reactor Context (Ex08) | Phase 3: reactive transactions are stored in the Context, not a ThreadLocal |
| Hot publisher with `share()` (Ex07) | Phase 2: one MongoDB change stream shared by every SSE client |

## Per-phase talking points
- [Phase 2: Product Service](../../product-service/README.md#how-to-explain-it-in-an-interview)
- [Phase 3: Inventory Service](../../inventory-service/README.md#how-to-explain-it-in-an-interview)
