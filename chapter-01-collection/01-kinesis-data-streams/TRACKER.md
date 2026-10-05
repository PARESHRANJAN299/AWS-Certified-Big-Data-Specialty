# Kinesis Data Streams: learning tracker

> AWS Certified Big Data - Specialty · Chapter 1: Collection · Topic 1 · The roadmap for mastering Kinesis Data Streams, one concept at a time.

🟩⬜⬜⬜⬜⬜⬜⬜⬜⬜🟨⬜⬜⬜⬜⬜⬜⬜⬜⬜

🟩 complete · 🟨 in progress · ⬜ pending

**7 concepts understood · 1 of 20 roadmap topics complete · 1 in progress · 5%**

## Concepts understood so far

Each concept below is marked complete once it is clearly understood. They feed the roadmap topics further down.

### ✅ 1. Basic flow

```text
Producer -- PUT --> Kinesis <-- GET -- Consumer
```

- `PUT` = the Producer writes / sends data into Kinesis
- `GET` = the Consumer reads data from Kinesis
- Kinesis sits in the middle
- The Consumer initiates the read

### ✅ 2. Kinesis vs direct database write

```text
Application --> MongoDB
```

This is completely possible. Kinesis is not mandatory just because data arrives continuously.

### ✅ 3. Transaction database vs event stream

```text
MongoDB = operational / current state
Kinesis = event flow
```

Example:

```text
MongoDB:  current playback position = 32:15

Kinesis:  PLAY
          PAUSE
          SEEK
          BUFFER_START
          BUFFER_END
```

### ✅ 4. Why not just use MongoDB?

MongoDB can:

- handle multiple readers
- serve multiple systems
- scale
- store event data
- support querying

Kinesis becomes more attractive when:

- event volume becomes very large
- we want to reduce read/write pressure on the transactional database
- we want a dedicated streaming layer
- multiple systems need to process events continuously

### ✅ 5. Independent processing

Different readers can process the same stream at different speeds, and each moves on its own.

```text
Reader A -> event 1000
Reader B -> event 850
Reader C -> event 700
```

### ✅ 6. Retention

Retention = how long Kinesis keeps the events available.

```text
after retention expires:  event -> no longer available in Kinesis
```

### ✅ 7. Replay

Replay = reading retained events again. The movie analogy:

```text
Already watched to 15 min.

Continue:  15 min -> onward
Replay:     0 min -> onward again
```

- Replay is not a separate ON/OFF setting
- Replay depends on the retained data still being available
- Reading does not delete the event

## Roadmap progress

- [x] 1. Basic Flow
- [ ] 2. Core Terminology
- [ ] 3. Record Structure
- [ ] 4. Partitioning
- [ ] 5. Shards
- [ ] 6. Ordering
- [ ] 7. Producer Side
- [ ] 8. Consumer Side
- [ ] 9. Consumer Types
- [ ] 10. Kinesis + Lambda
- [ ] 11. Retention and Replay (in progress: retention and replay understood; the retention period setting and recovering after a consumer failure are still to do)
- [ ] 12. Scaling Modes
- [ ] 13. Resharding
- [ ] 14. Throughput and Limits
- [ ] 15. Error Handling
- [ ] 16. Monitoring
- [ ] 17. Security
- [ ] 18. Architecture Patterns
- [ ] 19. Service Comparisons
- [ ] 20. Hands-on Build

## Topic details

| # | Topic | What you will master | Status |
| :-: | --- | --- | :-: |
| 1 | **Basic Flow** | What PUT means · What GET means · Who initiates PUT · Who initiates GET · Kinesis sits in the middle · Kinesis does not normally push directly to the consumer | ✅ Completed |
| 2 | **Core Terminology** | Producer · Record · Stream · Shard · Partition Key · Sequence Number · Consumer (one term at a time) | ⏳ Pending |
| 3 | **Record Structure** | What one Kinesis record contains: Data · Partition key · Sequence number · Approximate arrival timestamp | ⏳ Pending |
| 4 | **Partitioning** | How partition keys work · How Kinesis chooses a shard · Why the same partition key matters · Good vs bad partition keys · Hot partition / hot shard | ⏳ Pending |
| 5 | **Shards** | What a shard is · Why shards exist · Read capacity · Write capacity · Parallelism · Scaling shards | ⏳ Pending |
| 6 | **Ordering** | Ordering inside one shard · Why ordering is not global · How the partition key affects ordering | ⏳ Pending |
| 7 | **Producer Side** | PutRecord · PutRecords · Single vs batch writes · Producer retries · Producer failures | ⏳ Pending |
| 8 | **Consumer Side** | GetRecords · Shard iterator · Polling · Checkpointing · Consumer lag | ⏳ Pending |
| 9 | **Consumer Types** | Standard consumers vs enhanced fan-out consumers | ⏳ Pending |
| 10 | **Kinesis + Lambda** | Lambda event source mapping · Polling · Batch size · Retry · Failure handling · Partial batch failure | ⏳ Pending |
| 11 | **Retention and Replay** | Why Kinesis stores records temporarily · Retention period · Replay · Recovering after consumer failure | 🔨 In progress |
| 12 | **Scaling Modes** | Provisioned mode · On-demand mode · When to use each | ⏳ Pending |
| 13 | **Resharding** | Split shard · Merge shard · Scaling up · Scaling down | ⏳ Pending |
| 14 | **Throughput and Limits** | Write throughput · Read throughput · Records per second · MB/s · What happens when limits are exceeded | ⏳ Pending |
| 15 | **Error Handling** | ProvisionedThroughputExceededException · Retry · Backoff · Duplicate processing · Idempotency | ⏳ Pending |
| 16 | **Monitoring** | CloudWatch metrics: IncomingRecords · IncomingBytes · GetRecords.IteratorAgeMilliseconds · ReadProvisionedThroughputExceeded · WriteProvisionedThroughputExceeded | ⏳ Pending |
| 17 | **Security** | IAM permissions · Encryption · KMS · VPC endpoints · Least privilege | ⏳ Pending |
| 18 | **Architecture Patterns** | Application events · Clickstream · Logs · Fraud detection · IoT · Real-time analytics · Security monitoring | ⏳ Pending |
| 19 | **Service Comparisons** | Kinesis Data Streams vs Firehose · Kinesis vs SQS · Kinesis vs SNS · Kinesis vs Kafka / MSK | ⏳ Pending |
| 20 | **Hands-on Build** | Python producer and consumer on a Kinesis data stream, then extended with Lambda, an analytics consumer and an S3 path | ⏳ Pending |

## Hands-on build target (topic 20)

First a simple Python producer and consumer:

```text
Python Producer
      |
      | PUT
      v
Kinesis Data Stream
      ^
      | GET
      |
Python Consumer
```

Then extended with more consumers and a path to S3:

```text
Producer
   |
   v
Kinesis
   |
   +--> Lambda
   |
   +--> Analytics Consumer
   |
   +--> S3 path
```

## How this notebook is built

- One concept at a time. A topic is marked complete only when its section is written and understood.
- Core Terminology (topic 2) goes in this order: Producer → Record → Stream → Shard → Partition Key → Sequence Number → Consumer.
- The [Kinesis Data Streams README](README.md) holds the written explanations with animated diagrams. Concepts 2 to 7 above are understood but not yet written up there.
