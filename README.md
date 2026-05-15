# asyncnsq
[![Downloads](https://pepy.tech/badge/asyncnsq)](https://pepy.tech/project/asyncnsq)
[![PyPI version](https://badge.fury.io/py/asyncnsq.svg)](https://badge.fury.io/py/asyncnsq)
[![Python Verion](https://img.shields.io/pypi/pyversions/asyncnsq.svg?logo=python&logoColor=FBE072)](https://img.shields.io/pypi/pyversions/asyncnsq.svg?logo=python&logoColor=FBE072)
[![codecov](https://codecov.io/gh/aohan237/asyncnsq/branch/master/graph/badge.svg?token=Ezbgfka7p5)](https://codecov.io/gh/aohan237/asyncnsq)

## async nsq with asyncio

**if you dont like the pynsq(which use tornado) way to interact with nsq, then this library may be suitable for you**

you can use this library as the common way to write things

--------------

## User Documents
[Documents](https://aohan237.github.io/asyncnsq/)

## Install

This project uses `uv` for environment and dependency management. The uv index
is configured in `uv.toml` to use Tencent's PyPI mirror.

```bash
uv sync
```

## Usage examples

Consumer:

```python
import asyncio

from asyncnsq import create_reader


async def main():
    reader = await create_reader(
        nsqd_tcp_addresses=["127.0.0.1:4150"],
        max_in_flight=200,
    )
    await reader.subscribe("test_async_nsq", "nsq")
    try:
        async for message in reader.messages():
            print(message.body)
            await message.fin()
    finally:
        await reader.graceful_close()


asyncio.run(main())
```

Producer:

```python
import asyncio

from asyncnsq import create_writer


async def main():
    writer = await create_writer(
        host="127.0.0.1",
        port=4150,
        heartbeat_interval=30000,
        feature_negotiation=True,
        tls_v1=True,
        snappy=False,
        deflate=False,
        deflate_level=0,
    )
    for i in range(100):
        await writer.pub("test_async_nsq", f"test_async_nsq:{i}")
        await writer.dpub("test_async_nsq", i * 1000, f"delay:{i}")
    writer.close()


asyncio.run(main())
```

Requirements
------------

* Python 3.12+
* uv
* NSQ, tested with `nsqio/nsq:v1.3.0`
* Docker, for the local integration test cluster

Release 2.0.0
-------------

`2.0.0` is the Python 3.12+ modernization release. It intentionally raises the
baseline and cleans up old client behavior instead of preserving every Python
3.6-era compatibility detail.

Highlights:

* Python 3.12+ is now required. Production code and tests use modern asyncio
  APIs such as `asyncio.run()`, `asyncio.timeout()`, and task groups where they
  fit.
* The project uses `uv` for virtual environment and dependency management, with
  Tencent's PyPI mirror configured in `uv.toml`.
* HTTP support moved from `aiohttp` to `httpx`, including lookupd/nsqd HTTP
  helpers and the development auth server.
* `test_service/` provides one-command Docker lifecycle scripts for a local
  three-node NSQ cluster: `start.sh`, `stop.sh`, benchmark, and go-nsq
  comparison scripts.
* Reader shutdown is graceful by default through `reader.graceful_close()`: it
  sends `RDY 0`, requeues queued and unfinished in-flight messages, and then
  closes connections.
* `MessageTracker` now owns the in-flight message ledger. The queue remains the
  default handler path, while the tracker separately records messages that have
  been delivered by nsqd but not yet `FIN`ed or `REQ`ed.
* `set_message_handler()` is queue-backed by default. The TCP reader task keeps
  reading and enqueuing messages, and handler workers process them. The previous
  low-overhead behavior is still available with `direct=True`.
* RDY redistribution and low-water updates were tuned for high-throughput
  consumers while respecting nsqd `max_rdy_count`.
* TCP hot paths were reduced for `FIN`, `REQ`, `TOUCH`, `RDY`, and `PUB`; parser
  buffer handling avoids unnecessary copies.
* Benchmarks now cover PUB ACK throughput, MPUB throughput, end-to-end
  pub-to-FIN latency, graceful close requeue recovery, Python multi-process
  scale-out, and official `go-nsq` comparison.
* The test suite was refreshed with unit and integration coverage for protocol,
  HTTP, graceful shutdown, RDY, benchmark helpers, and edge cases.

Release tag: `2.0.0`


Running Tests
-------------

```bash
uv sync
./test_service/start.sh
uv run python -m pytest --cov=asyncnsq --cov-report=term-missing
./test_service/stop.sh
```

Without the Docker cluster, integration tests that need NSQ ports are skipped
and the unit test suite still runs.

For consumers, prefer `await reader.graceful_close()` during shutdown. It sends
`RDY 0`, requeues queued and in-flight unfinished messages with `REQ`, then
closes the TCP connections. `reader.close()` remains an immediate close.

`set_message_handler()` uses the internal `asyncio.Queue` by default. The TCP
reader task only receives and enqueues messages, while a handler worker drains
the queue and `FIN`/`REQ`s each message:

```python
def handle(message):
    process(message.body)

reader.set_message_handler(handle, auto_fin=True)
await reader.subscribe("topic", "channel")
```

CPU-bound synchronous handlers still occupy the Python event loop while they
run; use multiple consumer processes when you need real multi-core processing.

For very small, fast handlers where the queue overhead matters, opt into direct
handler mode. It bypasses the queue and can `FIN` messages on the TCP fast path:

```python
reader.set_message_handler(handle, auto_fin=True, direct=True)
```

Benchmark
---------

The benchmark suite is designed for pull requests: it is deterministic enough
for repeated local runs, fast by default, and strict enough to fail when the
client loses messages, duplicates messages, times out, or fails graceful
shutdown recovery.

It benchmarks the surfaces that matter for an NSQ client under high throughput:

* **TCP PUB ack throughput**: many concurrent `PUB` commands over persistent
  TCP connections, including per-message publish ACK latency.
* **TCP MPUB batch throughput**: high-volume batch publishing, including batch
  ACK latency and MiB/s.
* **End-to-end pub -> FIN latency**: messages are published, consumed through
  the reader, parsed, and `FIN`ed; the report includes p50/p95/p99 latency.
* **Graceful close requeue recovery**: messages are intentionally consumed but
  not finished, `reader.graceful_close()` sends `RDY 0` and `REQ`, and a fresh
  reader must recover and finish all of them.

One-command local PR benchmark:

```bash
uv sync
./test_service/benchmark.sh --profile pr --markdown benchmark.md
```

Run against an already running cluster:

```bash
uv run asyncnsq-benchmark --profile pr --markdown benchmark.md
```

Compare against the official Go client on the same local NSQ cluster:

```bash
PROFILE=pr ./test_service/benchmark_compare_go.sh
```

For single-core fairness, limit the Go scheduler to one OS thread:

```bash
GO_MAX_PROCS=1 PROFILE=pr ./test_service/benchmark_compare_go.sh
```

This writes:

* `benchmark-results/asyncnsq-benchmark.md`
* `benchmark-results/go-nsq-benchmark.md`

The Go baseline uses `github.com/nsqio/go-nsq v1.1.0` and runs the comparable
`PUB`, `MPUB`, and end-to-end consume/finish scenarios. The asyncnsq benchmark
also includes the Python-specific graceful shutdown requeue proof. Go runs as
one process, but goroutines can execute on multiple cores unless `GOMAXPROCS`
is limited; the Go report records the effective `GOMAXPROCS` value.

To measure Python scale-out across CPU cores, run the asyncnsq benchmark with
multiple consumer processes. Each process owns its own asyncio loop and joins
the same NSQ topic/channel, so NSQ distributes messages across them:

```bash
PROFILE=pr ./test_service/benchmark_compare_go.sh --consumer-processes 4
```

Latest local results
--------------------

These numbers were measured for the `2.0.0` release on 2026-05-15 with Python
3.12.4, NSQ `nsqio/nsq:v1.3.0`, a local Docker three-node nsqd cluster, 10,000
messages, 512 B payloads, concurrency 256, `max_in_flight=1024`, and
`output_buffer_timeout=25ms`. Treat them as a same-machine reference, not a
portable guarantee.

Current asyncnsq PR benchmark:

| Scenario | Messages | msg/s | MiB/s | p50 ms | p95 ms | p99 ms | Errors | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| TCP PUB ack | 10,000 | 82,918.92 | 40.49 | 2.859 | 3.633 | 11.704 | 0 | per-message publish ACK latency |
| TCP MPUB batch ack | 10,000 | 774,507.49 | 378.18 | 6.451 | 9.219 | 9.582 | 0 | ACK latency measured per MPUB batch |
| end-to-end pub->fin | 10,000 | 36,381.65 | 17.76 | 4.912 | 10.683 | 11.912 | 0 | missing=0, duplicates=0, fin_errors=0 |
| graceful close requeue | 512 | 13,096.60 | 6.39 | 14.580 | 16.847 | 16.947 | 0 | requeued=512, recovered=512 |

Handler dispatch comparison after making queue-backed handlers the default:

| Consumer mode | Messages | msg/s | p95 ms | Errors | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| `direct=True` handler | 10,000 | 36,469.93 | 10.000 | 0 | TCP reader calls the handler directly |
| default queue-backed handler | 10,000 | 36,310.22 | 12.778 | 0 | TCP reader enqueues; handler worker drains the queue |

The queue-backed path kept throughput within about 0.5% of the direct handler
path in this run. It is now the default because it keeps socket reading and user
handler execution decoupled while `MessageTracker` separately tracks unfinished
in-flight messages for safe `FIN`/`REQ` and graceful shutdown.

asyncnsq vs official go-nsq, default Go scheduler:

| Scenario | asyncnsq msg/s | asyncnsq p95 ms | go-nsq msg/s | go-nsq p95 ms | Errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP PUB ack | 82,793.09 | 3.373 | 191,773.18 | 1.914 | 0 / 0 |
| TCP MPUB batch ack | 601,022.00 | 15.343 | 645,822.53 | 12.322 | 0 / 0 |
| end-to-end pub->fin | 36,329.07 | 10.256 | 134,190.70 | 3.258 | 0 / 0 |

In that run, Go reported `GOMAXPROCS=16`. This is the normal production shape
for the Go client, but it is not a single-core comparison against one Python
asyncio process.

asyncnsq vs official go-nsq with Go limited to one OS thread:

| Scenario | asyncnsq msg/s | asyncnsq p95 ms | go-nsq msg/s | go-nsq p95 ms | Errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP PUB ack | 79,446.79 | 3.946 | 90,948.43 | 3.731 | 0 / 0 |
| TCP MPUB batch ack | 589,566.88 | 15.812 | 531,962.03 | 15.317 | 0 / 0 |
| end-to-end pub->fin | 36,390.14 | 9.908 | 50,251.00 | 89.039 | 0 / 0 |

The single-thread Go run narrows the throughput gap substantially. In this
sample, go-nsq still had higher end-to-end throughput, while asyncnsq had much
lower p95 end-to-end latency. For multi-core Python consumption, use
`--consumer-processes` so each process has its own event loop and NSQ can
distribute messages across processes.

Profiles:

| Profile | Use case | Messages | Payload | Concurrency | Batch |
| --- | --- | ---: | ---: | ---: | ---: |
| `quick` | fast smoke benchmark | 5,000 | 256 B | 64 | 100 |
| `pr` | recommended PR evidence | 50,000 | 512 B | 256 | 250 |
| `stress` | local high-throughput soak | 250,000 | 1,024 B | 512 | 500 |

Useful overrides:

```bash
uv run asyncnsq-benchmark \
  --profile pr \
  --messages 100000 \
  --payload-size 1024 \
  --concurrency 512 \
  --batch-size 500 \
  --markdown benchmark.md \
  --json benchmark.json
```

The generated markdown is PR-ready:

```markdown
| Scenario | Messages | Payload | Batch | Concurrency | Duration | msg/s | MiB/s | p50 ms | p95 ms | p99 ms | Errors | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| TCP PUB ack | ... | ... | n/a | ... | ... | ... | ... | ... | ... | ... | 0 | per-message publish ACK latency |
| TCP MPUB batch ack | ... | ... | ... | ... | ... | ... | ... | ... | ... | ... | 0 | ACK latency is measured per MPUB batch |
| end-to-end pub->fin | ... | ... | n/a | ... | ... | ... | ... | ... | ... | ... | 0 | missing=0, duplicates=0, fin_errors=0 |
| graceful close requeue | ... | ... | ... | ... | ... | ... | ... | ... | ... | ... | 0 | requeued=..., recovered=..., missing=0, unexpected=0 |
```

For a PR, attach `benchmark.md` and keep every row at `Errors = 0`. Compare
throughput and latency only between runs on the same machine and Docker setup.
For Go comparisons, attach both files from `benchmark-results/` and compare
only the matching `PUB`, `MPUB`, and end-to-end rows.

License
-------

The asyncnsq is offered under MIT license.

Donation
--------

If you like this repo, buy me a coffee.

**ETH wallet** 

<img src="./opensource_wallet.jpeg" alt="drawing" width="400"/>

**Or you can participate with this project.**
