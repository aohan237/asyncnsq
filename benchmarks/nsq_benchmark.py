from __future__ import annotations

import argparse
import asyncio
import json
import math
import multiprocessing
import platform
import queue
import struct
import sys
import time
import uuid
from array import array
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import httpx

from asyncnsq import __version__, create_reader, create_writer
from asyncnsq.http import NsqdHttpWriter


HEADER = struct.Struct(">QQ")
MIB = 1024 * 1024
ALL_SCENARIOS = ("pub", "mpub", "e2e", "graceful")

PROFILES = {
    "quick": {
        "messages": 5_000,
        "payload_size": 256,
        "concurrency": 64,
        "batch_size": 100,
        "max_in_flight": 256,
        "graceful_messages": 128,
        "output_buffer_timeout_ms": 25,
        "timeout": 60.0,
    },
    "pr": {
        "messages": 50_000,
        "payload_size": 512,
        "concurrency": 256,
        "batch_size": 250,
        "max_in_flight": 1024,
        "graceful_messages": 512,
        "output_buffer_timeout_ms": 25,
        "timeout": 180.0,
    },
    "stress": {
        "messages": 250_000,
        "payload_size": 1024,
        "concurrency": 512,
        "batch_size": 500,
        "max_in_flight": 2048,
        "graceful_messages": 1024,
        "output_buffer_timeout_ms": 25,
        "timeout": 600.0,
    },
}


@dataclass(frozen=True)
class Address:
    host: str
    port: int

    def __str__(self):
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class BenchmarkConfig:
    profile: str
    run_id: str
    nsqd_tcp_addresses: tuple[Address, ...]
    nsqd_http_addresses: tuple[Address, ...]
    scenarios: tuple[str, ...]
    messages: int
    payload_size: int
    concurrency: int
    batch_size: int
    max_in_flight: int
    graceful_messages: int
    output_buffer_timeout_ms: int
    writer_connections: int
    consumer_processes: int
    timeout: float
    warmup_messages: int
    snappy: bool
    deflate: bool
    tls_v1: bool
    cleanup: bool
    markdown_path: Path | None
    json_path: Path | None


@dataclass
class ScenarioResult:
    scenario: str
    messages: int
    payload_bytes: int
    batch_size: int | None
    concurrency: int
    duration_s: float
    throughput_msg_s: float
    throughput_mib_s: float
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    errors: int
    notes: str = ""


def parse_addresses(value: str, default_port: int) -> tuple[Address, ...]:
    addresses = []
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            continue
        if "://" in raw:
            raw = raw.split("://", 1)[1]
        if ":" in raw:
            host, port = raw.rsplit(":", 1)
            addresses.append(Address(host, int(port)))
        else:
            addresses.append(Address(raw, default_port))
    if not addresses:
        raise argparse.ArgumentTypeError("at least one address is required")
    return tuple(addresses)


def parse_scenarios(value: str) -> tuple[str, ...]:
    if value == "all":
        return ALL_SCENARIOS
    scenarios = tuple(item.strip() for item in value.split(",") if item.strip())
    unknown = sorted(set(scenarios) - set(ALL_SCENARIOS))
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown scenarios: {', '.join(unknown)}")
    return scenarios


def topic_name(run_id: str, scenario: str):
    return f"asq-bench-{scenario}-{run_id}"[:64]


def channel_name(run_id: str, scenario: str):
    return f"ch-{scenario}-{run_id}"[:64]


def payload_factory(payload_size: int):
    if payload_size < HEADER.size:
        raise ValueError(f"payload size must be >= {HEADER.size} bytes")
    pad_size = payload_size - HEADER.size
    seed = b"asyncnsq-benchmark|"
    padding = (seed * (pad_size // len(seed) + 1))[:pad_size]

    def build(seq: int):
        return HEADER.pack(seq, time.perf_counter_ns()) + padding

    return build


def parse_payload(body: bytes):
    if len(body) < HEADER.size:
        raise ValueError("message body is too small for benchmark header")
    return HEADER.unpack_from(body)


def percentile_ns(values: array, percentile: float):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * percentile / 100) - 1)
    return ordered[index] / 1_000_000


def make_result(
        scenario: str, messages: int, payload_bytes: int,
        batch_size: int | None, concurrency: int, duration_s: float,
        latencies_ns: array, errors: int, notes: str = ""):
    duration_s = max(duration_s, 1e-9)
    success_count = max(0, messages - errors)
    return ScenarioResult(
        scenario=scenario,
        messages=messages,
        payload_bytes=payload_bytes,
        batch_size=batch_size,
        concurrency=concurrency,
        duration_s=duration_s,
        throughput_msg_s=success_count / duration_s,
        throughput_mib_s=(success_count * payload_bytes) / duration_s / MIB,
        p50_ms=percentile_ns(latencies_ns, 50),
        p95_ms=percentile_ns(latencies_ns, 95),
        p99_ms=percentile_ns(latencies_ns, 99),
        errors=errors,
        notes=notes,
    )


def format_int(value: int):
    return f"{value:,}"


def format_float(value: float):
    return f"{value:,.2f}"


def format_ms(value: float | None):
    return "n/a" if value is None else f"{value:,.3f}"


async def open_writers(config: BenchmarkConfig):
    writers = []
    for index in range(config.writer_connections):
        address = config.nsqd_tcp_addresses[index % len(config.nsqd_tcp_addresses)]
        writer = await create_writer(
            host=address.host,
            port=address.port,
            tls_v1=config.tls_v1,
            snappy=config.snappy,
            deflate=config.deflate,
        )
        writers.append(writer)
    return writers


def close_writers(writers):
    for writer in writers:
        writer.close()


async def wait_for_cluster(config: BenchmarkConfig):
    async with httpx.AsyncClient(timeout=2.0) as client:
        for address in config.nsqd_http_addresses:
            url = f"http://{address}/ping"
            try:
                response = await client.get(url)
                response.raise_for_status()
            except Exception as exc:
                raise RuntimeError(
                    f"NSQ HTTP endpoint is not ready: {url}: {exc}") from exc
            if response.text.strip() != "OK":
                raise RuntimeError(
                    f"NSQ HTTP endpoint returned non-OK ping: {url}")


async def cleanup_topic(config: BenchmarkConfig, topic: str):
    if not config.cleanup:
        return
    for address in config.nsqd_http_addresses:
        conn = NsqdHttpWriter(address.host, address.port)
        try:
            await conn.delete_topic(topic)
        except Exception:
            pass
        finally:
            await conn.close()


async def run_pub_ack(
        config: BenchmarkConfig, topic: str, messages: int,
        scenario_name: str = "TCP PUB ack"):
    build_payload = payload_factory(config.payload_size)
    latencies = array("Q")
    errors = 0
    writers = await open_writers(config)
    worker_count = min(config.concurrency, max(1, messages))
    started = time.perf_counter()

    async def worker(worker_id: int):
        nonlocal errors
        writer = writers[worker_id % len(writers)]
        for seq in range(worker_id, messages, worker_count):
            started_ns = time.perf_counter_ns()
            try:
                await writer.pub(topic, build_payload(seq))
            except Exception:
                errors += 1
            else:
                latencies.append(time.perf_counter_ns() - started_ns)

    try:
        async with asyncio.timeout(config.timeout):
            async with asyncio.TaskGroup() as task_group:
                for worker_id in range(worker_count):
                    task_group.create_task(worker(worker_id))
    except TimeoutError:
        errors += messages - len(latencies)
    finally:
        duration = time.perf_counter() - started
        close_writers(writers)

    return make_result(
        scenario_name, messages, config.payload_size, None,
        worker_count, duration, latencies, errors,
        "per-message publish ACK latency",
    )


async def run_mpub_ack(config: BenchmarkConfig, topic: str):
    build_payload = payload_factory(config.payload_size)
    latencies = array("Q")
    errors = 0
    sent = 0
    writers = await open_writers(config)
    total_batches = math.ceil(config.messages / config.batch_size)
    worker_count = min(config.concurrency, max(1, total_batches))
    started = time.perf_counter()

    async def worker(worker_id: int):
        nonlocal errors, sent
        writer = writers[worker_id % len(writers)]
        for batch_id in range(worker_id, total_batches, worker_count):
            start_seq = batch_id * config.batch_size
            end_seq = min(start_seq + config.batch_size, config.messages)
            payloads = [build_payload(seq) for seq in range(start_seq, end_seq)]
            started_ns = time.perf_counter_ns()
            try:
                await writer.mpub(topic, *payloads)
            except Exception:
                errors += len(payloads)
            else:
                sent += len(payloads)
                latencies.append(time.perf_counter_ns() - started_ns)

    try:
        async with asyncio.timeout(config.timeout):
            async with asyncio.TaskGroup() as task_group:
                for worker_id in range(worker_count):
                    task_group.create_task(worker(worker_id))
    except TimeoutError:
        errors += config.messages - sent
    finally:
        duration = time.perf_counter() - started
        close_writers(writers)

    return make_result(
        "TCP MPUB batch ack", config.messages, config.payload_size,
        config.batch_size, worker_count, duration, latencies, errors,
        "ACK latency is measured per MPUB batch",
    )


async def collect_messages(
        reader, expected: int, timeout: float, *, fin: bool,
        record_seen: bool = True):
    latencies = array("Q")
    seen = set() if record_seen else None
    seen_flags = None if record_seen else bytearray(expected)
    seen_count = 0
    duplicates = 0
    decode_errors = 0
    fin_errors = 0
    generator = reader.messages()
    try:
        async with asyncio.timeout(timeout):
            while seen_count < expected:
                message = await generator.__anext__()
                try:
                    seq, sent_ns = parse_payload(message.body)
                except Exception:
                    decode_errors += 1
                else:
                    now_ns = time.perf_counter_ns()
                    if seq < 0 or seq >= expected:
                        decode_errors += 1
                    else:
                        if record_seen:
                            if seq in seen:
                                duplicates += 1
                            else:
                                seen.add(seq)
                                seen_count += 1
                                latencies.append(now_ns - sent_ns)
                        elif seen_flags[seq]:
                            duplicates += 1
                        else:
                            seen_flags[seq] = 1
                            seen_count += 1
                            latencies.append(now_ns - sent_ns)
                if fin:
                    try:
                        await message.fin()
                    except Exception:
                        fin_errors += 1
    except TimeoutError:
        pass
    finally:
        await generator.aclose()

    return {
        "seen": seen,
        "seen_count": seen_count,
        "latencies": latencies,
        "errors": duplicates + decode_errors + fin_errors,
        "duplicates": duplicates,
        "decode_errors": decode_errors,
        "fin_errors": fin_errors,
    }


def install_handler_collector(reader, expected: int, *, fin: bool):
    latencies = array("Q")
    seen_flags = bytearray(expected)
    seen_count = 0
    duplicates = 0
    decode_errors = 0
    handler_errors = 0
    done = asyncio.Event()

    def handler(message):
        nonlocal seen_count, duplicates, decode_errors, handler_errors
        try:
            seq, sent_ns = parse_payload(message.body)
        except Exception:
            decode_errors += 1
            return
        if seq < 0 or seq >= expected:
            decode_errors += 1
            return
        if seen_flags[seq]:
            duplicates += 1
            return
        seen_flags[seq] = 1
        seen_count += 1
        latencies.append(time.perf_counter_ns() - sent_ns)
        if seen_count >= expected:
            done.set()

    reader.set_message_handler(handler, auto_fin=fin, direct=True)

    async def wait(timeout):
        try:
            async with asyncio.timeout(timeout):
                await done.wait()
        except TimeoutError:
            pass
        finally:
            reader.clear_message_handler()
        missing = expected - seen_count
        return {
            "seen_count": seen_count,
            "latencies": latencies,
            "errors": duplicates + decode_errors + handler_errors,
            "duplicates": duplicates,
            "decode_errors": decode_errors,
            "handler_errors": handler_errors,
            "fin_errors": 0,
            "missing": missing,
        }

    return wait


async def collect_messages_worker(
        result_queue, ready_queue, done_event, total_delivered, topic: str,
        channel: str, tcp_addresses: tuple[tuple[str, int], ...],
        expected: int, timeout: float, max_in_flight: int,
        output_buffer_timeout_ms: int, tls_v1: bool, snappy: bool,
        deflate: bool):
    reader = None
    seqs = array("Q")
    latencies = array("Q")
    decode_errors = 0
    fin_errors = 0
    delivered = 0
    started = time.perf_counter()
    try:
        reader = await create_reader(
            nsqd_tcp_addresses=tcp_addresses,
            max_in_flight=max_in_flight,
            output_buffer_timeout=output_buffer_timeout_ms,
            tls_v1=tls_v1,
            snappy=snappy,
            deflate=deflate,
        )
        def handler(message):
            nonlocal delivered, decode_errors
            delivered += 1
            try:
                seq, sent_ns = parse_payload(message.body)
            except Exception:
                decode_errors += 1
            else:
                if 0 <= seq < expected:
                    seqs.append(seq)
                    latencies.append(time.perf_counter_ns() - sent_ns)
                else:
                    decode_errors += 1
            with total_delivered.get_lock():
                total_delivered.value += 1
                if total_delivered.value >= expected:
                    done_event.set()

        reader.set_message_handler(handler, auto_fin=True, direct=True)
        await reader.subscribe(topic, channel)
        ready_queue.put(True)
        await asyncio.to_thread(done_event.wait, timeout)
    except Exception as exc:
        ready_queue.put(False)
        result_queue.put({
            "seqs": array("Q"),
            "latencies": array("Q"),
            "delivered": delivered,
            "errors": 1,
            "notes": f"worker_error={type(exc).__name__}: {exc}",
            "duration_s": time.perf_counter() - started,
        })
        return
    finally:
        if reader is not None:
            try:
                await reader.graceful_close(requeue=False)
            except Exception:
                pass
    result_queue.put({
        "seqs": seqs,
        "latencies": latencies,
        "delivered": delivered,
        "errors": decode_errors + fin_errors,
        "notes": f"decode_errors={decode_errors}, fin_errors={fin_errors}",
        "duration_s": time.perf_counter() - started,
    })


def run_consumer_process(*args):
    asyncio.run(collect_messages_worker(*args))


async def start_multiprocess_collectors(config: BenchmarkConfig, topic: str,
                                        channel: str):
    process_count = max(1, config.consumer_processes)
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()
    ready_queue = ctx.Queue()
    done_event = ctx.Event()
    total_delivered = ctx.Value("i", 0)
    worker_max_in_flight = max(
        1, math.ceil(config.max_in_flight / process_count))
    tcp_addresses = tuple(
        (address.host, address.port)
        for address in config.nsqd_tcp_addresses
    )
    processes = [
        ctx.Process(
            target=run_consumer_process,
            args=(
                result_queue, ready_queue, done_event, total_delivered,
                topic, channel, tcp_addresses, config.messages,
                config.timeout, worker_max_in_flight,
                config.output_buffer_timeout_ms, config.tls_v1,
                config.snappy, config.deflate,
            ),
        )
        for _ in range(process_count)
    ]
    for process in processes:
        process.start()

    ready = 0
    ready_deadline = time.perf_counter() + min(10.0, config.timeout)
    while ready < process_count and time.perf_counter() < ready_deadline:
        try:
            if ready_queue.get(timeout=0.1):
                ready += 1
        except queue.Empty:
            continue
    if ready < process_count:
        done_event.set()

    return {
        "processes": processes,
        "result_queue": result_queue,
        "done_event": done_event,
        "ready": ready,
        "worker_max_in_flight": worker_max_in_flight,
    }


async def finish_multiprocess_collectors(collector, expected: int,
                                         timeout: float):
    done_event = collector["done_event"]
    processes = collector["processes"]
    completed = await asyncio.to_thread(done_event.wait, timeout)
    finished_at = time.perf_counter()
    done_event.set()

    results = []
    result_queue = collector["result_queue"]
    drain_deadline = time.perf_counter() + 10.0
    while len(results) < len(processes) and time.perf_counter() < drain_deadline:
        try:
            result = await asyncio.to_thread(result_queue.get, True, 0.1)
        except queue.Empty:
            if all(not process.is_alive() for process in processes):
                break
            continue
        else:
            results.append(result)

    for process in processes:
        await asyncio.to_thread(process.join, 5.0)
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 2.0)

    seen_flags = bytearray(expected)
    seen_count = 0
    duplicates = 0
    latencies = array("Q")
    errors = 0
    delivered = 0
    for result in results:
        delivered += result.get("delivered", 0)
        errors += result.get("errors", 0)
        latencies.extend(result.get("latencies", ()))
        for seq in result.get("seqs", ()):
            if seen_flags[seq]:
                duplicates += 1
            else:
                seen_flags[seq] = 1
                seen_count += 1

    missing = expected - seen_count
    errors += missing + duplicates
    if len(results) < len(processes):
        errors += len(processes) - len(results)
    return {
        "seen_count": seen_count,
        "delivered": delivered,
        "latencies": latencies,
        "errors": errors,
        "duplicates": duplicates,
        "missing": missing,
        "worker_results": len(results),
        "completed": completed,
        "finished_at": finished_at,
    }


async def run_e2e(config: BenchmarkConfig, topic: str):
    channel = channel_name(config.run_id, "e2e")
    if config.consumer_processes > 1:
        collector = await start_multiprocess_collectors(config, topic, channel)
        await asyncio.sleep(0.05)
        started = time.perf_counter()
        publish_result = await run_pub_ack(
            config, topic, config.messages, scenario_name="internal publisher")
        consumed = await finish_multiprocess_collectors(
            collector, config.messages, config.timeout)
        duration = consumed["finished_at"] - started
        errors = publish_result.errors + consumed["errors"]
        notes = (
            f"consumer_processes={config.consumer_processes}, "
            f"worker_max_in_flight={collector['worker_max_in_flight']}, "
            f"ready={collector['ready']}, completed={consumed['completed']}, "
            f"missing={consumed['missing']}, "
            f"duplicates={consumed['duplicates']}"
        )
    else:
        reader = await create_reader(
            nsqd_tcp_addresses=[
                (address.host, address.port)
                for address in config.nsqd_tcp_addresses
            ],
            max_in_flight=config.max_in_flight,
            output_buffer_timeout=config.output_buffer_timeout_ms,
            tls_v1=config.tls_v1,
            snappy=config.snappy,
            deflate=config.deflate,
        )
        wait_for_messages = install_handler_collector(
            reader, config.messages, fin=True)
        await reader.subscribe(topic, channel)
        await asyncio.sleep(0.05)

        started = time.perf_counter()
        publish_result = await run_pub_ack(
            config, topic, config.messages, scenario_name="internal publisher")
        consumed = await wait_for_messages(config.timeout)
        duration = time.perf_counter() - started
        await reader.graceful_close(requeue=False)

        missing = config.messages - consumed["seen_count"]
        errors = publish_result.errors + consumed["errors"] + missing
        notes = (
            f"consumer_processes=1, missing={missing}, "
            f"duplicates={consumed['duplicates']}, "
            f"fin_errors={consumed['fin_errors']}"
        )
    return make_result(
        "end-to-end pub->fin", config.messages, config.payload_size,
        None, config.concurrency, duration, consumed["latencies"], errors,
        notes,
    )


async def run_graceful_requeue(config: BenchmarkConfig, topic: str):
    target = min(config.graceful_messages, config.max_in_flight, config.messages)
    channel = channel_name(config.run_id, "graceful")
    single_node_config = replace(
        config,
        nsqd_tcp_addresses=(config.nsqd_tcp_addresses[0],),
        writer_connections=1,
    )
    reader = await create_reader(
        nsqd_tcp_addresses=[
            (address.host, address.port)
            for address in single_node_config.nsqd_tcp_addresses
        ],
        max_in_flight=max(target, 1),
        output_buffer_timeout=single_node_config.output_buffer_timeout_ms,
        tls_v1=single_node_config.tls_v1,
        snappy=single_node_config.snappy,
        deflate=single_node_config.deflate,
    )
    await reader.subscribe(topic, channel)
    await asyncio.sleep(0.05)

    started = time.perf_counter()
    publish_result = await run_mpub_subset(single_node_config, topic, target)
    first_pass = await collect_messages(reader, target, config.timeout, fin=False)
    requeued = await reader.graceful_close(timeout=0)

    recovery_reader = await create_reader(
        nsqd_tcp_addresses=[
            (address.host, address.port)
            for address in single_node_config.nsqd_tcp_addresses
        ],
        max_in_flight=max(target, 1),
        output_buffer_timeout=single_node_config.output_buffer_timeout_ms,
        tls_v1=single_node_config.tls_v1,
        snappy=single_node_config.snappy,
        deflate=single_node_config.deflate,
    )
    await recovery_reader.subscribe(topic, channel)
    recovered = await collect_messages(
        recovery_reader, target, config.timeout, fin=True)
    await recovery_reader.graceful_close(requeue=False)
    duration = time.perf_counter() - started

    expected = first_pass["seen"]
    missing = len(expected - recovered["seen"])
    unexpected = len(recovered["seen"] - expected)
    errors = (
        publish_result.errors + first_pass["errors"] + recovered["errors"] +
        missing + unexpected + abs(requeued - len(expected))
    )
    notes = (
        f"requeued={requeued}, recovered={len(recovered['seen'])}, "
        f"missing={missing}, unexpected={unexpected}, single_node=true"
    )
    return make_result(
        "graceful close requeue", target, config.payload_size,
        config.batch_size, config.concurrency, duration,
        recovered["latencies"], errors, notes,
    )


async def run_mpub_subset(config: BenchmarkConfig, topic: str, messages: int):
    subset = replace(config, messages=messages)
    return await run_mpub_ack(subset, topic)


async def run_warmup(config: BenchmarkConfig):
    if config.warmup_messages <= 0:
        return
    topic = topic_name(config.run_id, "warmup")
    try:
        await run_pub_ack(config, topic, config.warmup_messages, "warmup")
    finally:
        await cleanup_topic(config, topic)


async def run_benchmark(config: BenchmarkConfig):
    await wait_for_cluster(config)
    await run_warmup(config)

    results = []
    for scenario in config.scenarios:
        topic = topic_name(config.run_id, scenario)
        try:
            if scenario == "pub":
                results.append(await run_pub_ack(config, topic, config.messages))
            elif scenario == "mpub":
                results.append(await run_mpub_ack(config, topic))
            elif scenario == "e2e":
                results.append(await run_e2e(config, topic))
            elif scenario == "graceful":
                results.append(await run_graceful_requeue(config, topic))
        finally:
            await cleanup_topic(config, topic)
    return results


def markdown_report(config: BenchmarkConfig, results: list[ScenarioResult]):
    generated = datetime.now(UTC).isoformat(timespec="seconds")
    lines = [
        "# asyncnsq Benchmark Report",
        "",
        "| Field | Value |",
        "| --- | --- |",
        f"| Generated | `{generated}` |",
        f"| Run ID | `{config.run_id}` |",
        f"| Profile | `{config.profile}` |",
        f"| Python | `{sys.version.split()[0]}` |",
        f"| Platform | `{platform.platform()}` |",
        f"| asyncnsq | `{__version__}` |",
        f"| NSQD TCP | `{', '.join(map(str, config.nsqd_tcp_addresses))}` |",
        f"| NSQD HTTP | `{', '.join(map(str, config.nsqd_http_addresses))}` |",
        f"| Output buffer timeout | `{config.output_buffer_timeout_ms}ms` |",
        f"| Max in-flight | `{config.max_in_flight}` |",
        f"| Consumer processes | `{config.consumer_processes}` |",
        "",
        "| Scenario | Messages | Payload | Batch | Concurrency | Duration | msg/s | MiB/s | p50 ms | p95 ms | p99 ms | Errors | Notes |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for result in results:
        batch = "n/a" if result.batch_size is None else format_int(result.batch_size)
        lines.append(
            f"| {result.scenario} | {format_int(result.messages)} | "
            f"{format_int(result.payload_bytes)} B | {batch} | "
            f"{format_int(result.concurrency)} | {format_float(result.duration_s)}s | "
            f"{format_float(result.throughput_msg_s)} | "
            f"{format_float(result.throughput_mib_s)} | "
            f"{format_ms(result.p50_ms)} | {format_ms(result.p95_ms)} | "
            f"{format_ms(result.p99_ms)} | {format_int(result.errors)} | "
            f"{result.notes} |"
        )
    lines.extend([
        "",
        "Success criteria: every row must report `Errors = 0`. The graceful "
        "close row must show that all intentionally unfinished messages were "
        "requeued and recovered.",
    ])
    return "\n".join(lines) + "\n"


def json_report(config: BenchmarkConfig, results: list[ScenarioResult]):
    return {
        "generated": datetime.now(UTC).isoformat(timespec="seconds"),
        "run_id": config.run_id,
        "profile": config.profile,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "asyncnsq": __version__,
        "output_buffer_timeout_ms": config.output_buffer_timeout_ms,
        "max_in_flight": config.max_in_flight,
        "consumer_processes": config.consumer_processes,
        "nsqd_tcp_addresses": [str(address) for address in config.nsqd_tcp_addresses],
        "nsqd_http_addresses": [str(address) for address in config.nsqd_http_addresses],
        "results": [asdict(result) for result in results],
    }


def build_parser():
    parser = argparse.ArgumentParser(
        description="Run a PR-ready benchmark suite against an NSQ cluster.")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="quick")
    parser.add_argument(
        "--nsqd-tcp-addresses",
        default="127.0.0.1:4150,127.0.0.1:4250,127.0.0.1:4350",
        help="Comma-separated nsqd TCP addresses.")
    parser.add_argument(
        "--nsqd-http-addresses",
        default="127.0.0.1:4151,127.0.0.1:4251,127.0.0.1:4351",
        help="Comma-separated nsqd HTTP addresses for health and cleanup.")
    parser.add_argument("--scenarios", type=parse_scenarios, default=ALL_SCENARIOS)
    parser.add_argument("--messages", type=int)
    parser.add_argument("--payload-size", type=int)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--max-in-flight", type=int)
    parser.add_argument("--graceful-messages", type=int)
    parser.add_argument("--output-buffer-timeout-ms", type=int)
    parser.add_argument("--writer-connections", type=int)
    parser.add_argument("--consumer-processes", type=int)
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--warmup-messages", type=int)
    parser.add_argument("--snappy", action="store_true")
    parser.add_argument("--deflate", action="store_true")
    parser.add_argument("--tls-v1", action="store_true")
    parser.add_argument("--no-cleanup", action="store_true")
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--json", dest="json_path", type=Path)
    return parser


def resolve_config(args):
    profile = PROFILES[args.profile]
    tcp_addresses = parse_addresses(args.nsqd_tcp_addresses, 4150)
    http_addresses = parse_addresses(args.nsqd_http_addresses, 4151)
    writer_connections = args.writer_connections or len(tcp_addresses)
    return BenchmarkConfig(
        profile=args.profile,
        run_id=uuid.uuid4().hex[:8],
        nsqd_tcp_addresses=tcp_addresses,
        nsqd_http_addresses=http_addresses,
        scenarios=args.scenarios,
        messages=args.messages or profile["messages"],
        payload_size=args.payload_size or profile["payload_size"],
        concurrency=args.concurrency or profile["concurrency"],
        batch_size=args.batch_size or profile["batch_size"],
        max_in_flight=args.max_in_flight or profile["max_in_flight"],
        graceful_messages=args.graceful_messages or profile["graceful_messages"],
        output_buffer_timeout_ms=(
            args.output_buffer_timeout_ms
            if args.output_buffer_timeout_ms is not None
            else profile["output_buffer_timeout_ms"]
        ),
        writer_connections=writer_connections,
        consumer_processes=args.consumer_processes or 1,
        timeout=args.timeout or profile["timeout"],
        warmup_messages=(
            args.warmup_messages
            if args.warmup_messages is not None
            else min(1000, profile["messages"] // 10)
        ),
        snappy=args.snappy,
        deflate=args.deflate,
        tls_v1=args.tls_v1,
        cleanup=not args.no_cleanup,
        markdown_path=args.markdown,
        json_path=args.json_path,
    )


async def async_main(argv: list[str] | None = None):
    parser = build_parser()
    args = parser.parse_args(argv)
    config = resolve_config(args)
    if config.snappy and config.deflate:
        parser.error("--snappy and --deflate cannot be enabled together")

    try:
        results = await run_benchmark(config)
    except Exception as exc:
        print(f"benchmark failed before producing a report: {exc}", file=sys.stderr)
        return 2

    markdown = markdown_report(config, results)
    print(markdown)
    if config.markdown_path is not None:
        config.markdown_path.write_text(markdown, encoding="utf-8")
    if config.json_path is not None:
        config.json_path.write_text(
            json.dumps(json_report(config, results), indent=2),
            encoding="utf-8",
        )

    if any(result.errors for result in results):
        return 1
    return 0


def main():
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
