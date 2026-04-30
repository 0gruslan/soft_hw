#!/usr/bin/env python3
"""
Simple student-style benchmark:
- producer -> broker -> consumer
- brokers: RabbitMQ and Redis
- scenarios: baseline, message-size effect, rate effect
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import pika
except ImportError:
    pika = None

try:
    import redis
except ImportError:
    redis = None

try:
    import psutil
except ImportError:
    psutil = None


@dataclass
class RunConfig:
    broker: str
    scenario: str
    message_size: int
    target_rate: int
    duration_seconds: int
    producers: int
    consumers: int


@dataclass
class RunStats:
    sent: int = 0
    send_errors: int = 0
    consumed: int = 0
    consume_errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    backlog_max: int = 0
    cpu_samples: list[float] = field(default_factory=list)
    ram_samples: list[float] = field(default_factory=list)
    run_started: float = 0.0
    run_finished: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_send_ok(self) -> None:
        with self.lock:
            self.sent += 1

    def record_send_error(self) -> None:
        with self.lock:
            self.send_errors += 1

    def record_consume_ok(self, latency_ms: float) -> None:
        with self.lock:
            self.consumed += 1
            self.latencies_ms.append(latency_ms)

    def record_consume_error(self) -> None:
        with self.lock:
            self.consume_errors += 1

    def observe_backlog(self, backlog: int) -> None:
        with self.lock:
            if backlog > self.backlog_max:
                self.backlog_max = backlog

    def observe_system(self, cpu: float, ram: float) -> None:
        with self.lock:
            self.cpu_samples.append(cpu)
            self.ram_samples.append(ram)


class RabbitAdapter:
    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        if pika is None:
            raise RuntimeError("Package 'pika' is not installed.")
        creds = pika.PlainCredentials(user, password)
        self.params = pika.ConnectionParameters(host=host, port=port, credentials=creds)

    def _connect(self) -> Any:
        return pika.BlockingConnection(self.params)

    def prepare_queue(self, queue_name: str) -> None:
        conn = self._connect()
        ch = conn.channel()
        ch.queue_declare(queue=queue_name, durable=False, auto_delete=True)
        ch.queue_purge(queue=queue_name)
        conn.close()

    def cleanup_queue(self, queue_name: str) -> None:
        conn = self._connect()
        ch = conn.channel()
        ch.queue_delete(queue=queue_name)
        conn.close()

    def create_producer_client(self) -> tuple[Any, Any]:
        conn = self._connect()
        ch = conn.channel()
        return conn, ch

    def create_consumer_client(self) -> tuple[Any, Any]:
        conn = self._connect()
        ch = conn.channel()
        return conn, ch

    def close_client(self, client: tuple[Any, Any]) -> None:
        conn, _ = client
        conn.close()

    def publish(self, client: tuple[Any, Any], queue_name: str, body: bytes) -> None:
        _, ch = client
        ch.basic_publish(exchange="", routing_key=queue_name, body=body)

    def consume_once(self, client: tuple[Any, Any], queue_name: str, timeout_sec: float = 0.0) -> bytes | None:
        _ = timeout_sec
        _, ch = client
        method, _, body = ch.basic_get(queue=queue_name, auto_ack=True)
        if method is None:
            return None
        return body

    def get_backlog(self, queue_name: str) -> int:
        conn = self._connect()
        ch = conn.channel()
        state = ch.queue_declare(queue=queue_name, passive=True)
        count = int(state.method.message_count)
        conn.close()
        return count


class RedisAdapter:
    def __init__(self, host: str, port: int, db: int) -> None:
        if redis is None:
            raise RuntimeError("Package 'redis' is not installed.")
        self.host = host
        self.port = port
        self.db = db

    def _client(self) -> Any:
        return redis.Redis(host=self.host, port=self.port, db=self.db, decode_responses=False)

    def prepare_queue(self, queue_name: str) -> None:
        cli = self._client()
        cli.delete(queue_name)

    def cleanup_queue(self, queue_name: str) -> None:
        cli = self._client()
        cli.delete(queue_name)

    def create_producer_client(self) -> Any:
        return self._client()

    def create_consumer_client(self) -> Any:
        return self._client()

    def close_client(self, client: Any) -> None:
        client.close()

    def publish(self, client: Any, queue_name: str, body: bytes) -> None:
        client.rpush(queue_name, body)

    def consume_once(self, client: Any, queue_name: str, timeout_sec: float = 1.0) -> bytes | None:
        item = client.blpop(queue_name, timeout=timeout_sec)
        if item is None:
            return None
        _, body = item
        return body

    def get_backlog(self, queue_name: str) -> int:
        cli = self._client()
        try:
            return int(cli.llen(queue_name))
        finally:
            cli.close()


def parse_int_list(raw: str) -> list[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def build_message(payload: str, producer_id: int, seq: int) -> bytes:
    msg = {
        "id": f"p{producer_id}-{seq}",
        "ts_ns": time.time_ns(),
        "payload": payload,
    }
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")


def extract_latency_ms(body: bytes) -> float:
    data = json.loads(body.decode("utf-8"))
    sent_ns = int(data["ts_ns"])
    return (time.time_ns() - sent_ns) / 1_000_000.0


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * p
    low = math.floor(idx)
    high = math.ceil(idx)
    if low == high:
        return ordered[int(idx)]
    return ordered[low] + (ordered[high] - ordered[low]) * (idx - low)


def producer_worker(
    adapter: Any,
    cfg: RunConfig,
    queue_name: str,
    payload: str,
    stats: RunStats,
    producer_id: int,
) -> None:
    client = adapter.create_producer_client()
    try:
        end_at = time.perf_counter() + cfg.duration_seconds
        total_rate = float(cfg.target_rate)
        my_rate = total_rate / max(cfg.producers, 1)
        interval = 1.0 / my_rate if my_rate > 0 else 0.0
        next_tick = time.perf_counter()
        seq = 0

        while time.perf_counter() < end_at:
            seq += 1
            body = build_message(payload, producer_id, seq)
            try:
                adapter.publish(client, queue_name, body)
                stats.record_send_ok()
            except Exception:
                stats.record_send_error()

            if interval > 0:
                next_tick += interval
                sleep_for = next_tick - time.perf_counter()
                if sleep_for > 0:
                    time.sleep(sleep_for)
    finally:
        adapter.close_client(client)


def consumer_worker(
    adapter: Any,
    queue_name: str,
    producer_finished: threading.Event,
    stats: RunStats,
) -> None:
    client = adapter.create_consumer_client()
    empty_polls = 0
    try:
        while True:
            try:
                body = adapter.consume_once(client, queue_name, timeout_sec=1.0)
            except Exception:
                stats.record_consume_error()
                continue

            if body is None:
                empty_polls += 1
                if producer_finished.is_set():
                    try:
                        backlog = adapter.get_backlog(queue_name)
                    except Exception:
                        backlog = 0
                    stats.observe_backlog(backlog)
                    if backlog == 0 and empty_polls >= 2:
                        break
                continue

            empty_polls = 0
            try:
                latency = extract_latency_ms(body)
                stats.record_consume_ok(latency)
            except Exception:
                stats.record_consume_error()
    finally:
        adapter.close_client(client)


def monitor_worker(
    adapter: Any,
    queue_name: str,
    stop_event: threading.Event,
    stats: RunStats,
) -> None:
    if psutil is not None:
        psutil.cpu_percent(interval=None)

    while not stop_event.is_set():
        try:
            backlog = adapter.get_backlog(queue_name)
            stats.observe_backlog(backlog)
        except Exception:
            pass

        if psutil is not None:
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory().percent
            stats.observe_system(cpu, ram)

        time.sleep(1.0)


def calc_result(cfg: RunConfig, stats: RunStats) -> dict[str, Any]:
    elapsed = max(stats.run_finished - stats.run_started, 0.001)
    avg_latency = statistics.mean(stats.latencies_ms) if stats.latencies_ms else 0.0
    p95_latency = percentile(stats.latencies_ms, 0.95)
    max_latency = max(stats.latencies_ms) if stats.latencies_ms else 0.0
    throughput = stats.consumed / elapsed
    losses = max(stats.sent - stats.consumed, 0)
    error_count = stats.send_errors + stats.consume_errors

    if stats.cpu_samples:
        cpu_avg = statistics.mean(stats.cpu_samples)
        cpu_max = max(stats.cpu_samples)
    else:
        cpu_avg = 0.0
        cpu_max = 0.0

    if stats.ram_samples:
        ram_avg = statistics.mean(stats.ram_samples)
        ram_max = max(stats.ram_samples)
    else:
        ram_avg = 0.0
        ram_max = 0.0

    return {
        "broker": cfg.broker,
        "scenario": cfg.scenario,
        "message_size_bytes": cfg.message_size,
        "target_rate_msg_sec": cfg.target_rate,
        "duration_sec": cfg.duration_seconds,
        "producers": cfg.producers,
        "consumers": cfg.consumers,
        "sent": stats.sent,
        "consumed": stats.consumed,
        "send_errors": stats.send_errors,
        "consume_errors": stats.consume_errors,
        "errors_total": error_count,
        "lost_messages": losses,
        "messages_per_sec": round(throughput, 2),
        "latency_avg_ms": round(avg_latency, 3),
        "latency_p95_ms": round(p95_latency, 3),
        "latency_max_ms": round(max_latency, 3),
        "max_backlog": stats.backlog_max,
        "cpu_avg_percent": round(cpu_avg, 2),
        "cpu_max_percent": round(cpu_max, 2),
        "ram_avg_percent": round(ram_avg, 2),
        "ram_max_percent": round(ram_max, 2),
        "degraded": "YES" if (stats.backlog_max > 0 or error_count > 0 or losses > 0) else "NO",
    }


def print_run_line(result: dict[str, Any]) -> None:
    line = (
        f"[{result['broker']}] {result['scenario']} | "
        f"size={result['message_size_bytes']} B rate={result['target_rate_msg_sec']} msg/s | "
        f"sent={result['sent']} consumed={result['consumed']} "
        f"mps={result['messages_per_sec']} p95={result['latency_p95_ms']}ms "
        f"errors={result['errors_total']} backlog_max={result['max_backlog']} "
        f"degraded={result['degraded']}"
    )
    print(line)


def run_single(cfg: RunConfig, adapter: Any) -> dict[str, Any]:
    queue_name = f"bench_{cfg.broker}_{uuid.uuid4().hex[:8]}"
    payload = "x" * max(cfg.message_size, 1)

    adapter.prepare_queue(queue_name)
    stats = RunStats(run_started=time.perf_counter())

    producer_finished = threading.Event()
    stop_monitor = threading.Event()

    monitor = threading.Thread(
        target=monitor_worker,
        args=(adapter, queue_name, stop_monitor, stats),
        daemon=True,
    )
    monitor.start()

    consumer_threads: list[threading.Thread] = []
    for _ in range(cfg.consumers):
        t = threading.Thread(
            target=consumer_worker,
            args=(adapter, queue_name, producer_finished, stats),
            daemon=True,
        )
        t.start()
        consumer_threads.append(t)

    producer_threads: list[threading.Thread] = []
    for i in range(cfg.producers):
        t = threading.Thread(
            target=producer_worker,
            args=(adapter, cfg, queue_name, payload, stats, i),
            daemon=True,
        )
        t.start()
        producer_threads.append(t)

    for t in producer_threads:
        t.join()

    producer_finished.set()

    for t in consumer_threads:
        t.join(timeout=cfg.duration_seconds + 30)

    stats.run_finished = time.perf_counter()
    stop_monitor.set()
    monitor.join(timeout=2)

    result = calc_result(cfg, stats)
    adapter.cleanup_queue(queue_name)
    return result


def choose_adapter(args: argparse.Namespace, broker: str) -> Any:
    if broker == "rabbitmq":
        return RabbitAdapter(
            host=args.rabbit_host,
            port=args.rabbit_port,
            user=args.rabbit_user,
            password=args.rabbit_password,
        )
    if broker == "redis":
        return RedisAdapter(
            host=args.redis_host,
            port=args.redis_port,
            db=args.redis_db,
        )
    raise ValueError(f"Unknown broker: {broker}")


def build_experiments(args: argparse.Namespace, broker: str) -> list[RunConfig]:
    runs: list[RunConfig] = []

    # 1) Baseline
    runs.append(
        RunConfig(
            broker=broker,
            scenario="baseline",
            message_size=args.baseline_size,
            target_rate=args.baseline_rate,
            duration_seconds=args.duration,
            producers=args.producers,
            consumers=args.consumers,
        )
    )

    # 2) Message size impact
    for size in args.sizes:
        runs.append(
            RunConfig(
                broker=broker,
                scenario="size_impact",
                message_size=size,
                target_rate=args.baseline_rate,
                duration_seconds=args.duration,
                producers=args.producers,
                consumers=args.consumers,
            )
        )

    # 3) Rate impact
    for rate in args.rates:
        runs.append(
            RunConfig(
                broker=broker,
                scenario="rate_impact",
                message_size=args.baseline_size,
                target_rate=rate,
                duration_seconds=args.duration,
                producers=args.producers,
                consumers=args.consumers,
            )
        )

    return runs


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown_summary(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = []
    lines.append("# Benchmark Results")
    lines.append("")
    lines.append("## Table")
    lines.append("")
    lines.append(
        "| broker | scenario | size(B) | rate(msg/s) | sent | consumed | mps | p95(ms) | max backlog | errors | lost | degraded |"
    )
    lines.append(
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )

    for row in rows:
        lines.append(
            "| {broker} | {scenario} | {message_size_bytes} | {target_rate_msg_sec} | {sent} | {consumed} | {messages_per_sec} | {latency_p95_ms} | {max_backlog} | {errors_total} | {lost_messages} | {degraded} |".format(
                **row
            )
        )

    lines.append("")
    lines.append("## Quick Notes")
    lines.append("")
    lines.append("- `degraded = YES` means there was backlog, errors, or message loss.")
    lines.append("- Compare brokers only inside equal scenarios (same size and rate).")

    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple RabbitMQ vs Redis benchmark")

    parser.add_argument("--broker", choices=["rabbitmq", "redis", "both"], default="both")
    parser.add_argument("--duration", type=int, default=15, help="Run duration (seconds) for each test")
    parser.add_argument("--producers", type=int, default=1)
    parser.add_argument("--consumers", type=int, default=1)

    parser.add_argument("--baseline-size", type=int, default=1024)
    parser.add_argument("--baseline-rate", type=int, default=5000)
    parser.add_argument("--sizes", type=parse_int_list, default=parse_int_list("128,1024,10240,102400"))
    parser.add_argument("--rates", type=parse_int_list, default=parse_int_list("1000,5000,10000"))

    parser.add_argument("--output-dir", default="results")

    parser.add_argument("--rabbit-host", default="127.0.0.1")
    parser.add_argument("--rabbit-port", type=int, default=5672)
    parser.add_argument("--rabbit-user", default="guest")
    parser.add_argument("--rabbit-password", default="guest")

    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=0)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    brokers = ["rabbitmq", "redis"] if args.broker == "both" else [args.broker]
    all_results: list[dict[str, Any]] = []

    for broker in brokers:
        print(f"\n=== Running experiments for {broker} ===")
        adapter = choose_adapter(args, broker)
        runs = build_experiments(args, broker)

        for cfg in runs:
            result = run_single(cfg, adapter)
            all_results.append(result)
            print_run_line(result)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"results_{timestamp}.csv"
    md_path = out_dir / f"summary_{timestamp}.md"

    write_csv(csv_path, all_results)
    write_markdown_summary(md_path, all_results)

    print(f"\nSaved CSV: {csv_path}")
    print(f"Saved Markdown summary: {md_path}")


if __name__ == "__main__":
    main()
