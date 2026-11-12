from prometheus_client import Counter, Histogram

ATTEMPTS = Counter(
    "payments_attempts_total",
    "Provider create attempts",
    ["provider", "outcome", "failure_class"],
)
CASCADE_DEPTH = Histogram(
    "payments_cascade_depth", "Attempts per cascade run", buckets=(1, 2, 3, 4, 5, 8)
)
LATENCY = Histogram(
    "payments_provider_latency_seconds",
    "Provider call latency",
    ["provider", "op"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 4, 8, 15),
)
TASK_FAILED = Counter("payments_task_failed_total", "Tasks failed after retries", ["task"])


class PrometheusMetrics:
    def attempt(self, provider: str, outcome: str, failure_class: str) -> None:
        ATTEMPTS.labels(provider, outcome, failure_class).inc()

    def latency(self, provider: str, op: str, seconds: float) -> None:
        LATENCY.labels(provider, op).observe(seconds)

    def cascade_depth(self, depth: int) -> None:
        CASCADE_DEPTH.observe(depth)
