"""Environment-driven configuration.

Every tunable in FlowMesh is declared here and nowhere else. Nothing outside
`backend/config/` reads `os.environ` directly, so the full set of knobs a
deployment can turn is one readable file.

Field names mirror their environment variables exactly, lowercased
(`event_transport` <- `FLOWMESH_EVENT_TRANSPORT`), so there is never a
translation layer to get wrong.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings, loaded from the environment and `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        # Every variable is namespaced `FLOWMESH_`. Without the prefix,
        # `LOG_LEVEL`, `DATABASE_URL` and `REDIS_URL` would be read out of the
        # host environment, and a developer's shell would silently change how
        # the pipeline behaves depending on which directory they launched it
        # from.
        env_prefix="FLOWMESH_",
    )

    # --- Application ---
    app_name: str = "FlowMesh"
    app_env: str = "development"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"
    log_level_json: bool = False

    # --- Database (system of record) ---
    # Defaults to the compose service name so `docker compose up` needs no
    # override. `database_url_sqlite` is the same models against a file, used
    # by the load test, the chaos test and the local single-process run.
    database_url: str = "postgresql+asyncpg://flowmesh:flowmesh@postgres:5432/flowmesh"
    database_url_sqlite: str = "sqlite+aiosqlite:///./data/runtime/flowmesh.sqlite3"
    # Used by Alembic, which runs synchronously and cannot use asyncpg. Derived
    # from database_url when not set explicitly.
    database_url_sync: str = ""
    db_echo: bool = False
    db_pool_size: int = 5
    db_max_overflow: int = 10

    # --- Auth ---
    jwt_secret: str = "dev-only-insecure-secret-change-me"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    # Auth is entirely optional. With auth disabled the API runs open, which is
    # the only sensible posture for a local `docker compose up` demo -- and the
    # ONLY posture that is never acceptable in a real deployment. Startup
    # refuses to disable auth when app_env == "production".
    auth_enabled: bool = True

    # --- Kafka: the event log ---
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_order_topic: str = "order-events"
    kafka_inventory_topic: str = "inventory-events"
    # Six partitions is two more than the default consumer concurrency this
    # project runs, so consumer scaling is actually possible -- the stretch
    # goal of "autoscale consumers on partition lag" needs headroom to exist.
    kafka_partitions: int = 6
    # How the pipeline reaches the event log:
    #   "kafka"   always dial the broker; fail if it is not there
    #   "memory"  use the in-process partitioned log (same offsets, same groups,
    #             same replay -- see docs/adr/005-in-memory-log-not-a-mock.md)
    #   "auto"    dial it, and fall back to the in-process log if the broker is
    #             unreachable. The default, so one process is a working dev setup.
    event_transport: str = "auto"

    # --- RabbitMQ: the task queue ---
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    review_queue: str = "review-queue"
    review_dead_letter_queue: str = "review.dlq"
    notification_queue: str = "notification-queue"
    # Prefetch 32 per consumer. Higher trades memory for throughput and makes
    # prefetch-driven redelivery storms much harder to reason about.
    rabbitmq_prefetch: int = 32
    #   "rabbitmq" always dial the broker; fail if it is not there
    #   "memory"   use the in-process queue (real ack/nack/dead-letter)
    #   "auto"     dial it, fall back to the in-process queue if unreachable
    queue_transport: str = "auto"

    # --- gRPC fraud scoring ---
    grpc_server_host: str = "0.0.0.0"
    grpc_server_port: int = 50052
    grpc_client_target: str = "fraud:50052"
    # 2 seconds, not 30. This is a per-order decision on the hot path with a
    # p95 budget of 200ms; a deadline far above the budget converts a slow
    # dependency into a stalled consumer and a growing lag spike.
    grpc_timeout_seconds: float = 2.0
    # How the order processor reaches the scorer: "grpc" | "in_process" | "auto"
    fraud_transport: str = "auto"

    # --- Fraud model ---
    fraud_model_path: str = "data/model/fraud_weights.json"
    fraud_low_threshold: float = 0.35
    fraud_high_threshold: float = 0.65
    # Window over which per-customer velocity features are counted.
    fraud_velocity_window_seconds: int = 900

    # --- LLM reasoner (ambiguous band only) ---
    # Off by default. The deterministic reasoner explains the same score with no
    # network and no key, which is what keeps the p95 budget: an LLM call is
    # 300ms of best case and a tail with no upper bound. See ADR-006.
    llm_enabled: bool = False
    llm_model: str = "gpt-4o-mini"
    llm_api_key: str = ""
    llm_base_url: str = ""
    llm_timeout_seconds: float = 3.0
    llm_max_tokens: int = 200

    # --- Inventory ---
    # How long a reservation holds stock before it expires and the units return
    # to available. Without this, an order held for review holds its stock
    # forever and the catalogue quietly undersells.
    inventory_reservation_ttl_seconds: int = 900
    review_sla_seconds: int = 3600

    # --- Redis (rate limiting + idempotency-key cache) ---
    redis_url: str = "redis://redis:6379/0"
    rate_limit_per_minute: int = 600

    # --- Observability ---
    metrics_enabled: bool = True
    # Port each worker serves `/metrics` on. The API uses its own (8000); the
    # consumers and the scorer use this one, because a worker has no web framework
    # and a counter nobody scrapes is a counter nobody reads.
    #
    # Distinct per role in compose (see `docker-compose.yml`), so two workers on one
    # host do not collide. A single shared default would be a port conflict on
    # `docker compose --scale orders=2`.
    metrics_port: int = 50052
    # The scorer is the one process that serves gRPC *and* HTTP on two ports,
    # because Prometheus speaks HTTP and gRPC scoring speaks gRPC. `metrics_port`
    # above defaults to the same number as `grpc_server_port`, so reusing it here
    # binds two listeners to one address: the gRPC `add_insecure_port` then
    # reports failure while `serve_metrics` has already claimed the socket, and
    # the scorer exits at startup with an error that names neither port.
    # Verified, not hypothetical -- `test_the_scores_metrics_port_and_its_grpc_port_differ`
    # asserts the pair, because this collision was live in compose.
    fraud_metrics_port: int = 50055
    # Histogram buckets chosen for the 200ms scoring budget, not for general
    # use: default Prometheus buckets put 8 of 10 lines between 5ms and 100ms
    # and cannot resolve a 200ms p95 at all.
    scoring_latency_buckets: tuple[float, ...] = (
        0.005,
        0.010,
        0.025,
        0.050,
        0.075,
        0.100,
        0.150,
        0.200,
        0.300,
        0.500,
        1.000,
        2.000,
    )

    @field_validator("log_level")
    @classmethod
    def _upper_log_level(cls, value: str) -> str:
        return value.upper()

    @field_validator("event_transport")
    @classmethod
    def _known_event_transport(cls, value: str) -> str:
        allowed = {"auto", "kafka", "memory"}
        if value not in allowed:
            raise ValueError(f"event_transport must be one of {sorted(allowed)}, got {value!r}")
        return value

    @field_validator("queue_transport")
    @classmethod
    def _known_queue_transport(cls, value: str) -> str:
        allowed = {"auto", "rabbitmq", "memory"}
        if value not in allowed:
            raise ValueError(f"queue_transport must be one of {sorted(allowed)}, got {value!r}")
        return value

    @field_validator("fraud_transport")
    @classmethod
    def _known_fraud_transport(cls, value: str) -> str:
        allowed = {"auto", "grpc", "in_process"}
        if value not in allowed:
            raise ValueError(f"fraud_transport must be one of {sorted(allowed)}, got {value!r}")
        return value

    @model_validator(mode="after")
    def _thresholds_ordered(self) -> Settings:
        # A band whose edges have crossed does not fail loudly -- every score
        # silently takes one branch and the "ambiguous" band never runs, so
        # the LLM path looks untested and stays untested.
        if not 0.0 <= self.fraud_low_threshold < self.fraud_high_threshold <= 1.0:
            raise ValueError(
                "fraud thresholds must satisfy 0 <= low < high <= 1, got "
                f"low={self.fraud_low_threshold} high={self.fraud_high_threshold}"
            )
        return self

    @model_validator(mode="after")
    def _scorer_ports_differ(self) -> Settings:
        # The scorer is the only process binding two listeners, so it is the only
        # place this pair matters -- but it is a *startup* failure, discovered on a
        # deploy, with a bind error that reads like a port already in use rather
        # than like two services configured on top of each other.
        if self.fraud_metrics_port == self.grpc_server_port:
            raise ValueError(
                "the scorer serves HTTP /metrics and gRPC scoring from one process, "
                f"so fraud_metrics_port ({self.fraud_metrics_port}) must differ from "
                f"grpc_server_port ({self.grpc_server_port})"
            )
        return self

    @property
    def effective_database_url_sync(self) -> str:
        """Synchronous SQLAlchemy URL for Alembic and other blocking drivers.

        Derived from `database_url` by swapping the async driver for a blocking
        one when not set explicitly, so there is exactly one place a database
        DSN is configured.
        """
        if self.database_url_sync:
            return self.database_url_sync
        return (
            self.database_url.replace("+asyncpg", "")
            .replace("postgresql+psycopg2", "postgresql")
            .replace("+aiosqlite", "")
            .replace("sqlite+aiosqlite", "sqlite")
        )

    @property
    def ambiguous_fraud_band(self) -> tuple[float, float]:
        """The band that costs an LLM call, as `(low, high)`."""
        return (self.fraud_low_threshold, self.fraud_high_threshold)

    @property
    def is_sqlite(self) -> bool:
        """True when the configured database is SQLite.

        Three code paths branch on this -- the reservation lock, the engine
        arguments and the migration's dialect checks -- and a branch that has
        forgotten to check is how a "works on my machine" database bug survives
        review. One property, so there is one place to look.
        """
        return self.database_url.startswith("sqlite")

    @property
    def effective_database_url_async(self) -> str:
        """The async URL to actually use.

        `database_url` is the deployment's URL. Tests and the load test need a
        different one -- a scratch database -- and this is the single place that
        decides, so no caller reaches into `database_url` directly and sets up a
        SQLite URL of its own.
        """
        return self.database_url

    @property
    def storage_kind(self) -> str:
        """`"postgres"` or `"sqlite"`, for the few places that must branch.

        Named `storage_kind` rather than a boolean because every call site reads
        better as `if settings.storage_kind == "sqlite"` than as
        `if settings.is_sqlite`, and because "which backend" is a question with
        more than two future answers.
        """
        return "sqlite" if self.is_sqlite else "postgres"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings singleton.

    `lru_cache` because settings are immutable for the life of a process and
    re-parsing `.env` per request would be both wasteful and a source of
    confusing mid-process config changes. Tests that need to vary settings call
    `reload_settings()` first.
    """
    return Settings()


def reload_settings() -> Settings:
    """Clear the cache and re-read the environment.

    Exists for tests and for the one-off scripts that mutate the environment
    before touching anything (`backend/scripts/loadtest.py` points the pipeline at
    a scratch database).
    """
    get_settings.cache_clear()
    return get_settings()
