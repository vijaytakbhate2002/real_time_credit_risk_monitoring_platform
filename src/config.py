"""
config.py
─────────
Immutable configuration dataclasses for the CreditFlow streaming pipeline.

Usage
-----
    from src.config import KafkaConfig, ValidationStreamConfig

    cfg = ValidationStreamConfig(
        kafka=KafkaConfig(bootstrap_servers="broker:9092"),
        checkpoint_location="/mnt/checkpoints/validation",
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ── Kafka ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KafkaConfig:
    """Kafka connection and topic configuration."""

    bootstrap_servers: str = "localhost:9092"

    # Topics
    raw_topic: str = "application-events"
    validated_topic: str = "validated-events"
    dead_letter_topic: str = "dead-letter-events"

    # Consumer group used by the Validation Stream only.
    # Data Store and Compute streams have their own groups defined in their configs.
    consumer_group: str = "creditflow-validator"

    # "latest" for production; "earliest" for local dev / replay
    starting_offsets: str = "latest"

    # Kafka producer options (applied when writing back to Kafka)
    producer_acks: str = "all"
    producer_retries: int = 3


# ── Validation Stream ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ValidationStreamConfig:
    """Full configuration for the Validation Streaming query."""

    kafka: KafkaConfig = field(default_factory=KafkaConfig)

    # Spark application name shown in the Spark UI
    app_name: str = "CreditFlow-ValidationStream"

    # Where Spark stores streaming offsets and state (must be persistent in production)
    checkpoint_location: str = "checkpoints/validation"

    # Trigger interval in seconds (how often Spark processes a micro-batch)
    trigger_interval_seconds: int = 10

    # Spark / JVM log verbosity: "ERROR", "WARN", "INFO", "DEBUG"
    spark_log_level: str = "WARN"
