"""Tests for topic partition configuration."""

from de_platform.pipeline.topics import (
    ALERTS,
    AUDIT_COUNTS,
    AUDIT_FILE_UPLOADS,
    DEFAULT_PARTITIONS,
    DUPLICATES,
    EXECUTIONS_PERSISTENCE,
    NORMALIZATION_ERRORS,
    ORDERS_PERSISTENCE,
    TOPIC_PARTITIONS,
    TRADE_NORMALIZATION,
    TRADES_ALGOS,
    TRANSACTIONS_ALGOS,
    TRANSACTIONS_PERSISTENCE,
    TX_NORMALIZATION,
    get_partitions,
)

# -- Partition count values ----------------------------------------------------


def test_high_throughput_topics_have_12_partitions():
    for topic in [
        TRADE_NORMALIZATION,
        TX_NORMALIZATION,
        ORDERS_PERSISTENCE,
        EXECUTIONS_PERSISTENCE,
        TRANSACTIONS_PERSISTENCE,
    ]:
        assert get_partitions(topic) == 12, f"{topic} should have 12 partitions"


def test_medium_throughput_topics_have_6_partitions():
    for topic in [TRADES_ALGOS, TRANSACTIONS_ALGOS, ALERTS, NORMALIZATION_ERRORS, DUPLICATES]:
        assert get_partitions(topic) == 6, f"{topic} should have 6 partitions"


def test_low_throughput_topics_have_3_partitions():
    for topic in [AUDIT_COUNTS, AUDIT_FILE_UPLOADS]:
        assert get_partitions(topic) == 3, f"{topic} should have 3 partitions"


def test_unknown_topic_returns_default():
    assert get_partitions("nonexistent_topic") == DEFAULT_PARTITIONS


def test_all_pipeline_topics_have_partition_config():
    """Every named topic constant should be in TOPIC_PARTITIONS."""
    expected_topics = [
        TRADE_NORMALIZATION,
        TX_NORMALIZATION,
        NORMALIZATION_ERRORS,
        DUPLICATES,
        ORDERS_PERSISTENCE,
        EXECUTIONS_PERSISTENCE,
        TRANSACTIONS_PERSISTENCE,
        TRADES_ALGOS,
        TRANSACTIONS_ALGOS,
        ALERTS,
        AUDIT_COUNTS,
        AUDIT_FILE_UPLOADS,
    ]
    for topic in expected_topics:
        assert topic in TOPIC_PARTITIONS, f"{topic} missing from TOPIC_PARTITIONS"


def test_default_partitions_is_3():
    assert DEFAULT_PARTITIONS == 3
