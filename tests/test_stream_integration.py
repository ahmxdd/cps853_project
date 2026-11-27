import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
GAME_YEAR = 2024
GAME_PK = 745444
EXPECTED_PITCH_COUNT = 300
GAME_DATE = "2024-03-20"


def test_producer_consumer_stream_one_game():
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import KafkaTimeoutError, MetadataEmptyBrokerList

    data_dir = ROOT / "data" / "parquet"
    if not data_dir.is_dir():
        pytest.skip("Prepared data/parquet is required for the streaming integration test")

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVER,
            request_timeout_ms=5000,
        )
    except (KafkaTimeoutError, MetadataEmptyBrokerList) as exc:
        pytest.skip(f"Kafka broker is unavailable at {BOOTSTRAP_SERVER}: {exc}")

    try:
        try:
            admin.list_topics()
        except (KafkaTimeoutError, MetadataEmptyBrokerList) as exc:
            pytest.skip(f"Kafka broker is unavailable at {BOOTSTRAP_SERVER}: {exc}")

        topic = f"pitches-pytest-{uuid.uuid4().hex[:12]}"
        created = admin.create_topics([
            NewTopic(name=topic, num_partitions=1, replication_factor=1)
        ])
        assert created["topics"][0]["error_code"] == 0
        try:
            producer = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "src" / "stream" / "producer.py"),
                    "--data-dir", str(data_dir),
                    "--year", str(GAME_YEAR),
                    "--game-pk", str(GAME_PK),
                    "--topic", topic,
                    "--bootstrap-server", BOOTSTRAP_SERVER,
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=True,
                timeout=90,
            )
            assert f"Sent {EXPECTED_PITCH_COUNT} pitches" in producer.stdout

            consumer = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "src" / "stream" / "consumer.py"),
                    "--topic", topic,
                    "--bootstrap-server", BOOTSTRAP_SERVER,
                    "--group-id", f"pytest-{uuid.uuid4().hex}",
                    "--max-messages", str(EXPECTED_PITCH_COUNT),
                    "--no-db",      # earlier runs in the database must not affect this test
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=True,
                timeout=90,
            )
            assert (
                f"Consumed {EXPECTED_PITCH_COUNT} pitches; final counts by game date: "
                f"{{'{GAME_DATE}': {EXPECTED_PITCH_COUNT}}}; counts by game: "
                f"{{{GAME_PK}: {EXPECTED_PITCH_COUNT}}}"
            ) in consumer.stdout
        finally:
            deleted = admin.delete_topics([topic], timeout_ms=10000)
            assert deleted["topics"][0]["error_code"] == 0
    finally:
        admin.close()