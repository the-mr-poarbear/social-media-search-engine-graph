import json
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.config import settings


def _serialize(value: dict) -> bytes:
    return json.dumps(value).encode("utf-8")


def _deserialize(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


@asynccontextmanager
async def get_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=_serialize,
    )
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


def make_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=group_id,
        value_deserializer=_deserialize,
        enable_auto_commit=False,  # commit only after DB write succeeds
        auto_offset_reset="earliest",
        max_poll_interval_ms=650000,
    )
