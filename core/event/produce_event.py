from aiokafka import AIOKafkaProducer
from core.utils.settings import settings
import uuid
from core.helper.producer_helper import topic_exists, create_topic


async def produce_event(topic: str, value, key: str = str(uuid.uuid4()), headers: tuple | None = None) -> None:
    try:
        # Get the producer
        producer = AIOKafkaProducer(
            client_id=settings.api_event_streaming_client_id,
            bootstrap_servers=settings.api_event_streaming_host, 
        )

        # Start the producer and get brokers metedata
        await producer.start()

        # Check if topic already exists
        topic_exist = await topic_exists(topic=topic)
        if not topic_exist:
            await create_topic(topic=topic)

        # Produce message
        await producer.send_and_wait(key=key.encode(), value=value, topic=topic, headers=headers)

    except Exception as err:
        print(f"Am causing: {err}")
    finally:
        await producer.stop()