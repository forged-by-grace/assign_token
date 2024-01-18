
from aiokafka import AIOKafkaConsumer
from core.utils.settings import settings

async def consume_event(topic: str, group_id: str):
    # Initialize consumer
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=settings.api_event_streaming_host, group_id=group_id)

    # Start listening for messages
    await consumer.start()

    return consumer



    
