import aiokafka
from core.connection.cache_connector import redis
import asyncio
import json
import sys
from core.utils.settings import settings


async def get_last_committed_offsets(redis, consumer_group, topic, partitions):
    key = f"{consumer_group}_{topic}_last_committed_offsets"
    serialized_offsets = await redis.get(key)
    if serialized_offsets:
        return json.loads(serialized_offsets.decode())
    else:
        return {str(partition): None for partition in partitions}

async def set_last_committed_offsets(redis, consumer_group, topic, committed_offsets):
    key = f"{consumer_group}_{topic}_last_committed_offsets"
    await redis.set(key, json.dumps(committed_offsets))

async def kafka_consumer_liveness_probe(bootstrap_servers, group_id, topic, redis_host, redis_port):
    consumer_conf = {
        'bootstrap_servers': bootstrap_servers,
        'group_id': group_id,
        'auto_offset_reset': 'earliest',  # Set according to your requirements
    }

    consumer = aiokafka.AIOKafkaConsumer(
        topic,
        loop=asyncio.get_event_loop(),
        **consumer_conf
    )
    

    try:
        await consumer.start()

        # Check if we can read the current offset
        partitions = consumer.partitions_for_topic(topic)
        current_offsets = await consumer.position(partitions)
        if not partitions or not current_offsets:
            print("Error: Unable to read current offset.")
            sys.exit(1)

        # Check if we can read the committed offset
        committed_offsets = await consumer.committed_offsets(partitions)
        if not committed_offsets or None in committed_offsets.values():
            print("Error: Unable to read committed offset.")
            sys.exit(1)

        # Get the last committed offsets
        last_committed_offsets = await get_last_committed_offsets(redis, group_id, topic, partitions)

        # Fail liveness probe if committed offset hasn't changed for any partition
        for partition in partitions:
            if last_committed_offsets[str(partition)] == committed_offsets[partition]:
                print(f"Error: Committed offset for partition {partition} has not changed since last run.")
                sys.exit(1)

        # Pass liveness probe if current offset equals committed offset for all partitions
        if current_offsets == committed_offsets:
            print("Liveness probe passed: Current offset equals committed offset.")
            sys.exit(0)

        # Save the current committed offsets for each partition for the next run
        await set_last_committed_offsets(redis, group_id, topic, committed_offsets)

        while True:
            msg = await consumer.getone()

            # Process the received message as needed
            print(f"Received message: {msg.value.decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        await consumer.stop()
        redis.close()
        await redis.wait_closed()

if __name__ == "__main__":
    # Replace these values with your Kafka broker, consumer group, and topic
    bootstrap_servers = settings.api_event_streaming_host
    group_id = settings.api_assign_token
    topic = settings.api_assign_token
   
    asyncio.run(kafka_consumer_liveness_probe(bootstrap_servers, group_id, topic))
