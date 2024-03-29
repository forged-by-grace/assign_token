from core.connection.cache_connector import redis
import asyncio
import json
import sys
from core.utils.settings import settings
from core.utils.init_log import logger
from core.helper.consumer_helper import consume_event

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

async def kafka_consumer_liveness_probe(group_id, topic):
    
    consumer = await consume_event(topic=topic, group_id=settings.api_assign_token)

    try:
        await consumer.start()

        # Check if we can read the current offset
        partitions = consumer.assignment()
        
        if len(partitions) > 1:
            for partition in partitions:                
                if partition is None:
                    logger.error("Error: Unable to read current offset.")
                    sys.exit(1)

                current_offsets = await consumer.position(partition)
                if not current_offsets:
                    logger.error("Error: Unable to read current offset.")
                    sys.exit(1)

            # Check if we can read the committed offset
            for partition in partitions:
                committed_offsets = await consumer.committed(partition)
                if not committed_offsets:
                    logger.error("Error: Unable to read committed offset.")
                    sys.exit(1)

            # Get the last committed offsets
            last_committed_offsets = await get_last_committed_offsets(redis, group_id, topic, partitions)

            # Fail liveness probe if committed offset hasn't changed for any partition
            for partition in partitions:
                if last_committed_offsets[str(partition)] == committed_offsets[partition]:
                    logger.error(f"Error: Committed offset for partition {partition} has not changed since last run.")
                    sys.exit(1)

            # Pass liveness probe if current offset equals committed offset for all partitions
            if current_offsets == committed_offsets:
                logger.error("Liveness probe passed: Current offset equals committed offset.")
                sys.exit(0)

            # Save the current committed offsets for each partition for the next run
            await set_last_committed_offsets(redis, group_id, topic, committed_offsets)

            while True:
                msg = await consumer.getone()

                # Process the received message as needed
                logger.info(f"Received message: {msg.value.decode('utf-8')}")
        else:
            sys.exit(0)
    except KeyboardInterrupt:
        pass
    finally:
        await consumer.stop()
        await redis.aclose()        

if __name__ == "__main__":
    group_id = settings.api_assign_token
    topic = settings.api_assign_token
   
    asyncio.run(kafka_consumer_liveness_probe(group_id, topic))
