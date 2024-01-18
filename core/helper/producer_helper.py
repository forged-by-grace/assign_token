from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from core.utils.settings import settings


async def topic_exists(topic: str):
    try:
        admin_client = AIOKafkaAdminClient(bootstrap_servers=settings.api_event_streaming_host, client_id=settings.api_event_streaming_client_id)

        await admin_client.start()
        
        # Check if topic exists
        existing_topics = await admin_client.list_topics()
        if topic in existing_topics:
            return True
        return False    
       
    except Exception as err:
        print(str(err))
    finally:
        await admin_client.close()


async def create_topic(topic: str, partitions: int = 10, replication_factor: int = 3):
    try:
        admin_client = AIOKafkaAdminClient(bootstrap_servers=settings.api_event_streaming_host, client_id=settings.api_event_streaming_client_id)

        # Start the admin client    
        await admin_client.start()

        # Create a new topic
        topic_list = []
        topic_list.append(NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor))
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        return topic
    except Exception as err:
        print(str(err))
    finally:
        await admin_client.close()