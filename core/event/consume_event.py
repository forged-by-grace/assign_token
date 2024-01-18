from core.helper.consumer_helper import consume_event
from core.utils.settings import settings
from core.utils.init_log import logger
from core.helper.token_helper import assign_token, invalidate_account_tokens, send_threat_notification, update_refresh_token, revoke_refresh_token
from core.model.token_models import AssignToken, ReusedToken, RevokeToken, UpdateToken

# Processing event msg
event_processing_msg = "Processing event"


async def consume_assign_token_event():
    # consume event
    consumer = await consume_event(topic=settings.api_assign_token, group_id=settings.api_assign_token)
    
    try:
        # Consume messages
        async for msg in consumer: 
            logger.info('Received assign token event.') 
            # Deserialize event
            assign_token_data = AssignToken.deserialize(data=msg.value)
            
            # Assign token
            logger.info(event_processing_msg)
            await assign_token(data = assign_token_data)
    except Exception as err:
        logger.error(f'Failed to process event due to error: {str(err)}')
    finally:
        await consumer.stop()

