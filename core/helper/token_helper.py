from core.model.update_model import UpdateFieldAvro, UpdateAvro
from core.model.token_models import AssignToken
from core.model.device_model import AddDevice

from core.enums.token_enum import Role
from core.event.produce_event import produce_event

from core.utils.settings import settings
from core.utils.init_log import logger

from datetime import datetime


async def assign_token(data: AssignToken):
    # Create database field obj
    token_field = UpdateFieldAvro(action='$push', value={'tokens': data.token})
    is_active_field = UpdateFieldAvro(action='$set', value={'is_active': True})
    active_device_count_field = UpdateFieldAvro(action='$inc', value={'active_device_count': 1})
    active_devices_field = UpdateFieldAvro(action='$push', value={'active_devices': data.device_ip})
    role_field = UpdateFieldAvro(action='$set', value={'role.name':  Role.authenticated.value})
    disabled_field = UpdateFieldAvro(action='$set', value={'disabled': False})
    last_update_field = UpdateFieldAvro(action='$set', value={'last_update': datetime.utcnow().isoformat()})

    # Create update list
    account_updates = UpdateAvro(
        db_metadata={'provider': 'mongoDB', 
                     'database': 'account_db', 
                     'collection': 'accounts'},
        db_filter={'_id': data.id},
        updates=[
            token_field, 
            is_active_field, 
            active_device_count_field, 
            active_devices_field, 
            role_field,
            disabled_field, 
            last_update_field
        ]
    )

    # Convert to dict
    device_dict = data.device_info.model_dump()
    
    # Add email to device data
    device_dict.update({'email': data.email})

    # Create new device obj
    new_device = AddDevice(**device_dict)
    new_device_event = new_device.serialize()
    
    logger.info('Emitting add new device event.')
    await produce_event(topic=settings.api_add_device, value=new_device_event)
    
    # Emit update event
    await emit_update_event(account_updates=account_updates)


async def emit_update_event(account_updates: UpdateAvro) -> None:
    # Serialize    
    account_updates_event = account_updates.serialize()

    # Emit event
    logger.info('Emitting update account event')
    await produce_event(topic=settings.api_update_account, value=account_updates_event)
