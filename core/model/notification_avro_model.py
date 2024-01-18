from dataclasses_avroschema.pydantic import AvroBaseModel
from pydantic import Field
from typing import Dict, Optional


class Notification(AvroBaseModel):
     recipient_name: str = Field(description='Recipient account firstname')
     send_to: str = Field(description='Email or phone number used for notification')
     content: Optional[Dict[str, str | int]] = Field(description="The notification body")
     channel: str = Field(description='Channel to send the nofication')
     template: str = Field(description='Email template to use')
      
