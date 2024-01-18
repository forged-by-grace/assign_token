from pydantic import EmailStr, Field
from dataclasses_avroschema.pydantic import AvroBaseModel
from core.model.device_model import Device

id_description: str = "Used to identify the account"


class AssignToken(AvroBaseModel):
    id: str = Field(description=id_description)
    email: EmailStr = Field(description="Used to identify the account.")
    device_ip: str = Field(description='Used to track the device IP address')
    token: str = Field(description='Encrypted refresh token')
    device_info: Device = Field(description='Device meta data.')


class ReusedToken(AvroBaseModel):
    id: str = Field(description=id_description)


class UpdateToken(AvroBaseModel):
    id: str = Field(description='Used to identify the account')
    old_token: str = Field(description='Encrypted old refresh token')
    new_token: str = Field(description='Encrypted new refresh token')


class RevokeToken(AvroBaseModel):
    id: str = Field(description='A string used to identify an account')
    token: str = Field(description='A string representing the refresh token.')
    device_ip: str = Field(description='A string representing the IP address of the current device.')
