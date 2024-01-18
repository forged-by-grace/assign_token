from dataclasses_avroschema.pydantic import AvroBaseModel
from pydantic import EmailStr, Field

from typing import Optional

class Screen(AvroBaseModel):
    height: int = Field(description='An integer representing the device screen height',
                        examples=[1920])
    width: int = Field(description='An integer representing the device width',
                       examples=[720])
    resolution: Optional[int] = Field(description='An integer representing the device resolution',
                            examples=[1200])

class Device(AvroBaseModel):
    device_name: str = Field(description='A string representing the user device name', 
                             examples=['Samsong s23 ultra', 'iphone 15 pro max'])
    platform: str = Field(description='A string representing the device platform',
                          examples=['IOS', 'Android', 'Web', 'MacOS', 'Linux', 'Windows'])
    ip_address: str = Field(description='A string representing the device ipv4 address',
                            examples=['127.0.0.1'])
    device_model: str = Field(description='A string representing the device model')
    device_id: str = Field(description='A string representing the device id')
    screen_info: Screen = Field(description='A dict containing the device screen meta data')
    device_serial_number: Optional[str] = Field(description='An optional string representing the device serial number',
                                      examples=['124578963'])
    is_active: bool = Field(description='A boolean used to track the login state of the device')


class AddDevice(Device):
    email: EmailStr = Field(description='An email str used to create an account')
