from dataclasses_avroschema.pydantic import AvroBaseModel
from pydantic import Field
from typing import Dict, List
from datetime import datetime
from core.utils.settings import settings


class UpdateFieldAvro(AvroBaseModel):
   action: str
   value: Dict[str, str | int | bool | List[str | None] | datetime | Dict[str, str | int | bool | datetime]] = Field(description='This is the new value to be updated')


class UpdateAvro(AvroBaseModel):
   db_metadata: Dict[str, str] = Field(description='This is used to specify the name of the mongoDB database and collection.')
   db_filter: Dict[str, str] = Field(description='This is used to match the document to be updated')
   updates: List[UpdateFieldAvro] = Field(description='Update to be made')
