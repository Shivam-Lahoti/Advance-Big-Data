from pydantic import BaseModel

class DataModel(BaseModel):
    key: str
    value: dict