from pydantic import BaseModel


class ApplyRequest(BaseModel):
    stream_id: int


class StreamPatch(BaseModel):
    enabled: bool | None = None


class SettingPut(BaseModel):
    value: str


class EntryPatch(BaseModel):
    enabled: bool | None = None


class FanInRequest(BaseModel):
    entry_ids: list[int] | None = None
    max_per_entry: int = 0


class FanInResetRequest(BaseModel):
    entry_ids: list[int] | None = None
