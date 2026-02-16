from pydantic import BaseModel, Field, validator
from datetime import datetime
from uuid import uuid4
from enum import Enum
from typing import Optional, Dict, Any

class EventType(str, Enum):
    USER_SIGNUP = "user_signup"
    PAYMENT_SUCCESS = "payment_success"
    SYSTEM_ALERT = "system_alert"
    DATA_INGEST = "data_ingest"

class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    type: EventType
    payload: Dict[str, Any]
    priority: int = Field(default=1, ge=1, le=5)

    @validator('priority')
    def validate_priority(cls, v):
        if not (1 <= v <= 5):
            raise ValueError("Priority must be between 1 and 5")
        return v
