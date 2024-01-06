
from typing import Any, Generic, List, TypeVar
from typing_extensions import TypedDict

from pydantic import BaseModel


class TimeStep(TypedDict):
    days: int
    hours: int
    minutes: int


class ScheduleDate(TypedDict):
    year: int
    month: int
    day: int
    hour: int



class ValidateForm(BaseModel):
    name: str
    timeStep: TimeStep


