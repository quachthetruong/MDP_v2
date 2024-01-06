from typing_extensions import TypedDict


class TimeStep(TypedDict):
    days: int
    hours: int
    minutes: int
class ScheduleDate(TypedDict):
    year: int
    month: int
    day: int
    hour: int