from typing_extensions import TypedDict


class TimeStep(TypedDict):
    days: int
    hours: int
    minutes: int
class StartDate(TypedDict):
    year: int
    month: int
    day: int
    hour: int