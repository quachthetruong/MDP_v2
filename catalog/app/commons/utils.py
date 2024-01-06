
from datetime import timedelta
from schemas.other import TimeStep,ScheduleDate
import re
# def format_timedelta(timestep:TimeStep, fmt:str):
#     return fmt.format(**d)

def generated_identified_name(signal_name:str, timestep:TimeStep, version:str):
    return "{}_{}_{}".format(signal_name, '{days}d{hours}h{minutes}m'.format(**timestep), version)

def camel_case(name:str):
    # Uppercase first letter after underscore and remove underscore
    name = re.sub(r"_(\w)", lambda m: m.group(1).upper(), name)
    # Uppercase first letter
    name = name[0].upper() + name[1:]
    return name
