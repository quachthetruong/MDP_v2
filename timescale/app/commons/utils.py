
from datetime import datetime, timedelta
import re



from schemas.other import TimeStep,StartDate

# def format_timedelta(timestep:TimeStep, fmt:str):
#     t_delta = timedelta(**timestep)
#     d = {"days": t_delta.days}
#     d["hours"], rem = divmod(t_delta.seconds, 3600)
#     d["minutes"], d["seconds"] = divmod(rem, 60)
#     return fmt.format(**d)

def generated_identified_name(signal_name:str, timestep:TimeStep, version:str):
    return "{}_{}_{}".format(signal_name, '{days}d{hours}h{minutes}m'.format(**timestep), version)

def convert_datetime_to_dict(timestamp:datetime):
    return {"year": timestamp.year,"month": timestamp.month,"day": timestamp.day, "hour":timestamp.hour}

def camel_case(name:str):
    # Uppercase first letter after underscore and remove underscore
    name = re.sub(r"_(\w)", lambda m: m.group(1).upper(), name)
    # Uppercase first letter
    name = name[0].upper() + name[1:]
    return name
