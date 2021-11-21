import re
from string import Template
from typing import Tuple
from datetime import timedelta


_strpdelta_regex = re.compile(r'((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?\.?\d+?)s)?')
_strpwell_regex = re.compile(r'([a-z]+)([0-9]+)', re.I)

class _DeltaTemplate(Template):
    delimiter = "%"


def strfdelta(td: timedelta, fmt: str):
    d = {"D": td.days}
    d["H"], rem = divmod(td.seconds, 3600)
    d["M"], d["S"] = divmod(rem, 60)
    t = _DeltaTemplate(fmt)
    return t.substitute(**d)


def strpdelta(s: str):
    parts = _strpdelta_regex.match(s)
    if not parts:
        raise ValueError(s)
    time_params = dict([(k, float(v)) for k, v in parts.groupdict().items() if v])
    return timedelta(**time_params)


def strffdelta(td: timedelta) -> str:
    if td.days > 0:
        return strfdelta(td, "%{D}d%{H}h%{M}m%{S}s")
    elif td.seconds > 3600:
        return strfdelta(td, "%{H}h%{M}m%{S}s")
    elif td.seconds > 60:
        return strfdelta(td, "%{M}m%{S}s")
    else:
        return strfdelta(td, "%{S}s")


def strpwell(w: str) -> Tuple[str, int]:
    if not isinstance(w, str):
        raise KeyError(w)
    match = _strpwell_regex.match(w)
    if not match or len(match.groups()) != 2:
        raise KeyError(w)
    row, column = match.groups()
    return row, int(column)
