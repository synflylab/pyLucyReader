import re
from string import Template
from typing import Tuple


class DeltaTemplate(Template):
    delimiter = "%"


def strfdelta(td, fmt):
    d = {"D": td.days}
    d["H"], rem = divmod(td.seconds, 3600)
    d["M"], d["S"] = divmod(rem, 60)
    t = DeltaTemplate(fmt)
    return t.substitute(**d)


def _str_pp_delta(td):
    if td.days > 0:
        return strfdelta(td, "%{D}d%{H}h%{M}m%{S}s")
    elif td.seconds > 3600:
        return strfdelta(td, "%{H}h%{M}m%{S}s")
    elif td.seconds > 60:
        return strfdelta(td, "%{M}m%{S}s")
    else:
        return strfdelta(td, "%{S}s")


def _split_well(w: str) -> Tuple[str, int]:
    if not isinstance(w, str):
        raise KeyError(w)
    match = re.match(r"([a-z]+)([0-9]+)", w, re.I)
    if not match or len(match.groups()) != 2:
        raise KeyError(w)
    row, column = match.groups()
    return row, int(column)
