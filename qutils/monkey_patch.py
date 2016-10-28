from datetime import timedelta

import pandas as pd


def timedelta_to_human(self: timedelta, short=True, precision=1, unit=None) -> str:
    secs = self.total_seconds()
    negative = secs < 0
    secs = abs(secs)
    magnitudes = [(86400, ('day',)), (3600, ('hour',)), (60, ('min', 'minute')), (0, ('sec', 'second'))]
    if unit is None:
        for threshold, unit_strs in magnitudes:
            if secs >= threshold:
                val = secs / threshold
                unit = unit_strs[0 if short else 1]
                break
        else:
            raise ValueError('No proper unit can be found for "{!r}"'.format(self))
    else:
        if unit.endswith('s'):
            unit = unit[:-1]
        for threshold, unit_strs in magnitudes:
            if unit in unit_strs:
                val = secs / threshold
                break
        else:
            raise ValueError('Unrecognized time unit "{}" for "{!r}"'.format(unit, self))
    val = round(val, precision)
    if val > 1:
        unit += 's'
    return '{} {}{}'.format(val, unit, ' ago' if negative else '')


pd.Timedelta.to_human = timedelta_to_human
