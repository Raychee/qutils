from datetime import datetime, timedelta

import pandas as pd
import re


def to_seconds(timedelta_str):
    return to_timedelta(timedelta_str).total_seconds()


def to_timedelta(timedelta_repr):
    return pd.to_timedelta(str(timedelta_repr), unit='s')


def to_datetime(time_repr, from_datetime=None, if_invalid='use_now'):
    """
    :param time_repr: A timedelta object / timedelta literal / datetime object / datetime literal
    :param from_datetime: base datetime when time_repr is an offset (timedelta)
    :return: A datetime object
    """
    def get_from_datetime():
        ret = from_datetime
        if callable(ret):
            ret = ret()
        if not ret:
            if if_invalid == 'raise':
                raise ValueError('Cannot convert to datetime {!r} from {!r}'
                                 .format(time_repr, from_datetime))
            elif if_invalid == 'return_none':
                return None
            elif if_invalid == 'use_now':
                return pd.Timestamp.now()
            else:
                raise NotImplementedError('Unsupported fallback when invalid: {!r}'
                                          .format(if_invalid))
        return ret

    if time_repr is None:
        return get_from_datetime()
    if isinstance(time_repr, pd.Timestamp):
        return time_repr
    if isinstance(time_repr, datetime):
        return pd.to_datetime(time_repr)
    if isinstance(time_repr, str):
        time_repr = str(time_repr)
        if time_repr.lower() == 'now':
            return get_from_datetime()
        if time_repr.isdigit() or re.match('\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', time_repr):
            try:  # assume it is a datetime object
                return pd.to_datetime(time_repr, errors='raise')
            except (ValueError, TypeError):
                pass
    # it must be a timedelta
    # the following line works no matter it is already a timedelta object or a literal
    tdelta = to_timedelta(time_repr)
    _from_datetime = get_from_datetime()
    if _from_datetime is None:
        return None
    return _from_datetime + tdelta


def ensure_timestamps(timestamps, func_get_latest_time=None, if_fail='ignore'):
    t_is_not_timestamp = [t is None or isinstance(t, (str, pd.Timedelta, timedelta))
                          for t in timestamps]
    if any(t_is_not_timestamp):
        if t_is_not_timestamp[-1]:
            last_timestamp = to_datetime(timestamps[-1], from_datetime=func_get_latest_time,
                                         if_invalid='return_none')
            if last_timestamp is None:
                if if_fail == 'ignore':
                    return []
                elif if_fail == 'raise':
                    raise ValueError('Cannot convert timestamps {!r}'.format(timestamps))
                else:
                    raise NotImplementedError('Unsupported handling method when fail: {}'
                                              .format(if_fail))
            timestamps[-1] = last_timestamp
        else:
            last_timestamp = timestamps[-1]
        timestamps[:-1] = [to_datetime(t, from_datetime=last_timestamp) if is_not_timestamp else t
                           for t, is_not_timestamp in zip(timestamps[:-1], t_is_not_timestamp[:-1])]
    # if all(t == timestamps[0] for t in timestamps[1:]):
    #     timestamps = [timestamps[0]]
    return timestamps
