import collections
import json
import logging
import os
import re
import socket
import time
from copy import copy
from datetime import datetime, timedelta
from multiprocessing.dummy import Pipe, Process

import pandas as pd
import yaml


logger = logging.getLogger(__name__)


class lazy(object):
    def __init__(self, func_with_no_args):
        super(lazy, self).__init__()
        self.func = func_with_no_args
        self.called = False
        self.res = None

    def __call__(self):
        if not self.called:
            self.res = self.func()
            self.called = True
        return self.res


class timeit(object):
    def __init__(self, title='', output_fn=print):
        self.title = title
        self.output_fn = output_fn

    def __enter__(self):
        if callable(self.output_fn):
            self.output_fn('Start {}.'.format(self.title))
        self.time_start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.time_end = time.time()
        self.interval = self.time_end - self.time_start
        if callable(self.output_fn):
            self.output_fn('Done {}. Took {} seconds.'.format(self.title, self.interval))


def is_single_instance(port):
    this, that = Pipe()

    def try_binding_port(that):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('', port))
            s.listen(1)
            that.send(True)
            s.accept()
        except socket.error:
            that.send(False)

    process = Process(target=try_binding_port, args=(that,))
    process.daemon = True
    process.start()
    return this.recv()


def pretty_str_mongo_collection(collection):
    database = collection.database
    client = database.connection
    return 'mongodb://{}:{}/{}/{}'.format(client.host, client.port, database.name, collection.name)


def load_yaml(yaml_path):
    with open(yaml_path, 'r') as f:
        loaded = yaml.load(f.read())
    return loaded


def load_json(json_path):
    with open(json_path, 'r') as f:
        loaded = json.load(f)
    return loaded


def save_yaml(data, yaml_path):
    with open(yaml_path, 'w') as f:
        f.write(yaml.dump(data, default_flow_style=False))


def save_json(data, json_path):
    with open(json_path, 'w') as f:
        json.dump(data, f)


def is_list(obj):
    return isinstance(obj, collections.Iterable) and \
           isinstance(obj, collections.Sized) and \
           not isinstance(obj, (basestring, collections.Mapping))


def update(current, to_update,
           deep=False, update_list=False, extend_list=False, ignore_exist=False, ignore_none=False):
    def update_mapping(current, to_update):
        current = copy(current)
        for k, v in to_update.iteritems():
            if k in current:
                current[k] = update_one(current[k], v, deep)
            else:
                if ignore_none and v is None:
                    pass
                else:
                    current[k] = v
        return current

    def update_one(cur, up, deep_this):
        if deep_this and isinstance(cur, collections.Mapping) and isinstance(up, collections.Mapping):
            return update_mapping(cur, up)
        elif update_list and is_list(cur) and is_list(up):
            len_cur, len_up = len(cur), len(up)
            uped = [c if ignore_none and u is None else update_one(c, u, deep)
                    for c, u in zip(cur, up)]
            if len_cur < len_up:
                rest = up[(len_cur - len_up):]
            elif len_cur > len_up:
                rest = cur[(len_up - len_cur):]
            else:
                rest = []
            uped.extend(rest)
            return uped
        elif extend_list and is_list(cur) and is_list(up):
            cur = list(cur)
            cur.extend(up)
            return cur
        elif ignore_exist or ignore_none and up is None:
            return cur
        else:
            return up

    return update_one(current, to_update, True)


def deep_equal(lhs, rhs):
    if isinstance(lhs, collections.Mapping) and isinstance(rhs, collections.Mapping):
        if len(lhs) != len(rhs):
            return False
        for key in lhs:
            if key not in rhs:
                return False
            if not deep_equal(lhs[key], rhs[key]):
                return False
    if is_list(lhs) and is_list(rhs):
        if len(lhs) != len(rhs):
            return False
        for i, j in zip(lhs, rhs):
            if not deep_equal(i, j):
                return False
    return lhs == rhs


def all_equal(seq):
    it = iter(seq)
    first = next(it)
    for next_it in it:
        if next_it != first:
            return False
    return True


class NumberSequence(object):
    def __init__(self):
        super(NumberSequence, self).__init__()
        self.reset()

    def __iter__(self):
        return self

    def next(self):
        raise NotImplementedError()

    def reset(self):
        raise NotImplementedError()


class Fibonacci(NumberSequence):
    def reset(self):
        self.a = 0
        self.b = 1

    def next(self):
        self.a = self.b
        self.b = self.a + self.b
        return self.a


def parse_numeric_value(value_str):
    try:
        result = yaml.load(value_str)
    except (ValueError, yaml.parser.ParserError):
        result = None
    if isinstance(result, (int, float)):
        return result
    else:
        raise ValueError('"{!r}" cannot be parsed into numeric value.'.format(value_str))


def str_timedelta(timedelta):
    secs = timedelta.total_seconds()
    if secs < 60:
        val, unit = secs, 'sec'
    elif 60 <= secs < 3600:
        val, unit = secs / 60., 'min'
    elif 3600 <= secs < 86400:
        val, unit = secs / 3600., 'hour'
    else:
        val, unit = secs / 86400., 'day'
    if val > 1:
        unit += 's'
    return '{:.1f} {}'.format(val, unit)


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
    if isinstance(time_repr, basestring):
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
    t_is_not_timestamp = [t is None or isinstance(t, (basestring, pd.Timedelta, timedelta))
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


def reverse_readline(filename, buf_size=8192):
    """a generator that returns the lines of a file in reverse order"""
    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        total_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(total_size, offset + buf_size)
            fh.seek(-offset, os.SEEK_END)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split('\n')
            # the first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # if the previous chunk starts right from the beginning of line
                # do not concact the segment to the last line of new chunk
                # instead, yield the segment first
                if buffer[-1] != '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                yield lines[index]
        yield segment
