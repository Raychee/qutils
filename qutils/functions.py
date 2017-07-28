import time

import collections
from copy import copy

import functools


class lazy:
    def __init__(self, fn):
        super().__init__()
        self.fn = fn
        self.called = False
        self.res = None

    def __call__(self):
        if not self.called:
            self.res = self.fn()
            self.called = True
        return self.res


class timeit:
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


def pretty_str_mongo_collection(collection):
    database = collection.database
    client = database.connection
    return 'mongodb://{}:{}/{}/{}'.format(client.host, client.port, database.name, collection.name)


def is_list(obj):
    return isinstance(obj, collections.Iterable) and \
           isinstance(obj, collections.Sized) and \
           not isinstance(obj, (str, collections.Mapping))


def update(current, to_update,
           deep=False, update_list=False, extend_list=False, ignore_exist=False, ignore_none=False):
    def update_mapping(current, to_update):
        current = copy(current)
        for k, v in to_update.items():
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


def deep_equal_naive(lhs, rhs):
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


def freeze(obj, unordered_list=False):
    @functools.cmp_to_key
    def cmp_with_types(lhs, rhs):
        try:
            return (lhs > rhs) - (lhs < rhs)
        except TypeError:
            lhs = type(lhs).__name__
            rhs = type(rhs).__name__
            return (lhs > rhs) - (lhs < rhs)

    if isinstance(obj, dict):
        return tuple(sorted(((freeze(k, unordered_list), freeze(v, unordered_list))
                            for k, v in obj.items()), key=cmp_with_types))
    elif isinstance(obj, list):
        if unordered_list:
            return tuple(sorted((freeze(i, unordered_list) for i in obj), key=cmp_with_types))
        else:
            return tuple(freeze(i, unordered_list) for i in obj)
    else:
        return obj


def deep_equal(lhs, rhs, unordered_list=False):
    return freeze(lhs, unordered_list) == freeze(rhs, unordered_list)


def all_equal(seq):
    it = iter(seq)
    first = next(it)
    for next_it in it:
        if next_it != first:
            return False
    return True