import json

import pandas as pd
from datetime import datetime

from . import is_list


class JsonModel(object):
    __fields__ = {}
    __values__ = {}

    class Converter(object):
        valid_types = ()

        def json2value(self, json_obj):
            return ''

        def value2json(self, value):
            return None

        def has_valid_type(self, obj):
            return isinstance(obj, self.valid_types)

    class DateTimeType(Converter):
        valid_types = (pd.Timestamp, datetime)

        def __init__(self, dt_format='%Y-%m-%dT%H:%M:%S.%f'):
            super(JsonModel.DateTimeType, self).__init__()
            self.dt_format = dt_format

        def json2value(self, time_str):
            return pd.to_datetime(time_str, format=self.dt_format)

        def value2json(self, dt):
            return dt.strftime(self.dt_format)

    class ModelType(Converter):
        def __init__(self, model_cls):
            super(JsonModel.ModelType, self).__init__()
            self.model_cls = model_cls

        def json2value(self, dict_obj):
            return self.model_cls(dict_obj)

        def value2json(self, model):
            return model.to_dict()

        def has_valid_type(self, obj):
            return isinstance(obj, self.model_cls)

    def __init__(self, *args, **kwargs):
        super(JsonModel, self).__init__()
        if len(args) > 0:
            json_obj = args[0]
        else:
            json_obj = {}
        if isinstance(json_obj, basestring):
            json_obj = json.loads(json_obj)
        self.from_dict(json_obj)
        self.from_dict(kwargs)

    def from_dict(self, dict_obj):
        for key, value in dict_obj.items():
            try:
                self.__setitem__(key, value)
            except KeyError:
                pass

    def to_dict(self):
        d = self.__values__.copy()
        for key, value in d.items():
            converter = self.__fields__[key]
            if converter is not None:
                if is_list(value):
                    value = [converter.value2json(v) for v in value]
                else:
                    value = converter.value2json(value)
                d[key] = value
        return d

    def to_json(self, *args, **kwargs):
        d = self.to_dict()
        return json.dumps(d, *args, **kwargs)

    def __getitem__(self, key):
        try:
            return self.__values__[key]
        except KeyError:
            if key in self.__fields__:
                return None
            else:
                raise KeyError('{!r} has no field "{}"'.format(self, key))

    def __setitem__(self, key, value):
        try:
            converter = self.__fields__[key]
        except KeyError:
            raise KeyError('{!r} has no field "{}"'.format(self, key))
        else:
            if value is not None:
                if converter is not None:
                    if is_list(value):
                        value = [v if converter.has_valid_type(v) else converter.json2value(v) for v in value]
                    else:
                        value = value if converter.has_valid_type(value) else converter.json2value(value)
                self.__values__[key] = value

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __setattr__(self, key, value):
        return self.__setitem__(key, value)

    def __repr__(self):
        return '{}({})'.format(type(self).__name__,
                               ', '.join('{}={!r}'.format(k, getattr(self, k))
                                         for k in self.__fields__ if hasattr(self, k)))
