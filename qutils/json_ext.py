from abc import abstractmethod
from json import JSONEncoder, JSONDecoder


class RegisterJSONableTypeMeta(type):
    def __init__(cls, what, bases=None, dict=None):
        super().__init__(what, bases, dict)
        if not hasattr(cls, 'all_types'):
            cls.all_types = {}
        type_key = dict.get('type')
        if type_key is not None:
            if type_key in cls.all_types:
                raise TypeError('type key "{}" conflicts between class "{}" and "{}"'
                                .format(type_key, cls.__name__, cls.all_types[type_key].__name__))
            cls.all_types[type_key] = cls


class SkyNetJSONable(object, metaclass=RegisterJSONableTypeMeta):

    # this should be a unique identifier that will be used to
    # distinguish the JSON object of this class from the other plain JSON objects
    type = None

    @abstractmethod
    def encode(self):
        """return a serializable object of self"""
        return self.__dict__

    @classmethod
    @abstractmethod
    def decode(cls, obj):
        """given a json object, convert it to an object of this class"""
        return cls()


class SkyNetJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SkyNetJSONable):
            encoded = obj.encode()
            if '_type' not in encoded:
                encoded['_type'] = obj.type
            return encoded
        return super().default(obj)


class SkyNetJSONDecoder(JSONDecoder):
    def __init__(self, object_hook=None, parse_float=None, parse_int=None, parse_constant=None, strict=True,
                 object_pairs_hook=None):
        super().__init__(object_hook or self.object_hook, parse_float, parse_int, parse_constant, strict, object_pairs_hook)

    @staticmethod
    def object_hook(obj: dict):
        if '_type' in obj:
            cls = SkyNetJSONable.all_types.get(obj['_type'])
            if cls is not None:
                encoded = obj.copy()
                encoded.pop('_type', None)
                return cls.decode(encoded)
        return obj
