from unittest import TestCase

import pandas as pd
from qutils import deep_equal

from .json_model import JsonModel


class SubToyModel(JsonModel):
    __fields__ = {
        'sstrf': None,
        'sintf': None,
        'sfloatf': None,
        'sdatetimef': JsonModel.DateTimeType()
    }


class ToyModel(JsonModel):
    __fields__ = {
        'strf': None,
        'intf': None,
        'floatf': None,
        'datetimef': JsonModel.DateTimeType(),
        'modelf': JsonModel.ModelType(SubToyModel)
    }


class TestJsonModel(TestCase):
    def setUp(self):
        super().setUp()
        self.sub_toy_json = {
            'sstrf': 'this is a sub-model string field',
            'sintf': 98765,
            'sfloatf': 0.618,
            'sdatetimef': '1990-07-09T23:42:55.325'
        }
        self.toy_json = {
            'strf': 'this is a string field',
            'intf': 12345,
            'floatf': 3.14159,
            'datetimef': '1990-07-09T07:00:05.007',
            'modelf': self.sub_toy_json
        }
        self.toy_model = ToyModel(self.toy_json)
        self.sub_toy_model = self.toy_model.modelf

    def test_toy_model(self):
        self.assertEqual('this is a string field', self.toy_model.strf)
        self.assertEqual(12345, self.toy_model.intf)
        self.assertEqual(3.14159, self.toy_model.floatf)
        self.assertEqual(pd.to_datetime('1990-07-09T07:00:05.007'), self.toy_model.datetimef)

    def test_sub_toy_model(self):
        self.assertIsInstance(self.sub_toy_model, SubToyModel)
        self.assertEqual('this is a sub-model string field', self.sub_toy_model.sstrf)
        self.assertEqual(98765, self.sub_toy_model.sintf)
        self.assertEqual(0.618, self.sub_toy_model.sfloatf)
        self.assertEqual(pd.to_datetime('1990-07-09T23:42:55.325'), self.sub_toy_model.sdatetimef)

    def test_modify_toy_models(self):
        self.sub_toy_model.sintf = 142857
        self.toy_model.datetimef = pd.to_datetime('2016-07-09T00:00:00.001')
        self.assertTrue(deep_equal(self.toy_model.to_dict(), {
            'strf': 'this is a string field',
            'intf': 12345,
            'floatf': 3.14159,
            'datetimef': '2016-07-09T00:00:00.001000',
            'modelf': {
                'sstrf': 'this is a sub-model string field',
                'sintf': 142857,
                'sfloatf': 0.618,
                'sdatetimef': '1990-07-09T23:42:55.325000'
            }
        }))

    def test_invalid_json_object(self):
        with self.assertRaises(TypeError):
            ToyModel(None)
        with self.assertRaises(TypeError):
            ToyModel({
                'sstrf': 'this is a sub-model string field',
                'sintf': 98765,
                'sfloatf': 0.618,
                'sdatetimef': '1990-07-09T23:42:55.325',
                'modelf': 'this is an invalid field value'
            })
        with self.assertRaises(KeyError):
            print(self.toy_model.not_exist_field)


