from unittest import TestCase

from qutils.json_ext import SkyNetJSONable


class TestJSONExt(TestCase):
    def test_meta_class(self):
        class A(SkyNetJSONable):
            type = 'a'

        class B(SkyNetJSONable):
            type = 'b'

        self.assertTrue(hasattr(A, 'all_types'))
        self.assertTrue(hasattr(B, 'all_types'))
        self.assertTrue(A.all_types is B.all_types)
        self.assertEqual(2, len(A.all_types))
        self.assertTrue(A.all_types.get('a') is A)
        self.assertTrue(A.all_types.get('b') is B)

        with self.assertRaises(TypeError):
            class C(SkyNetJSONable):
                type = 'a'


