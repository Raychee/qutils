from unittest import TestCase

from qutils.dsl import check_dsl_errors, DSLSyntaxError, DSLValidationError, eval_dsl, DSL_GLOBALS, async_eval_dsl


class TestDSL(TestCase):

    def test_dsl_valid(self):
        code = """
a = 1
b = 2
c = a + b
data_frame = metric.get_data(now, now - '3d')
data_frame.sort_values(by="timestamp")
d = sum([a, b])
return []
        """
        self.assertEqual([], check_dsl_errors(code))

    def test_dsl_syntax_error(self):
        code = """ i append j """
        errors = check_dsl_errors(code)
        self.assertEqual(1, len(errors))
        self.assertTrue(isinstance(errors[0], DSLSyntaxError))

        code = """
        a = 1
        b = 2
        c = a + b
        data_frame = metric.get_data(now, now - '3d')
        data_frame.sort_values(by="timestamp")
        d = sum([a, b])
        return []
        """
        errors = check_dsl_errors(code)
        self.assertEqual(1, len(errors))
        self.assertTrue(isinstance(errors[0], DSLSyntaxError))

    def test_dsl_invalid_name(self):
        code = """
_x = 4
_y = 5
        """
        errors = check_dsl_errors(code)
        self.assertEqual(2, len(errors))
        self.assertTrue(all(isinstance(e, DSLValidationError) for e in errors))

    def test_dsl_disallowed_syntax_usage(self):
        code = """
import json
def f(self):
    yield 1
        """
        errors = check_dsl_errors(code)
        self.assertEqual(4, len(errors))
        self.assertTrue(all(isinstance(e, DSLValidationError) for e in errors))

    def test_dsl_disallowed_function_call(self):
        code = """
a = 1
b = a + 2
if isinstance(a, int):
    print(a)
        """
        errors = check_dsl_errors(code)
        self.assertEqual(2, len(errors))
        self.assertTrue(all(isinstance(e, DSLValidationError) for e in errors))

    def test_exec_dsl(self):
        code = """
a = 1
b = 2
return a + b
c = 4
        """
        self.assertEqual([], check_dsl_errors(code))

        namespace = {}
        self.assertEqual(3, eval_dsl(code, namespace, '__result__'))
        self.assertEqual(3, len(namespace))
        self.assertEqual(1, namespace['a'])
        self.assertEqual(2, namespace['b'])
        self.assertEqual(3, namespace['__result__'])

    def test_exec_dsl_one_line_expr(self):
        code = """return 4 + 7"""
        self.assertEqual([], check_dsl_errors(code))

        namespace = {}
        self.assertEqual(11, eval_dsl(code, namespace, '__result__'))
        self.assertEqual(1, len(namespace))
        self.assertEqual(11, namespace['__result__'])

    def test_async_exec_dsl(self):
        import asyncio

        DSL_GLOBALS['asyncio'] = asyncio
        code = """
a = 1
b = 2
await asyncio.sleep(0.1)
return a + b
c = 4
        """
        loop = asyncio.get_event_loop()
        namespace = {}
        self.assertEqual(3, loop.run_until_complete(async_eval_dsl(code, namespace, '__result__')))
        self.assertEqual(3, len(namespace))
        self.assertEqual(3, namespace['__result__'])
        DSL_GLOBALS.pop('asyncio', None)
