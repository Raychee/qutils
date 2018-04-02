import ast
from typing import Union

import pandas as pd
from cachetools import LFUCache


DSL_ALLOWED_AST_NODES = (
    # mod
    ast.Module,
    # ast.Interactive,
    # ast.Expression,
    # ast.Suite,

    # stmt
    # ast.FunctionDef,
    # ast.AsyncFunctionDef,
    # ast.ClassDef,
    ast.Return,
    # ast.Delete,
    ast.Assign,
    ast.AugAssign,
    ast.For,
    # ast.AsyncFor,
    ast.While,
    ast.If,
    # ast.With,
    # ast.AsyncWith,
    # ast.Raise,
    # ast.Try,
    # ast.Assert,
    # ast.Import,
    # ast.ImportFrom,
    # ast.Global,
    # ast.Nonlocal,
    ast.Expr,
    ast.Pass,
    ast.Break,
    ast.Continue,

    # expr
    ast.BoolOp,
    ast.BinOp,
    ast.UnaryOp,
    ast.Lambda,
    ast.IfExp,
    ast.Dict,
    ast.Set,
    ast.ListComp,
    ast.SetComp,
    ast.DictComp,
    # ast.GeneratorExp,
    # ast.Await,
    # ast.Yield,
    # ast.YieldFrom,
    ast.Compare,
    ast.Call,
    ast.Num,
    ast.Str,
    # ast.FormattedValue,
    # ast.JoinedStr,
    ast.Bytes,
    ast.NameConstant,
    ast.Ellipsis,
    # ast.Constant,
    ast.Attribute,
    ast.Subscript,
    ast.Starred,
    ast.Name,
    ast.List,
    ast.Tuple,

    ast.expr_context,
    ast.slice,
    ast.boolop,
    ast.operator,
    ast.unaryop,
    ast.cmpop,
    ast.comprehension,
    # ast.excepthandler,
    ast.arguments,
    ast.arg,
    ast.keyword,
    # ast.alias,
    # ast.withitem,
)
DSL_ALLOWED_BUILTIN_FUNCTIONS = (
    # python built-in functions
    'abs',
    'all',
    'any',
    # 'ascii',
    'bin',
    'bool',
    # 'bytearray',
    # 'bytes',
    # 'callable',
    'chr',
    # 'classmethod',
    # 'compile',
    'complex',
    # 'delattr',
    'dict',
    # 'dir',
    'divmod',
    'enumerate',
    # 'eval',
    # 'exec',
    # 'filter',
    'float',
    'format',
    'frozenset',
    # 'getattr',
    # 'globals',
    # 'hasattr',
    'hash',
    # 'help',
    'hex',
    # 'id',
    # 'input',
    'int',
    # 'isinstance',
    # 'issubclass',
    # 'iter',
    'len',
    'list',
    # 'locals',
    # 'map',
    'max',
    # 'memoryview',
    'min',
    # 'next',
    # 'object',
    'oct',
    # 'open',
    'ord',
    'pow',
    # 'print',
    # 'property',
    'range',
    # 'repr',
    'reversed',
    'round',
    'set',
    # 'setattr',
    # 'slice',
    'sorted',
    # 'staticmethod',
    'str',
    'sum',
    # 'super',
    'tuple',
    'type',
    # 'vars',
    'zip',
    # '__import__',
)
DSL_CODE_CACHE_SIZE = 100
DSL_GLOBALS = {
    '__builtins__': {k: globals()['__builtins__'][k] for k in DSL_ALLOWED_BUILTIN_FUNCTIONS + ('locals',)},
    'pd': pd,
    'np': pd.np
}


class DSLError(Exception):
    pass


class DSLSyntaxError(DSLError):

    def __init__(self, err: Union[SyntaxError, IndentationError]) -> None:
        super().__init__(*err.args)
        self.filename = err.filename
        self.msg = err.msg
        self.lineno = err.lineno
        self.offset = err.offset
        self.text = err.text


class DSLValidationError(DSLError):
    def __init__(self, node, message) -> None:
        super().__init__(node, message)
        self.node = node
        self.message = message


class DSLRuntimeError(DSLError):
    pass


class DSLTransformer(ast.NodeTransformer):
    def __init__(self, return_target) -> None:
        super().__init__()
        self.return_target = return_target

    def visit_Return(self, node):
        self.generic_visit(node)
        return [
            # change --
            # return <expr>
            # -- into --
            # target_variable_name = <expr>
            ast.Assign(targets=[ast.Name(id=self.return_target, ctx=ast.Store())], value=node.value),
            # add these lines of code before every return --
            # _ns.update(**locals())
            ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(value=ast.Name(id='_ns', ctx=ast.Load()), attr='update', ctx=ast.Load()),
                    args=[],
                    keywords=[ast.keyword(value=ast.Call(func=ast.Name(id='locals', ctx=ast.Load()),
                                                         args=[], keywords=[]))]
                )
            ),
            # _ns.pop('_ns', None)
            ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(value=ast.Name(id='_ns', ctx=ast.Load()),
                                       attr='pop', ctx=ast.Load()),
                    args=[ast.Str(s='_ns'), ast.NameConstant(value=None)],
                    keywords=[]
                )
            ),
            ast.Return()
        ]


class AsyncDSLTransformer(DSLTransformer):
    def visit_Module(self, node):
        self.generic_visit(node)
        # insert function def before the code, like --
        # async def _f(_ns): <code>
        return ast.Module(
            body=[
                ast.AsyncFunctionDef(
                    name='_f',
                    args=ast.arguments(args=[ast.arg(arg='_ns')],
                                       vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]),
                    body=node.body,
                    decorator_list=[],
                    returns=None
                )
            ]
        )


class SyncDSLTransformer(DSLTransformer):
    def visit_Module(self, node):
        self.generic_visit(node)
        # insert function def before the code, like --
        # def _f(_ns): <code>
        return ast.Module(
            body=[
                ast.FunctionDef(
                    name='_f',
                    args=ast.arguments(args=[ast.arg(arg='_ns')],
                                       vararg=None, kwonlyargs=[], kw_defaults=[], kwarg=None, defaults=[]),
                    body=node.body,
                    decorator_list=[],
                    returns=None
                )
            ]
        )


def check_dsl_errors(code):
    errors = []
    if isinstance(code, ast.AST):
        code_tree = code
    else:
        try:
            code_tree = ast.parse(code)
        except SyntaxError as err:
            errors.append(DSLSyntaxError(err))
            code_tree = ast.Module()
    for node in ast.walk(code_tree):
        if isinstance(node, ast.Name):
            if node.id.startswith('_'):
                errors.append(DSLValidationError(node, 'names cannot start with "_", but got "{}"'.format(node.id)))
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                node_func_name = node.func.id
                if node_func_name not in DSL_ALLOWED_BUILTIN_FUNCTIONS:
                    errors.append(DSLValidationError(node, 'function "{}" is not callable'.format(node_func_name)))
        elif isinstance(node, ast.Assign):
            for assign_target in node.targets:
                if isinstance(assign_target, ast.Name) and (
                        assign_target.id in DSL_ALLOWED_BUILTIN_FUNCTIONS or
                        assign_target.id in DSL_GLOBALS):
                    errors.append(DSLValidationError(node, 'object "{}" cannot be overwritten'.format(assign_target.id)))
        elif not isinstance(node, DSL_ALLOWED_AST_NODES):
            errors.append(DSLValidationError(node, '"{}" is not allowed'.format(type(node).__name__)))

    return errors


def to_eval_dsl_function(code, return_target, mode='sync'):
    if isinstance(code, ast.AST):
        code_tree = code
    else:
        try:
            code_tree = ast.parse(code)
        except SyntaxError as err:
            raise DSLSyntaxError(err)

    if len(code_tree.body) == 1 and isinstance(code_tree.body[0], ast.Expr):
        # normalize one-line expression into "return <expr>"
        code_tree.body[0] = ast.Return(value=code_tree.body[0])

    if mode == 'sync':
        transformer = SyncDSLTransformer(return_target)
    elif mode == 'async':
        transformer = AsyncDSLTransformer(return_target)
    else:
        raise ValueError('unrecognized mode for generating eval function: {}'.format(mode))

    code_tree = transformer.visit(code_tree)
    ast.fix_missing_locations(code_tree)

    compiled_code = compile(code_tree, '<ast>', 'exec')
    function_space = {}
    exec(compiled_code, DSL_GLOBALS, function_space)
    return function_space['_f']


SYNC_CODE_CACHE = LFUCache(DSL_CODE_CACHE_SIZE, missing=lambda k: to_eval_dsl_function(k[0], k[1], mode='sync'))
ASYNC_CODE_CACHE = LFUCache(DSL_CODE_CACHE_SIZE, missing=lambda k: to_eval_dsl_function(k[0], k[1], mode='async'))


def eval_dsl(code, namespace: dict, return_target: str):
    code_fn = SYNC_CODE_CACHE[(code, return_target)]
    code_fn(namespace)
    return namespace.get(return_target)


async def async_eval_dsl(code, namespace: dict, return_target: str):
    code_fn = ASYNC_CODE_CACHE[(code, return_target)]
    await code_fn(namespace)
    return namespace.get(return_target)
