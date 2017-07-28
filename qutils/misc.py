import socket
from multiprocessing.dummy import Process, Pipe

import yaml


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


class NumberSequence:
    def __init__(self):
        super().__init__()
        self.reset()

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplementedError()

    def reset(self):
        raise NotImplementedError()


class Fibonacci(NumberSequence):
    def reset(self):
        self.a = 0
        self.b = 1

    def __next__(self):
        self.a = self.b
        self.b = self.a + self.b
        return self.a


def numeric(value_str):
    try:
        result = yaml.load(value_str)
    except (ValueError, yaml.parser.ParserError):
        result = None
    if isinstance(result, (int, float)):
        return result
    else:
        raise ValueError('"{!r}" cannot be parsed into numeric value.'.format(value_str))
