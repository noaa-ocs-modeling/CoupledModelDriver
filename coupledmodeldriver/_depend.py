import importlib.util
import logging
import sys


_logger = logging.getLogger(__file__)
# TODO: Forward proper types?


def optional_import(name):
    if name in sys.modules:
        _logger.warning(f'{name!r} already in sys.modules')
        return sys.modules[name]
    elif (spec := importlib.util.find_spec(name)) is not None:
        # If you chose to perform the actual import ...
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    _logger.warning(f"can't find the {name!r} module")
    return None


def can_import(name):
    if name in sys.modules:
        return True
    elif (spec := importlib.util.find_spec(name)) is not None:
        return True
    else:
        return False


HAS_ADCIRCPY = can_import('adcircpy')


def requires_adcircpy(func):
    def wrapper(*args, **kwargs):
        if not HAS_ADCIRCPY:
            raise ImportError(f"{func.__name__} requires `adcircpy`, but it's not available!")
        return func(*args, **kwargs)

    return wrapper
