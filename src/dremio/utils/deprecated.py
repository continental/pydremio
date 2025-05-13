import functools
from typing import Callable
import logging


def deprecated(hint: str):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logging.warn(
                DeprecationWarning(f"{func.__name__} is deprecated. {hint}"),
                stacklevel=2,
            )
            func.__doc__ = (
                f"> :warning: `{func.__name__}` **is deprecated**: {hint}\n"
                + (func.__doc__ or "")
            )
            return func(*args, **kwargs)

        return wrapper

    return decorator


def deprecated_interface(func: Callable):
    def wrapper(*args, **kwargs):
        s = f"{func.__name__} is deprecated."
        logging.warning(DeprecationWarning(s), stacklevel=2)
        func.__doc__ = s + "\n" + (func.__doc__ or "")
        return func(*args, **kwargs)

    return wrapper
