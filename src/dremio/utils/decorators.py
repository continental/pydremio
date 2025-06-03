import functools, warnings, logging
from typing import Callable


def deprecated(hint: str):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
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


already_warned = False

def experimental(func):
    note = "\n\n⚠️ EXPERIMENTAL: This method may change or be removed in future versions.\n"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global already_warned
        if not already_warned:
            warnings.warn(
                f"The function '{func.__name__}' is experimental and may change or be removed in future versions.",
                category=UserWarning,
                stacklevel=2
            )
            already_warned = True
        return func(*args, **kwargs)

    # Modify the docstring
    doc = func.__doc__ or ""
    if note.strip() not in doc:
        wrapper.__doc__ = doc + note

    return wrapper
