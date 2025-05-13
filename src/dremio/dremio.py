#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .version import *  # for __version__, __author__

__all__ = ["Dremio"]

from ._mixins import (
    BaseClass,
    _MixinCatalog,
    _MixinSQL,
    _MixinQuery,
    _MixinDataset,
    _MixinFolder,
    _MixinFlight,
    _MixinReflection,
    _MixinUser,
    _MixinRole,
)


# TODO: write a few lines about mixins
class Dremio(
    _MixinRole,
    _MixinUser,
    _MixinReflection,
    _MixinQuery,
    _MixinFlight,
    _MixinFolder,
    _MixinDataset,
    _MixinSQL,
    _MixinCatalog,
    BaseClass,
):
    """This class is the main class of the Dremio connector. [learn more](https://github.com/continental/pydremio/blob/master/docs/DREMIO_METHODS.md)"""
