# -*- coding: utf-8 -*-
"""
Common tools for working with COSIMA model output
"""

from . import database
from . import querying
from . import explore

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("cosima-cookbook")
except PackageNotFoundError:
    pass
