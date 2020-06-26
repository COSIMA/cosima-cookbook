# -*- coding: utf-8 -*-
"""
Common tools for working with COSIMA model output
"""

import pkg_resources

from . import database
from . import querying

try:
    __version__ = pkg_resources.get_distribution("cosima-cookbook").version
except Exception:
    __version__ = "999"
