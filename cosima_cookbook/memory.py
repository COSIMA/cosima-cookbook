"""
Caching

The memory object lives in this module.
Other components of the cookbook access by

from ..memory import memory
"""

from joblib import Memory

# pick up cachedir from an environment variable?
cachedir='/tmp'
memory = Memory(cachedir=cachedir, verbose=0)
