"""
Caching

The memory object lives in this module.
Other components of the cookbook access by

from ..memory import memory
"""

from joblib import Memory

import os, getpass, tempfile

username = getpass.getuser()



# pick up cachedir from an environment variable?
# Append username to prevent clashes with others users
cachedir = os.path.join(tempfile.gettempdir(),username)
memory = Memory(cachedir=cachedir, verbose=0)
