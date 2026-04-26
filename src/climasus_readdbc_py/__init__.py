"""Canonical import path for the climasus_readdbc_py package.

The implementation still lives in ``climasus_readdbc`` for compatibility with
versions already published, but new code should import this package name.
"""

from __future__ import annotations

from climasus_readdbc import *  # noqa: F403
from climasus_readdbc import __all__ as __all__
from climasus_readdbc import __version__ as __version__
