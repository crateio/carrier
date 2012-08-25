from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


class HashMismatch(ValueError):
    """
    Raised when the incoming hash of a file does not match the expected.
    """
