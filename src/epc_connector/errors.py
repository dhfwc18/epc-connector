"""
Base class of every error this package raises on purpose.
"""


class EpcConnectorError(Exception):
    """
    An expected failure: bad input, unexpected data or a failed download.

    Catch this to handle every such error at once; anything else is a bug.
    """
