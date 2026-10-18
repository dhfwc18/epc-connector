"""
Run the command line as ``python -m epc_connector``; see ``pipeline.cli``.
"""

import sys

from epc_connector.pipeline.cli import main

if __name__ == "__main__":
    sys.exit(main())
