"""
Run the command line as ``python -m epc_connector``; see ``_cli``.
"""

import sys

from epc_connector._cli import main

if __name__ == "__main__":
    sys.exit(main())
