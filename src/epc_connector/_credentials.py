"""
The England and Wales API token: where it comes from and where it is saved.

A token is looked for in this order, as ``huggingface_hub`` does:

1. the ``api_token`` argument of the function called;
2. the ``EPC_CONNECTOR_API_TOKEN`` environment variable (CI and scripts);
3. the token saved by the command line (``save_api_token``), in the user's
   configuration directory, e.g. ``~/.config/epc-connector/token``.

Only the command line asks for a token and saves it, in a file readable by
the user only, as git's credential store does; library functions take it as
an argument. No ``.env`` file is read. The token is never logged.
"""

import os
from pathlib import Path

from epc_connector._environment import EnvVar, read_path, read_text, user_config_dir
from epc_connector._errors import EpcConnectorError


class ApiTokenError(EpcConnectorError, ValueError):
    """No England and Wales API token was found, or the API rejected it."""


def token_path() -> Path:
    """
    Return the file the command line saves the token to.

    ``EPC_CONNECTOR_TOKEN_PATH`` if set, otherwise ``token`` in the user
    settings directory (``_environment.user_config_dir``).
    """
    return read_path(EnvVar.TOKEN_PATH) or user_config_dir() / "token"


def resolve_api_token(api_token: str | None = None) -> str | None:
    """Return the first token found, in the module's order, or ``None``."""
    if api_token and api_token.strip():
        return api_token.strip()
    if token := read_text(EnvVar.API_TOKEN):
        return token
    path = token_path()
    if path.is_file() and (token := path.read_text(encoding="ascii").strip()):
        return token
    return None


def require_api_token(api_token: str | None = None) -> str:
    """
    Return the token ``resolve_api_token`` finds.

    Raises
    ------
    ApiTokenError
        If there is none.
    """
    token = resolve_api_token(api_token)
    if token is None:
        raise ApiTokenError(
            f"no England and Wales API token: pass api_token, set "
            f"{EnvVar.API_TOKEN}, or run epc-connector login"
        )
    return token


def checked_api_token(api_token: str) -> str:
    """
    Return the token without surrounding spaces, if it looks like one.

    Raises
    ------
    ApiTokenError
        If it is empty, or has spaces or non-ASCII characters inside.
    """
    token = api_token.strip()
    if not token or not token.isascii() or any(c.isspace() for c in token):
        raise ApiTokenError("that does not look like an API token")
    return token


def save_api_token(api_token: str) -> Path:
    """
    Save the token for later runs, readable by the current user only.

    Returns
    -------
    Path
        The file written (see ``token_path``).

    Raises
    ------
    ApiTokenError
        If it does not look like a token (see ``checked_api_token``).
    """
    token = checked_api_token(api_token)
    path = token_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="ascii") as token_file:
        token_file.write(token + "\n")
    path.chmod(0o600)  # also when the file already existed
    return path


def delete_api_token() -> bool:
    """Delete the saved token; return whether there was one."""
    path = token_path()
    if not path.is_file():
        return False
    path.unlink()
    return True
