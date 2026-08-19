from __future__ import annotations

from collections.abc import Mapping
import os
from pathlib import Path
from typing import Any

try:
    from downloader.common import load_env_file
except ModuleNotFoundError:  # Direct execution from downloader/.
    from common import load_env_file


# Repository-facing names stay conventional and provider-oriented.  OpenBB's
# settings model uses lower-case field names, so keep the translation in one
# auditable place instead of teaching every downloader two credential formats.
OPENBB_ENV_TO_CREDENTIAL_FIELD: dict[str, str] = {
    "FRED_API_KEY": "fred_api_key",
    "BLS_API_KEY": "bls_api_key",
    "EIA_API_KEY": "eia_api_key",
    "CONGRESS_GOV_API_KEY": "congress_gov_api_key",
    "FMP_API_KEY": "fmp_api_key",
    "TIINGO_TOKEN": "tiingo_token",
    "BENZINGA_API_KEY": "benzinga_api_key",
    "INTRINIO_API_KEY": "intrinio_api_key",
    "ECONDB_API_KEY": "econdb_api_key",
    "TRADINGECONOMICS_API_KEY": "tradingeconomics_api_key",
    "CFTC_APP_TOKEN": "cftc_app_token",
}


def load_openbb_environment(
    env_file: str | Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> set[str]:
    """Load and report configured OpenBB env names without exposing values.

    Process variables have precedence over the dotenv file.  Supplying a
    custom mapping is intended for tests; production uses ``os.environ`` and
    the repository's allowlisted dotenv parser.
    """

    if environ is None:
        load_env_file(
            env_file,
            allowed_names=OPENBB_ENV_TO_CREDENTIAL_FIELD,
            override=False,
        )
        environ = os.environ
    return {
        env_name
        for env_name in OPENBB_ENV_TO_CREDENTIAL_FIELD
        if str(environ.get(env_name, "")).strip()
    }


def apply_openbb_environment_credentials(
    obb: Any,
    *,
    env_file: str | Path,
    environ: Mapping[str, str] | None = None,
) -> set[str]:
    """Inject configured dotenv credentials into OpenBB's runtime model.

    Only field names are returned.  A configured value that OpenBB rejects is
    a hard startup error: silently falling back to a stale settings file would
    make the credential audit disagree with the actual downloader.
    """

    configured_env_names = load_openbb_environment(env_file, environ=environ)
    values = os.environ if environ is None else environ
    applied_fields: set[str] = set()
    for env_name, field_name in OPENBB_ENV_TO_CREDENTIAL_FIELD.items():
        if env_name not in configured_env_names:
            continue
        value = str(values[env_name]).strip()
        try:
            setattr(obb.user.credentials, field_name, value)
        except Exception as exc:
            raise RuntimeError(
                f"OpenBB rejected configured credential field {field_name!r} "
                f"from environment variable {env_name!r}"
            ) from exc
        applied_fields.add(field_name)
    return applied_fields
