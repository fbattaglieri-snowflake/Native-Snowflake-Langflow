"""SnowflakeLangflow (`lfx_snowflake`) — Account-Agnostic Snowflake Bundle for Langflow on SPCS."""

from __future__ import annotations

import sys as _sys

from . import _lfx_compat as _lfx_compat  # noqa: F401
from . import context_helpers as _context_helpers
from .api_routes import SnowflakeAPIResponse, SnowflakeAPIRouter, redact_secrets_in_obj

_sys.modules.setdefault(
    "lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner",
    _context_helpers,
)
_sys.modules.setdefault(
    "lfx_snowflake.native_category_components.data_source.snowflake_object_provisioner",
    _context_helpers,
)
from .bundle_manifest import (
    BUNDLE_ID,
    BUNDLE_PACKAGE,
    BUNDLE_VERSION,
    ERROR_TAXONOMY_CONTRACT,
    INVARIANT_CATEGORIES_MANIFEST,
    NEW_FLOW_IMPORT_MODES,
    SETTINGS_CONTRACT,
    SNOWFLAKE_BRANDING_CONTRACT,
    SNOWFLAKE_BUNDLE_COMPONENTS,
    UPSTREAM_BASELINE_TAG,
    BundleComponentSpec,
    get_bundle_manifest,
    verify_invariant_category_integrity,
)

__version__ = BUNDLE_VERSION

__all__ = [
    "BUNDLE_ID",
    "BUNDLE_PACKAGE",
    "BUNDLE_VERSION",
    "UPSTREAM_BASELINE_TAG",
    "SNOWFLAKE_BRANDING_CONTRACT",
    "ERROR_TAXONOMY_CONTRACT",
    "INVARIANT_CATEGORIES_MANIFEST",
    "SETTINGS_CONTRACT",
    "NEW_FLOW_IMPORT_MODES",
    "SNOWFLAKE_BUNDLE_COMPONENTS",
    "BundleComponentSpec",
    "get_bundle_manifest",
    "verify_invariant_category_integrity",
    "SnowflakeAPIRouter",
    "SnowflakeAPIResponse",
    "redact_secrets_in_obj",
]
