#!/usr/bin/env python3
"""Provision bridge-owned Snowflake routines (deploy-time / admin action).

Creates the fixed-code LangChain text splitter used by the Split Text component's LangChain mode
(``lfx_snowflake.bridge_routines``) with ``CREATE FUNCTION IF NOT EXISTS`` under the bridge SERVICE role.
Flow components never create routines; they only invoke these exact FQNs.

Usage:
    python scripts/provision_bridge_routines.py            # dry run: print the DDL that would run
    python scripts/provision_bridge_routines.py --apply    # create the routines (service role)

Configuration: ``LANGFLOW_BRIDGE_ROUTINES_SCHEMA=<DATABASE>.<SCHEMA>`` (default: the bridge database/schema).
Exits 1 with a clear message when the service role lacks USAGE / CREATE FUNCTION (no fallback).
"""

from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="execute the DDL under the service role")
    args = parser.parse_args(argv)

    from lfx_snowflake import bridge_routines

    try:
        fqn = bridge_routines.langchain_splitter_udf_fqn()
        ddl = bridge_routines.build_langchain_splitter_udf_ddl(fqn)
        if not args.apply:
            print(f"-- dry run (use --apply to execute)\n{ddl};")
            return 0
        print(json.dumps(bridge_routines.provision_bridge_routines(), indent=2))
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI: report and fail
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
