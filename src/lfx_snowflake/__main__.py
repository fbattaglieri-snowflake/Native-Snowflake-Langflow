"""CLI entrypoint for `python3 -m lfx_snowflake`."""

from __future__ import annotations

import json

from .bundle_manifest import get_bundle_manifest


def main() -> int:
    print(json.dumps(get_bundle_manifest(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
