from __future__ import annotations

import argparse
import logging

from src import log_conf  # noqa: F401
from src.corrector import SchemaCorrector


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Synchronize target DB schema by source DB schema',
    )
    parser.add_argument('--source-url', required=True)
    parser.add_argument('--target-url', required=True)
    parser.add_argument('--schema', default=None)
    parser.add_argument('--lock-timeout', type=int, default=10)
    parser.add_argument('--statement-timeout', type=int, default=0)
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Apply changes (otherwise dry-run)',
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    corrector = SchemaCorrector(
        source_url=args.source_url,
        target_url=args.target_url,
        schema=args.schema,
        lock_timeout_seconds=args.lock_timeout,
        statement_timeout_seconds=args.statement_timeout,
    )

    ops = corrector.diff()
    corrector.apply(ops, dry_run=not args.apply)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
