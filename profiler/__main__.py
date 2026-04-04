"""
__main__.py — entry point for the profiler.

Usage:
    python -m profiler run --config config.yaml
"""

import argparse
import sys

from .config import load_config
from .engine import run


def main():
    parser = argparse.ArgumentParser(description="Data Profiler")
    sub = parser.add_subparsers(dest="command")

    run_cmd = sub.add_parser("run", help="Run the profiler")
    run_cmd.add_argument("--config", default="config.yaml", help="Path to config.yaml")

    args = parser.parse_args()

    if args.command == "run":
        config = load_config(args.config)
        run(config)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()