#!/usr/bin/env python3
"""CLI entry point.

The implementation lives in the ``domain_scout`` package, which the web UI shares.
This shim keeps the documented ``python scanner.py --tld com`` command working.
"""
from domain_scout.cli import main

if __name__ == "__main__":
    main()
