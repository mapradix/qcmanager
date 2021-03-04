#!/usr/bin/env python3

import os
import sys
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from manager import QCManager
from manager.exceptions import ConfigError


def main(config_files, delete_job, delete_data):
    cleanup = -1
    if delete_job:
        cleanup = int(delete_job)
    try:
        manager = QCManager(config_files, cleanup=cleanup)
        if delete_data:
            manager.cleanup_data()
    except ConfigError as e:
        return 1

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean up QCManager (remove logs).")
    parser.add_argument(
        '-c', '--config',
        help="Use case config file(s)",
        type=str,
        required=True
    )
    parser.add_argument(
        '-j', '--delete_job',
        help="Delete selected job only",
        type=str
    )
    parser.add_argument(
        '-d', '--delete_data',
        help="DELETE ALSO DOWNLOADED IMAGERY DATA",
        action='store_true'
    )

    args = parser.parse_args()

    sys.exit(
        main(args.config.split(','), args.delete_job, args.delete_data)
    )
