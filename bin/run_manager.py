#!/usr/bin/env python3

import os
import sys
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from manager import QCManager
from manager.exceptions import ConfigError, CatalogError
from processors.exceptions import ProcessorCriticalError


def main(config_files, processors=[], list_processors=False, quiet=True):
    try:
        manager = QCManager(config_files, quiet=quiet)
    except ConfigError:
        return 1

    if list_processors:
        manager.list_processors()
        return 0

    try:
        # define list of processors
        manager.set_processors(processors)

        # run processors in given order
        manager.run()
    except (ConfigError, ProcessorCriticalError, CatalogError):
        return 1

    # send response to catalog & print response to standard output
    manager.send_response()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run QCManager as standalone program.")
    parser.add_argument(
        '-c', '--config',
        help="Use case config file(s)",
        type=str
    )
    parser.add_argument(
        '-p', '--processors',
        help="List of processors to run (overrides configuration)",
        type=str
    )
    parser.add_argument(
        '-l', '--list-processors',
        help="Print processors documentation and exit",
        action='store_true'
    )
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help="Do not print QI metadata on stdout",
    )
    args = parser.parse_args()

    if args.list_processors is False and args.config is None:
        sys.exit("{}: error: argument -c/--config: expected one argument".format(__file__))
    elif args.list_processors is True:
        # use dummy configuration for listing processors
        config_files = [
            'use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague.yaml'
        ]
    else:
        config_files = args.config.split(',')
        
    sys.exit(
        main(
            config_files,
            args.processors.split(',') if args.processors else [],
            args.list_processors,
            args.quiet
        )
    )
