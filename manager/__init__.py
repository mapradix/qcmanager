import os
import sys
import time
import logging
import shutil
from copy import copy

__version__ = 2.0

from manager.config import QCConfigParser
from manager.exceptions import ConfigError, CatalogError
from manager.logger import Logger
from manager.logger.db import DbConnectionError, DbLogger
from manager.catalog import QCCatalogPoller
from manager.response.composer import QCResponseComposer
from manager.io import JsonIO

from processors.exceptions import ProcessorResultError, ProcessorFailedError
from processors import QCProcessorBase

# import processors dynamically
# see https://www.bnmetrics.com/blog/dynamic-import-in-python3
import inspect
import pkgutil
from importlib import import_module
processors_path = os.path.join(os.path.dirname(__file__), '..', 'processors')
processors_list = []
for (_, name, _) in pkgutil.iter_modules([processors_path]):
    if name in ['exceptions', 'landsat', 'sentinel']:
        continue
    imported_module = import_module('processors.' + name, package=__name__)
    for i in dir(imported_module):
        attribute = getattr(imported_module, i)
        if inspect.isclass(attribute) and issubclass(attribute, QCProcessorBase) and \
           attribute.__name__ not in (
               'QCProcessorBase',
               'QCProcessorIPBase',
               'QCProcessorLPBase',               
               'QCProcessorMultiBase'):
            processors_list.append(attribute)
            setattr(sys.modules[__name__], name, attribute)


class QCManager:
    """
    QC Jobs Manager implementation.

    :param list config_files: list of configuration files
    :param int cleanup: clean-up before running the manager
                        job id to be removed (-1 for deleting all downloaded
                        files, remove logs)
    :param bool quiet: run in quiet mode (no response is printed out to stdout)
    """
    def __init__(self, config_files, cleanup=None, quiet=False):
        self._quiet = quiet

        # parse config file
        self._config_files = copy(config_files)
        if 'config.yaml' not in self._config_files:
            self._config_files.insert(0, 'config.yaml')
        self.config = QCConfigParser(self._config_files)

        # used by _add_db_handler()
        self._db_file = os.path.abspath(self.config['logging']['db'])
        Logger.debug("Using logging db: {}".format(self._db_file))

        # force cleanup if requested
        if cleanup is not None:
            self._cleanup(cleanup)
            return

        # list of processor to run (will be defined by
        # self.set_processors())
        self._processors = []

        # set up db-based logger
        log_dir = os.path.dirname(self._db_file)
        # create logging directory if not exists
        self.config.create_dir(
            log_dir
        )
        # add DB handler
        self._add_db_handler()

        # create logging directory if not exists
        self.config.create_dir(
            self.config['logging']['directory']
        )

        # set up file-based logger
        Logger.add_handler(
            logging.FileHandler(self._get_log_file())
        )

        self.response = []

        # response composer
        self._response_composer = QCResponseComposer(self.config['logging']['directory'])

        Logger.info("QCManager started")

    def __del__(self):
        """QC Job Manager destructor.
        """
        if not hasattr(self, "config"):
            return
        www_dir = self.config['catalog'].get('www_dir')
        if www_dir:
            # copy also logs
            target = os.path.join(
                www_dir,
                self.config['catalog']['ip_parent_identifier'].split(':')[-1],
                os.path.basename(self.config['logging']['directory'])
            )
            if os.path.exists(target):
                shutil.rmtree(target)
            if os.path.exists(self.config['logging']['directory']):
                shutil.copytree(
                    self.config['logging']['directory'],
                    target
                )
                Logger.debug("Logs from {} copied to {}".format(
                    self.config['logging']['directory'],
                    target
                ))

    def _add_db_handler(self):
        """Add DB logging handler.

        :param str db_file: path to DB file
        """
        try:
            Logger.addHandler(
                DbLogger(self.config['catalog']['ip_parent_identifier'])
            )
            Logger.db_handler().set_session(
                self._db_file
            )
        except DbConnectionError as e:
            raise ConnectionError('{}: {}'.format(e, self._db_file))

    def _get_log_file(self, job_id=None):
        """Get log file filepath

        :param int job_id: selected job id or None for current
        """
        if not job_id:
            job_id = Logger.db_handler().job_id()

        return os.path.abspath(
            os.path.join(
                self.config['logging']['directory'],
                '{0:05d}.log'.format(job_id)
        ))

    def set_processors(self, processors=[]):
        """Set list of processors to be performed.

        :param list processors: list of processors to be registered
                                (if none than configuration will be used)
        """
        if not processors:
            try:
                processors = self.config['processors']
            except KeyError:
                raise ConfigError(self._config_files,
                                  "list of processors not defined"
                )
        else:
            # override configuration
            self.config['processors'] = processors

        if not processors:
            return

        for identifier in processors:
            found = False
            for processor in processors_list:
                if processor.identifier == identifier:
                    Logger.debug("'{}' processor registered".format(
                        processor.identifier
                    ))
                    self._processors.append(
                        processor(self.config, self.response)
                    )
                    found = True
                    break

            if not found:
                self.config.processor_not_found(identifier)

    def processors(self):
        """Get list of registered processors.

        :return list: list of processors
        """
        return self._processors

    def run(self):
        """Run all registered processors from queue.
        """
        # check if processors defined
        if not self._processors:
            raise ConfigError(self._config_files,
                              "list of processors not defined"
            )

        # determine current/previous job id
        job_id = Logger.db_handler().job_id()
        prev_job_id = Logger.db_handler().last_job_id(self.config['processors'][0])
        Logger.info("Job started (id {})".format(job_id))
        Logger.db_handler().job_started()
        if prev_job_id:
            Logger.debug("Previous job found (id {})".format(prev_job_id))
        else:
            Logger.debug("No previous job found. Starting from scratch")

        start = time.time()
        idx = 0
        for proc in self._processors:
            try:
                # run the processor
                proc.run()
                try:
                    Logger.info('{} processor result: {}'.format(
                        proc.identifier, proc.result()
                    ))
                except ProcessorResultError:
                    pass

                # store JSON after each processor
                self.save_response(proc)
            except ProcessorFailedError:
                pass

            idx += 1

        Logger.success(
            "Job {} successfully finished in {:.6f} sec".format(
                job_id, time.time() - start
        ))

    def list_processors(self, processors=[]):
        """Print processors documentation to stdout.

        :param list processors: list of processors (empty list for all)
        """
        print('-' * 80)
        for processor in processors_list:
            if not processors or (processors and processor.identifier in processors):
                print('*', processor.identifier)
                if processor.__doc__:
                    print(processor.__doc__.splitlines()[0])
                print('-' * 80)

    def _cleanup(self, job_id=None):
        """Perform manager clean up.

        :param int job_id: remove only selected job
        """
        # logging DB
        if job_id < 0:
            # all
            log_db = self.config['logging']['db']
            if os.path.exists(log_db):
                os.remove(log_db)
                Logger.debug("Logging DB {} removed".format(
                    log_db
                ))
        else:
            # single job
            self._add_db_handler()
            Logger.db_handler().delete_job(job_id)

        # logging dir
        if job_id < 0:
            # all
            log_dir = self.config['logging']['directory']
            if os.path.exists(log_dir):
                shutil.rmtree(log_dir)
                Logger.debug("Logging directory {} removed".format(
                    log_dir
                ))
        else:
            # single job
            log_file = self._get_log_file(job_id)
            if os.path.exists(log_file):
                os.remove(log_file)
                Logger.debug("Logging file {} removed".format(
                    log_file
                ))
            log_dir = os.path.splitext(log_file)[0]
            if os.path.exists(log_dir):
                shutil.rmtree(log_dir)
                Logger.debug("Logging directory {} removed".format(
                    log_dir
                ))

        if job_id > 0:
            return

    def cleanup_data(self):
        """Cleanup/Remove downloaded data."""
        # remove downloaded data
        for d in ('metapath', 'downpath'):
            dirpath = os.path.join(self.config['project']['path'],
                                   self.config['project'][d]
            )
            if os.path.exists(dirpath):
                shutil.rmtree(dirpath)
                Logger.debug("Project directory {} removed".format(
                    dirpath
                ))

    def save_response(self, processor=None):
        """Produce manager response. Save QI metadata on disk.

        :param QCProcesor processor: processor name for incremental response

        :return str: target path
        """
        response_content = None
        for response in self.response:
            # render IP response into string
            response_content = self._response_composer.render(
                response
            )

            # save response content to JSON file (incremental)
            if processor:
                self._response_composer.save(
                    response_content,
                    self._response_composer.get_filename(response, processor.identifier)
                )

            # save response content to JSON file (final)
            self._response_composer.save(
                response_content,
                self._response_composer.get_filename(response)
            )

        if not response_content:
            Logger.warning("No response content")

        return self._response_composer.target_dir

    def send_response(self):
        """
        Send response (QI metadata) to catalog.

        QI metadata is also printed into stdout if queit flag not specified.
        """
        # connect to catalog
        catalog = QCCatalogPoller(self.config['catalog'])

        for response in self.response:
            response_file = self._response_composer.get_filename(response)

            # print response to standard output
            if not self._quiet:
                print(self._response_composer.dumps(response_file))

            # check validity
            if not self._response_composer.is_valid(response_file):
                continue

            # catalog access
            try:
                catalog.send(self._response_composer.load(response_file))
            except CatalogError as e:
                return

    def reset_response(self):
        """Reset response stack.
        """
        self.response = []
