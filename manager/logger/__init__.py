import os
import logging
import logging.config

from manager.logger.db import DbLogger, SUCCESS_JOB, \
    SUCCESS_IP_OPERATION, FAILED_IP_OPERATION, REJECTED_IP_OPERATION, \
    DbIpOperationStatus

# define custom level
logging.addLevelName(SUCCESS_JOB, "SUCCESS_JOB")
logging.addLevelName(SUCCESS_IP_OPERATION, "SUCCESS_IP_OPERATION")
logging.addLevelName(FAILED_IP_OPERATION, "FAILED_IP_OPERATION")
logging.addLevelName(REJECTED_IP_OPERATION, "REJECTED_IP_OPERATION")


class QCManagerLogger(logging.getLoggerClass()):
    """QC Job Manager logger.
    """
    def db_handler(self):
        """Return DbLogger.

        Raise Exception when DbLogger not defined.

        :return obj: db handler
        """
        for handler in self.handlers:
            if isinstance(handler, DbLogger):
                return handler

        raise Exception("DbLogger not found")

    def add_handler(self, handler):
        """Add a new handler.

        :param obj handler: handler to be added
        """
        if isinstance(handler, logging.FileHandler):
            handler.setFormatter(
                logging.root.handlers[0].formatter
            )

        super(QCManagerLogger, self).addHandler(handler)

    def success(self, msg, *args, **kwargs):
        """Print success job message.

        :param str msg: message to be printed
        """
        if self.isEnabledFor(SUCCESS_JOB):
            self._log(SUCCESS_JOB, msg, args, **kwargs)

    def ip_operation(self, msg, *args, **kwargs):
        """Print success ip_operation message.

        :param str msg: message to be printed
        """
        kwargs_ip_operation = {}
        for item in ['identifier',
                     'ip',
                     'timestamp',
                     'status',
                     'platform_type']:
            if item in kwargs:
                kwargs_ip_operation[item] = kwargs[item]
                del kwargs[item]

        self.db_handler().set_ip_operation(**kwargs_ip_operation)

        failed = DbIpOperationStatus.failed.value
        rejected = DbIpOperationStatus.rejected.value
        if self.isEnabledFor(SUCCESS_IP_OPERATION) and \
           kwargs_ip_operation['status'] not in (failed, rejected):
            log_level = SUCCESS_IP_OPERATION
        elif self.isEnabledFor(FAILED_IP_OPERATION) and \
             kwargs_ip_operation['status'] == failed:
            log_level = FAILED_IP_OPERATION
        elif self.isEnabledFor(REJECTED_IP_OPERATION) and \
             kwargs_ip_operation['status'] == rejected:
            log_level = REJECTED_IP_OPERATION
        self._log(log_level, msg, args, **kwargs)


def logger():
    """Return a logger.
    """
    logging.config.fileConfig(
        os.path.join(os.path.dirname(__file__), 'logging.conf')
    )

    logging.setLoggerClass(QCManagerLogger)
    logger = logging.getLogger('QCMMS')

    return logger


Logger = logger()
