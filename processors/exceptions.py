import os

from manager.logger import Logger
from manager.logger.db import DbIpOperationStatus


class ProcessorResultError(Exception):
    """Processor result error.

    :param object processor: processor class
    """
    def __init__(self, processor):
        Logger.critical(
            "{} processor: results not defined".format(processor.__class__.__name__)
        )

class ProcessorCriticalError(Exception):
    """Processor critical error.

    :param object processor: processor class
    :param str msg: message to be printed
    """
    def __init__(self, processor, msg):
        Logger.critical(
            "{} processor critical error: {}".format(
                processor.__class__.__name__, msg
        ))


class ProccessorDependencyError(Exception):
    """Processor software dependecy error.

    :param object processor: processor class
    :param str msg: message to be printed
    """
    def __init__(self, processor, msg):
        Logger.critical(
            "{} processor dependency error: {}".format(
                processor.__class__.__name__, msg
        ))


class ProcessorFailedError(Exception):
    """Processor computation fails.

    :param object processor: processor class
    :param str msg: message to be printed
    :param bool set_status: True to call set_response_status
    """
    def __init__(self, processor, msg, set_status=True):
        Logger.error(
            "{} processor failed: {}".format(
                processor.__class__.__name__, msg
        ))
        if set_status:
            # set processor response status
            processor.set_response_status(
                DbIpOperationStatus.failed
            )


class ProcessorRejectedError(Exception):
    """Processor computation ends with rejection.

    :param object processor: processor class
    :param str msg: message to be printed
    :param bool set_status: True to call set_response_status
    """
    def __init__(self, processor, msg, set_status=True):
        Logger.error(
            "{} processor rejected: {}".format(
                processor.__class__.__name__, msg
        ))
        if set_status:
            # set processor response status
            processor.set_response_status(
                DbIpOperationStatus.rejected
            )
