from manager.logger import Logger

class ConfigError(Exception):
    """Config parse error.

    :param str config_file: path to config file
    :param str msg: message to be printed
    """
    def __init__(self, config_file, msg):
        Logger.fatal("Config file(s) {}: {}".format(
            config_file, msg)
        )

class CatalogError(Exception):
    """Catalog access error.

    :param str msg: message to be printed
    """
    def __init__(self, msg):
        Logger.fatal("{}".format(msg))

class ResponseError(Exception):
    """Response access error.

    :param str msg: message to be printed
    """
    def __init__(self, msg):
        Logger.critical("{}".format(msg))
        
        
