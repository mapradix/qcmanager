import os
import yaml
from copy import copy

from manager.exceptions import ConfigError
from manager.logger import Logger


class QCConfigParser:
    """Parse input configuration files (IF-MNG-PROCESS).
    
    :param list config_files: list of config files
    """
    def __init__(self, config_files):
        # read configuration
        self._config_files = []
        self._cfg = dict()
        for config_file in config_files:
            self._read_config_file(config_file)

        # be sure that abs path are used
        self._cfg['project']['path'] = self.abs_path(
            self._cfg['project']['path']
        )
        self._cfg['logging']['db'] = self.abs_path(
            self._cfg['logging']['db']
        )
        self._cfg['logging']['directory'] = self.abs_path(
            self._cfg['logging']['directory']
        )
        self._cfg['logging']['directory'] = self.abs_path(
            self._cfg['logging']['directory']
        )
        if self._cfg['project'].get('geometry_reference'):
            self._cfg['project']['geometry_reference'] = self.abs_path(
                self._cfg['project']['geometry_reference']
            )

        # store configuration into path directory
        self._store_to_project_path(config_files)

        # translate pixel metadata coding
        self._cfg['pixel_metadata_coding'] = self._get_raster_coding()

    def _get_raster_coding(self):
        """Get pixel metadata raster coding.

        :return dict: raster coding dict
        """
        rc = {}
        for section, items in self._cfg['pixel_metadata_coding'].items():
            rc[section] = []
            if section == 'geometry_quality':
                for k in items:
                    rc[section].append({
                        "name": k,
                        "min": items[k]['min'],
                        "max": items[k]['max']
                    })
            else:
                for k in items:
                    rc[section].append({
                        "name": k,
                        "min": items[k],
                        "max": items[k]
                    })

        return rc

    def _store_to_project_path(self, config_files):
        """Store config to project path.

        :param list config_files: list of config files
        """
        path = self._cfg['project']['path']
        if not os.path.exists(path):
            # create path if not exists
            os.makedirs(path)
        cfile = os.path.join(path, 'config.yaml')
        with open(cfile, 'w') as fd:
            for config_file in config_files:
                with open(config_file) as fd_c:
                    fd.write('### {}{}'.format(self.abs_path(config_file), os.linesep))
                    fd.write(fd_c.read())

        Logger.debug('Configuration stored to {}'.format(cfile))

    @staticmethod
    def abs_path(path):
        """Get QCManager-related absolute path.

        :return str: absolute path
        """
        if not os.path.isabs(path):
            # convert relative path in order to work in docker
            return os.path.normpath(
                os.path.join(
                    os.path.dirname(__file__),
                    '..',
                    path
            ))

        return path

    def update(self, config_file):
        """Update config from file.

        :param str config_file: path to config file
        """
        self._read_config_file(config_file)

    def _read_config_file(self, config_file):
        """Read configuration for single file

        :param str config_file: path to config file
        """
        if not os.path.isabs(config_file):
            config_file = os.path.normpath(
                os.path.join(
                    os.path.dirname(__file__),
                    '..',
                    config_file
            ))
        self._config_files.append(config_file)

        # read configuration into dictionary
        # see https://martin-thoma.com/configuration-files-in-python/
        try:
            with open(config_file, 'r') as ymlfile:
                try:
                    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
                except AttributeError:
                    # support also older versions of pyyaml
                    cfg = yaml.load(ymlfile)

                if 'logging' in cfg:
                    # set logging level
                    try:
                        Logger.setLevel(cfg['logging']['level'])
                    except KeyError:
                        pass # keep default log level
                # self._cfg.update(cfg)
                for key in cfg.keys():
                    if key in self._cfg:
                        if isinstance(cfg[key], list):
                            self._cfg[key] = cfg[key]
                        else: # assuming dict
                            for k, v in cfg[key].items():
                                self._cfg[key][k] = v
                    else:
                        self._cfg[key] = copy(cfg[key])
            Logger.debug("Config file '{}' processed".format(config_file))
        except Exception as e:
            raise ConfigError(config_file, e)

    def __getitem__(self, key):
        """Get item.
        
        :param str key: key to be queried
        
        :return str: value
        """
        return self._cfg[key]

    def __setitem__(self, key, item):
        """Set item.
        
        :param str key: key to be set
        :param str value: value to be set
        """
        self._cfg[key] = item

    def processor_not_found(self, identifier):
        """Processor not found, raise ConfigError.

        :param str identifier: processor identifier
        """
        raise ConfigError(','.join(self._config_files),
                          "processor '{}' not found".format(identifier)
        )

    def has_section(self, section):
        """Check if section is defined.

        :param str section: section name to be checked
    
        :return bool: True if section defined
        """
        return section in self._cfg.keys()

    def create_dir(self, directory):
        """Create directory if not exists.

        Raise ConfigError on error.

        :param str directory: directory to be created
        """
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
                Logger.debug(
                    "Directory {} created".format(
                        directory
                ))
        except PermissionError as e:
            raise ConfigError(
                self._config_files,
                "Directory {} failed to created: {}".format(
                    directory, e
            ))
