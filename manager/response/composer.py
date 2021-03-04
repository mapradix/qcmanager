import os
import json
from string import Template
from jsonschema import validate, ValidationError

from manager.logger import Logger
from manager.io import JsonIO

class QCResponseComposer:
    """QI metadata response composer.

    :param str directory: path to directory where to store composed QI metadata (JSON files)
    """
    def __init__(self, directory):
        # switch to safe mode when template processor is used
        self.safe = True
        # if self.processors()[0].__class__.__name__ == 'QCProcessorTemplate' else False

        # read JSON schema
        with open(os.path.join(os.path.dirname(__file__),
                               'schema.json')) as fd:
            self._schema = json.load(fd)

        # create directory where output JSON file will be stored
        self.target_dir = os.path.join(
            directory,
            '{0:05d}'.format(Logger.db_handler().job_id())
        )
        if not os.path.exists(self.target_dir):
            os.makedirs(
                self.target_dir
            )

    @staticmethod
    def isnumeric(value):
        """Check if value is numeric of floating point.

        Note: isnumeric() cannot be used since it returns False for
        floating point data

        :param str value: value to be checked

        :return bool:
        """
        try:
            float(value)
        except ValueError:
            return False

        return True

    def render(self, response_ip):
        """Render template into composed QI metadata response.
        
        :param QCResponse response_ip: image product response to be rendered

        :return str: rendered JSON response
        """
        return json.dumps(
            response_ip.content(),
            indent=4,
            default=JsonIO.json_formatter_response
        )

    def get_filename(self, response_ip, processor=''):
        """Get response filename.

        :param QCResponse response_ip: image product QI metadata response to be rendered
        :param str processor: processor name for incremental responses

        :return str: filename
        """
        content = response_ip.content()

        return os.path.join(
            self.target_dir,
            processor,
            content['properties']['identifier'] + '.json'
        )

    def save(self, response_content, response_file):
        """Save rendered QI metadata response to file

        :param str response_content: response content to be saved
        :param str response_file: output file where response content (QI metadata) is stored
        """
        dir_name = os.path.dirname(response_file)
        if not os.path.exists(dir_name):
            # create response target dir if not exists
            os.makedirs(dir_name)

        with open(response_file, 'w') as fd:
            fd.write(response_content)

    def load(self, response_file):
        """Load QI metadata content from file.

        :param str response_file: input filename (JSON file)

        :return dict: file content or None on error
        """
        try:
            with open(response_file) as fd:
                response_content = fd.read()
        except FileNotFoundError:
            Logger.critical("No response file found")
            return None

        try:
            return json.loads(response_content)
        except json.decoder.JSONDecodeError as e:
            Logger.error("File {} is not valid JSON file: {}".format(
                response_file, e
            ))

        return response_content

    def dumps(self, response_file):
        """Dumps response file content.

        :param str response_file: input filename (JSON file)

        :return str: dumped content
        """
        return json.dumps(
            self.load(response_file),
            indent=4,
            default=JsonIO.json_formatter_response
        )

    def is_valid(self, response_file):
        """Validate response QI metadata file.

        :param str response_file: response file to be validated

        :return bool:
        """
        data = self.load(response_file)
        if data is None:
            return False

        try:
            validate(data, schema=self._schema)
            Logger.debug("JSON response {} is valid".format(response_file))
        except ValidationError as e:
            Logger.error("File {} validation against schema failed: {}".format(
                response_file, e
            ))
            return False

        return True
