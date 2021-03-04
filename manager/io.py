import os
import sys
import json
import datetime
import re

from manager.logger import Logger


def datetime_format(dt, format_msec=True):
    """Format datetime

    :param datetime dt: date-time object
    :param bool format_msec: True to round miliseconds to two digits

    :return str: formated datetime
    """
    str_dt = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    if not format_msec:
        return str_dt

    return str_dt[:-4] + "Z"


class JsonIO:
    """JSON reader/writter.
    """
    @staticmethod
    def json_formatter(o):
        """Format JSON object.

        Datetime objects are formatted properly.

        :param o: data object

        :return str: formatted datetime
        """
        if isinstance(o, (datetime.date, datetime.datetime)):
            return datetime_format(o, format_msec=False)

    @staticmethod        
    def json_formatter_response(o):
        """Format JSON object for QI metadata response.

        Datetime objects are formatted properly.

        :param o: data object

        :return str: formatted datetime
        """
        if isinstance(o, (datetime.date, datetime.datetime)):
            return datetime_format(o)

    @staticmethod
    def _json_parser(o):
        """JSON parser.

        :param dict: data to be parsed

        :return dict: parsed data
        """
        pattern = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:"
        for k, v in o.items():
            if isinstance(v, str) and re.search(pattern + ".*", v):
                if re.search(pattern + "[0-9]{2}\.[0-9]*", v):
                    p = '%Y-%m-%dT%H:%M:%S.%f'
                else:
                    p = '%Y-%m-%dT%H:%M:%S'
                o[k] = datetime.datetime.strptime(v, p)

        return o

    @classmethod
    def json_parser(cls, o):
        """Parse input JSON object.

        Timestamps are parsed as datetime objects.

        :param dict o: data object to be parsed

        :return dict: parsed data
        """
        return cls._json_parser(o)

    @classmethod
    def read(cls, filename, response=False):
        """Read JSON file.

        :param str filename: JSON filename to parse
        :param bool parse_response: True for parsing JSON to be response-valid

        :return dict: data content
        """
        Logger.debug("Reading {} (response: {})".format(filename, response))
        parser = None if response else cls.json_parser
        with open(filename) as fd:
            data = json.load(
                fd,
                object_hook=parser
            )
        return data

    @classmethod
    def write(cls, filename, data):
        """Write JSON data into file.

        :param str filename: filename where to write data
        :param dict data: dictionary data to be saved
        """
        with open(filename, 'w') as fd:
            json.dump(
                data,
                fd,
                default=cls.json_formatter,
            )

class CsvIO:
    """CSV reader/writter.
    """
    @staticmethod
    def csv_formatter(o):
        """Format CSV object.

        Datetime objects are formatted properly.

        :param o: data object

        :return str: formatted datetime
        """
        if isinstance(o, (datetime.date, datetime.datetime)):
            return datetime_format(o, format_msec=False)
        if not isinstance(o, str):
            return str(o)

        if os.linesep in o:
            o = o.replace(os.linesep, '')
        return str(o)

    @classmethod
    def write(cls, filename, data, delimiter=';', append=False):
        """Write CSV data into file.

        :param str filename: filename where to write data
        :param dict data: dictionary data to be saved
        :param str delimiter: delimiter to be used
        :param bool append: True for append otherwise overwrite
        """
        mode = 'a' if append else 'w'
        Logger.debug("File {} open with {} mode".format(
            filename, mode
        ))
        with open(filename, mode) as fd:
            fd.write(delimiter.join(data[0].keys()))
            fd.write(os.linesep)
            for item in data:
                fd.write(delimiter.join(map(cls.csv_formatter, item.values())))
                fd.write(os.linesep)
