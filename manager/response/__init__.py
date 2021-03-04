import os
from string import Template

from manager.logger import Logger
from manager.io import JsonIO
from manager.exceptions import ResponseError

class QCResponse:
    """QI metadata response.

    :param dict response_data: QI metadata
    """
    def __init__(self, response_data):
        self._response_data = response_data
        self._qi = self._response_data["properties"]\
            ["productInformation"]\
            ["qualityInformation"]\
            ["qualityIndicators"]

        self.status = None # DbOperationStatus to be set by processors

    def content(self):
        """Get QI metadata content.
        
        :return dict: QI metadata
        """
        return self._response_data

    def get(self, isMeasurementOf):
        """Get isMeasurementOf data item.

        :param str isMeasurementOf: isMeasurementOf tag name

        :return str: tag value or None if not found
        """
        for qi_item in self._qi:
            is_m_o = qi_item.get('isMeasurementOf')
            # http://qcmms.esa.int/quality-indicators/#feasibilityControlMetric
            # -> feasibilityControlMetric
            if is_m_o and \
               is_m_o[is_m_o.find('#')+1:] == isMeasurementOf:
                return qi_item

        return None

    def update(self, data, isMeasurementOf):
        """Update response data.

        :param dict data: updated data
        :param str isMeasurementOf: isMeasurementOf tag name
        """
        qi_item = self.get(isMeasurementOf)
        if qi_item:
            qi_item.update(data)
        else:
            self._qi.append(data)

    def get_value(self, isMeasurementOf):
        """Get attribute value of isMeasurementOf tag.

        :param str isMeasurementOf: isMeasurementOf tag name

        :return str: value
        """
        return self.get(isMeasurementOf)['value']

    def set_value(self, isMeasurementOf, value):
        """Set attribute value of isMeasurementOf tag.

        :param str isMeasurementOf: isMeasurementOf tag name
        :param str value: value to be set
        """
        self.get(isMeasurementOf)['value'] = value
