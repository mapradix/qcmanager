from processors import QCProcessorLPBase, identifier_from_file

import os
import glob
import datetime
from lxml import etree

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger


class QCProcessorLPMetadataControl(QCProcessorLPBase):
    """Land Product metadata control processor [validation control].
    """
    identifier = identifier_from_file(__file__)
    isMeasurementOf = "lpMetadataControlMetric"
    isMeasurementOfSection = "qualityIndicators"

    def check_dependency(self):
        """Check processor's software dependencies.
        """
        from lxml import etree

    def _run(self):
        """Perform processor's tasks.

        :return dict: QI metadata
        """
        from lxml import etree

        Logger.info('Running LP metadata control')

        response_data = {
            "isMeasurementOf": '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
            "generatedAtTime": datetime.datetime.now(),
            "value": False,
        }

        try:
            lp_meta_fn = glob.glob(os.path.join(
                self.config['map_product']['path'],
                self.config['land_product']['product_metadata'] + '*.xml'))[0]
        except IndexError:
            self.set_response_status(DbIpOperationStatus.rejected)            

        if 'product_metadata' in self.config['land_product']:
            response_data['metadataSpecification'] = self.config['land_product']['product_metadata']
        else:
            response_data['metadataSpecification'] = ''

        try:
            Parser = etree.HTMLParser()
            XMLDoc = etree.parse(open(lp_meta_fn, 'r'), Parser)
            Logger.info('Land Product metadata available')
            response_data['metadataAvailable'] = True

            # validate xml
            Elements = XMLDoc.xpath('//characterstring')
            i = 0
            for Element in Elements:
                if (Element.text) is not None:
                    i += 1
            if i > 5:
                response_data['metadataCompliancy'] = self.validate_xml_metadata(lp_meta_fn)

        except:
            Logger.error('Land Prodcut metadata not available')
            response_data['metadataAvailable'] = False
            response_data['metadataCompliancy'] = False

        if response_data['metadataAvailable'] == True and \
           response_data['metadataCompliancy'] == True:
            response_data['value'] = True
    
        if response_data['value'] is False:
            self.set_response_status(DbIpOperationStatus.rejected)

        return response_data



    def validate_xml_metadata(self, lp_meta_fn):
        """Check validity of XML LP metdata

        :param str lp_meta_fn: LP mentadata path and filename

        :return bool: true if metadata valid
        """
        xsd_path = './processors/lp_metadata_control/lp_schema.xsd'
        xmlschema_doc = etree.parse(xsd_path)
        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_doc = etree.parse(lp_meta_fn)
        result = xmlschema.validate(xml_doc)
        if result:
            Logger.info('Land Product INSPIRE metadata is valid')

        return result
