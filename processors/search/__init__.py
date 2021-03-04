#------------------------------------------
# author:    Lukas Brodsky
# copyright: Mapradix s.r.o.
# email:     lukas.brodsky@mapradix.cz
# status:    testing
# project:   QCMMS
# processor: 1: Image Products Access
# purpose:   Download S2 metdata, etc.
# site:      CZ - Prague
#------------------------------------------

import os

from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorSearch(QCProcessorMultiBase):
    """Multi-mission search processor [feasibility control]."""
    identifier = identifier_from_file(__file__)

    def __init__(self, config, response):
        super(QCProcessorSearch, self).__init__(
            config, response
        )

        self.metapath = os.path.join(
            self.config['project']['path'], self.config['project']['metapath']
        )

        # remove csv file (platform-dependent processors append new lines)
        csv_file = os.path.join(
            self.metapath,
            '{}_fullmetadata.csv'.format(
                self.config['land_product']['product_abbrev']
            )
        )
        if os.path.exists(csv_file):
            os.remove(csv_file)

    def processor_sentinel2(self):
        from processors.search.sentinel import QCProcessorSearchSentinel
        return QCProcessorSearchSentinel

    def processor_landsat8(self):
        from processors.search.landsat import QCProcessorSearchLandsat
        return QCProcessorSearchLandsat
