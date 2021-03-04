import os
from datetime import datetime

from processors import QCProcessorLPBase, identifier_from_file
from processors.exceptions import ProcessorRejectedError

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger
from manager.io import JsonIO


class QCProcessorLPInterpretationControl(QCProcessorLPBase):
    """Land Product interpretation control processor [interpretation control].
       Be aware it is an optional processor!
    """
    identifier = identifier_from_file(__file__)
    isMeasurementOf = "lpInterpretationMetric"
    isMeasurementOfSection = "qualityIndicators"

    def check_dependency(self):
        """Check processor's software dependencies.
        """
        pass

    def _run(self):
        """Perform processor's tasks.

        :return dict: QI metadata
        """
        Logger.info('Running interpretation control')
        response_data = {
            "isMeasurementOf": '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
            "generatedAtTime": datetime.now(),
            "value": False
        }

        lp_interpretation = self._read_interpretation_qi()
        if lp_interpretation is None:
            # no LP interpretation metadata available
            return {}

        for ltype in self.config['land_product']['product_type']:
            if ltype == 'classification':
                if lp_interpretation[ltype]['overallAccuracy'] >= \
                   self.config['land_product']['thematic_accuracy']:
                    response_data['value'] = True
                else:
                    response_data['value'] = False

                response_data[ltype] = lp_interpretation[ltype]

            elif ltype == 'regression':
                reg_name = self.config['land_product']['regression_name']
                if 'rmse_accuracy' in self.config['land_product']:
                    if lp_interpretation[reg_name]['rmse'] <= \
                       self.config['land_product']['rmse_accuracy']:
                        response_data['value'] = True
                    else:
                        response_data['value'] = False

                response_data[reg_name] = lp_interpretation[reg_name]

        if response_data['value'] is False:
            self.set_response_status(DbIpOperationStatus.rejected)

        return response_data

    def _read_interpretation_qi(self):
        """Read LP interpretation quality indicators.

        :return dict: quality indicators
        """
        try:
            lp_interpretation_fn = os.path.join(
                self.config['map_product']['path'],
                self.config['map_product']['map_interpretation_qi']
            )
        except KeyError:
            Logger.info("{} is not defined".format('map_interpretation_qi'))
            return None
            
        if not os.path.isfile(lp_interpretation_fn):
            Logger.info("File {} not found".format(lp_interpretation_fn))
            return None

        return JsonIO.read(lp_interpretation_fn)
