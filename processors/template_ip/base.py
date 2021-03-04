from processors import QCProcessorIPBase
from abc import ABC

from manager.logger import Logger
from manager.io import JsonIO

class QCProcessorTemplateIPBase(QCProcessorIPBase):
    """Template image product processor base class.
    """
    def run(self):
        """Run processor.

        Define this functions only if your processor is the first in a queue.
        
        Check processors.search.base for a real example.
        """
        self.add_response(
            {
                "Sattelite name": self.name,
                "properties": {
                    "identifier": "template_ip"
                }
            }
        )
    
    def _run(self, meta_file, data_dir, output_dir):
        """Perform processor's tasks.

        Check processors.download.base for a real example.

        :param meta_file: path to JSON metafile
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        # get IP metadata
        data = JsonIO.read(meta_file)

        # print log meesage
        Logger.info("Processing IP title: {}".format(data['title']))

        # modify response attributes
        response = {
            'qcmms_delivery_status': 'finished'
        }

        return response
