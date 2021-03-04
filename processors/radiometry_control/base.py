import os
from datetime import datetime
import tempfile
from abc import ABC, abstractmethod

from manager.logger import Logger # must be called before processors.exceptions

from processors import QCProcessorIPBase
from processors.exceptions import ProcessorFailedError

class QCProcessorRadiometryControlBase(QCProcessorIPBase, ABC):
    """Radiometry control processor abstract base class.

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    isMeasurementOf = "detailedControlMetric"
    isMeasurementOfSection = 'radiometry'

    def __init__(self, config, response):
        super(QCProcessorRadiometryControlBase, self).__init__(
            config, response
        )

        self.target_res = int(self.config['land_product']['geometric_resolution'])
        # results
        # -> self._result['qi.files']['output']
        self.add_qi_result(
            'output',
            '{}m.tif'.format(self.target_res)
        )

    @abstractmethod
    def compute(self, data_dir, output_dir):
        """Compute radiometry for image product.
        
        :param str data_dir: image product data directory
        :param str output_dir: directory where to store results
        """
        pass

    @staticmethod
    @abstractmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2 changed to L2H.

        :param str data_dir: image product data directory
        """
        pass

    def check_dependency(self):
        """Check processor's software dependecies.
        """
        import rasterio

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.

        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        response_data = {
            self.isMeasurementOfSection: [
                {
                    "id": "http://qcmms.esa.int/detailed_control#SATURATION",
                    "generatedAtTime": datetime.now()
                }
            ]
        }

        try:
            rq_file = self.compute(data_dir, output_dir)
        except ProcessorFailedError:
            return response_data

        # resample mask to target resolution
        try:
            output_target = self._result['qi.files']['output']
            output_resp = self.resample_output(rq_file, output_target, self.target_res)
        except ProcessorFailedError:
            return response_data

        has_stack = 'harmonization_stack' in self.config['processors']
        try:
            stack_on = self.config['geometry']['stack_on']
            if stack_on is True and has_stack is False:
                Logger.warning("Harmonization stack not available. "
                               "Radiometry not appended")
                stack_on = False
        except KeyError:
            stack_on = has_stack

        # radiometry stats
        response_data[self.isMeasurementOfSection][0].update(
            self._compute_stats(output_target)
        )

        # add mask file & coding
        response_data[self.isMeasurementOfSection][0].update({
            'mask': self.file_basename(output_resp),
            'rasterCoding': self.config['pixel_metadata_coding'][self.identifier],
            'lineage': self.get_lineage()
        })

        return response_data

    def _compute_stats(self, filename):
        """Compute radiometry statistics.

        :param str filename: image file

        :return dict: QI metadata
        """
        value, count, ncells = self.compute_value_count(filename)

        rq_sat_Cnt = 0.0
        rq_sat_Pct = 0.0
        for idx in range(len(value)):
            if value[idx] == 1:
                rq_sat_Cnt = float(count[idx])
                rq_sat_Pct = count[idx] / ncells * 100

        return { "noRqSatPx": int(rq_sat_Cnt),
                 "rqSatPct": int(rq_sat_Pct)
        }

    def _get_target_stack(self, output_dir, correct_shifts):
        """Get stack of all bands for a scene.

        :param output_dir: path to a directory where the stack is saved
        :param bool correct_shifts: True for correcting shifts

        :return: path to the stacked tif
        """
        lh_title = os.path.split(output_dir)[-1]
        stack_name = self.get_stack_name(lh_title, correct_shifts)
        return os.path.join(output_dir, stack_name)

    @staticmethod
    def get_stack_name(lh_title, correct_shifts):
        """Get filename in format stack_[lh_title].tif.

        :param lh_title: title of L2H product
        :param bool correct_shifts: True for correcting shifts

        :return str: filename
        """
        if correct_shifts:
            return 'coreg_stack_{}.tif'.format(lh_title)
        else:
            return 'stack_{}.tif'.format(lh_title)

    @staticmethod
    def append_to_stack(stack_file, radiometry_file, default_stack_length):
        """Append raster to a stack.

        :param str stack_file: target stack file
        :param str radiometry_file: radiometry control raster file
        :param int default_stack_length: number of bands
        """
        import rasterio
        import shutil

        with rasterio.open(stack_file) as stack:
            meta = stack.meta

            if meta['count'] > default_stack_length + 3:
                Logger.debug(
                    'Not appending radiometry file {} as a new band to stack '
                    '{}. Stack already existed with the radiometry '
                    'appended.'.format(radiometry_file, stack_file)
                )
                return 0

            Logger.debug('Appending radiometry file {} as a new band to stack '
                         '{}'.format(radiometry_file, stack_file))

            dtype = meta['dtype']

            stack_length = stack.meta['count'] + 1
            meta.update(count=stack_length)
            fn = tempfile.mkstemp(prefix='stack_tmp_radiometry_',
                                  suffix='.tif')[1]
            stack_tmp_path = os.path.join(tempfile.gettempdir(), fn)

            with rasterio.open(stack_tmp_path, 'w', **meta) as stack_tmp:
                stack_tmp.write(stack.read(1), 1)

                for band_id in range(2, stack_length):
                    stack_tmp.write(stack.read(band_id), band_id)

                stack_tmp.write(
                    rasterio.open(radiometry_file).read(1).astype(dtype),
                    stack_length)

        # replace the orig stack with the one with radiometry_control appended
        os.remove(stack_file)
        shutil.move(stack_tmp_path, stack_file)

    @staticmethod
    def get_stack_length():
        """Get count of bands to be used in the stack.

        :return int:
        """
        return 6
