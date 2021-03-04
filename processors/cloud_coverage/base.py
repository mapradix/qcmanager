import os
from datetime import datetime
import copy
import tempfile
from abc import ABC, abstractmethod

from processors import QCProcessorIPBase, identifier_from_file
from processors.exceptions import ProcessorFailedError

from manager.logger import Logger
from manager.io import JsonIO

from styles import StyleReader

import numpy as np

class QCProcessorCloudCoverageBase(QCProcessorIPBase, ABC):
    """Cloud coverage control processor abstract base class.

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    isMeasurementOf = "detailedControlMetric"
    isMeasurementOfSection = 'cloudCover'

    def __init__(self, config, response):
        super(QCProcessorCloudCoverageBase, self).__init__(
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
        """Compute cloud cover.

        :param str data_dir: image product directory
        :param str output_dir: target directory where to store cloud cover
        """
        pass

    @staticmethod
    @abstractmethod
    def get_lh_dir(data_dir):
        """Get data directory with L2 changed to L2H.

        :param str data_dir: level2 data directory.
        """
        pass

    @abstractmethod
    def compute_cc_stats(self, meta_data, data_dir, output_dir):
        """Compute cloud coverage statistics.

        :param str meta_data: metadata directory
        :param str data_dir: image product directory
        :param str output_dir: directory where cloud cover is stored
        """
        pass

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.

        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        response_data = {
            'isMeasurementOf': '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
            'value': False,
            "generatedAtTime": datetime.now(),
            self.isMeasurementOfSection : []
        }

        forced = False
        if not os.path.exists(self._result['qi.files']['fmask']):
            forced = True
            # compute only if not available (fmask can be time consuming task)
            if self.compute(data_dir, output_dir) is None:
                return response_data
        else:
            Logger.debug("Fmask file {} found, computation skipped".format(
                self._result['qi.files']['fmask']
            ))

        # resample mask to target resolution
        try:
            output_resp = self.resample_output(
                self._result['qi.files']['fmask'],
                self._result['qi.files']['output'], self.target_res, forced
            )
        except ProcessorFailedError:
            return response_data

        has_stack = 'harmonization_stack' in self.config['processors']
        try:
            stack_on = self.config['geometry']['stack_on']
            if stack_on is True and has_stack is False:
                Logger.warning("Harmonization stack not available. "
                               "Cloud coverage not appended")
                stack_on = False
        except KeyError:
            stack_on = has_stack

        # cloud cover section
        response_data[self.isMeasurementOfSection].append(
            self.compute_cc_stats(meta_data, data_dir, output_dir)
        )

        # alternate cloud cover
        response_data[self.isMeasurementOfSection].append(
            self._compute_alternate_cc_stats(self._result['qi.files']['output'])
        )
        response_data[self.isMeasurementOfSection][1].update({
            'mask': self.file_basename(output_resp),
            'rasterCoding': self.config['pixel_metadata_coding'][self.identifier],
            'lineage': self.get_lineage()
        })

        try:
            response_data['value'] = float(
                response_data[self.isMeasurementOfSection][1]['validPct']) > 0
        except KeyError:
            # no valid pixels
            response_data['value'] = False

        return response_data

    @staticmethod
    def _compute_alternate_cc_stats(filename):
        """Compute statistics for alternate cloud cover performed by Fmask.

        :param str filename: filename of cloud cover

        :return dict: QI metadata
        """
        from osgeo import gdal

        def value2key(value):
            conv = {
                0: "noDataPct",
                1: "validPct",
                2: "cloudPct",
                3: "cloudPct", # shadows
                4: "snowPct",
                5: "waterPct",
            }
            return conv[value]

        response_data = {
            "id": "http://qcmms.esa.int/detailed_control#ALTERNATIVE_CLOUD_COVER",
        }
        try:
            ds = gdal.Open(filename)
            band = ds.GetRasterBand(1)
            array = np.array(band.ReadAsArray())
            values, counts = np.unique(array, return_counts=True)
            ncells = band.XSize * band.YSize
            if sum(counts) != ncells:
                raise RuntimeError("mismatch")
            for idx in range(len(values)):
                key = value2key(values[idx])
                if key not in response_data:
                    response_data[key] = 0
                response_data[key] += int(counts[idx] / ncells * 100)
            ds = None
        except RuntimeError as e:
            raise ProcessorFailedError(
                self,
                "Computing statistics failed: {}".format(e)
            )

        return response_data

    def _get_target_stack(self, output_dir, data_dir):
        """Get stack of all bands for a scene.

        :param str output_dir: path to a directory where the stack is saved
        :param str data_dir: path to image product data directory

        :return str: path to the stacked tif
        """
        lh_title = os.path.split(output_dir)[-1]
        im_products = os.path.split(data_dir)[0]
        return os.path.join(
            im_products, output_dir, self.get_stack_name(output_dir)
        )

    @staticmethod
    def get_stack_length():
        """Get count of bands to be used in the stack.

        :return int:
        """
        return 6

    @staticmethod
    def get_stack_name(lh_title):
        """Get filename in format stack_[lh_title].tif.

        :param lh_title: title of L2H product

        :return str: stack name
        """
        return 'stack_{}.tif'.format(lh_title)

    @staticmethod
    def append_to_stack(stack_file, cloud_coverage_file, default_stack_length):
        """Append raster to a stack.

        :param str stack_file: target stack file
        :param str cloud_coverage_file: cloud coverage raster file
        :param int default_stack_length: number of bands
        """
        import rasterio
        import shutil

        with rasterio.open(stack_file) as stack:
            meta = stack.meta

            if meta['count'] > default_stack_length:
                Logger.debug(
                    'Not appending cloud coverage {} as a new band to stack '
                    '{}. Stack already existed with the cloud coverage '
                    'appended.'.format(cloud_coverage_file, stack_file)
                )
                return 0

            Logger.debug(
                'Appending cloud coverage {} as a new band to stack '
                '{}'.format(cloud_coverage_file, stack_file)
            )

            dtype = meta['dtype']

            stack_length = meta['count'] + 1
            meta.update(count=stack_length)
            fn = tempfile.mkstemp(prefix='stack_tmp_cloud_coverage_',
                                  suffix='.tif')[1]
            stack_tmp_path = os.path.join(tempfile.gettempdir(), fn)

            with rasterio.open(stack_tmp_path, 'w', **meta) as stack_tmp:
                stack_tmp.write(stack.read(1), 1)

                for band_id in range(2, stack_length):
                    stack_tmp.write(stack.read(band_id), band_id)

                stack_tmp.write(
                    rasterio.open(cloud_coverage_file).read(1).astype(dtype),
                    stack_length)

        # replace the orig stack with the one with radiometry_control appended
        os.remove(stack_file)
        shutil.move(stack_tmp_path, stack_file)
