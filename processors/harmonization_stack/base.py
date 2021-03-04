from abc import ABC, abstractmethod
import os
import tempfile
import shutil

from manager.logger import Logger # must be called before processors.exceptions
from manager.logger.db import DbIpOperationStatus

from processors import QCProcessorIPBase
from processors.exceptions import ProcessorRejectedError, ProcessorFailedError


class QCProcessorHarmonizationStackBase(QCProcessorIPBase, ABC):
    """Processor creating a resampled stack from individual band files (abstract base class).

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """

    isMeasurementOf = "ordinaryControlMetric"
    isMeasurementOfSection = "stack"

    def __init__(self, config, response):
        super(QCProcessorHarmonizationStackBase, self).__init__(
            config, response
        )

        # force output_dir (requires defining _get_ip_output_path())
        self.output_path = os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath']
        )

        # temp dir (data can be large)
        self._tmpdir = os.path.join(
            tempfile.gettempdir(),
            '{}_{}'.format(self.__class__.__name__, os.getpid())
        )
        os.makedirs(self._tmpdir)

    def __del__(self):
        """Override normal __del__ method to delete also temp files."""
        shutil.rmtree(self._tmpdir)

    def check_dependency(self):
        """Check processor software dependecies.
        """
        import rasterio
        from osgeo import gdal, gdalconst

    @abstractmethod
    def get_band_ids(self, data_dir):
        """Get filenames for all bands.

        :param str data_dir: path to data directory
        """
        pass

    @abstractmethod
    def _get_ip_output_path(self, ip):
        """Get IP output path with L2 changed to L2H.

        :param str ip: image product title

        :retutn str: file path
        """
        pass

    @staticmethod
    @abstractmethod
    def get_ordinary_control_processor():
        """Get platform specific processor for ordinary control."""
        pass

    @staticmethod
    @abstractmethod
    def reproject(driver, out_file, in_image, in_proj, ref_image, ref_trans,
                  ref_trans_new, ref_proj):
        """Reproject and resample a band intended resolution.

        :param driver: Driver determining the format of the written file
        :param out_file: Path to the temporary file where the resampled band
            will be created
        :param in_image: Path to the original, not-resampled band
        :param in_proj: Projection of in_image
        :param ref_image: A GDAL object representing the reference image
        :param ref_trans: Geo transform (transformation coefficients) of
            the reference image
        :param ref_trans_new: Geo transform of the reference image, where
            the resolution is changed to the intended one
        :param ref_proj: Projection of ref_image
        """
        pass

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.

        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        response_data = {}

        # reference image must be defined
        try:
            im_reference = self.config.abs_path(
                self.config['geometry']['reference_image']
            )
        except KeyError:
            Logger.error(
                "Reference image not defined"
            )
            self.set_response_status(DbIpOperationStatus.failed)
            return response_data
        if not os.path.exists(im_reference):
            Logger.error(
                "Reference image '{}' not found".format(im_reference)
            )
            self.set_response_status(DbIpOperationStatus.failed)
            return response_data

        # check if stack is already available
        output_dir = self._get_ip_output_path(meta_data['title'])
        if os.path.exists(output_dir):
            files = self.filter_files(output_dir, extension='.tif',
                                      pattern='stack_*')

            if len(files) > 0:
                if all([os.stat(f).st_size > 0 for f in files]):
                    Logger.debug('Stack ({}) already available, no operation '
                                 'done'.format(output_dir))
                    response_data.update(self._run_stack_ordinary_control(
                        meta_data, data_dir, output_dir, response_data
                    ))
                    return response_data

                Logger.debug('Stack ({}) already available, but has size 0 B. '
                             'A new one will be created.'.format(output_dir))

        # compute stack
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        try:
            lh_title = os.path.split(output_dir)[-1]
            stack_name = self.get_stack_name(lh_title)
            self.create_stack(data_dir, output_dir, stack_name)
            response_data.update(self._run_stack_ordinary_control(
                meta_data, data_dir, output_dir, response_data
            ))
        except ProcessorRejectedError:
            self.set_response_status(DbIpOperationStatus.rejected)
        except ProcessorFailedError:
            self.set_response_status(DbIpOperationStatus.failed)

        return response_data

    def create_stack(self, data_dir, output_dir, stack_name):
        """Create stack of all bands.

        :param data_dir: directory with the Sentinel scene
        :param output_dir: path to a directory where the stack will be saved
        :param stack_name: stack filename
        """
        import rasterio

        paths_resampled = self._resample_bands(data_dir)

        with rasterio.open(paths_resampled[0]) as band1:
            meta = band1.meta

            if meta['driver'] != 'GTiff':
                meta['driver'] = 'GTiff'

            stack_length = self.get_stack_length()
            meta.update(count=stack_length)
            stack_path = os.path.join(output_dir, stack_name)
            Logger.debug("Creating stack {} from {} bands...".format(
                stack_path, len(paths_resampled)
            ))
            with rasterio.open(stack_path, 'w', **meta) as stack:
                stack.write(band1.read(1), 1)

                for band_id, band in enumerate(paths_resampled[1:], start=2):
                    with rasterio.open(band) as b:
                        x = b.read(1)
                        stack.write(x, band_id)

                    # resampled single band not needed anymore
                    os.remove(band)

        # delete also the first band
        os.remove(paths_resampled[0])

    @staticmethod
    def get_stack_length():
        """Get count of bands to be used in the stack."""
        return 6

    @staticmethod
    def get_stack_name(lh_title):
        """Get filename in format stack_[lh_title].tif.

        :param lh_title: title of L2H product

        :return str: stack name
        """
        return 'stack_{}.tif'.format(lh_title)

    def _resample_bands(self, data_dir):
        """Resample bands with different spatial resolution to the intended one.

        Create in the same directory also symlinks to the 10m resolution ones.

        :param str data_dir: directory with the Sentinel scene

        :return: paths to resampled bands
        """
        from osgeo import gdal, gdalconst

        paths = []

        resolution = self.config['land_product'].get('geometric_resolution',
                                                     10)

        ref_path = self.config['geometry'].get('reference_image')
        if ref_path is None:
            raise ProcessorFailedError(
                self,
                "Reference image (geometry:reference_image) not defined "
                "by configuration"
            )
        ref_image = gdal.Open(ref_path, gdalconst.GA_ReadOnly)
        if ref_image is None:
            raise ProcessorFailedError(
                self,
                "Unable to open reference image {}".format(ref_path)
            )

        ref_proj = ref_image.GetProjection()
        ref_trans = ref_image.GetGeoTransform()
        driver = gdal.GetDriverByName('GTiff')

        # update geotransform to new values
        ref_trans_list = list(ref_trans)
        ref_trans_list[1] = resolution
        ref_trans_list[5] = -resolution
        ref_trans_new = tuple(ref_trans_list)

        bands = self.get_band_ids(data_dir)

        for band in bands:
            pref_string = 'resampled_{}_'.format(band[:3])
            try:
                im_path = self.filter_files(data_dir, band)[0]
            except IndexError:
                raise ProcessorRejectedError(
                    self,
                    "Unable to find {} in {}".format(band, data_dir)
                )

            in_image = gdal.Open(im_path, gdalconst.GA_ReadOnly)
            in_proj = in_image.GetProjection()
            in_ref_trans = in_image.GetGeoTransform()

            if in_proj == ref_proj and in_ref_trans == ref_trans_new:
                pref_string = os.path.join(self._tmpdir, pref_string)
                rand_string = next(tempfile._get_candidate_names())
                out_file = '{}{}.{}'.format(pref_string, rand_string,
                                            self.img_extension)
                os.symlink(os.path.abspath(im_path), out_file)
            else:
                out_file = tempfile.mkstemp(prefix=pref_string,
                                            suffix='.tif')[1]
                self.reproject(driver, out_file, in_image, in_proj, ref_image,
                               ref_trans, ref_trans_new, ref_proj)

            paths.append(out_file)

            # close GDAL band
            in_image = None

        # close GDAL ref image
        ref_image = None

        return paths

    def _run_stack_ordinary_control(self, meta_data, data_dir, output_dir,
                                    response):
        """Run modified ordinary control, this time for the stack.

        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory
        :param response: processor response managed by the manager

        :return dict: QI metadata
        """
        proc_empty = self.get_ordinary_control_processor()
        proc = proc_empty(
            self.config,
            response
        )
        response_data_control = proc._run(meta_data, data_dir, output_dir)

        response.update(
            {'harmonized': response_data_control['harmonized']}
        )

        return response
