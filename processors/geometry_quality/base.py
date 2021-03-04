import os
from datetime import datetime
import tempfile
from abc import ABC, abstractmethod

from manager.logger import Logger # must be called before processors.exceptions
from manager.logger.db import DbIpOperationStatus

from processors import QCProcessorIPBase
from processors.exceptions import ProcessorFailedError, \
    ProcessorCriticalError, ProcessorRejectedError


class QCProcessorGeometryQualityBase(QCProcessorIPBase, ABC):
    """Geometry quality control processor abstract base class.

    Based on Arosics package.

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """

    isMeasurementOf = "detailedControlMetric"
    isMeasurementOfSection = 'geometry'

    def __init__(self, config, response):
        super(QCProcessorGeometryQualityBase, self).__init__(
            config, response
        )

        # results
        # -> self._result['qi.files']['output']
        self.add_qi_result(
            'gml_before_correction',
            'tie_points_before_correction.gml'
        )

        if self.config['geometry'].get('correct_shifts', False):
            self.add_qi_result(
                'gml_after_correction',
                'tie_points_after_correction.gml'
            )

    @staticmethod
    @abstractmethod
    def bands2stack(b2s, data_dir):
        """Get bands intended for the use.

        :param str b2s: value band4match from the config file
        :param str data_dir: Path to data directory
        """
        pass

    @staticmethod
    @abstractmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2 changed to L2H.

        :param data_dir: Path to data directory

        :return str: directory name
        """
        pass

    @staticmethod
    @abstractmethod
    def get_nodata_values():
        """Get the platform-dependent nodata tuple."""
        pass

    @staticmethod
    @abstractmethod
    def get_valid_pixels_processor(config, response):
        """Get ValidPixels processor corresponding to the platform.

        :param config: processor-related config file
        :param response: processor QI metadata response managed by the manager
        """
        pass

    def check_dependency(self):
        """Check processor's software dependencies.
        """
        import arosics
        from osgeo import gdal

        geometry_conf = self.config['geometry']
        self.visualize = geometry_conf.get('show', False)
        self.save_visualization = geometry_conf.get('save_visualization',
                                                    False)

        if self.visualize is True or self.save_visualization is True:
            import matplotlib.pyplot as plt
        if self.save_visualization is True:
            import pykrige

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.

        :param meta_data: IP metadata
        :param str data_dir: Path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        import arosics

        response_data = {
            self.isMeasurementOfSection: [
                {
                    "id": "http://qcmms.esa.int/detailed_control#GEOMETRY",
                    "generatedAtTime": datetime.now(),
                    "requirement": False
                }
            ]
        }

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
            return
        if not os.path.exists(im_reference):
            Logger.error(
                "Reference image '{}' not found".format(im_reference)
            )
            self.set_response_status(DbIpOperationStatus.failed)
            return

        lh_output_dir = self.get_lh_dir(data_dir)

        bands2stack = self.config['geometry'].get('band4match', 3)
        resolution = self.config['land_product'].get('geometric_resolution')
        correct_shifts = self.config['geometry'].get('correct_shifts', False)
        has_stack = 'harmonization_stack' in self.config['processors']
        try:
            stack_on = self.config['geometry']['stack_on']
            if stack_on is True and has_stack is False:
                Logger.warning("Harmonization stack not available")
                stack_on = False
        except KeyError:
            stack_on = has_stack
        try:
            if stack_on:
                im_target = self._get_target_stack(lh_output_dir)
            else:
                filepattern_suff = '_{}m{}'.format(
                    resolution,
                    self.img_extension) \
                    if 'L2A' in data_dir else '{}'.format(self.img_extension)
                filepattern = '{}{}'.format(self.bands2stack(bands2stack,
                                                             data_dir),
                                            filepattern_suff)
                try:
                    im_target = self.filter_files(data_dir, filepattern)[0]
                except IndexError:
                    raise ProcessorCriticalError(
                        self,
                        "Pattern '{}' not found in {}".format(
                            filepattern, data_dir
                        )
                    )
            Logger.debug("Target image: {}".format(im_target))
        except ProcessorFailedError:
            self.set_response_status(DbIpOperationStatus.failed)
            return

        # check EPSG codes
        if self.getEpsgCode(im_target) != self.getEpsgCode(im_reference):
            Logger.error(
                "Inconsistent reference and image EPSG codes"
            )
            self.set_response_status(DbIpOperationStatus.rejected)
            return

        try:
            mask = tempfile.mktemp(prefix='mask_')
            self.create_mask(data_dir, mask, resolution)
        except ProcessorFailedError:
            self.set_response_status(DbIpOperationStatus.failed)
            return

        # arocics settings
        coreg_file_prefix = ''
        if correct_shifts is True:
            coreg_file_prefix += 'coreg_'
        if stack_on:
            coreg_file_prefix += 'stack_'
        aro_raster_path = os.path.join(
            lh_output_dir,
            '{}{}.tif'.format(coreg_file_prefix,
                              os.path.split(lh_output_dir)[-1])
        )
        kwargs = {
            'fmt_out'          : 'GTIFF',
            'grid_res'         : 30,
            'max_points'       : 1000,
            'mask_baddata_tgt' : mask,
            'path_out'         : aro_raster_path,
            'projectDir'       : lh_output_dir,
            'nodata'           : self.get_nodata_values(),
            'window_size'      : (128, 128)
        }
        if stack_on:
            kwargs['s_b4match'] = bands2stack

        # run arocics
        Logger.debug(
            "Running coreg for reference: {}; image: {} with args {}".format(
                im_reference, im_target, kwargs
            )
        )
        try:
            CRL = arosics.COREG_LOCAL(im_reference, im_target, **kwargs)
        except AssertionError as e:
            Logger.error("Coreg failed: {}".format(e))
            self.set_response_status(DbIpOperationStatus.failed)
            return response_data

        try:
            CRL_points_table = CRL.tiepoint_grid.get_CoRegPoints_table()
        except ValueError:
            CRL_points_table = None

        try:
            self.to_PointFormat(
                CRL_points_table,
                self._result['qi.files']['gml_before_correction'],
                CRL.outFillVal,
                epsg=self.getEpsgCode(im_target)
            )
        except ProcessorRejectedError:
            return response_data

        if correct_shifts is True:
            CRL.correct_shifts()
            CRL_after_corr = arosics.COREG_LOCAL(im_reference, CRL.path_out,
                                                 **kwargs)

            try:
                CRL_points_table_after_corr = \
                    CRL_after_corr.tiepoint_grid.get_CoRegPoints_table()
            except ValueError:
                CRL_points_table_after_corr = None

            try:
                self.to_PointFormat(
                    CRL_points_table_after_corr,
                    self._result['qi.files']['gml_after_correction'],
                    CRL_after_corr.outFillVal,
                    epsg=self.getEpsgCode(im_target)
                )
            except ProcessorRejectedError:
                Logger.debug(
                    'No GCP points found in the GML after correction. Copying '
                    'the one before the correction as the one after.')
                from shutil import copy2
                copy2(self._result['qi.files']['gml_before_correction'],
                      self._result['qi.files']['gml_after_correction'])
        else:
            CRL_after_corr = None

        if self.visualize is True or self.save_visualization is True:
            self._visualize(CRL,
                            CRL_after_corr,
                            self.visualize,
                            self.save_visualization,
                            correct_shifts,
                            output_dir,
                            resolution,
                            aro_raster_path,
                            stack_on)

        # update response attributes
        pixel_metadata_coding_conf = self.config['pixel_metadata_coding']
        response_data[self.isMeasurementOfSection][0].update({
            'mask': self.file_basename(
                self._result['qi.files']['gml_before_correction']),
            'rasterCoding': pixel_metadata_coding_conf[self.identifier],
            'lineage': 'http://qcmms.esa.int/Arosics_v{}'.format(
                arosics.__version__
            )
        })
        try:
            stats = self.compute_stats(CRL_points_table, resolution,
                                       CRL.outFillVal)
            response_data[self.isMeasurementOfSection][0].update(stats)
        except ProcessorFailedError:
            return response_data

        os.remove(mask)

        return response_data

    def _get_fmask_raster(self, data_dir, resolution):
        """Get path to the cloud_coverage file created with fmask.

        :param data_dir: path to data directory
        :param resolution: value geometric_resolution from the config file

        :return str: path to cloud_coverage_[resolution]m.tif
        """
        try:
            cc_file = 'cloud_coverage_{}m.tif'.format(resolution)
            input_file = self.filter_files(
                data_dir, cc_file)[0]
        except IndexError:
            raise ProcessorFailedError(
                self,
                "Fmask product ({}) not found in {}".format(cc_file, data_dir)
            )

        return input_file

    def _get_target_stack(self, output_dir):
        """Get stack of all bands for a scene.

        :param output_dir: path to a directory where the stack is saved

        :return: path to the stacked tif
        """
        lh_title = os.path.split(output_dir)[-1]
        return os.path.join(output_dir, self.get_stack_name(lh_title))

    @staticmethod
    def get_stack_name(lh_title):
        """Get filename in format stack_[lh_title].tif.

        :param lh_title: title of L2H product

        :return str: filename
        """
        return 'stack_{}.tif'.format(lh_title)

    def compute_stats(self, points_table, resolution, out_fill_val):
        """Compute statistics.

        :param points_table: CRL CoRegPoints table
        :param resolution: resolution used to threshold stats
        :param out_fill_val: CRL.outFillValue

        :return dict: QI metadata
        """
        import numpy as np

        def rmse(diff):
            """Compute rmse."""
            return (diff ** 2).mean() ** .5

        try:
            points_table = points_table[points_table['ABS_SHIFT'] != out_fill_val]
            points_table = points_table[points_table['OUTLIER'] == False]
        except ValueError as e:
            raise ProcessorFailedError(
                self,
                "Statistics failed: {}".format(e)
            )

        X_diff = points_table['X_SHIFT_M']
        Y_diff = points_table['Y_SHIFT_M']
        abs_shift = points_table['ABS_SHIFT']

        rmseX = rmse(X_diff)
        rmseY = rmse(Y_diff)
        differenceXmax = X_diff.max()
        differenceYmax = Y_diff.max()
        differenceXmin = X_diff.min()
        differenceYmin = Y_diff.min()
        abs_shift_median = np.median(abs_shift)

        if abs(differenceXmin) > differenceXmax:
            differenceXmax = differenceXmin
        if abs(differenceYmin) > differenceYmax:
            differenceYmax = differenceYmin

        response_data = {
            'validGCPs': len(X_diff)
        }
        if len(X_diff) > 0:
            response_data.update({
                'rmseX': round(rmseX, 1),
                'rmseY': round(rmseY, 1),
                'diffXmax': round(differenceXmax, 1),
                'diffYmax': round(differenceYmax, 1),
                "medianAbsShift": round(abs_shift_median, 1)
            })

        # check geometric accuracy level
        threshold = self.config['land_product']['geometric_accuracy'] * \
            resolution
        Logger.debug("Geometry quality threshold: {}m".format(threshold))
        npassed = 0
        for i in abs_shift:
            if i <= threshold:
                npassed += 1
        Logger.debug(
            "Validity check: number of points: {} (passed: {})".format(
                len(X_diff), npassed
            )
        )
        if npassed == 0:
            Logger.debug("No passed point -> Rejected")
            self.set_response_status(DbIpOperationStatus.rejected)
        else:
            response_data.update({'requirement': True})

        return response_data

    def to_PointFormat(self, points_table, path_out, out_fill_val,
                       skip_nodata=True, skip_nodata_col='ABS_SHIFT',
                       driver='GML', epsg=None):
        """Write the calculated tie points grid to a point shapefile.

        Shapefile could be used for example for visualization by a GIS
        software. The shapefile uses Tie_Point_Grid.CoRegPoints_table as
        attribute table.

        TAKEN FROM: http://danschef.gitext.gfz-potsdam.de/arosics/doc/_modules/arosics/Tie_Point_Grid.html#Tie_Point_Grid.to_PointShapefile

        :param points_table:    CRL CoRegPoints table
        :param path_out:        <str> the output path. If not given, it is
                                automatically defined.
        :param out_fill_val:    CRL.outFillValue
        :param skip_nodata:     <bool> whether to skip all points where no
                                valid match could be found
        :param skip_nodata_col: <str> determines which column of
                                Tie_Point_Grid.CoRegPoints_table is used to
                                identify points where no valid match could be
                                found
        :param driver: driver determining the format of the written file
        :param epsg: epsg to be used for the output file
        """
        import numpy as np
        from osgeo import ogr
        from fiona.crs import from_epsg

        if points_table is None:
            # something wrong with arosics -> create empty GML
            driver = ogr.GetDriverByName("GML")
            raise ProcessorRejectedError(
                self,
                "ValueError raised in the corregistration points table"
            )

        if skip_nodata:
            if skip_nodata_col not in points_table:
                # something wrong with arosics -> create empty GML
                driver = ogr.GetDriverByName("GML")
                raise ProcessorRejectedError(
                    self,
                    "Attribute {} not found in the corregistration points "
                    "table. Found attributes: {}. The scene is rejected for "
                    "one of the following reasons: No corregistration points "
                    "or something went wrong in arosics".format(
                        skip_nodata_col, points_table.keys()
                    )
                )
            GDF2pass = points_table[
                points_table[skip_nodata_col] != out_fill_val
            ].copy()
            GDF2pass = GDF2pass[GDF2pass['OUTLIER'] == False]
        else:
            GDF2pass = points_table
            GDF2pass.LAST_ERR = GDF2pass.apply(
                lambda GDF_row: repr(GDF_row.LAST_ERR), axis=1
            )

        # replace boolean values (cannot be written)
        # replace booleans where column dtype is not np.bool but np.object
        GDF2pass = GDF2pass.replace(False, 0).copy()
        GDF2pass = GDF2pass.replace(True, 1).copy()
        for col in GDF2pass.columns:
            if GDF2pass[col].dtype == np.bool:
                GDF2pass[col] = GDF2pass[col].astype(int)

        if epsg:
            GDF2pass.crs = from_epsg(epsg)
        try:
            GDF2pass.to_file(path_out, driver=driver)
        except ValueError:
            # empty DataFrame -> create empty GML file
            driver = ogr.GetDriverByName("GML")
            raise ProcessorRejectedError(
                self,
                "No GCP points found"
            )

        # fiona produces XSD file, let's remove it
        xsd_file = os.path.splitext(path_out)[0] + '.xsd'
        if os.path.exists(xsd_file):
            os.remove(xsd_file)

    @staticmethod
    def getEpsgCode(imfile):
        """Get the EPSG code of the imfile.

        :param imfile: File from which we want to get the EPSG code
        """
        from osgeo import gdal, osr

        ds = gdal.Open(imfile)
        srs = osr.SpatialReference()
        srs.ImportFromWkt(ds.GetProjectionRef())
        epsg = srs.GetAuthorityCode(None)
        ds = None

        return epsg

    def _visualize(self, CRL, CRL_after_corr, visualize, save_visualization,
                   correct_shifts, output_dir, resolution, coreg_file,
                   stack_on):
        """Visualize the Arosics corrections.

        :param CRL: COREG_LOCAL object of the input image
        :param CRL_after_corr: COREG_LOCAL object of the image after
            correct_shifts()
        :param visualize: Value visualize from the config file
        :param save_visualization: Value save_visualization from the config
            file
        :param correct_shifts: Value correct_shifts from the config file
        :param output_dir: path to output processor directory
        :param resolution: Value geometric_resolution from the config file
        :param coreg_file: Name of the arosics output file
        :param stack_on: Value stack_on from the config file
        """
        import matplotlib.pyplot as plt
        import numpy as np

        # create scatterplots
        abs_shift_before_raw = CRL.CoRegPoints_table.ABS_SHIFT
        filter1 = abs_shift_before_raw != CRL.outFillVal  # filter no data vals
        filter2 = CRL.CoRegPoints_table.OUTLIER != 1
        filter_full = filter1 & filter2
        median_before_raw = abs_shift_before_raw[filter_full].median()
        median_before = round(median_before_raw, 2)

        # configure scatterplots
        fig = plt.figure()
        ax = fig.add_subplot(121, projection='polar')
        ax.set_ylim([0, 2 * resolution])
        ax.set_yticks(np.arange(0, 2 * resolution, resolution/2))

        ax.title.set_text(
            'Before correction\nAbsolute shift median: {}\n'.format(
                median_before
            )
        )
        ax.scatter(
            np.abs(CRL.CoRegPoints_table.X_SHIFT_M[filter_full]),
            np.abs(CRL.CoRegPoints_table.Y_SHIFT_M[filter_full]),
            edgecolors='none',
            alpha=0.1
        )

        # visualize
        if save_visualization is True:
            before_path = os.path.join(output_dir,
                                       'correction-before.tif')
            after_path = os.path.join(output_dir,
                                      'correction-after.tif')

            # create geometry shift rasters
            x_shifts_path = os.path.join(output_dir, 'geometry_X_shifts.tif')
            y_shifts_path = os.path.join(output_dir, 'geometry_Y_shifts.tif')
            CRL.tiepoint_grid.to_Raster_using_Kriging(
                attrName='X_SHIFT_M',
                skip_nodata=True,
                fName_out=x_shifts_path
            )
            CRL.tiepoint_grid.to_Raster_using_Kriging(
                attrName='Y_SHIFT_M',
                skip_nodata=True,
                fName_out=y_shifts_path
            )

        else:
            before_path = ''
            after_path = ''

        CRL.view_CoRegPoints(figsize=(20, 20),
                             backgroundIm='tgt',
                             title='Before correction',
                             vmin=0,
                             vmax=resolution,
                             shapes2plot='vectors',
                             vector_scale=5,
                             showFig=visualize,
                             savefigPath=before_path)

        if correct_shifts is True:
            # do everything also for the state after the correction
            abs_shift_after_raw = CRL_after_corr.CoRegPoints_table.ABS_SHIFT
            filter1 = abs_shift_after_raw != CRL_after_corr.outFillVal
            filter2 = CRL_after_corr.CoRegPoints_table.OUTLIER != 1
            filter_cor = filter1 & filter2
            median_after_raw = abs_shift_after_raw[filter_cor].median()
            median_after = round(median_after_raw, 2)

            ax_corrected = fig.add_subplot(122, projection='polar')
            ax_corrected.set_ylim([0, 2 * resolution])
            ax_corrected.set_yticks(np.arange(0, 2 * resolution, resolution/2))

            ax_corrected.title.set_text(
                'After correction\nAbsolute shift median: {}\n'.format(
                    median_after
                )
            )
            ax_corrected.scatter(
                np.abs(CRL_after_corr.CoRegPoints_table.X_SHIFT_M[filter_cor]),
                np.abs(CRL_after_corr.CoRegPoints_table.Y_SHIFT_M[filter_cor]),
                edgecolors='none',
                alpha=0.1
            )

            CRL_after_corr.view_CoRegPoints(figsize=(20, 20),
                                            backgroundIm='tgt',
                                            title='After correction',
                                            vmin=0,
                                            shapes2plot='vectors',
                                            vector_scale=5,
                                            vmax=resolution,
                                            showFig=visualize,
                                            savefigPath=after_path)

        if save_visualization is True:
            plt.subplots_adjust(wspace=0.4)
            fig.savefig(os.path.join(output_dir, 'scatterplot.png'), dpi=200)

    def create_mask(self, data_dir, mask_fn, resolution):
        """Create temporary mask file with ValidPixels.

        :param data_dir: Path to data directory
        :param mask_fn: Filename to be used for the VPX mask
        :param resolution: Value geometric_resolution from the config file
        """
        valid_pixels = self.get_valid_pixels_processor(self.config,
                                                       self._response)
        fmask_file = self._get_fmask_raster(data_dir, resolution)
        valid_pixels._run_vpx(fmask_file, mask_fn, True)

    @staticmethod
    def get_stack_length():
        """Get count of bands to be used in the stack."""
        return 6

    @staticmethod
    def append_to_stack(stack_file, geometry_shift_file_x,
                        geometry_shift_file_y, default_stack_length):
        """Append geometry shifts to the stack containing scene bands.

        :param stack_file: Path to the stack file with bands and cloud coverage
        :param geometry_shift_file_x: Path to the geometry shifts in X dir
        :param geometry_shift_file_y: Path to the geometry shifts in Y dir
        :param default_stack_length: Length of stack excluding cloud_coverage
        """
        import rasterio
        import shutil

        with rasterio.open(stack_file) as stack:
            meta = stack.meta

            if meta['count'] > default_stack_length + 2:
                Logger.debug(
                    'Not appending geometry shifts as new bands to stack '
                    '{}. Stack already existed with the cloud coverage '
                    'appended.'.format(stack_file)
                )
                return 0

            Logger.debug(
                'Appending geometry shifts as new bands to stack '
                '{}'.format(stack_file)
            )

            dtype = meta['dtype']

            stack_length = meta['count'] + 2
            meta.update(count=stack_length)
            fn = tempfile.mkstemp(prefix='stack_tmp_with_shifts_',
                                  suffix='.tif')[1]
            stack_tmp_path = os.path.join(tempfile.gettempdir(), fn)

            with rasterio.open(stack_tmp_path, 'w', **meta) as stack_tmp:
                stack_tmp.write(stack.read(1), 1)

                for band_id in range(2, stack_length - 1):
                    stack_tmp.write(stack.read(band_id), band_id)

                stack_tmp.write(
                    rasterio.open(geometry_shift_file_x).read(1).astype(dtype),
                    stack_length - 1
                )
                stack_tmp.write(
                    rasterio.open(geometry_shift_file_y).read(1).astype(dtype),
                    stack_length
                )

        # replace the orig stack with the one with radiometry_control appended
        os.remove(stack_file)
        shutil.move(stack_tmp_path, stack_file)
