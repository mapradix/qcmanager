import os
from datetime import datetime

from processors import QCProcessorLPBase, identifier_from_file
from processors.exceptions import ProcessorRejectedError

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger


class QCProcessorLPThematicValidationControl(QCProcessorLPBase):
    """Land Product thematic validation control processor [validation control].
    """
    identifier = identifier_from_file(__file__)
    isMeasurementOf = "lpThematicValidationMetric"
    isMeasurementOfSection = "qualityIndicators"

    def check_dependency(self):
        """Check processor's software dependecies.
        """
        import sklearn
        import scipy

    def _run(self):
        """Perform processor's tasks.

        :return dict: QI metadata
        """
        Logger.info('Running thematic validation control')

        response_data = {
            'isMeasurementOf': '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
            "generatedAtTime": datetime.now(),
            "value": False
        }

        reference_fn = os.path.join(self.config['map_product']['path'],
                                    self.config['map_product']['reference_layer'])

        if not os.path.isfile(reference_fn):
            Logger.error("File {} not found".format(reference_fn))
            self.set_response_status(DbIpOperationStatus.rejected)
            return response_data

        themes = self.config['land_product']['product_type']
        for theme in themes:
            if theme == 'classification':
                classification_qi_ = self._lp_classification_validation(self.config)
                response_data.update({"classification": classification_qi_})
                if float(classification_qi_['overallAccuracy']) >= \
                        float(self.config['land_product']['thematic_accuracy']):
                    response_data['value'] = True
                else:
                    response_data['value'] = False
            elif theme == 'regression':
                regression_qi = self._lp_regression_validation(self.config)
                regression_prod_name = self.config['land_product']['regression_name']
                response_data.update({regression_prod_name: regression_qi})
                print(float(regression_qi['rmse']))
                print(float(self.config['land_product']['rmse_accuracy']))
                if float(regression_qi['rmse']) <= \
                   float(self.config['land_product']['rmse_accuracy']):
                    response_data['value'] = True
                else:
                    response_data['value'] = False
                    
        if response_data['value'] is False:
            self.set_response_status(DbIpOperationStatus.rejected)

        return response_data

    def _read_point_data(self, ras_fn, vec_fn, attrib, no_data):
        """Raster map ~ vector reference spatial overlay.

        :param str ras_fn: input raster file
        :param str vec_fn: input vector file
        :param str attrib: attribute
        :param int no_data: no data value
        """
        from osgeo import gdal, ogr
        
        src_ds = gdal.Open(ras_fn)
        gt = src_ds.GetGeoTransform()
        rb = src_ds.GetRasterBand(1)

        ds = ogr.Open(vec_fn)
        lyr = ds.GetLayer()
        ref_val = []
        map_val = []
        for feat in lyr:
            geom = feat.GetGeometryRef()

            if geom.GetGeometryName() == 'POLYGON':
                mx, my = geom.Centroid().GetX(), geom.Centroid().GetY()
            elif geom.GetGeometryName() == 'POINT':
                mx, my = geom.GetX(), geom.GetY()

            px = int((mx - gt[0]) / gt[1])
            py = int((my - gt[3]) / gt[5])
            intval = rb.ReadAsArray(px, py, 1, 1)
            if not ((intval[0][0] == no_data)
                    or (feat.GetField(attrib) == no_data)):
                map_val.append(intval[0][0])
                ref_val.append(feat.GetField(attrib))

        return ref_val, map_val

    def _lp_classification_validation(self, config):
        """Extract classification thematic quality indicators.

        :param dict config: configuration

        :return dict: QI metadata
        """
        import numpy as np

        from sklearn.metrics import classification_report
        from sklearn.metrics import confusion_matrix
        from sklearn.metrics import cohen_kappa_score

        classification_qi = {}

        lp_map_fn = os.path.join(self.config['map_product']['path'],
                                 self.config['map_product']['classification_raster'])
        reference_fn = os.path.join(self.config['map_product']['path'],
                                    self.config['map_product']['reference_layer'])
        reference_attrib = self.config['map_product']['classification_attribute']
        no_data = self.config['land_product']['raster_coding']['out_of_aoi']

        ref_val_, map_val_ = self._read_point_data(lp_map_fn, reference_fn, reference_attrib, no_data)

        c_report = classification_report(ref_val_, map_val_, output_dict=True)
        c_matrix = confusion_matrix(ref_val_, map_val_)

        ref_lineage_ = reference_fn.split('/')[-1]
        overall_accuracy_ = (np.sum(c_matrix.diagonal()) / np.sum(c_matrix)) * 100
        producers_accuracy_ = (c_report['weighted avg']['precision']) * 100
        users_accuracy_ = (c_report['weighted avg']['recall']) * 100
        kappa_ = (cohen_kappa_score(ref_val_, map_val_, labels=None, weights=None)) * 100

        classes = config['land_product']['raster_coding']['classification']
        classes_names = [k for k, v in sorted(classes.items(), key=lambda item: item[1])]

        return {
            "codingClasses": classes_names,
            "lineage": "http://qcmms.esa.int/{}".format(ref_lineage_),
            "overallAccuracy": round(overall_accuracy_, 2),
            "producersAccuracy": round(producers_accuracy_, 2),
            "usersAccuracy": round(users_accuracy_, 2),
            "kappa": round(kappa_, 2),
            "confusionMatrix":  c_matrix.tolist()
        }

    def _lp_regression_validation(self, config):
        """Extract regression thematic quality indicators.

        :param dict config: configuration

        :return dict: QI metadata
        """
        import numpy as np
        
        from sklearn.metrics import mean_absolute_error
        from sklearn.metrics import mean_squared_error
        from scipy.stats import pearsonr

        regression_qi = {}

        lp_map_fn = os.path.join(self.config['map_product']['path'],
                                 self.config['map_product']['regression_raster'])
        reference_fn = os.path.join(self.config['map_product']['path'],
                                    self.config['map_product']['reference_layer'])
        reference_attrib = self.config['map_product']['regression_attribute']
        no_data = self.config['land_product']['raster_coding']['out_of_aoi']

        ref_val_, map_val_ = self._read_point_data(
            lp_map_fn, reference_fn, reference_attrib, no_data
        )

        MAE_ = mean_absolute_error(ref_val_, map_val_)
        MSE_ = mean_squared_error(ref_val_, map_val_)
        RMSE_ = np.sqrt(mean_squared_error(ref_val_, map_val_))
        pearson_r_, p_val_ = pearsonr(ref_val_, map_val_)
        ref_lineage_ = reference_fn.split('/')[-1]

        regression_values = config['land_product']['raster_coding']['regression']

        return {
            "lineage": "http://qcmms.esa.int/{}".format(ref_lineage_),
            "codingValues": regression_values,
            "mae": round(MAE_, 2),
            "mse": round(MSE_, 2),
            "rmse": round(RMSE_, 2),
            "pearsonR": round(pearson_r_, 2)
        }
