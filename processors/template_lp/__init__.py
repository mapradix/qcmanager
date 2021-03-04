from processors import QCProcessorLPBase, identifier_from_file

from manager.logger import Logger

class QCProcessorTemplateLP(QCProcessorLPBase):
    """Template land product processor.
    """
    identifier = identifier_from_file(__file__)
    isMeasurementOf = "lpInterpretationMetric"
    
    def run(self):
        """Run processor.

        Define this functions only if your processor is the first in a queue.
        
        Check processors.lp_init for a real example.
        """
        self.add_response(
            self._run()
        )

    def _run(self):
        """Perform processor's tasks.

        Check processors.lp_ordinary_control for a real example.

        :param meta_file: path to JSON metafile
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        response_data = {
            "type": "Feature",
            "id": "http://qcmms-cat.spacebel.be/eo-catalog/series/EOP:MAPRADIX:LP_TUC1/datasets/IMD_2018_010m",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            13.58808613662857,
                            50.54373233188457
                        ],
                        [
                            13.616762732856103,
                            49.55648784343155
                        ],
                        [
                            15.134970502622016,
                            49.56467629785398
                        ],
                        [
                            15.137769755767613,
                            50.552210622261306
                        ],
                        [
                            13.58808613662857,
                            50.54373233188457
                        ]
                    ]
                ]
            },
            "properties": {
                "title": "IMD_2018_010m",
                "identifier": "IMD_2018_010m",
                "status": "PLANNED",
                "kind": "http://purl.org/dc/dcmitype/Dataset",
                "parentIdentifier": "EOP:ESA:LP:TUC1",
                "collection": "EOP:ESA:GR1:UC1",
                "abstract": "The high-resolution imperviousness product capture the percentage of soil sealing. Built-up areas are characterized by the substitution of the original (semi-) natural land cover or water surface with an artificial, often impervious cover. This product of imperviousness layer constitutes the main status layer. There is per-pixel estimates of impermeable cover of soil (soil sealing) and are mapped as the degree of imperviousness (0-100%). Imperviousness 2018 is the continuation of the existing HRL imperviousness status product for the 2018 reference year, but with an increase in spatial resolution from 20m to (now) 10m.",
                "date": "2020-05-12T00:00:00Z/2020-05-12T23:59:59Z",
                "categories": [
                    {
                        "term": "https://earth.esa.int/concept/urban",
                        "label": "Forestry"
                    },
                    {
                        "term": "http://www.eionet.europa.eu/gemet/concept/4599",
                        "label": "land"
                    },
                    {
                        "term": "https://earth.esa.int/concept/sentinel-2",
                        "label": "Sentinel-2"
                    }
                ],
                "updated": "2020-05-12T15:23:57Z",
                "qualifiedAttribution": [
                    {
                        "type": "Attribution",
                        "agent": [
                            {
                                "type": "Organization",
                                "email": "eohelp@eo.esa.int",
                                "name": "ESA/ESRIN",
                                "phone": "tel:+39 06 94180777",
                                "uri": "http://www.earth.esa.int",
                                "hasAddress": {
                                    "country-name": "Italy",
                                    "postal-code": "00044",
                                    "locality": "Frascati",
                                    "street-address": "Via Galileo Galilei CP. 64"
                                }
                            }
                        ],
                        "role": "originator"
                    }
                ],
                "acquisitionInformation": [
                    {
                        "platform": {
                            "id": "https://earth.esa.int/concept/sentinel-2",
                            "platformShortName": "Sentinel-2"
                        },
                        "instrument": {
                            "id": "https://earth.esa.int/concept/s2-msi",
                            "instrumentShortName": "MSI"
                        }
                    }
                ],
                "productInformation": {
                    "productType": "classification",
                    "availabilityTime": "2019-06-20T15:23:57Z",
                    "format": "geoTIFF",
                    "referenceSystemIdentifier": "http://www.opengis.net/def/crs/EPSG/0/3035",
                    "qualityInformation": {
                        "qualityIndicators": [
                            {
                                "isMeasurementOf": "http://qcmms.esa.int/quality-indicators/#lpInterpretationMetric",
                                "generatedAtTime": "2019-10-17T17:20:32.30Z",
                                "value": True,
                                "classification": {
                                    "codingClasses": ["non-urban","urban"],
                                    "overallAccuracy": 91.0,
                                    "producersAccuracy": 99.0,
                                    "usersAccuracy": 92.0,
                                    "kappa": 88.0,
                                    "confusionMatrix": [
                                        [
                                            92,
                                            8
                                        ],
                                        [
                                            1,
                                            99
                                        ]
                                    ],
                                    "lineage": "http://qcmms.esa.int/Mx_sealing_simple_v0.9"
                                },
                                "densityCover": {
                                    "mae": 10.25,
                                    "mse": 174.86,
                                    "rmse": 13.22,
                                    "pearsonR": 0.69,
                                    "lineage": "http://qcmms.esa.int/Mx_sealing_simple_v0.9"
                                }
                            },
                            {
                                "isMeasurementOf": "http://qcmms.esa.int/quality-indicators/#ipForLpInformationMetric",
                                "value": True,
                                "generatedAtTime": "2019-10-17T17:20:32.30Z",
                                "vpxCoverage": {
                                    "2018": {
                                        "min": 3,
                                        "max": 22,
                                        "gapPct": 0.0,
                                        "mask": "http://93.91.57.111/UC1/image_products/vpx_coverage_10m.jpg"
                                    }
                                },
                                "fitnessForPurpose": "PARTIAL",
                                "lineage": "http://qcmms.esa.int/Mx_vpx_v0.9", 
                                "generatedAtTime": "2020-05-12T17:20:32.30Z"
                            },
                            {
                                "isMeasurementOf": "http://qcmms.esa.int/quality-indicators/#lpMetadataControlMetric",
                                "value": True,
                                "generatedAtTime": "2020-05-12T17:20:32.30Z", 
                                "metadataAvailable": True,
                                "metadataSpecification": "INSPIRE",
                                "metadataCompliancy": True
                            },
                            {
                                "isMeasurementOf": "http://qcmms.esa.int/quality-indicators/#lpOrdinaryControlMetric",
                                "value": True,
                                "generatedAtTime": "2019-10-17T17:20:32.30Z",
                                "read": True,
                                "xRes": 10.0,
                                "yRes": 10.0,
                                "epsg": "3035",
                                "dataType": "u8",
                                "rasterFormat": "GeoTIFF",
                                "extentUlLr": [
                                    4621500.0,
                                    3019160.0,
                                    4659740.0,
                                    2988580.0
                                ],
                                "aoiCoveragePct": 100.0
                            },
                            {
                                "isMeasurementOf": "http://qcmms.esa.int/quality-indicators/#lpThematicValidationMetric",
                                "value": True,
                                "generatedAtTime": "2020-05-12T17:20:32.30Z",
                                "classification": {
                                    "codingClasses": ["non-urban","urban"],
                                    "lineage": "http://qcmms.esa.int/prague_sealing_references_330.shp",
                                    "overallAccuracy": 91.2,
                                    "producersAccuracy": 90.4,
                                    "usersAccuracy": 93.0,
                                    "kappa": 88.3,
                                    "confusionMatrix": [
                                        [
                                            92,
                                            8
                                        ],
                                        [
                                            1,
                                            99
                                        ]
                                    ]
                                },
                                "densityCover": {
                                    "mae": 10.2,
                                    "mse": 120.3,
                                    "rmse": 11.0,
                                    "pearsonR": 0.71,
                                    "lineage": "http://qcmms.esa.int/prague_sealing_references_330.shp",
                                    "codingValues": {
                                        "non-sealing": 0,
                                        "sealingMin": 1,
                                        "sealingMax": 100,
                                        "unclassified": 254,
                                        "outsideAoi": 254
                                    }
                                }
                            }
                        ]
                    }
                },
                "additionalAttributes": {
                    "product_focus": "classification",
                    "lpReference": "EN-EEA.IDM.R0.18.009_Annex_8 Table 11",
                    "temporal_coverage": "status",
                    "geometric_resolution": 10,
                    "grid": "EEA Reference Grid",
                    "crs": "European ETRS89 LAEA",
                    "geometric_accuracy": 0.5,
                    "thematic_accuracy": 90,
                    "data_type": "u8",
                    "mmu_pixels": 1,
                    "necessary_attributes": [
                        "raster value",
                        "count",
                        "class names"
                    ],
                    "raster_coding": [
                        {
                            "name": "non-impervious",
                            "min": 0,
                            "max": 0
                        },
                        {
                            "name": "impervious",
                            "min": 1,
                            "max": 100
                        },
                        {
                            "name": "unclassified",
                            "min": 254,
                            "max": 254
                        },
                        {
                            "name": "outside area",
                            "min": 255,
                            "max": 255
                    }
                    ],
                    "seasonal_window": [
                        5,
                        6,
                        7,
                        8
                    ]
                },
                "links": {
                    "previews": [
                        {
                            "href": "https://qcmms-cat.spacebel.be/archive/lp/land_tuc1.png",
                            "type": "image/png",
                            "title": "Quicklook"
                        }
                    ],
                    "via": [
                        {
                            "href": "http://qcmms-cat.spacebel.be/eo-catalog/series/EOP:ESA:GR1:UC1/datasets",
                            "type": "application/geo+json",
                            "title": "Input data"
                        }
                    ]
                }
            }
        }

        return response_data
