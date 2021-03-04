class QCProcessorSentinelMeta:
    """Sentinel-2 metaclass"""
    name = 'Sentinel-2'
    extension = '.zip'
    data_dir_suf = '.SAFE'
    level2_data = True
    img_extension = '.jp2'
    parent_identifier = 'ESA:SCIHUB:S2'
