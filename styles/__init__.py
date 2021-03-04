import os

from collections import OrderedDict

from xml.etree import ElementTree


class StyleReaderError(Exception):
    pass


class StyleReader:
    """Raster style reader

    :param str filename: input SLD file
    """
    def __init__(self, filename):
        self._filename = filename
        if not self._filename.endswith('.sld'):
            self._filename += '.sld'

        if not os.path.isabs(self._filename):
            self._filename = os.path.join(
                os.path.dirname(__file__),
                self._filename
            )
        
        self._read_sld_file()

    def _read_sld_file(self):
        """Read style from input SLD file"""
        with open(self._filename, encoding='utf-8') as fd:
            root = ElementTree.fromstring(fd.read())
            if not root.tag.endswith('StyledLayerDescriptor'):
                raise StyleReaderError(
                    "File {} is not a valid SLD".format(self._filename)
                )

            ns_prefix = root.tag[:root.tag.index('}')+1]
            ns_dict = {'sld': ns_prefix[1:-1]}
            rs_node = root.find('.//sld:RasterSymbolizer', ns_dict)
            if rs_node is None:
                raise StyleReaderError(
                    "File {} is not a valid SLD: no RasterSymbolizer defined".format(
                        self._filename
                ))
            cl_node = rs_node.find('sld:ColorMap', ns_dict)
            if cl_node is None:
                raise StyleReaderError(
                    "File {} is not a valid SLD: no ColorMap defined".format(
                        self._filename
                ))

            cl_entries = cl_node.findall('sld:ColorMapEntry', ns_dict)
            if not cl_entries:
                raise StyleReaderError(
                    "No color entries defined in file {}".format(
                        self._filename
                ))

            self._vc = OrderedDict()
            for ce in cl_entries:
                v = int(ce.attrib.get('quantity'))
                h = ce.attrib.get('color').lstrip('#')
                rgb = tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
                self._vc[v] = rgb

    def get_values(self):
        """Get values

        :return list: list of values
        """
        return self._vc.keys()

    def get_rgb_color(self, value):
        """Get RGB color code for specified value

        :param int value: value

        :return tuple: RGB codes
        """
        return self._vc[value]

    def set_band_colors(self, ds, ib=1):
        """Set color table for specifed GDAL band
        
        :param gdal.DataSource: GDAL data source
        :param int band: band identifier
        """
        from osgeo import gdal

        colors = gdal.ColorTable()
        for v in self.get_values():
            colors.SetColorEntry(v, self.get_rgb_color(v))

        band = ds.GetRasterBand(ib)
        band.SetRasterColorTable(colors)
        band.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)
        band = None


if __name__ == "__main__":
    sr = StyleReader('vpx_coverage.sld')
    for v in sr.get_values():
        print (v, sr.get_rgb_color(v))
