FROM centos:8.1.1911

RUN yum -y update

# create temporary dir for compilation
RUN mkdir /tmp/src

# install basic stuff
RUN yum -y install gcc-c++ gcc wget make

# install sqlite
RUN yum install -y sqlite.x86_64 sqlite-devel.x86_64

# install Python3.8 prerequisities
RUN yum -y install openssl-devel bzip2-devel libffi-devel xz-devel

# install Python3.8
RUN cd /tmp/src && \
    wget https://www.python.org/ftp/python/3.8.2/Python-3.8.2.tgz && \
    tar xvf Python-3.8.2.tgz && \
    cd Python-3.8*/ && \
    ./configure --enable-optimizations && \
    make install

# install PROJ dependencies
RUN yum -y install libtiff libtiff-devel curl-devel

# compile & install PROJ
RUN cd /tmp/src && \
    wget https://download.osgeo.org/proj/proj-7.0.0.tar.gz && \
    tar -zvxf proj-7.0.0.tar.gz && \
    cd proj-7.0.0 && \
    ./configure --disable-dependency-tracking && \
    make -j6 && \
    make install

# install GDAL dependecies
RUN yum -y install libpng cmake zlib-devel

# install JPEG 2.3.1
RUN cd /tmp/src && \
    wget https://github.com/uclouvain/openjpeg/archive/v2.3.1/openjpeg-2.3.1.tar.gz && \
    tar -zvxf openjpeg-2.3.1.tar.gz && \
    cd openjpeg-2.3.1 && \
    mkdir -v build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DBUILD_STATIC_LIBS=OFF .. && \
    make -j6 && \
    make install

# install pip dependecies
COPY requirements.txt /tmp/src/requirements.txt
RUN python3.8 -m pip install -r /tmp/src/requirements.txt

# install libtool (xerces dependency)
RUN yum -y install libtool

# install xerces
RUN cd /tmp/src && \
    wget https://github.com/apache/xerces-c/archive/v3.2.2.tar.gz && \
    tar -zxvf v3.2.2.tar.gz && \
    cd xerces-c-3.2.2 && \
    ./reconf && \
    ./configure && \
    make -j6 && \
    make install

# install geotiff
RUN cd /tmp/src && \
    wget https://github.com/OSGeo/libgeotiff/releases/download/1.5.1/libgeotiff-1.5.1.tar.gz && \
    tar -zxvf libgeotiff-1.5.1.tar.gz && \
    cd libgeotiff-1.5.1 && \
    ./configure && \
    make -j6 && \
    make install

# install GDAL 3.0.4
RUN cd /tmp/src && \
    wget http://download.osgeo.org/gdal/3.0.4/gdal-3.0.4.tar.gz && \
    tar -zvxf gdal-3.0.4.tar.gz && \
    cd gdal-3.0.4 && \
    PKG_CONFIG_PATH=/usr/lib/pkgconfig:/usr/local/lib/pkgconfig \
    ./configure --with-threads --with-libtiff=/usr/local \
    --with-geotiff=/usr/local --with-jpeg=internal --with-gif=internal \
    --with-png=internal --with-libz=/usr --with-python --with-openjpeg \
    --with-xerces && \
    make -j6 && \
    make install && \
    ldconfig

# install matplotlib dependencies
RUN yum -y install freetype-devel

# install rios
RUN cd /tmp/src && \
    wget https://github.com/ubarsc/rios/releases/download/rios-1.4.10/rios-1.4.10.tar.gz && \
    tar -zvxf rios-1.4.10.tar.gz && \
    cd rios-1.4.10 && \
    python3.8 setup.py build && \
    python3.8 setup.py install

# install fmask
RUN cd /tmp/src && \
    wget https://github.com/ubarsc/python-fmask/releases/download/pythonfmask-0.5.4/python-fmask-0.5.4.tar.gz && \
    tar -zvxf python-fmask-0.5.4.tar.gz && \
    cd python-fmask-0.5.4 && \
    python3.8 setup.py build && \
    python3.8 setup.py install

# download basemap
RUN cd /tmp/src && \
    wget https://github.com/matplotlib/basemap/archive/v1.2.2rel.tar.gz && \
    tar -zvxf v1.2.2rel.tar.gz

# copy CMakeLists fix for GEOS
COPY CMakeLists.txt /tmp/src/basemap-1.2.2rel/geos-3.3.3

# install GEOS
RUN cd /tmp/src/basemap-1.2.2rel/geos-3.3.3 && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. &&\
    make -j6 && \
    make install

# install arosics
RUN python3.8 -m pip install shapely==1.6.4
RUN python3.8 -m pip install py-tools-ds==0.15.7
RUN python3.8 -m pip install arosics==1.0.1

# install basemap
RUN cd /tmp/src/basemap-1.2.2rel && \
    python3.8 setup.py build && \
    python3.8 -m pip install .

# export paths
ENV LD_LIBRARY_PATH=/usr/local/lib/:$LD_LIBRARY_PATH
ENV PROJ_LIB=/usr/local/share/proj

# set up ESPA path and download there static data needed for installation of
# product formatter
RUN mkdir /usr/local/lib/espa_product_formatter && \
    mkdir /usr/local/lib/espa_product_formatter/static_data && \
    cd /usr/local/lib/espa_product_formatter/static_data && \
    wget http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/land_water_poly/land_no_buf.ply.gz && \
    gunzip land_no_buf.ply

# install hdf4 dependencies
RUN yum -y install byacc flex libjpeg-turbo-devel libtirpc libtirpc-devel

# set paths necessary for hdf4 compilation
ENV CFLAGS="-I/usr/include/tirpc"
ENV LIBS="-ltirpc"

# install HDF4
RUN cd /tmp/src && \
    wget https://support.hdfgroup.org/ftp/HDF/releases/HDF4.2.15/src/hdf-4.2.15.tar.gz && \
    tar -xzvf hdf-4.2.15.tar.gz && \
    cd hdf-4.2.15 && \
    ./configure --disable-fortran --prefix=/usr/local/ --disable-netcdf --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig

# install HDF-EOS2
RUN cd /tmp/src/ && \
    wget https://observer.gsfc.nasa.gov/ftp/edhs/hdfeos/latest_release/HDF-EOS2.20v1.00.tar.Z && \
    tar zxvf HDF-EOS2.20v1.00.tar.Z && \
    cd hdfeos && \
    ./configure --prefix=/usr/local && \
    make -j6 && \
    make install && \
    cd include && \
    mkdir /usr/local/include/hdfeos2 && \
    cp * /usr/local/include/hdfeos2/

# install HDF5
RUN cd /tmp/src/ && \
    wget https://observer.gsfc.nasa.gov/ftp/edhs/hdfeos5/latest_release/hdf5-1.8.19.tar.gz && \
    tar zxvf hdf5-1.8.19.tar.gz && \
    cd hdf5-1.8.19 && \
    ./configure --prefix=/usr/local && \
    make -j6 && \
    make install  && \
    ldconfig

# install HDF-EOS5
RUN cd /tmp/src/ && \
    wget https://observer.gsfc.nasa.gov/ftp/edhs/hdfeos5/latest_release/HDF-EOS5.1.16.tar.Z && \
    tar zxvf HDF-EOS5.1.16.tar.Z && \
    cd hdfeos5 && \
    ./configure --prefix=/usr/local && \
    make -j6 && \
    make install && \
    cd include && \
    mkdir /usr/local/include/hdfeos5 && \
    cp * /usr/local/include/hdfeos5/

# install ESPA product formatter dependencies
RUN yum -y install libxml2-devel

# install netCDF (dependency for ESPA product formatter)
RUN cd /tmp/src && \
    wget https://github.com/Unidata/netcdf-c/archive/v4.7.4.tar.gz && \
    tar xvzf v4.7.4.tar.gz && \
    cd netcdf-c-4.7.4 && \
    ./configure --enable-hdf4 && \
    make -j6 && \
    make install

# install libidn (dependency for ESPA product formatter)
RUN cd /tmp/src && \
    wget https://ftp.gnu.org/gnu/libidn/libidn-1.35.tar.gz && \
    tar xvzf libidn-1.35.tar.gz && \
    cd libidn-1.35 && \
    ./configure --prefix=/usr/local && \
    make -j6 && \
    make install

# create environment variables needed for the installation of product formatter
ENV PREFIX=/usr/local/lib/espa_product_formatter
ENV ESPA_LAND_MASS_POLYGON=$PREFIX/static_data/land_no_buf.ply
ENV TIFFINC=/usr/include
ENV TIFFLIB=/usr/lib64
ENV GEOTIFF_INC=/usr/local/include
ENV GEOTIFF_LIB=/usr/local/lib
ENV HDFINC=/usr/local/include
ENV HDFLIB=/usr/local/lib
ENV XML2INC=/usr/include/libxml2
ENV XML2LIB=/usr/lib64
ENV ESPAINC=/usr/local/lib/espa_product_formatter/include
ENV ESPALIB=/usr/local/lib/espa_product_formatter/lib
ENV HDFEOS_GCTPINC=/usr/local/include
ENV HDFEOS_GCTPLIB=/usr/local/lib
ENV HDF5INC=/usr/local/include
ENV HDF5LIB=/usr/local/lib
ENV HDFEOS_INC=/usr/local/include/hdfeos2
ENV HDFEOS_LIB=/usr/local/lib
ENV HDFEOS5_INC=/usr/local/include/hdfeos5
ENV HDFEOS5_LIB=/usr/local/lib
ENV JPEGINC=/usr/local/include
ENV JPEGLIB=/usr/lib64
ENV ZLIBINC=/usr/local/include
ENV ZLIBLIB=/usr/lib64
ENV CURLINC=/usr/include/curl
ENV CURLLIB=/usr/lib64
ENV LZMAINC=/usr/include/lzma
ENV LZMALIB=/usr/lib64
ENV IDNINC=/usr/local/include
ENV IDNLIB=/usr/local/lib
ENV NCDF4INC=/usr/local/include
ENV NCDF4LIB=/usr/local/lib

# download espa product formatter
RUN cd /tmp/src && \
    # wget https://github.com/USGS-EROS/espa-product-formatter/archive/product_formatter_v1.19.0.tar.gz && \
    wget https://github.com/pesekon2/espa-packages/releases/download/product_formatter/product_formatter_v1.19.0.tar.gz && \
    tar -xzvf product_formatter_v1.19.0.tar.gz && \
    cd espa-product-formatter-product_formatter_v1.19.0 && \
    make -j6 && \
    make install

# download and untar auxiliary files for lasrc
RUN mkdir /usr/local/lib/espa_surface_reflectance && \
    mkdir /usr/local/lib/espa_surface_reflectance/aux && \
    cd /usr/local/lib/espa_surface_reflectance/aux && \
    wget -nv http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/MSILUT.tar.gz && \
    tar -xzvf MSILUT.tar.gz && \
    rm MSILUT.tar.gz && \
    wget -nv http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/lasrc_aux.2013-2017.tar.gz && \
    tar -xzvf lasrc_aux.2013-2017.tar.gz && \
    rm lasrc_aux.2013-2017.tar.gz && \
    cd LADS && \
    wget -r -nv -np -nH -R "index.html*" -e robots=off --cut-dirs 5 https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/LADS/2018/ && \
    rm -r 2017 && \
    wget -r -nv -np -nH -R "index.html*" -e robots=off --cut-dirs 5 https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/LADS/2017/

# set environment variables for lasrc
ENV PREFIX=/usr/local/lib/espa_surface_reflectance
ENV LASRC_AUX_DIR=/usr/local/lib/espa_surface_reflectance/aux
ENV ESPA_SCHEMA=/usr/local/lib/espa_product_formatter/schema/espa_internal_metadata_v2_2.xsd
ENV BIN=/usr/local/lib/espa_product_formatter/bin/
ENV PATH=$BIN:$PATH
ENV PATH=/usr/local/lib/espa_surface_reflectance/bin/:$PATH

# install lasrc
RUN cd /tmp/src && \
    # wget https://github.com/USGS-EROS/espa-surface-reflectance/archive/lasrc_v2.0.0.tar.gz && \
    wget https://github.com/pesekon2/espa-packages/releases/download/lasrc/lasrc_v2.0.0.tar.gz && \
    tar -xzvf lasrc_v2.0.0.tar.gz && \
    cd espa-surface-reflectance-lasrc_v2.0.0 && \
    cd lasrc/c_version/src && \
    make -j6 && \
    make install && \
    cd ../../landsat_aux && \
    make -j6 && \
    make install && \
    cd ../ && \
    make -j6 && \
    make install && \
    cd ../scripts && \
    make install

# apply patches to ESPA libraries
COPY do_lasrc_landsat_patch.py /usr/local/lib/espa_surface_reflectance/bin/do_lasrc_landsat.py
COPY gdal_remove_drivers_patch.py /usr/local/lib/espa_surface_reflectance/bin/gdal_remove_drivers.py
COPY mask_per_pixel_angles_patch.py /usr/local/lib/espa_surface_reflectance/bin/mask_per_pixel_angles.py
COPY surface_reflectance_patch.py /usr/local/lib/espa_product_formatter/bin/surface_reflectance.py

# install file (sen2cor dependency)
RUN yum -y install file

# install sen2cor
RUN cd /tmp/src && \
    wget http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run && \
    mkdir /usr/local/lib/sen2cor && \
    bash Sen2Cor-02.08.00-Linux64.run --target /usr/local/lib/sen2cor

# add sen2cor bins into PATH
ENV PATH=$PATH:/usr/local/lib/sen2cor/bin/

# change access rights to make the call as a user available
RUN chmod -R 777 /usr/local/lib/sen2cor
RUN chmod -R 777 /root


# remove temporary dir for compilation
RUN rm -rf /tmp/src

# clean yum cache
RUN yum clean all

WORKDIR /opt/qcmms
ENTRYPOINT ["python3", "/opt/qcmms/bin/run_manager.py"]
