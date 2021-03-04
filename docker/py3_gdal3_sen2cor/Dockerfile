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
