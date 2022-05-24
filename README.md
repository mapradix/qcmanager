# QCMMS

This repository is meant for Mapradix development of the QCMMS
(Quality Control Metadata Management System) QC Manager subsystem.

## QC Manager

The QC Manager is a part of QCMMS system responsible for the tasks related to collection 
and processing the quality related metadata along the whole Land Production process. 
And secondly providing processed metadata to the QI Catalog via defined REST API. 

To achieve this functionality as described in the QCMMS Software System Specification 
a set of processor groups have been identified. Each group can be represented 
by one or more processors given the thematic scope, complexity and scalability of the manager system:

A/ Image Products processors
* 1) IP search 
* 2) IP delivery control; 
* 3A) IP ordinary control; 
* 3B) IP detail control [cloud cover, radiometry control, geometry quality control, valid pixels]; 
* 4A) IP multi-sensor control (optional, if multi-sensor IP set acquired); 
* 4B) IP coverage control [vpx_coverage]; 

B/ Land Product processors
* 5) LP interpretation QI control (optional, if possible); 
* 6) LP validation QI control. 

Beside the processors to extract and process the quality metadata, there is a processor dispatcher 
and logger as the essential parts of the QC Manager subsystem. 

IMPORTANT: All commands must be run from QCMMS source code root directory!

## Docker

Install [Docker](https://docs.docker.com/get-docker/).

A lot of downloading is necessary, so be prepared to wait some time and
reserve some space in the Docker root for the image. It is around 482 GBs for
the first of the following images (Sentinel and Landsat calibration) and
around 16 GBs for the second one (only Sentinel calibration). There is also
a way to have the docker image doing both calibrations, but storing most of
data elsewhere - you can find this info in the section
`Download Landsat auxiliary files outside the image`.

Go to docker directory and build the image.

If you plan to do the L2 calibration for both the Sentinel and Landsat
products:

```bash
cd docker/py3_gdal3_sen2cor_lasrc
docker build . -t qcmms:2.0
```

If you plan to do the L2 calibration only for Sentinel products:

```bash
cd docker/py3_gdal3_sen2cor
docker build . -t qcmms:2.0
```

If you plan to run the manager as a currect user, copy
`docker/passwd.template` to `docker/passwd` and modify the file in the
following way:

* instead of `INSERT RESULT OF "id -un"`, insert the result of `id -un`
* instead of `INSERT RESULT OF "id -u"`, insert the result of `id -u`

and store as `docker/passwd`.

Tip: File `passwd` can be easily generated by running:

```bash
./docker/generate_passwd.sh
```

If it is not the case and you plan to run it as a root instead, you can skip
modifying the file and remove
`--user $(id -u) -v docker/passwd:/etc/passwd:ro` from each
of the following commands.

### Standard way - Run the manager in the Docker container

Before running QC Manager credentials (username, password) for primary
and secondary platform in `config.yaml` must be defined.

Then you can run the image and it will run the QC Manager with
specified configuration files.

Example how to run image processors for TUC1:

```bash
docker run --user $(id -u) -v `pwd`/docker/passwd:/etc/passwd:ro -v `pwd`:/opt/qcmms:rw qcmms:2.0 \
 -c use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague.yaml,use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague_ip.yaml
```

Example how to run image processors for TUC2:

```bash
docker run --user $(id -u) -v `pwd`/docker/passwd:/etc/passwd:ro -v `pwd`:/opt/qcmms:rw qcmms:2.0 \
 -c use_cases/tuc2_tccm_1518_020m/tuc2_tccm_2015_2018_20m_sumava.yaml,use_cases/tuc2_tccm_1518_020m/tuc2_tccm_2015_2018_20m_sumava_ip.yaml
```

Example how to run image processors for TUC3:

```bash
docker run --user $(id -u) -v `pwd`/docker/passwd:/etc/passwd:ro -v `pwd`:/opt/qcmms:rw qcmms:2.0 \
 -c use_cases/tuc3_lss_2018_100m/tuc3_lss_2018_100m_jeseniky.yaml,use_cases/tuc3_lss_2018_100m/tuc3_lss_2018_100m_jeseniky_ip.yaml
```

Note: replace `_ip` by `_lp` to run land processors instead of image processors.

If you plan to use data not contained in the `qcmms` directory in the manager,
link them with extra `-v` parameters:

```bash
docker run --user $(id -u) -v `pwd`/docker/passwd:/etc/passwd:ro -v `pwd`:/opt/qcmms:rw \
 -v path_to_your_data:path_to_your_data:rw qcmms:2.0 \ 
 -c use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague.yaml,use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague_ip.yaml
```

### Open the container in an interactive mode

If you wish to do something different than just run the manager in the Docker
environment, you can run the container in the interactive mode and "get" inside
the image.

```bash
docker run -it --user $(id -u) -v `pwd`/docker/passwd:/etc/passwd:ro -v `pwd`:/opt/qcmms:rw \
 --entrypoint bash qcmms:2.0
```

### Download Landsat auxiliary files outside the image

Because the image with Landsat calibrators is big and most users have linked
docker to their root directory, some users could prefer building the docker
image without Landsat auxiliary files, downloading auxiliary files somewhere
else and then link them to the container when running it.

Firstly, you need to skip this download section of the `docker build`. Go to
[Dockerfile](docker/py3_gdal3_sen2cor_lasrc/Dockerfile), comment/delete
the part titled `# download and untar auxiliary files for lasrc` and replace
it with the following:

```dockerfile
RUN mkdir /usr/local/lib/espa_surface_reflectance
```

Then build the image the same way as you would normally:

```bash
cd docker/py3_gdal3_sen2cor_lasrc
docker build . -t qcmms:2.0
```

Go to the directory where you want to store Landsat auxiliary data and run
the following (deleting the year 2017 and downloading it again is due to the
fact that the data in the official `tar` are incomplete):

```bash
wget -nv http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/MSILUT.tar.gz
tar -xzvf MSILUT.tar.gz
rm MSILUT.tar.gz
wget -nv http://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/lasrc_aux.2013-2017.tar.gz
tar -xzvf lasrc_aux.2013-2017.tar.gz
rm lasrc_aux.2013-2017.tar.gz
cd LADS
wget -r -nv -np -nH -R "index.html*" -e robots=off --cut-dirs 5 https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/LADS/2018/
rm -r 2017
wget -r -nv -np -nH -R "index.html*" -e robots=off --cut-dirs 5 https://edclpdsftp.cr.usgs.gov/downloads/auxiliaries/lasrc_auxiliary/L8/LADS/2017/
```

Then run the docker container any of the previous ways, but with an extra
`-v` volume link to the auxiliary directory:

```bash
docker run --user $(id -u) -v `pwd`/docker/passwd:/etc/passwd:ro -v `pwd`:/opt/qcmms:rw \
 -v path_to_your_auxiliary_dir:/usr/local/lib/espa_surface_reflectance/aux:ro qcmms:2.0
```

## Tests

### Run tests in the Docker container

For TUC1/TUC3:

```
docker run -it --user $(id -u) -v `pwd`/docker/passwd:/etc/passwd:ro -v `pwd`:/opt/qcmms:rw \
 --entrypoint pytest qcmms:2.0 tests/test_tc1.py -xvs
```

For TUC2:

```
docker run -it --user $(id -u) -v `pwd`/docker/passwd:/etc/passwd:ro -v `pwd`:/opt/qcmms:rw \
 --entrypoint pytest qcmms:2.0 tests/test_tc2.py -xvs
```

## EO Sensors

QC Manager sub-system works currently with Sentinel-2 and Landsat-8 images 

Sentinel-2:  
<https://sentinel.esa.int/documents/247904/685211/Sentinel-2_User_Handbook>

Landsat 8:  
<https://landsat.usgs.gov/landsat-8-data-users-handbook>

## Contact

info at mapradix.cz
