==============================
 Processors API Documentation
==============================

Overview of processors base classes.

Processor base
==============

.. automodule:: processors
    :members:

Exceptions
==========

.. automodule:: processors.exceptions
    :members:

Utils
=====

.. automodule:: processors.utils
    :members:

Supported platforms
===================

Sentinel-2
----------

.. automodule:: processors.sentinel
    :members:

Landsat-8
---------

.. automodule:: processors.landsat
    :members:

Image product processors
========================

Image product processors are subclassed from
:class:`processors.QCProcessorIPBase` class.

Multi-mission search (1)
------------------------

Provided by :class:`processors.search.base.QCProcessorSearchBase` class.

.. automodule:: processors.search.base
    :members:
   
Delivery control (2)
--------------------

Provided by
:class:`processors.download.base.QCProcessorDownloadBase` class.
       
Level 2 image products supported by
:class:`processors.l2_calibration.base.QCProcessorL2CalibrationBase`
class.

.. automodule:: processors.download.base
    :members:

.. automodule:: processors.l2_calibration.base
    :members:
       
Ordinary control (3a)
---------------------

Provided by :class:`processors.ordinary_control.base.QCProcessorOrdinaryControlBase` class.

.. automodule:: processors.ordinary_control.base
    :members:
       
Detailed control (3b)
---------------------

Detailed control consists:

* cloud coverage control provided by :class:`processors.cloud_coverage.base.QCProcessorCloudCoverageBase` class,
* radiometry control provided by
  :class:`processors.radiometry_control.base.QCProcessorRadiometryControlBase` class,
* geometry quality control provided by :class:`processors.geometry_quality.base.QCProcessorGeometryQualityBase` class, and
* valid pixels control provided by :class:`processors.valid_pixels.base.QCProcessorValidPixelsBase` class.

.. automodule:: processors.cloud_coverage.base
    :members:

.. automodule:: processors.radiometry_control.base
    :members:

.. automodule:: processors.geometry_quality.base
    :members:

.. automodule:: processors.valid_pixels.base
    :members:
   
Multi-sensor control (4a)
-------------------------

Provided by
:class:`processors.harmonization_control.base.QCProcessorHarmonizationControlBase`
class.

Harmonization stack is implemented by
:class:`processors.harmonization_stack.base.QCProcessorHarmonizationStackBase` class.

.. automodule:: processors.harmonization_control.base
    :members:

.. automodule:: processors.harmonization_stack.base
    :members:

Coverage control (4b)
---------------------

Provided by :class:`processors.vpx_coverage.QCProcessorVpxCoverage` class.

.. automodule:: processors.vpx_coverage
    :members:

Land product processors
=======================

Land product processors are subclassed from
:class:`processors.QCProcessorLPBase` class.

Initialization is implemented by
:class:`processors.lp_init.QCProcessorLPInit` class.

.. automodule:: processors.lp_init
    :members:

Interpretation control (5)
--------------------------

Provided by
:class:`processors.lp_interpretation_control.QCProcessorLPInterpretationControl`
class.

.. automodule:: processors.lp_interpretation_control
    :members:

Validation control (6)
----------------------

Validation control consists:

* metadata control provided by
  :class:`processors.lp_metadata_control.QCProcessorLPMetadataControl`
  class,
* ordinary control provided by
  :class:`processors.lp_ordinary_control.QCProcessorLPOrdinaryControl`
  class, and
* thematic validation control provided by
  :class:`processors.lp_thematic_validation_control.QCProcessorLPThematicValidationControl`
  class.

.. automodule:: processors.lp_metadata_control
    :members:

.. automodule:: processors.lp_ordinary_control
    :members:

.. automodule:: processors.lp_thematic_validation_control
    :members:


Template processors
===================

Image product processor template
--------------------------------

See :class:`processors.template_ip.QCProcessorTemplateIP` for a
multi-sensor example which is derived from
:class:`processors.QCProcessorMultiBase` class.

Each supported sensor platform is defined by separated method which
returns sensor-specific processor class. Sensor specific classes
(:class:`processors.template_ip.sentinel.QCProcessorTemplateIPSentinel`,
:class:`processors.template_ip.landsat.QCProcessorTemplateIPLandsat`)
are derived from base
:class:`processors.template_ip.base.QCProcessorTemplateIPBase` class.

.. automodule:: processors.template_ip
    :members:
    :private-members:
       
.. automodule:: processors.template_ip.base
    :members:
    :private-members:
       
.. automodule:: processors.template_ip.sentinel
    :members:
    :private-members:
       
.. automodule:: processors.template_ip.landsat
    :members:
    :private-members:
          
Land product processor template
-------------------------------

Example :class:`processors.template_lp.QCProcessorTemplateLP` which is
derived from :class:`processors.QCProcessorLPBase`.

.. automodule:: processors.template_lp
    :members:
    :private-members:
