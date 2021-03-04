=========
 Example
=========

How to run Jobs Manager from Python code
========================================

QC Jobs Manager is implemented by :class:`manager.QCManager`
class. Manager instance reads on start configuration files. Processors
defined in configuration are registered by
:mod:`manager.QCManager.set_processors()`. Processor's tasks are triggered by
:mod:`manager.QCManager.run()` method. Generated QI metadata
responses can be send to the catalog by
:mod:`manager.QCManager.send_response()`.

.. code-block:: python

    from manager import QCManager

    # create QCManager instance
    manager = QCManager(
        ['config.yaml',
         'use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague.yaml',
         'use_cases/tuc1_imd_2018_010m/tuc1_imd_2018_010m_prague_ip.yaml'
        ]
    )

    # set processors (based on input configuration) to be run
    manager.set_processors()

    # run manager (perform all registered processors in given order)
    manager.run()

    # print QI metadata response to standard output
    manager.send_response()

.. tip::

   List of processors to be registered can be also directly defined by
   :mod:`manager.QCManager.set_processors()`.

   .. code-block:: python

      manager.set_processors(['search', 'download'])
