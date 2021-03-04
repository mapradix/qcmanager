"""Test suite checking QC Manager in general."""

import os
import yaml

from manager.logger import Logger


class TestProcessors:

    @staticmethod
    def test_tc_026a(pytestconfig):
        """Run one individual QC Manager processor

        This test case consists to check that the QC Manager runs individual
        QC Manager processor.
        """
        from bin import run_manager
        from manager import QCManager
        QCManager(
            config_file_all,
            cleanup=-1
        )

        with open(config_files[3]) as config_yaml:
            parsed_config = yaml.load(config_yaml, Loader=yaml.FullLoader)
            log_dir_rel = parsed_config['logging']['directory']
            log_dir = os.path.join(pytestconfig.rootdir, log_dir_rel)

            for test_id in range(1, 7):
                assert not os.path.isdir(log_dir), \
                    'Logs not cleaned up - no way to check if the next ' \
                    'processor works or not'
                ip_config_file = os.path.join(
                    os.path.dirname(__file__), '..', 'tests',
                    'manager_tests_configs', 'test_{}.yaml'.format(test_id))
                test_config_files = config_files + [ip_config_file]
                run_manager.main(test_config_files)
                assert len(os.listdir(log_dir)) > 1, \
                    'No logs created for config test_{}.yaml'.format(test_id)
                QCManager(
                    config_file_all,
                    cleanup=-1
                )
        Logger.info("Running individual QC Manager processor")

    @staticmethod
    def test_tc_026b():
        """Run the full processors stack.

        This test case consists to check that the QC Manager runs the set of
        QC Manager processors in correct order.
        """
        from bin import run_manager
        run_manager.main(config_file_all)
        Logger.info("Running set of QC Manager processors")

    @staticmethod
    def test_tc_027(pytestconfig):
        """Test if JSON metadata were created in the previous test.

        This test case consists to check that the QC Manager creates JSON
        metadata to be passed to Catalog.
        """
        with open(config_files[3]) as config_yaml:
            parsed_config = yaml.load(config_yaml, Loader=yaml.FullLoader)
            log_dir_rel = parsed_config['logging']['directory']
            log_dir = os.path.join(pytestconfig.rootdir, log_dir_rel)
            assert len(os.listdir(log_dir)) > 1, \
                'No logs to be checked'

            for i in os.listdir(log_dir):
                if os.path.isdir(os.path.join(log_dir, i)):
                    for root, dirs, files in os.walk(os.path.join(log_dir, i)):
                        assert all('json' in file for file in files), \
                            'Dir {} not containing any .json file'.format(root)

        Logger.info("Creating JSON metadata")


config_files = [
    os.path.join(
        os.path.dirname(__file__), '..', 'config.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'use_cases',
        'tuc1_imd_2018_010m', 'tuc1_imd_2018_010m_prague.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'use_cases',
        'tuc1_imd_2018_010m', 'tuc1_imd_2018_010m_prague_sample.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'tests', 'test.yaml'),
]

config_file_all = [
    os.path.join(
        os.path.dirname(__file__), '..', 'config.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'use_cases',
        'tuc1_imd_2018_010m', 'tuc1_imd_2018_010m_prague.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'use_cases',
        'tuc1_imd_2018_010m', 'tuc1_imd_2018_010m_prague_sample.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'tests', 'test.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'tests', 'manager_tests_configs',
        'test_all.yaml')
]
