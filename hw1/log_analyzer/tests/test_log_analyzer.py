import log_analyzer
import unittest
import shutil
import os


class TestArgumentParse(unittest.TestCase):
    def test_positional_params(self):
        args = ['1', '2', '3']
        expected = {None: ['1', '2', '3']}

        result = log_analyzer.parse_args(args)

        self.assertEqual(result, expected)

    def test_named_params(self):
        args = ['-a', '1', '2', '-b']
        expected = {None: [], '-a': ['1', '2'], '-b': []}

        result = log_analyzer.parse_args(args)

        self.assertEqual(result, expected)

    def test_param_single_value(self):
        args = ['-a', 'single']
        expected = {None: [], '-a': 'single'}

        result = log_analyzer.parse_args(args)
        self.assertEqual(result, expected)


class TestConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = './test_data/tmp'
        cls.logs_dir = os.path.join(cls.temp_dir, 'logs')
        cls.reports_dir = os.path.join(cls.temp_dir, 'reports')

        os.makedirs(cls.temp_dir)
        os.mkdir(cls.logs_dir)
        os.mkdir(cls.reports_dir)

    def setUp(self):
        self.full_config = {
            'REPORT_SIZE': 1000,
            'REPORT_DIR': self.reports_dir,
            'LOG_DIR': self.logs_dir,
            'OUTPUT_LOG_FILE': os.path.join(self.temp_dir, 'monitoring_log.txt'),
            'TIMESTAMP_FILE': os.path.join(self.temp_dir, 'log_analyzer.ts')
        }

    def test_returns_default_if_no_path(self):
        conf = log_analyzer.get_config(None)
        self.assertEqual(conf, log_analyzer.DEFAULT_CONFIG)

    def test_validation_failed_if_no_report_size(self):
        del self.full_config['REPORT_SIZE']
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_report_size_not_an_integer(self):
        self.full_config['REPORT_SIZE'] = 'NaN'
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_report_size_less_than_one(self):
        self.full_config['REPORT_SIZE'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_no_report_dir(self):
        del self.full_config['REPORT_DIR']
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_report_dir_not_a_string(self):
        self.full_config['REPORT_DIR'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_no_log_dir(self):
        del self.full_config['LOG_DIR']
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_log_dir_does_not_exist(self):
        self.full_config['LOG_DIR'] = self.full_config['LOG_DIR'] + 'not_exists'
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_log_dir_not_a_string(self):
        self.full_config['LOG_DIR'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_output_log_file_path_not_a_string(self):
        self.full_config['OUTPUT_LOG_FILE'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_timestamp_file_path_not_a_string(self):
        self.full_config['TIMESTAMP_FILE'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation(self):
        log_analyzer.validate_config(self.full_config)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir)