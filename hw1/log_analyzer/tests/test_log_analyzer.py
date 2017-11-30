import log_analyzer
import unittest
import shutil
import os

log_analyzer.logger.disabled = True


class TestAnalyze(unittest.TestCase):
    def test_parse_log_record(self):
        line = ('1.138.198.128 '
                '-  '
                '- '
                '[29/Jun/2017:04:24:24 +0300] '
                '"GET /api/v2//group/7085835/banners HTTP/1.1" '
                '200 '
                '3777 '
                '"-" '
                '"python-requests/2.8.1" '
                '"-" '
                '"1498699463-440360380-4707-9845441" '
                '"4e9627334" '
                '1.349\n')

        href, response_time = log_analyzer.parse_log_record(line)
        self.assertEqual(href, '/api/v2//group/7085835/banners')
        self.assertAlmostEqual(response_time, 1.349)

    def test_parse_log_record_returns_none_if_href_invalid(self):
        line = ('1.138.198.128 '
                '-  '
                '- '
                '[29/Jun/2017:04:24:24 +0300] '
                '"INVALID_HREF" '
                '200 '
                '3777 '
                '"-" '
                '"python-requests/2.8.1" '
                '"-" '
                '"1498699463-440360380-4707-9845441" '
                '"4e9627334" '
                '1.349\n')

        record = log_analyzer.parse_log_record(line)

        self.assertIsNone(record)

    def test_parse_log_record_returns_none_if_response_time_invalid(self):
        line = ('1.138.198.128 '
                '-  '
                '- '
                '[29/Jun/2017:04:24:24 +0300] '
                '"GET /api/v2//group/7085835/banners HTTP/1.1" '
                '200 '
                '3777 '
                '"-" '
                '"python-requests/2.8.1" '
                '"-" '
                '"1498699463-440360380-4707-9845441" '
                '"4e9627334" '
                'INVALID_RESPONSE_TIME\n')

        record = log_analyzer.parse_log_record(line)

        self.assertIsNone(record)

    def test_parse_log_file_plain(self):
        plain_log_file = './test_data/log_plain'
        records = list(log_analyzer.get_log_records(plain_log_file))
        self.assertEqual(len(records), 2)

    def test_parse_log_file_gzip(self):
        gzip_log_file = './test_data/log_gzip.gz'
        records = list(log_analyzer.get_log_records(gzip_log_file))
        self.assertEqual(len(records), 2)


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