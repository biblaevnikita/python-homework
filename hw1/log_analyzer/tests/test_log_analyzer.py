import logging
import unittest
import shutil
import os
import log_analyzer

logging.root.disabled = True


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
        plain_log_file = os.path.join(os.path.dirname(__file__), 'test_data', 'log_plain')
        records = list(log_analyzer.get_log_records(plain_log_file))
        self.assertEqual(len(records), 2)

    def test_parse_log_file_gzip(self):
        gzip_log_file = os.path.join(os.path.dirname(__file__), 'test_data', 'log_gzip.gz')
        records = list(log_analyzer.get_log_records(gzip_log_file))
        self.assertEqual(len(records), 2)

    def test_create_result_item(self):
        total_time = 2.0
        total_records = 12

        href = '/api/smth'
        requests_count = 5
        responses = [0.01, 0.015, 0.03, 0.01, 0.007]
        response_time_sum = 0.072
        max_response_time = 0.03
        response_time_avg = 0.014

        intermediate_item = {'href': href,
                             'requests_count': requests_count,
                             'response_time_sum': response_time_sum,
                             'max_response_time': max_response_time,
                             'response_time_avg': response_time_avg,
                             'all_responses_time': responses}

        expect_url = '/api/smth'
        expect_requests_count = 5
        expect_count_perc = 41.666666
        expect_time_avg = 0.0144
        expect_time_max = 0.03
        expect_time_med = 0.01
        expect_time_perc = 3.599999
        expect_time_sum = 0.072

        result_item = log_analyzer.create_result_item(intermediate_item, total_records, total_time)

        self.assertEqual(result_item['url'], expect_url)
        self.assertEqual(result_item['count'], expect_requests_count)
        self.assertAlmostEqual(result_item['count_perc'], expect_count_perc, delta=0.001)
        self.assertAlmostEqual(result_item['time_avg'], expect_time_avg, delta=0.001)
        self.assertAlmostEqual(result_item['time_max'], expect_time_max, delta=0.001)
        self.assertAlmostEqual(result_item['time_med'], expect_time_med, delta=0.001)
        self.assertAlmostEqual(result_item['time_perc'], expect_time_perc, delta=0.001)
        self.assertAlmostEqual(result_item['time_sum'], expect_time_sum, delta=0.001)

    def test_create_intermediate_item(self):
        intermediate_data = {}
        href = '/api/smth'
        response_time = 0.17

        expected_href = '/api/smth'
        expected_requests_count = 1
        expected_response_time_sum = 0.17
        expected_max_response_time = 0.17
        expected_response_time_avg = 0.17
        expected_all_responses_time = [0.17]

        log_analyzer.create_or_update_intermediate_item(intermediate_data, href, response_time)

        self.assertEqual(len(intermediate_data), 1)
        self.assertIn(expected_href, intermediate_data)

        item = intermediate_data[expected_href]

        self.assertEqual(item['href'], expected_href)
        self.assertEqual(item['requests_count'], expected_requests_count)
        self.assertAlmostEqual(item['response_time_sum'], expected_response_time_sum)
        self.assertAlmostEqual(item['max_response_time'], expected_max_response_time)
        self.assertAlmostEqual(item['response_time_avg'], expected_response_time_avg)
        self.assertListEqual(item['all_responses_time'], expected_all_responses_time)

    def test_update_intermediate_item(self):
        intermediate_data = {
            '/api/smth': {
                'href': '/api/smth',
                'requests_count': 1,
                'max_response_time': 0.17,
                'response_time_avg': 0.17,
                'response_time_sum': 0.17,
                'all_responses_time': [0.17]
            }
        }

        href = '/api/smth'
        response_time = 0.25

        expected_href = '/api/smth'
        expected_requests_count = 2
        expected_response_time_sum = 0.42
        expected_max_response_time = 0.25
        expected_response_time_avg = 0.21
        expected_all_responses_time = [0.17, 0.25]

        log_analyzer.create_or_update_intermediate_item(intermediate_data, href, response_time)

        self.assertEqual(len(intermediate_data), 1)
        self.assertIn(expected_href, intermediate_data)

        item = intermediate_data[expected_href]

        self.assertEqual(item['href'], expected_href)
        self.assertEqual(item['requests_count'], expected_requests_count)
        self.assertAlmostEqual(item['response_time_sum'], expected_response_time_sum)
        self.assertAlmostEqual(item['max_response_time'], expected_max_response_time)
        self.assertAlmostEqual(item['response_time_avg'], expected_response_time_avg)
        self.assertListEqual(item['all_responses_time'], expected_all_responses_time)

    def test_median_for_an_even_number_of_items(self):
        data = [1, 12, 4, 15, 3, 2]
        self.assertAlmostEqual(log_analyzer.median(data), 3.5)

    def test_median_for_an_odd_number_of_items(self):
        data = [7, 10, 22, 3, 1, 1, 15]
        self.assertEqual(log_analyzer.median(data), 7)

    def test_median_for_two_items(self):
        data = [15, 33]
        self.assertEqual(log_analyzer.median(data), 24)

    def test_median_for_one_item(self):
        data = [6]
        self.assertEqual(log_analyzer.median(data), 6)

    def test_median_for_an_empty_list(self):
        data = []
        self.assertIsNone(log_analyzer.median(data))


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
            'MAX_REPORT_SIZE': 1000,
            'REPORTS_DIR': self.reports_dir,
            'LOGS_DIR': self.logs_dir,
            'MONITORING_LOG_FILE': os.path.join(self.temp_dir, 'monitoring_log.txt'),
            'TIMESTAMP_FILE': os.path.join(self.temp_dir, 'log_analyzer.ts'),
            'ERRORS_LIMIT': 0.5
        }

    def test_validation_failed_if_no_report_size(self):
        del self.full_config['MAX_REPORT_SIZE']
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_report_size_not_an_integer(self):
        self.full_config['MAX_REPORT_SIZE'] = 'NaN'
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_report_size_less_than_one(self):
        self.full_config['MAX_REPORT_SIZE'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_no_report_dir(self):
        del self.full_config['REPORTS_DIR']
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_report_dir_not_a_string(self):
        self.full_config['REPORTS_DIR'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_no_log_dir(self):
        del self.full_config['LOGS_DIR']
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_log_dir_does_not_exist(self):
        self.full_config['LOGS_DIR'] = self.full_config['LOGS_DIR'] + 'not_exists'
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_log_dir_not_a_string(self):
        self.full_config['LOGS_DIR'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_output_log_file_path_not_a_string(self):
        self.full_config['MONITORING_LOG_FILE'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation_failed_if_timestamp_file_path_not_a_string(self):
        self.full_config['TIMESTAMP_FILE'] = 0
        self.assertRaises(ValueError, log_analyzer.validate_config, self.full_config)

    def test_validation(self):
        exc = None
        try:
            log_analyzer.validate_config(self.full_config)
        except Exception as e:
            exc = e

        self.assertIsNone(exc)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir)
