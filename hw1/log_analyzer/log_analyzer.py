import sys
import os
import logging
import json
import re
import time
import gzip
from datetime import datetime

usage_text = '''
usage: log_analyzer.py [-h] [--config CONFIG_PATH]

optional arguments:
  -h, --help           show this help message and exit
  --config CONFIG      config file path
'''

####################################
# Constants
####################################

CONFIG_REPORT_SIZE = 'REPORT_SIZE'
CONFIG_REPORT_DIR = 'REPORT_DIR'
CONFIG_LOG_DIR = 'LOG_DIR'
CONFIG_OUTPUT_LOG_FILE = 'OUTPUT_LOG_FILE'
CONFIG_TIMESTAMP_FILE = 'TIMESTAMP_FILE'
CONFIG_REQUIRED_FIELDS = frozenset([CONFIG_REPORT_SIZE, CONFIG_REPORT_DIR, CONFIG_LOG_DIR])
DEFAULT_CONFIG = {
    CONFIG_REPORT_SIZE: 1000,
    CONFIG_REPORT_DIR: "./reports",
    CONFIG_LOG_DIR: "./log"
}

LOG_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOG_DATE_TIME_FORMAT = '%Y.%m.%d %H:%M:%S'
DEFAULT_LOG_FORMATTER = logging.Formatter(LOG_FORMAT, LOG_DATE_TIME_FORMAT)

ARGUMENT_CONFIG = '--config'

REPORT_NAME_PATTERN = 'report-{}.html'
REPORT_FILENAME_RE = re.compile(r'^report-(?P<date>\d{4}\.\d{2}\.\d{2})\.html')
LOG_FILENAME_RE = re.compile(r'^nginx-access-ui\.log-(?P<date>\d{8})')

REPORT_RE_DATE_GROUP_NAME = LOG_RE_DATE_GROUP_NAME = 'date'

REPORT_FILENAME_DATE_FORMAT = '%Y.%m.%d'
LOG_FILENAME_DATE_FORMAT = '%Y%m%d'

REPORT_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'template.html')

LOG_RECORD_RE = re.compile(
    '^'
    '\S+ '  # remote_addr
    '\S+\s+'  # remote_user (note: ends with double space)
    '\S+ '  # http_x_real_ip
    '\[\S+ \S+\] '  # time_local [datetime tz] i.e. [29/Jun/2017:10:46:03 +0300]
    '"\S+ (?P<href>\S+) \S+" '  # request "method href proto" i.e. "GET /api/v2/banner/23815685 HTTP/1.1"
    '\d+ '  # status
    '\d+ '  # body_bytes_sent
    '"\S+" '  # http_referer
    '".*" '  # http_user_agent
    '"\S+" '  # http_x_forwarded_for
    '"\S+" '  # http_X_REQUEST_ID
    '"\S+" '  # http_X_RB_USER
    '(?P<time>\d+\.\d+)'  # request_time
)
GZIP_FILE_SIG = bytearray.fromhex('1f8b')

TEMPLATE_DATA_PLACEHOLDER = '$table_json'


####################################
# Logging
####################################


class LevelsLogFilter(object):
    def __init__(self, *levels):
        self.levels = frozenset(levels)

    def filter(self, record):
        return record.levelno in self.levels


def create_default_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.addFilter(LevelsLogFilter(logging.INFO))
    stdout_handler.setFormatter(DEFAULT_LOG_FORMATTER)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.addFilter(LevelsLogFilter(logging.ERROR))
    stderr_handler.setFormatter(DEFAULT_LOG_FORMATTER)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    return logger


def handle_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        logger.info('Interrupted by user')

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


logger = create_default_logger()
sys.excepthook = handle_uncaught_exceptions


def redirect_logger_to_file(logger, log_file_name):
    if not log_file_name:
        return

    log_dir = os.path.dirname(log_file_name)
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_file_name)
    file_handler.setFormatter(DEFAULT_LOG_FORMATTER)
    file_handler.addFilter(LevelsLogFilter(logging.INFO, logging.ERROR))

    logger.addHandler(file_handler)


####################################
# Config
####################################

def get_config(conf_path=None):
    if not conf_path:
        return DEFAULT_CONFIG

    with open(conf_path, 'r') as conf:
        return json.load(conf)


def validate_config(config):
    assert config is not None, 'config required'

    missing_fields = [field for field in CONFIG_REQUIRED_FIELDS if field not in config]
    if missing_fields:
        raise ValueError('Required fields: {}'.format(', '.join(missing_fields)))

    report_size = config[CONFIG_REPORT_SIZE]
    if not report_size or not isinstance(report_size, int) or report_size < 1:
        raise ValueError('"{}" must be a positive integer'.format(CONFIG_REPORT_SIZE))

    logs_dir = config[CONFIG_LOG_DIR]
    if not logs_dir or not os.path.isdir(logs_dir) or not isinstance(logs_dir, basestring):
        raise ValueError('"{}" must be an existing directory'.format(CONFIG_LOG_DIR))

    report_dir = config[CONFIG_REPORT_DIR]
    if not report_dir or not isinstance(logs_dir, basestring):
        raise ValueError('"{}" must be a path string'.format(CONFIG_REPORT_DIR))

    if CONFIG_OUTPUT_LOG_FILE in config and not config.get(CONFIG_OUTPUT_LOG_FILE):
        raise ValueError('"{}" must be a file path'.format(CONFIG_REPORT_DIR))

    if CONFIG_TIMESTAMP_FILE in config and not config.get(CONFIG_TIMESTAMP_FILE):
        raise ValueError('"{}" must be a file path'.format(CONFIG_TIMESTAMP_FILE))


####################################
# Analyzing
####################################


def analyze(records, max_records):
    total_records = 0
    total_time = 0
    intermediate_data = {}

    for href, response_time in records:
        total_records += 1
        total_time += response_time
        create_or_update_intermediate_item(intermediate_data, href, response_time)

    sorted_values = sorted(intermediate_data.itervalues(), key=lambda i: i['response_time_avg'], reverse=True)
    if len(sorted_values) > max_records:
        del sorted_values[max_records-1:]

    return [create_result_item(intermediate_item, total_records, total_time) for intermediate_item in sorted_values]


def create_or_update_intermediate_item(intermediate_data, href, response_time):
    item = intermediate_data.get(href)
    if not item:
        item = {'href': href,
                'requests_count': 0,
                'response_time_sum': 0,
                'max_response_time': response_time,
                'response_time_avg': 0,
                'all_responses_time': []}
        intermediate_data[href] = item

    item['requests_count'] += 1
    item['response_time_sum'] += response_time
    item['max_response_time'] = max(item['max_response_time'], response_time)
    item['response_time_avg'] = item['response_time_sum'] / item['requests_count']
    item['all_responses_time'].append(response_time)


def create_result_item(intermediate_item, total_records, total_time):
    url = intermediate_item['href']
    count = intermediate_item['requests_count']
    count_perc = intermediate_item['requests_count'] / float(total_records) * 100
    time_avg = intermediate_item['response_time_avg']
    time_max = intermediate_item['max_response_time']
    time_med = median(intermediate_item['all_responses_time'])
    time_perc = intermediate_item['response_time_sum'] / total_time * 100
    time_sum = intermediate_item['response_time_sum']

    return {
        'url': url,
        'count': count,
        'count_perc': round(count_perc, 3),
        'time_avg': round(time_avg, 3),
        'time_max': round(time_max, 3),
        'time_med': round(time_med, 3),
        'time_perc': round(time_perc, 3),
        'time_sum': round(time_sum, 3)
    }


def get_log_records(log_path):
    open_fn = open
    if is_gzip_file(log_path):
        open_fn = gzip.open

    with open_fn(log_path, 'r') as log_file:
        while True:
            line = log_file.readline()
            if not line:
                break

            record = parse_log_record(line)
            if not record:
                continue

            yield record


def parse_log_record(log_line):
    match = LOG_RECORD_RE.match(log_line)
    if not match:
        logger.error('Unable to parse line: "{}"'.format(log_line.rstrip()))
        return None

    href = match.groupdict()['href']
    request_time = float(match.groupdict()['time'])

    return href, request_time


def median(values_list):
    if not values_list:
        return None

    sorted_list = sorted(values_list)
    size = len(sorted_list)
    half_size = size / 2

    return sorted_list[half_size] if size % 2 else (sorted_list[half_size - 1] + sorted_list[half_size]) / 2.0


####################################
# Utils
####################################


def parse_args(args, help=None):
    """
    A simple argument parser. Returns a dictionary where keys are argument names and values are argument values.
    If only one value associated with a key it will be represented as it is otherwise value will be represented as list.
    None-key contains positional arguments.
    :param args: arguments list i.e. sys.argv[1:]
    :param help: help message that will be printed if the -h or --help arguments were passed
    :return: dict(arg_name, [values ...])
    """
    args_dict = {None: []}

    current_arg_name = None
    for arg in args:
        is_arg_name = arg.startswith('-')
        if is_arg_name:
            current_arg_name = arg
            if arg not in args_dict:
                args_dict[arg] = []
        else:
            args_dict[current_arg_name].append(arg)

    for key, value in args_dict.iteritems():
        if len(value) == 1:
            args_dict[key] = value[0]

    if help and ('-h' in args_dict or '--help' in args_dict):
        print usage_text
        sys.exit(0)

    return args_dict


def parse_file_date(filename, date_format, date_re, date_re_group_name):
    """
    Getting a date from the filename
    :param filename: File name
    :param date_format: strptime compatible date format
    :param date_re: file name regex
    :param date_re_group_name: regex group name that contains a string date value
    :return: datetime
    """
    if not filename:
        return None

    filename = os.path.basename(filename)
    match = date_re.match(filename)
    if not match:
        return None

    date = match.groupdict()[date_re_group_name]
    return datetime.strptime(date, date_format)


def get_latest_date_named_file(files_dir, date_format, date_re, date_re_group_name):
    """
    Getting a file from given directory with latest date in it's name
    :param files_dir: search directory
    :param date_format: strptime compatible date format
    :param date_re: file name regex
    :param date_re_group_name: regex group name that contains a string date value
    :return: file path with a latest date in file name
    """
    if not os.path.isdir(files_dir):
        return None

    files = [os.path.join(files_dir, filename)
             for filename
             in os.listdir(files_dir)
             if date_re.match(filename)]

    if not files:
        return None

    return max(files, key=lambda f: parse_file_date(f, date_format, date_re, date_re_group_name))


def get_latest_log(logs_dir):
    return get_latest_date_named_file(logs_dir, LOG_FILENAME_DATE_FORMAT, LOG_FILENAME_RE, LOG_RE_DATE_GROUP_NAME)


def get_latest_report(logs_dir):
    return get_latest_date_named_file(logs_dir, REPORT_FILENAME_DATE_FORMAT,
                                      REPORT_FILENAME_RE, REPORT_RE_DATE_GROUP_NAME)


def parse_log_date(filename):
    return parse_file_date(filename, LOG_FILENAME_DATE_FORMAT, LOG_FILENAME_RE, LOG_RE_DATE_GROUP_NAME)


def parse_report_date(filename):
    return parse_file_date(filename, REPORT_FILENAME_DATE_FORMAT, REPORT_FILENAME_RE, REPORT_RE_DATE_GROUP_NAME)


def is_up_to_date(latest_log, latest_report):
    latest_log_date = parse_log_date(latest_log)
    latest_report_date = parse_report_date(latest_report)
    if not latest_log_date:
        return True

    if not latest_report_date:
        return False

    return latest_report_date >= latest_log_date


def is_gzip_file(file_path):
    with open(file_path, 'rb') as f:
        return f.read(2) == GZIP_FILE_SIG


def render_template(template_path, to, data):
    assert template_path, 'template_path required'
    assert to, 'target file path required'

    if data is None:
        data = []

    target_dir = os.path.dirname(to)
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    with open(template_path, 'r') as template:
        template_string = template.read()

    template_head, template_bottom = template_string.split(TEMPLATE_DATA_PLACEHOLDER)
    with open(to, 'w') as render_target:
        render_target.write(template_head)
        json.dump(data, render_target)
        render_target.write(template_bottom)


def write_timestamp(file_path, timestamp):
    assert file_path, 'file_path required'
    assert timestamp, 'timestamp required'

    timestamp = int(timestamp)
    ts_dir = os.path.dirname(file_path)
    if not os.path.isdir(ts_dir):
        os.makedirs(ts_dir)

    with open(file_path, 'w') as ts_file:
        ts_file.write(str(timestamp))
        ts_file.truncate()

    a_time = os.stat(file_path).st_atime
    os.utime(file_path, (a_time, timestamp))


def main(args):
    args = parse_args(args, help=usage_text)
    config = get_config(args.get(ARGUMENT_CONFIG))
    validate_config(config)

    output_log_file = config.get(CONFIG_OUTPUT_LOG_FILE)
    if output_log_file:
        redirect_logger_to_file(logger, output_log_file)

    latest_report = get_latest_report(config[CONFIG_REPORT_DIR])
    latest_log = get_latest_log(config[CONFIG_LOG_DIR])

    if is_up_to_date(latest_log, latest_report):
        logger.info("Looks like everything is up-to-date")
        return

    logger.info('Collecting data from "{}"'.format(os.path.normpath(latest_log)))

    analyzed_data = analyze(get_log_records(latest_log), config[CONFIG_REPORT_SIZE])

    report_date = parse_log_date(latest_log).strftime(REPORT_FILENAME_DATE_FORMAT)
    report_filename = REPORT_NAME_PATTERN.format(report_date)
    report_file_path = os.path.join(config[CONFIG_REPORT_DIR], report_filename)

    render_template(REPORT_TEMPLATE_PATH, report_file_path, analyzed_data)

    logger.info('Report saved to {}'.format(os.path.normpath(report_file_path)))

    if CONFIG_TIMESTAMP_FILE in config:
        write_timestamp(config[CONFIG_TIMESTAMP_FILE], time.time())


if __name__ == '__main__':
    main(sys.argv[1:])
