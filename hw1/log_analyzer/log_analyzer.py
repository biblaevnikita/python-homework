import sys
import os
import logging
import json
import re
import time
import gzip
import argparse
from datetime import datetime
from collections import namedtuple
from string import Template

####################################
# Constants
####################################
DEFAULT_CONFIG = {
    'MAX_REPORT_SIZE': 1000,
    'REPORTS_DIR': "./reports",
    'LOGS_DIR': "./log"
}

LOG_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOG_DATE_TIME_FORMAT = '%Y.%m.%d %H:%M:%S'

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

DateNamedFileInfo = namedtuple('DateNamedFileInfo', ['file_path', 'file_date'])

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format=LOG_FORMAT, datefmt=LOG_DATE_TIME_FORMAT)

####################################
# Config
####################################


def get_config(conf_path=None):
    if not conf_path:
        return DEFAULT_CONFIG

    with open(conf_path, 'r') as conf:
        config = json.load(conf)

    for key, value in DEFAULT_CONFIG.iteritems():
        if key not in config:
            config[key] = value

    return config


def validate_config(config):
    report_size = config.get('MAX_REPORT_SIZE')
    if not report_size or not isinstance(report_size, int) or report_size < 1:
        raise ValueError('MAX_REPORT_SIZE must be a positive integer')

    logs_dir = config.get('LOGS_DIR')
    if not logs_dir or not os.path.isdir(logs_dir) or not isinstance(logs_dir, basestring):
        raise ValueError('LOGS_DIR must be an existing directory')

    report_dir = config.get('REPORTS_DIR')
    if not report_dir or not isinstance(logs_dir, basestring):
        raise ValueError('REPORTS_DIR must be a path string')

    if 'MONITORING_LOG_FILE' in config and not config.get('MONITORING_LOG_FILE'):
        raise ValueError('MONITORING_LOG_FILE must be a file path')

    if 'TIMESTAMP_FILE' in config and not config.get('TIMESTAMP_FILE'):
        raise ValueError('TIMESTAMP_FILE must be a file path')


####################################
# Analyzing
####################################


def create_report(records, max_records):
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
                'requests_count': 0.,
                'response_time_sum': 0.,
                'max_response_time': response_time,
                'response_time_avg': 0.,
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
    open_fn = gzip.open if is_gzip_file(log_path) else open

    with open_fn(log_path, 'r') as log_file:
        for line in log_file:
            record = parse_log_record(line)
            if not record:
                continue

            yield record


def parse_log_record(log_line):
    match = LOG_RECORD_RE.match(log_line)
    if not match:
        logging.error('Unable to parse line: "{}"'.format(log_line.rstrip()))
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


def parse_args():
    parser = argparse.ArgumentParser(prog='Log analyzer')
    parser.add_argument('--config', help='Config file path')
    return parser.parse_args()


def get_latest_log_with_date(logs_dir):
    return get_latest_date_named_file_info(logs_dir, LOG_FILENAME_DATE_FORMAT, LOG_FILENAME_RE, LOG_RE_DATE_GROUP_NAME)


def get_latest_date_named_file_info(files_dir, date_format, date_re, date_re_group_name):
    if not os.path.isdir(files_dir):
        return None

    latest_file_info = None
    for filename in os.listdir(files_dir):
        match = date_re.match(filename)
        if not match:
            continue

        date_string = match.groupdict()[date_re_group_name]
        file_date = datetime.strptime(date_string, date_format)

        if not latest_file_info or file_date > latest_file_info.file_date:
            latest_file_info = DateNamedFileInfo(file_path=os.path.join(files_dir, filename),
                                                 file_date=file_date)

    return latest_file_info


def is_gzip_file(file_path):
    return file_path.split('.')[-1] == 'gz'


def reset_logging():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)


def render_template(template_path, to, data):
    if data is None:
        data = []

    target_dir = os.path.dirname(to)
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    with open(template_path, 'r') as template:
        template = Template(template.read())

    rendered = template.safe_substitute(table_json=json.dumps(data))

    with open(to, 'w') as render_target:
        render_target.write(rendered)


def write_timestamp(file_path, timestamp):
    timestamp = int(timestamp)
    ts_dir = os.path.dirname(file_path)
    if not os.path.isdir(ts_dir):
        os.makedirs(ts_dir)

    with open(file_path, 'w') as ts_file:
        ts_file.write(str(timestamp))

    a_time = os.stat(file_path).st_atime
    os.utime(file_path, (a_time, timestamp))


def main():
    args = parse_args()
    config = get_config(args.config)
    validate_config(config)

    # setup logger
    monitoring_log_file = config.get('MONITORING_LOG_FILE')
    if monitoring_log_file:
        monitoring_log_dir = os.path.dirname(monitoring_log_file)
        if not os.path.isdir(monitoring_log_dir):
            os.makedirs(monitoring_log_dir)
        reset_logging()
        logging.basicConfig(filename=monitoring_log_file, level=logging.INFO,
                            format=LOG_FORMAT, datefmt=LOG_DATE_TIME_FORMAT)

    # resolving actual log
    latest_log_info = get_latest_log_with_date(config['LOGS_DIR'])
    if not latest_log_info:
        logging.info('Ooops. No log files yet')
        return

    report_date_string = latest_log_info.file_date.strftime(REPORT_FILENAME_DATE_FORMAT)
    report_filename = REPORT_NAME_PATTERN.format(report_date_string)
    report_file_path = os.path.join(config['REPORTS_DIR'], report_filename)

    if os.path.isfile(report_file_path):
        logging.info("Looks like everything is up-to-date")
        return

    # report creation
    logging.info('Collecting data from "{}"'.format(os.path.normpath(latest_log_info.file_path)))
    log_records = get_log_records(latest_log_info.file_path)
    report_data = create_report(log_records, config['MAX_REPORT_SIZE'])

    render_template(REPORT_TEMPLATE_PATH, report_file_path, report_data)

    logging.info('Report saved to {}'.format(os.path.normpath(report_file_path)))

    if 'TIMESTAMP_FILE' in config:
        write_timestamp(config['TIMESTAMP_FILE'], time.time())


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.exception('Unhandled exception:')
