import os
import logging
import json
import re
import time
import gzip
import argparse
import io
from datetime import datetime
from collections import namedtuple
from string import Template

####################################
# Constants
####################################
DEFAULT_CONFIG_PATH = './default.conf'
REPORT_TEMPLATE_PATH = './template.html'

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

####################################
# Config
####################################


def load_conf(conf_path):
    with open(conf_path, 'rb') as conf_file:
        conf = json.load(conf_file, encoding='utf8')
    return conf


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
    sorted_values = sorted_values[:max_records]

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


def get_log_records(log_path, errors_limit=None):
    open_fn = gzip.open if is_gzip_file(log_path) else io.open
    errors = 0
    records = 0
    with open_fn(log_path, mode='rb') as log_file:
        for line in log_file:
            records += 1
            line = line.decode('utf8')
            record = parse_log_record(line)
            if not record:
                errors += 1
                continue

            yield record

    if errors_limit is not None and records > 0 and errors / float(records) > errors_limit:
        raise Exception('Errors limit exceeded')


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

def setup_logger(log_path):
    if log_path and not os.path.isdir(log_path):
        os.makedirs(log_path)
    logging.basicConfig(filename=log_path, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Config file path')
    return parser.parse_args()


def get_latest_log_info(files_dir):
    if not os.path.isdir(files_dir):
        return None

    latest_file_info = None
    for filename in os.listdir(files_dir):
        match = re.match(r'^nginx-access-ui\.log-(?P<date>\d{8})(\.gz)?$', filename)
        if not match:
            continue

        date_string = match.groupdict()['date']
        file_date = datetime.strptime(date_string, "%Y%m%d")

        if not latest_file_info or file_date > latest_file_info.file_date:
            latest_file_info = DateNamedFileInfo(file_path=os.path.join(files_dir, filename),
                                                 file_date=file_date)

    return latest_file_info


def is_gzip_file(file_path):
    return file_path.split('.')[-1] == 'gz'


def render_template(template_path, to, data):
    if data is None:
        data = []

    target_dir = os.path.dirname(to)
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    with open(template_path, 'rb') as template_file:
        template_string = template_file.read().decode('utf8')
        template = Template(template_string)

    rendered = template.safe_substitute(table_json=json.dumps(data))

    with open(to, 'wb') as render_target:
        rendered = rendered.encode('utf8')
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


def main(config):
    # resolving an actual log
    latest_log_info = get_latest_log_info(config['LOGS_DIR'])
    if not latest_log_info:
        logging.info('Ooops. No log files yet')
        return

    report_date_string = latest_log_info.file_date.strftime("%Y.%m.%d")
    report_filename = "report-{}.html".format(report_date_string)
    report_file_path = os.path.join(config['REPORTS_DIR'], report_filename)

    if os.path.isfile(report_file_path):
        write_timestamp(config['TIMESTAMP_FILE'], time.time())
        logging.info("Looks like everything is up-to-date")
        return

    # report creation
    logging.info('Collecting data from "{}"'.format(os.path.normpath(latest_log_info.file_path)))
    log_records = get_log_records(latest_log_info.file_path, config.get('ERRORS_LIMIT'))
    report_data = create_report(log_records, config['MAX_REPORT_SIZE'])

    render_template(REPORT_TEMPLATE_PATH, report_file_path, report_data)

    logging.info('Report saved to {}'.format(os.path.normpath(report_file_path)))

    write_timestamp(config['TIMESTAMP_FILE'], time.time())


if __name__ == '__main__':
    args = parse_args()

    config = load_conf(DEFAULT_CONFIG_PATH)
    if args.config:
        external_config = load_conf(args.config)
        config.update(external_config)

    setup_logger(config.get('MONITORING_LOG_FILE'))

    try:
        main(config)
    except Exception as e:
        logging.exception('Unhandled exception:')
