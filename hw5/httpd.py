import argparse
import logging
import mimetypes
import os
import socket
import sys
import urllib
import datetime

import asyncore_epoll as asyncore

DOCUMENTS_ROOT = None
SERVER_NAME = 'DunnoServer'
HTTP_VERSION = 'HTTP/1.1'

OK = 200
BAD_REQUEST = 400
NOT_FOUND = 404
METHOD_NOT_SUPPORTED = 405
INTERNAL_ERROR = 500
HTTP_VERSION_NOT_SUPPORTED = 505

RESPONSE_CODES = {OK: 'OK',
                  BAD_REQUEST: 'Bad Request',
                  NOT_FOUND: 'Not Found',
                  METHOD_NOT_SUPPORTED: 'Method Not Allowed',
                  INTERNAL_ERROR: 'Internal Error',
                  HTTP_VERSION_NOT_SUPPORTED: 'HTTP Version Not Supported'}


class Response(object):
    def __init__(self, status_code, status_message=None):
        self._status_code = status_code
        self._status_message = status_message or RESPONSE_CODES.get(status_code)
        self._headers = {}

        self._content_data = None
        self._content_type = None
        self._content_file = None
        self._content_encoding = None
        self._content_length = None

    def status(self, status_code, status_message):
        self._status_code = status_code
        self._status_message = status_message

    def add_header(self, name, value):
        self._headers[name] = value

    def content_file(self, file_path):
        self._content_type, self._content_encoding = mimetypes.guess_type(file_path)
        self._content_file = file_path
        self._content_length = os.stat(file_path).st_size

    def content(self, data, content_type=None, encoding=None):
        self._content_length = len(data)
        self._content_data = data
        self._content_type = content_type
        self._content_encoding = encoding

    def build(self):
        if not self._status_code:
            raise Exception('Status code required')

        self.add_header('Server', SERVER_NAME)
        self.add_header('Date', self._get_date())
        if self._content_type:
            content_type = self._content_type
            if self._content_encoding:
                content_type += '; charset={}'.format(self._content_encoding)
            self.add_header('Content-Type', content_type)

        if self._content_length:
            self.add_header('Content-Length', str(self._content_length))

        response_str = ''
        if self._status_message:
            response_str += '{} {} {}\r\n'.format(HTTP_VERSION, self._status_code, self._status_message)
        else:
            response_str += '{} {}\r\n'.format(HTTP_VERSION, self._status_code)

        response_str += ''.join('{}: {}\r\n'.format(k, v) for k, v in self._headers.iteritems())
        response_str += '\r\n'

        if self._content_file:
            with open(self._content_file, 'rb') as fp:
                response_str += fp.read()
        elif self._content_data:
            response_str += self._content_data

        return response_str

    def _get_date(self):
        dt = datetime.datetime.utcnow()
        weekday = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]
        month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
                 "Oct", "Nov", "Dec"][dt.month - 1]
        return "%s, %02d %s %04d %02d:%02d:%02d GMT" % (weekday, dt.day, month,
                                                        dt.year, dt.hour, dt.minute, dt.second)



class HttpRequestHandler(asyncore.dispatcher_with_send):
    def __init__(self, sock):
        asyncore.dispatcher_with_send.__init__(self, sock)
        self.method = None
        self.uri = None

    def handle_read(self):
        data = self.recv(8 * 1024)
        method, uri, http_version = self._parse_request(data)

        self.uri = self._clean_uri(uri)
        self.method = method

        print(self.uri)

        if http_version != HTTP_VERSION:
            self.send_status_code(HTTP_VERSION_NOT_SUPPORTED)
            return

        try:
            response = self.handle_request()
        except Exception:
            logging.exception('Exception occurs during handling request')
            self.send_status_code(INTERNAL_ERROR)
        else:
            self.send_response(response)

    def handle_request(self):
        handler_method = getattr(self, self.method.lower(), None)
        if not handler_method:
            return Response(METHOD_NOT_SUPPORTED)

        return handler_method()

    def get(self):
        target = self._get_target_file_path(self.uri)
        if target:
            r = Response(OK)
            r.content_file(target)
            return r
        elif os.path.basename(self.uri) == 'index.html':
            r = Response(OK)
            r.content(self._gen_index(self.uri), 'text/html', 'utf-8')
        else:
            r = Response(NOT_FOUND)

        return r

    def head(self):
        r = self.get()
        r._content_data = None
        r._content_file = None
        return r

    def _parse_request(self, request_string):
        request_lines = request_string.splitlines(False)
        status_line = request_lines[0]
        return status_line.split(' ')

    def send_response(self, response):
        response.add_header('Connection', 'close')
        self.send(response.build())
        self.close()

    def send_status_code(self, code, message=None):
        self.send_response(Response(code, message))

    def _clean_uri(self, uri):
        uri = urllib.unquote(uri).decode('utf-8')
        uri = uri.split('?')[0].split('#')[0]
        return uri.lstrip('/')

    def _get_target_file_path(self, uri):
        fs_path = os.path.join(DOCUMENTS_ROOT, uri)
        if os.path.isfile(fs_path):
            return fs_path

        if os.path.isdir(fs_path):
            index_path = os.path.join(uri, 'index.html')
            return self._get_target_file_path(index_path)

        return None

    def _gen_index(self, uri):
        return ''



class HttpServer(asyncore.dispatcher):
    documents_root = None

    def __init__(self, host, port, handler_class):
        asyncore.dispatcher.__init__(self)
        self.host = host
        self.port = port
        self.handler_class = handler_class
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            logging.info('Incoming connection from {}'.format(addr))
            self.handler_class(sock)

    def start(self):
        logging.info('Listening on {}:{}'.format(self.host, self.port))
        try:
            asyncore.loop(5, poller=asyncore.select_poller)
        except Exception as e:
            logging.exception('Server exception')
        finally:
            self.close()


def parse_args():
    parser = argparse.ArgumentParser('Simple async http-server')
    # parser.add_argument('-w', dest='workers', help='Workers count', type=int, required=True)
    parser.add_argument('-r', dest='documents_root', help='Documents root path', required=True)
    parser.add_argument('-a', dest='address', help='IP address to listen on', default='0.0.0.0')
    parser.add_argument('-p', dest='port', help='Port', type=int, default=80)
    parser.add_argument('-l', dest='log_file', help='Log file path')
    return parser.parse_args()


def main(args):
    s = HttpServer(args.address, args.port, handler_class=HttpRequestHandler)
    s.start()


if __name__ == '__main__':
    parsed_args = parse_args()
    logging.basicConfig(filename=parsed_args.log_file, stream=sys.stderr, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    DOCUMENTS_ROOT = parsed_args.documents_root
    try:
        main(parsed_args)
    except Exception as e:
        logging.exception('Exception occurred:')
