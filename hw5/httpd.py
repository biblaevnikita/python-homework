# coding=utf-8
import abc
import argparse
import contextlib
import datetime
import logging
import mimetypes
import multiprocessing
import os
import socket
import sys
import urllib
from StringIO import StringIO

import asyncore_epoll as asyncore

DOCUMENTS_ROOT = None
SERVER_NAME = 'DunnoServer'
SUPPORTED_HTTP_VERSIONS = {'HTTP/1.1', 'HTTP/1.0'}

OK = 200
BAD_REQUEST = 400
NOT_FOUND = 404
FORBIDDEN = 403
METHOD_NOT_SUPPORTED = 405
INTERNAL_ERROR = 500
HTTP_VERSION_NOT_SUPPORTED = 505

RESPONSE_CODES = {OK: 'OK',
                  BAD_REQUEST: 'Bad Request',
                  NOT_FOUND: 'Not Found',
                  METHOD_NOT_SUPPORTED: 'Method Not Allowed',
                  INTERNAL_ERROR: 'Internal Error',
                  HTTP_VERSION_NOT_SUPPORTED: 'HTTP Version Not Supported'}

INDEX_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'index.html')


class FileContent(object):
    def __init__(self, f_path):
        self._f_path = f_path
        self.length = os.path.getsize(f_path)
        self.type, self.encoding = mimetypes.guess_type(f_path)

    @contextlib.contextmanager
    def stream(self):
        stream = open(self._f_path, 'rb')
        yield stream
        stream.close()


class Response(object):
    def __init__(self, status_code, http_ver, status_message=None):
        self._http_ver = http_ver
        self._status_code = status_code
        self._status_message = status_message or RESPONSE_CODES.get(status_code)
        self._headers = {}

        self._content = None

    def status(self, status_code, status_message):
        self._status_code = status_code
        self._status_message = status_message

    def add_header(self, name, value):
        self._headers[name] = value

    def set_content(self, content):
        self._content = content
        self.set_content_meta(content)

    def set_content_meta(self, content):
        if content.type:
            content_type = content.type
            if content.encoding:
                content_type += '; charset={}'.format(content.encoding)
            self.add_header('Content-Type', content_type)

        if content.length:
            self.add_header('Content-Length', content.length)

    def build_head(self):
        if not self._status_code:
            raise Exception('Status code required')

        self.add_header('Server', SERVER_NAME)
        self.add_header('Date', self._get_date())

        head = ''
        if self._status_message:
            head += '{} {} {}\r\n'.format(self._http_ver, self._status_code, self._status_message)
        else:
            head += '{} {}\r\n'.format(self._http_ver, self._status_code)

        head += ''.join('{}: {}\r\n'.format(k, v) for k, v in self._headers.iteritems())
        head += '\r\n'

        return head

    @property
    def content(self):
        return self._content

    @staticmethod
    def _get_date():
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
        self.http_version = None

    def handle_read(self):
        request = self._parse_request()
        if not request:
            return
        method, uri, http_version = request
        logging.info('{} {} {}'.format(http_version, method, uri))
        self.uri = self._clean_uri(uri)
        self.method = method
        self.http_version = http_version

        if http_version not in SUPPORTED_HTTP_VERSIONS:
            self.send_response(self.make_response(HTTP_VERSION_NOT_SUPPORTED))
            return

        try:
            response = self.handle_request()
        except Exception:
            logging.exception('Handle request exception')
            self.send_response(self.make_response(INTERNAL_ERROR))
        else:
            self.send_response(response)

    def handle_request(self):
        handler_method = getattr(self, self.method.lower(), None)
        if not handler_method:
            return self.make_response(METHOD_NOT_SUPPORTED)

        return handler_method()

    def get(self):
        code, content = self._get_content()
        return self.make_response(code, content=content)

    def head(self):
        code, content = self._get_content()
        r = self.make_response(code)
        if content:
            r.set_content_meta(content)

        return r

    def _get_content(self):
        path = os.path.join(DOCUMENTS_ROOT, self.uri)
        if os.path.isfile(path):
            return OK, FileContent(path)

        if os.path.isdir(path):
            dir_index_path = os.path.join(path, 'index.html')
            if os.path.isfile(dir_index_path):
                return OK, FileContent(dir_index_path)
            else:
                return FORBIDDEN, None  # 403.14 - Directory listing denied.

        return NOT_FOUND, None

    def _parse_request(self):
        data = self.recv(8 * 1024)
        if not data:
            return None
        
        request_lines = data.splitlines(False)
        status_line = request_lines[0]
        return status_line.split(' ')

    def send_response(self, response):
        response.add_header('Connection', 'close')
        self.send(response.build_head())
        if response.content:
            with response.content.stream() as stream:
                while True:
                    data = stream.read(512)
                    if not data:
                        break
                    self.send(data)
        self.close()

    def make_response(self, status_code, status_message=None, content=None, headers=None):
        r = Response(status_code, self.http_version, status_message)
        if content:
            r.set_content(content)
        if headers:
            for name, value in headers.iteritems():
                r.add_header(name, value)

        return r

    def _clean_uri(self, uri):
        uri = urllib.unquote(uri)
        uri = os.path.normpath(uri)
        uri = uri.split('?')[0].split('#')[0]
        return uri.lstrip('/')


class HttpServer(asyncore.dispatcher):
    documents_root = None

    def __init__(self, host, port, handler_class):
        asyncore.dispatcher.__init__(self)
        self.host = host
        self.port = port
        self.handler_class = handler_class
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
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
            asyncore.loop(5, poller=asyncore.epoll_poller)
        except Exception as e:
            logging.exception('Server exception')
        finally:
            self.close()


def parse_args():
    parser = argparse.ArgumentParser('Simple async http-server')
    parser.add_argument('-w', dest='workers', help='Workers count', type=int, required=True)
    parser.add_argument('-r', dest='documents_root', help='Documents root path', required=True)
    parser.add_argument('-a', dest='address', help='IP address to listen on', default='0.0.0.0')
    parser.add_argument('-p', dest='port', help='Port', type=int, default=8080)
    parser.add_argument('-l', dest='log_file', help='Log file path')
    return parser.parse_args()


def main(args):
    pool = multiprocessing.Pool(args.workers)
    for i in range(args.workers):
        pool.apply_async(start_server, args=(args,))
    pool.close()
    pool.join()


def start_server(args):
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
