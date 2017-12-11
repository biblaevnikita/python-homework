#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import json
import datetime
import logging
import hashlib
import uuid
import scoring
import abc
from optparse import OptionParser
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

SALT = "Otus"
ADMIN_LOGIN = "admin"
ADMIN_SALT = "42"
OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
INVALID_REQUEST = 422
INTERNAL_ERROR = 500
ERRORS = {
    BAD_REQUEST: "Bad Request",
    FORBIDDEN: "Forbidden",
    NOT_FOUND: "Not Found",
    INVALID_REQUEST: "Invalid Request",
    INTERNAL_ERROR: "Internal Server Error",
}
UNKNOWN = 0
MALE = 1
FEMALE = 2
GENDERS = {
    UNKNOWN: "unknown",
    MALE: "male",
    FEMALE: "female",
}


class InvalidRequestError(Exception):
    pass


class InvalidFieldError(Exception):
    pass


class RequestField(object):
    __metaclass__ = abc.ABCMeta

    _name_prefix = '_$'

    def __init__(self, required=False, nullable=False):
        self.name = None
        self.required = required
        self.nullable = nullable

    def __get__(self, instance, owner):
        if not instance:
            return self
        name = self._name_prefix + self.name
        return getattr(instance, name, None)

    def __set__(self, instance, value):
        if not instance:
            raise ValueError('can not set a value')
        name = self._name_prefix + self.name
        self.check_value(value)
        setattr(instance, name, value)

    def check_value(self, value):
        if not value:
            if not self.nullable:
                raise InvalidFieldError('Value cannot be empty')
            else:
                return  # no need to validate null value

        self.validate(value)

    @abc.abstractmethod
    def validate(self, value):
        raise NotImplementedError


class CharField(RequestField):
    def validate(self, value):
        if not isinstance(value, basestring):
            raise InvalidFieldError('String value required')


class ArgumentsField(RequestField):
    def validate(self, value):
        if not isinstance(value, dict):
            raise InvalidFieldError('Dict value required')


class EmailField(CharField):
    def validate(self, value):
        super(EmailField, self).validate(value)
        if '@' not in value:
            raise InvalidFieldError('Not a valid e-mail')


class PhoneField(RequestField):
    error_message = 'Required a 7xxxxxxxxxx formatted string or an integer value'

    def validate(self, value):
        if not isinstance(value, (int, basestring)):
            raise InvalidFieldError(self.error_message)

        phone_number = str(value)
        if not phone_number.startswith('7'):
            raise InvalidFieldError(self.error_message)
        if len(phone_number) != 11:
            raise InvalidFieldError(self.error_message)


class DateField(CharField):
    def validate(self, value):
        super(DateField, self).validate(value)
        try:
            datetime.datetime.strptime(value, '%d.%m.%Y')
        except ValueError:
            raise InvalidFieldError('Required DD.MM.YYYY date string')


class BirthDayField(DateField):
    def validate(self, value):
        super(BirthDayField, self).validate(value)
        date = datetime.datetime.strptime(value, '%d.%m.%Y')
        now = datetime.datetime.now()
        if now.year - date.year > 70:
            raise InvalidFieldError("TOO OLD!!!")


class GenderField(RequestField):
    def validate(self, value):
        if not isinstance(value, int) or value not in GENDERS:
            raise InvalidFieldError('Required an integer value in range [0,2]')


class ClientIDsField(RequestField):
    error_message = 'Required a list of an integers'

    def validate(self, value):
        if not isinstance(value, list):
            raise InvalidFieldError(self.error_message)
        if any((not isinstance(item, int) for item in value)):
            raise InvalidFieldError(self.error_message)


class RequestMeta(type):
    def __init__(cls, name, bases, namespace):
        super(RequestMeta, cls).__init__(name, bases, namespace)
        fields = {}

        # inheritance support
        for base in bases:
            if not isinstance(base, RequestMeta) or not hasattr(base, '_fields'):
                continue
            fields.update(base._fields)

        # searching for a fields in a class namespace
        for key, value in namespace.iteritems():
            if not isinstance(value, RequestField):
                continue
            value.name = key
            fields[key] = value

        setattr(cls, '_fields', fields)


class RequestObject(object):
    __metaclass__ = RequestMeta

    def __init__(self, **kwargs):
        kwargs = kwargs or {}
        for name, field in self._fields.iteritems():
            value = kwargs.get(name, None)
            try:
                setattr(self, name, value)
            except InvalidFieldError as field_error:
                raise InvalidRequestError('Bad value for field "{}". {}'.format(name, field_error.message))

        self.validate()

    def validate(self):
        for name, field in self._fields.iteritems():
            value = getattr(self, name)
            if value is None and field.required:
                raise InvalidRequestError('Field "{}" required')


class ClientsInterestsRequest(RequestObject):
    client_ids = ClientIDsField(required=True)
    date = DateField(required=False, nullable=True)


class OnlineScoreRequest(RequestObject):
    first_name = CharField(required=False, nullable=True)
    last_name = CharField(required=False, nullable=True)
    email = EmailField(required=False, nullable=True)
    phone = PhoneField(required=False, nullable=True)
    birthday = BirthDayField(required=False, nullable=True)
    gender = GenderField(required=False, nullable=True)

    @property
    def non_empty_fields(self):
        return [k for k in self._fields.iterkeys() if getattr(self, k)]

    def validate(self):
        super(OnlineScoreRequest, self).validate()
        required_pairs = [
            ("first_name", "last_name"),
            ("email", "phone"),
            ("birthday", "gender")
        ]
        non_empty_fields = self.non_empty_fields
        if not any(f in non_empty_fields and s in non_empty_fields for f, s in required_pairs):
            raise InvalidRequestError('Required at least one pair: '
                                      '("first_name", "last_name"), '
                                      '("email", "phone"), '
                                      '("birthday","gender")')


class MethodRequest(RequestObject):
    account = CharField(required=False, nullable=True)
    login = CharField(required=True, nullable=True)
    token = CharField(required=True, nullable=True)
    arguments = ArgumentsField(required=True, nullable=True)
    method = CharField(required=True, nullable=False)

    @property
    def is_admin(self):
        return self.login == ADMIN_LOGIN


def check_auth(request):
    if request.login == ADMIN_LOGIN:
        digest = hashlib.sha512(datetime.datetime.now().strftime("%Y%m%d%H") + ADMIN_SALT).hexdigest()
    else:
        digest = hashlib.sha512(request.account + request.login + SALT).hexdigest()
    if digest == request.token:
        return True
    return False


def method_handler(request, ctx, store):
    method_routes = {
        'online_score': online_score_handler,
        'clients_interests': clients_interests_handler
    }
    body = request.get('body', None)
    try:
        request = MethodRequest(**body)
    except InvalidRequestError as e:
        return e.message, INVALID_REQUEST

    if not check_auth(request):
        return None, FORBIDDEN

    if request.method not in method_routes:
        return 'Unable to find method "{}"'.format(request.method), NOT_FOUND

    return method_routes[request.method](request, ctx, store)


def online_score_handler(request, ctx, store):
    try:
        score_request = OnlineScoreRequest(**request.arguments)
    except InvalidRequestError as e:
        return e.message, INVALID_REQUEST

    ctx['has'] = score_request.non_empty_fields

    if request.is_admin:
        return {'score': 42}, OK

    response = {'score': scoring.get_score(store=store,
                                           phone=score_request.phone,
                                           email=score_request.email,
                                           birthday=score_request.birthday,
                                           gender=score_request.gender,
                                           first_name=score_request.first_name,
                                           last_name=score_request.last_name)}
    return response, OK


def clients_interests_handler(request, ctx, store):
    try:
        interests_requests = ClientsInterestsRequest(**request.arguments)
    except InvalidRequestError as e:
        return e.message, INVALID_REQUEST

    client_ids = set(interests_requests.client_ids)

    ctx['nclients'] = len(client_ids)

    response = {}
    for cid in client_ids:
        response[str(cid)] = scoring.get_interests(store, cid)

    return response, OK


class MainHTTPHandler(BaseHTTPRequestHandler):
    router = {
        "method": method_handler
    }
    store = None

    def get_request_id(self, headers):
        return headers.get('HTTP_X_REQUEST_ID', uuid.uuid4().hex)

    def do_POST(self):
        response, code = {}, OK
        context = {"request_id": self.get_request_id(self.headers)}
        request = None
        try:
            data_string = self.rfile.read(int(self.headers['Content-Length']))
            request = json.loads(data_string)
        except:
            code = BAD_REQUEST

        if request:
            path = self.path.strip("/")
            logging.info("%s: %s %s" % (self.path, data_string, context["request_id"]))
            if path in self.router:
                try:
                    response, code = self.router[path]({"body": request, "headers": self.headers}, context, self.store)
                except Exception, e:
                    logging.exception("Unexpected error: %s" % e)
                    code = INTERNAL_ERROR
            else:
                code = NOT_FOUND

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if code not in ERRORS:
            r = {"response": response, "code": code}
        else:
            r = {"error": response or ERRORS.get(code, "Unknown Error"), "code": code}
        context.update(r)
        logging.info(context)
        self.wfile.write(json.dumps(r))
        return


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    server = HTTPServer(("localhost", opts.port), MainHTTPHandler)
    logging.info("Starting server at %s" % opts.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
