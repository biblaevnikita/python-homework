import unittest
import api
import functools
import hashlib
import datetime


def cases(cases):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args):
            for c in cases:
                new_args = args + (c if isinstance(c, tuple) else (c,))
                f(*new_args)

        return wrapper

    return decorator


class TestRequests(unittest.TestCase):
    def setUp(self):
        self.context = {}
        self.headers = {}
        self.store = None

    def get_response(self, request):
        return api.method_handler({"body": request, "headers": self.headers}, self.context, self.store)

    def set_auth(self, request):
        if request.get("login") == api.ADMIN_LOGIN:
            request["token"] = hashlib.sha512(datetime.datetime.now().strftime("%Y%m%d%H") + api.ADMIN_SALT).hexdigest()
        else:
            msg = request.get("account", "") + request.get("login", "") + api.SALT
            request["token"] = hashlib.sha512(msg).hexdigest()

    def test_empty_request(self):
        _, code = self.get_response({})
        self.assertEqual(api.INVALID_REQUEST, code)

    @cases([{'account': 'acc', 'login': 'login', 'method': 'meth', 'token': '', 'arguments': {}},
            {'account': 'acc', 'login': 'login', 'method': 'meth', 'token': 'invalid_token', 'arguments': {}},
            {'account': 'acc', 'login': 'admin', 'method': 'meth', 'token': '', 'arguments': {}},
            {'account': 'acc', 'login': 'admin', 'method': 'meth', 'token': 'invalid_token', 'arguments': {}}])
    def test_forbidden(self, request):
        _, code = self.get_response(request)
        self.assertEqual(api.FORBIDDEN, code)

    @cases([{'login': 'login', 'method': 'online_score', 'arguments': {}},
            {'account': 'acc', 'method': 'online_score', 'arguments': {}},
            {'account': 'acc', 'login': 'login', 'arguments': {}},
            {'account': 'acc', 'login': 'login', 'method': 'online_score'}])
    def test_invalid_requests(self, request):
        self.set_auth(request)
        _, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)

    def test_not_found(self):
        request = {'account': 'acc', 'login': 'login', 'method': 'invalid_method', 'arguments': {}}
        self.set_auth(request)
        _, code = self.get_response(request)
        self.assertEqual(api.NOT_FOUND, code)

    @cases([{},
            {'first_name': 'f_name'},
            {'last_name': 'l_name'},
            {'email': 'e@mail.ru'},
            {'phone': '79231231212'},
            {'birthday': '01.01.1999'},
            {'gender': 1},
            {'last_name': 'l_name', 'phone': '79231231212'},
            {'first_name': 'f_name', 'email': 'e@mail.ru', 'birthday': '01.01.1999'}])
    def test_online_score_invalid_request(self, args):
        request = {'account': 'acc', 'login': 'login', 'method': 'online_score', 'arguments': args}
        self.set_auth(request)
        _, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)

    @cases([{'first_name': 'f_name', 'last_name': 'l_name'},
            {'email': 'e@mail.ru', 'phone': '79231231212'},
            {'birthday': '01.01.1999', 'gender': 1}])
    def test_online_score_ok_request(self, args):
        request = {'account': 'acc', 'login': 'login', 'method': 'online_score', 'arguments': args}
        self.set_auth(request)
        _, code = self.get_response(request)
        self.assertEqual(api.OK, code)

    def test_online_score_admin_invalid_request(self):
        args = {'birthday': '01.01.1999', 'gender': 1}
        request = {'account': 'acc', 'login': 'admin', 'method': 'online_score', 'arguments': args}
        self.set_auth(request)
        response_json, code = self.get_response(request)
        self.assertEqual(response_json['score'], 42)

    @cases([{'client_ids': [1, 2, 3]},
            {'client_ids': [1, 2, 3], 'date': '01.01.1991'}])
    def test_client_interests_ok_request(self, args):
        request = {'account': 'acc', 'login': 'login', 'method': 'clients_interests', 'arguments': args}
        self.set_auth(request)
        _, code = self.get_response(request)
        self.assertEqual(api.OK, code)

    @cases([{},
            {'date': '01.01.1991'}])
    def test_client_interests_invalid_request(self, args):
        request = {'account': 'acc', 'login': 'login', 'method': 'clients_interests', 'arguments': args}
        self.set_auth(request)
        _, code = self.get_response(request)
        self.assertEqual(api.INVALID_REQUEST, code)


class TestFields(unittest.TestCase):
    class Dummy(object):
        pass

    def assertValid(self, field, value):
        instance = self.Dummy()
        field.set_value(instance, value)
        exception = None
        try:
            field.validate(instance)
        except Exception as e:
            exception = e

        self.assertIsNone(exception)

    def assertInvalid(self, field, value):
        instance = self.Dummy()
        field.set_value(instance, value)
        self.assertRaises(api.InvalidFieldError, field.validate, instance)

    def test_valid_char_field(self):
        value = 'char_value'
        field = api.CharField()
        self.assertValid(field, value)

    @cases([500, 2.5, object, True, dict(), None])
    def test_invalid_char_field(self, value):
        field = api.CharField()
        self.assertInvalid(field, value)

    def test_valid_arguments_field(self):
        value = {"some": 'dict'}
        field = api.ArgumentsField()
        self.assertValid(field, value)

    @cases([500, 2.5, object, True, 'string', None])
    def test_invalid_arguments_field(self, value):
        field = api.ArgumentsField()
        self.assertInvalid(field, value)

    @cases(['@mail.lol', 'simple@mail.ru', 'one_more_strange_email@'])
    def test_valid_email_field(self, value):
        field = api.EmailField()
        self.assertValid(field, value)

    @cases([500, 2.5, object, True, 'no_at_string', None])
    def test_invalid_email_field(self, value):
        field = api.EmailField()
        self.assertInvalid(field, value)

    def test_valid_phone_field(self):
        value = '79998887766'
        field = api.PhoneField()
        self.assertValid(field, value)

    @cases(['78887776655', '799988877665', '7999888776', 'string', 500, 2.5, object, True, None])
    def test_invalid_email_field(self, value):
        field = api.PhoneField()
        self.assertInvalid(field, value)

    def test_valid_date_field(self):
        value = '17.05.2004'
        field = api.DateField()
        self.assertValid(field, value)

    @cases(['50.17.2000', 'string', 500, 2.5, object, True, None])
    def test_invalid_date_field(self, value):
        field = api.DateField()
        self.assertInvalid(field, value)

    def test_valid_birthday_field(self):
        value = '17.05.2004'
        field = api.BirthDayField()
        self.assertValid(field, value)

    @cases(['50.17.2000', '01.01.1947', 'string', 500, 2.5, object, True, None])
    def test_invalid_birthday_field(self, value):
        field = api.BirthDayField()
        self.assertInvalid(field, value)

    @cases([0, 1, 2])
    def test_valid_gender_field(self, value):
        field = api.GenderField()
        self.assertValid(field, value)

    @cases([3, -1, 10, 'string', 2.5, object, True, None])
    def test_invalid_gender_field(self, value):
        field = api.GenderField()
        self.assertInvalid(field, value)

    @cases([[0], [0, 1], [0, 1, 2]])
    def test_valid_client_ids_field(self, value):
        field = api.ClientIDsField()
        self.assertValid(field, value)

    @cases([[], ['1', '2'], [1, '2'], 'string', 2.5, object, True, None])
    def test_invalid_client_ids_field(self, value):
        field = api.ClientIDsField()
        self.assertInvalid(field, value)


if __name__ == "__main__":
    unittest.main()
