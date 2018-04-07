import memcache
import time


class Store(object):
    _timeout = 30
    _retries = 5
    _retries_delay = 5

    def __init__(self, address):
        self._client = memcache.Client([address], socket_timeout=self._timeout)

    def get(self, key):
        exception = None
        for i in range(self._retries):
            try:
                return self._client.get(key)
            except Exception as e:
                exception = e

            time.sleep(self._retries_delay)

        if exception:
            raise exception

    def get_cache(self, key):
        try:
            return self._client.get(key)
        except Exception:
            pass

        return None

    def set_cache(self, key, value, expire=0):
        try:
            self._client.set(key, value, time=expire)
        except Exception:
            pass
