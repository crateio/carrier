class BaseStore(object):

    def set(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError


class InMemoryStore(BaseStore):

    def __init__(self, *args, **kwargs):
        super(InMemoryStore, self).__init__(*args, **kwargs)

        self._data = {}

    def set(self, key, value):
        self._data[key] = value

    def get(self, key):
        return self._data[key]


class RedisStore(BaseStore):
    def __init__(self, url=None, prefix=None, *args, **kwargs):
        super(RedisStore, self).__init__(*args, **kwargs)
        import redis

        self.redis = redis.from_url(url)
        self.prefix = prefix

    def set(self, key, value):
        if self.prefix is not None:
            key = self.prefix + key

        self.redis.set(key, value)

    def get(self, key):
        if self.prefix is not None:
            key = self.prefix + key

        return self.redis.get(key)
