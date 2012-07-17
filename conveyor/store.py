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
