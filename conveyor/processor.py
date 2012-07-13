from __future__ import absolute_import
from __future__ import division


from xmlrpc2 import client as xmlrpc2


class BaseProcessor(object):

    def __init__(self, index, *args, **kwargs):
        super(BaseProcessor, self).__init__(*args, **kwargs)

        self.index = index
        self.client = xmlrpc2.Client(self.index)

    def process(self):
        raise NotImplementedError


class BulkProcessor(BaseProcessor):

    def process(self):
        pass
