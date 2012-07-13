from __future__ import absolute_import
from __future__ import division


from .processor import BulkProcessor
# @@@ Switch all Urls to SSL


class Conveyor(object):

    def __init__(self, *args, **kwargs):
        super(Conveyor, self).__init__(*args, **kwargs)

        self.config = {
            "index": "http://pypi.python.org/pypi"
        }

        self.previous_time = None

        # @@@ Initialize values from a data store

    def run(self):
        processor = self.get_processor_class()(index=self.config["index"])
        processor.process()

    def get_processor_class(self):
        if self.previous_time is None:
            # This is the first time we've ran so we need to do a bulk import
            return BulkProcessor
        else:
            # @@@ Normal Processor
            pass
