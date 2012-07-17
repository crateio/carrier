from __future__ import absolute_import
from __future__ import division


import urlparse

from conveyor.processor import BulkProcessor
# @@@ Switch all Urls to SSL


class Conveyor(object):

    def __init__(self, index_url, warehouse_url, store=None, *args, **kwargs):
        super(Conveyor, self).__init__(*args, **kwargs)

        warehouse_url = urlparse.urlparse(warehouse_url)
        warehouse = (
            [urlparse.urlunparse([warehouse_url.scheme, warehouse_url.hostname, warehouse_url.path, warehouse_url.params, warehouse_url.query, warehouse_url.fragment])],
            {
                "auth": (warehouse_url.username, warehouse_url.password),
            },
        )

        self.config = {
            "index": index_url,
            "warehouse": warehouse,
            "store": store,
        }

    def run(self):
        processor = self.get_processor_class()(
                        index=self.config["index"],
                        warehouse=self.config["warehouse"],
                        store=self.config["store"],
                    )
        processor.process()

    def get_processor_class(self):
        if self.previous_time is None:
            # This is the first time we've ran so we need to do a bulk import
            return BulkProcessor
        else:
            # @@@ Normal Processor
            pass

    @property
    def previous_time(self):
        return None
