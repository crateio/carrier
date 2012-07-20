from __future__ import absolute_import
from __future__ import division


import logging
import logging.config
import time

import redis
import slumber
import yaml

from apscheduler.scheduler import Scheduler

from conveyor.processor import BulkProcessor
# @@@ Switch all Urls to SSL


logger = logging.getLogger(__name__)


class Conveyor(object):

    def __init__(self, config_file=None, *args, **kwargs):
        super(Conveyor, self).__init__(*args, **kwargs)

        if config_file is None:
            config_file = "config.yml"

        with open(config_file) as f:
            self.config = yaml.safe_load(f.read())

        logging.config.dictConfig(self.config["logging"])

        self.scheduler = Scheduler()

    def run(self):
        self.scheduler.add_interval_job(self.process, **self.config["conveyor"]["schedule"])
        self.scheduler.start()

        while True:
            time.sleep(5)

    def process(self):
        warehouse = slumber.API(
                        self.config["conveyor"]["warehouse"]["url"],
                        auth=(
                            self.config["conveyor"]["warehouse"]["auth"]["username"],
                            self.config["conveyor"]["warehouse"]["auth"]["password"],
                        )
                    )

        processor_class = self.get_processor_class()
        processor = processor_class(index=self.config["conveyor"]["index"], warehouse=warehouse)

        processor.process()

    def get_processor_class(self):
        if self.previous_time is None:
            # This is the first time we've ran so we need to do a bulk import
            return BulkProcessor
        else:
            # @@@ Normal Processor
            raise Exception("Use Normal Processor")

    @property
    def previous_time(self):
        return None
