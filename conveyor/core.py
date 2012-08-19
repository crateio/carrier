from __future__ import absolute_import
from __future__ import division


import logging
import logging.config
import time

import redis
import slumber
import yaml

from apscheduler.scheduler import Scheduler

from conveyor.processor import Processor, get_key

# @@@ Switch all Urls to SSL
# @@@ Switch to better exception classes


logger = logging.getLogger(__name__)


class Conveyor(object):

    def __init__(self, config_file=None, *args, **kwargs):
        super(Conveyor, self).__init__(*args, **kwargs)

        if config_file is None:
            config_file = "config.yml"

        with open(config_file) as f:
            self.config = yaml.safe_load(f.read())

        logging.config.dictConfig(self.config["logging"])

        self.redis = redis.StrictRedis(**self.config.get("redis", {}).get("connection", {}))

    def run(self):
        self.scheduler = Scheduler()

        if self.config["conveyor"].get("schedule", {}).get("packages", {}):
            self.scheduler.add_interval_job(self.packages, **self.config["conveyor"]["schedule"]["packages"])

        if self.config["conveyor"].get("schedule", {}).get("downloads", {}):
            self.scheduler.add_interval_job(self.downloads, **self.config["conveyor"]["schedule"]["downloads"])

        self.scheduler.start()

        try:
            while True:
                time.sleep(999)
        except KeyboardInterrupt:
            logger.info("Shutting down Conveyor...")
            self.scheduler.shutdown(wait=False)

    def packages(self):
        if not self.redis.get(get_key(self.config.get("redis", {}).get("prefix", None), "pypi:since")):
            # This is the first time we've ran so we need to do a bulk import
            raise Exception(" Cannot process changes with no value for the last successful run.")

        warehouse = slumber.API(
                        self.config["conveyor"]["warehouse"]["url"],
                        auth=(
                            self.config["conveyor"]["warehouse"]["auth"]["username"],
                            self.config["conveyor"]["warehouse"]["auth"]["password"],
                        )
                    )

        processor = Processor(
                        index=self.config["conveyor"]["index"],
                        warehouse=warehouse,
                        store=self.redis,
                        store_prefix=self.config.get("redis", {}).get("prefix", None)
                    )

        processor.process()
