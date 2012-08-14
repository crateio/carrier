import datetime
import logging
import time

import slumber

from requests.exceptions import ConnectionError, HttpServerError

from conveyor.core import Conveyor
from conveyor.processor import Processor

logger = logging.getLogger(__name__)


# We ignore the last component as we cannot properly handle it
def get_jobs(last=0):
    try:
        current = time.mktime(datetime.datetime.utcnow().timetuple())

        app = Conveyor()

        logger.info("Current time is '%s'", current)

        warehouse = slumber.API(
                            app.config["conveyor"]["warehouse"]["url"],
                            auth=(
                                app.config["conveyor"]["warehouse"]["auth"]["username"],
                                app.config["conveyor"]["warehouse"]["auth"]["password"],
                            )
                        )
        processor = Processor(
                        index=app.config["conveyor"]["index"],
                        warehouse=warehouse,
                        store=app.redis,
                        store_prefix=app.config.get("redis", {}).get("prefix", None)
                    )

        names = set(processor.client.list_packages())

        for package in names:
            yield package
    except Exception as e:
        logger.exception(str(e))
        raise


def handle_job(name):
    try:
        tried = 0

        while True:
            try:
                tried += 1

                app = Conveyor()

                warehouse = slumber.API(
                                    app.config["conveyor"]["warehouse"]["url"],
                                    auth=(
                                        app.config["conveyor"]["warehouse"]["auth"]["username"],
                                        app.config["conveyor"]["warehouse"]["auth"]["password"],
                                    )
                                )
                processor = Processor(
                                index=app.config["conveyor"]["index"],
                                warehouse=warehouse,
                                store=app.redis,
                                store_prefix=app.config.get("redis", {}).get("prefix", None)
                            )

                # Process the Name
                processor.get_or_create_project(name)

                for release in processor.get_releases(name):
                    processor.sync_release(release)

                break
            except (ConnectionError, HttpServerError):
                # Attempt to process again if we have a connection error
                if tried >= 10:  # Try a max of 10 times
                    raise
                else:
                    # Wait a moment
                    time.sleep(10)
    except Exception as e:
        logger.exception(str(e))
        raise
