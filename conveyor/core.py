from __future__ import absolute_import

# @@@ Switch all Urls to SSL


class Conveyor(object):

    def __init__(self, *args, **kwargs):
        super(Conveyor, self).__init__(*args, **kwargs)

        self.config = {
            "index": "http://pypi.python.org/"
        }

    def run(self):
        pass
