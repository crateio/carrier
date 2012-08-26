import os

SCHEDULE = {
    "packages": {"seconds": 30},
}

WAREHOUSE_URI = "https://api.crate.io/v1/"

PYPI_URI = "https://pypi.python.org/pypi"
PYPI_SSL_VERIFY = os.path.join(os.path.dirname(__file__), "pypi.crt")

REDIS = {}  # We leave this empty so client defaults occur

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    "formatters": {
        "default": {
            "format": "[%(asctime)s][%(levelname)s] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },

    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        },
        "devnull": {
            "class": "logging.NullHandler",
        }
    },

    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },

    "loggers": {
        "requests.packages.urllib3": {
            "handlers": ["devnull"],
            "propagate": False,
        }
    },
}
