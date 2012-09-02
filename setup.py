#!/usr/bin/env python
from setuptools import setup, find_packages

import carrier


install_requires = [
    "APScheduler",
    "distutils2",
    "forklift",
    "redis",
    "six",
    "xmlrpc2",
]

setup(
    name="carrier",
    version=carrier.__version__,

    description="Warehouse and PyPI Synchronization",
    long_description=open("README.rst").read(),
    url="https://github.com/crateio/carrier/",
    license=open("LICENSE").read(),

    author="Donald Stufft",
    author_email="donald.stufft@gmail.com",

    install_requires=install_requires,

    packages=find_packages(exclude=["tests"]),
    package_data={"": ["LICENSE"], "carrier": ["config/*.crt"]},
    zip_safe=False,

    entry_points={
        "console_scripts": [
            "carrier = carrier.__main__:main",
        ],
    },
)
