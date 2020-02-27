#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-cassandra",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_cassandra"],
    install_requires=[
        "singer-python>=5.0.12",
        "cassandra-driver>=3.21.0",
        "python-dateutil>=1.4"
    ],
    entry_points="""
    [console_scripts]
    target-cassandra=target_cassandra:main
    """,
    packages=["target_cassandra"],
    package_data={},
    include_package_data=True,
)
