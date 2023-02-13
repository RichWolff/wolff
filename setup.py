#!/usr/bin/env python

"""The setup script."""


import pathlib
from setuptools import setup, find_packages

readme = pathlib.Path("README.rst").read_text()
history = pathlib.Path("HISTORY.rst").read_text()

requirements = [
    "Click>=7.0",
    "pyodbc==4.0.35",
    "sqlalchemy==1.4.39",
    "pandas==1.4.4",
    "duckdb==0.6.1",
    "duckdb_engine==0.6.8",
    "tqdm==4.64.1",
    "pyarrow",
]

test_requirements = []

setup(
    author="Richard Wolff",
    author_email="richwolff12@gmail.com",
    python_requires=">=3.9<3.10",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Python Boilerplate contains all the boilerplate you need to create a Python package.",
    entry_points={
        "console_scripts": [
            "wolff=wolff.cli:main",
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="wolff",
    name="wolff",
    packages=find_packages(include=["wolff", "wolff.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/richwolff/wolff",
    version="0.0.1",
    zip_safe=False,
)
