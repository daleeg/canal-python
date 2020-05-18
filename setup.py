# -*- coding: utf-8 -*-

from setuptools import setup

with open("README.md", "r",encoding="UTF-8") as fh:
    long_description = fh.read()

setup(
    name="py3canal",
    version="1.0.4",
    packages=["pycanal", "pycanal.protocol"],
    author="vallee",
    author_email="xph_wangly10@163.com",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vallee11/canal-python"
)