from setuptools import setup

import os, glob

def read(fname):
    return open(fname).read()

setup(
    name = "skinnyproxy",
    version = "0.0.1",
    author = "Marek Wiewiorski",
    author_email = "mwicat@gmail.com",
    description = ("Skinny proxy for recording and replaying traffic"),
    license = "GPLv3",
    packages=['skinnyproxy'],
    install_requires = ['plac'],
    long_description=read('README'),
    entry_points = {
        'console_scripts': [
            'sccpreplay = skinnyproxy.replay:main',
            'sccpreassemble = skinnyproxy.reassemble:main',
            'sccpinject = skinnyproxy.inject:main',
            'sccpproxy = skinnyproxy.proxy:main',
            ]
        },


)
