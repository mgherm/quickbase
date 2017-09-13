__author__ = 'mike'

from setuptools import setup

setup(name='quickbase',
      version='1.142',
      description='quickbase api tools',
      url="https://github.com/cictr/quickbase",
      author='Mike Herman',
      author_email='herman@cictr.com',
      packages=['quickbase'],
      zip_safe=False,
      install_requires=[
            'influxdb==4.1.1',
            'pytz==2017.2',
      ])