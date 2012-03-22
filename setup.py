import os, sys
from setuptools import setup, find_packages

VERSION = '0.1.0'

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()


setup(name = 'ploud.utils',
      version = VERSION,
      description = 'ploud infrastructure utils',
      long_description = README + '\n\n' +  CHANGES,
      classifiers=[
        "Programming Language :: Python",
        ],
      author = 'Nikolay Kim',
      author_email = 'Nikolay Kim <nikolay@enfoldsystems.com>',
      url = 'http://www.enfoldsystems.com',
      packages = find_packages(),
      namespace_packages = ['ploud'],
      zip_safe = False,
      include_package_data = True,
      install_requires = [
        'setuptools',
        'psycopg2',
        'ptah',
      ],
      entry_points = {
        'ptah': ['package = ploud.utils'],
      },
)
