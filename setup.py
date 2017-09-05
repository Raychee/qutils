from distutils.core import setup

from qutils import VERSION


setup(
    name='qutils',
    packages=['qutils'],
    version=VERSION,
    description='utility functions / modules that may be frequently used in simple scripts',
    author='Raychee Zhang',
    author_email='raychee.zhang@gmail.com',
    url='https://github.com/Raychee/qutils',
    download_url='https://github.com/Raychee/qutils/tarball/' + VERSION,
    keywords=['utils', 'common', 'dlmanager', 'json'],
    test_suite='qutils.tests',
    install_requires=[
        'pandas>=0.17.0',
        'PyYAML>=3.12',
        'teradata>=15.10.0.20'
    ]
)
