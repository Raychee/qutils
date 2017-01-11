from distutils.core import setup


VERSION = '0.2.6'


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
    install_requires=['pandas>=0.19', 'pyyaml>=3.12', 'requests>=2.12']
)
