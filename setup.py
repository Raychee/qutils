from setuptools import setup, find_packages


VERSION = ''

with open('qutils/__init__.py') as f:
    exec(f.readline())

setup(
    name='qutils',
    packages=find_packages(exclude=['*.tests']),
    version=VERSION,
    description='utility functions / modules that may be frequently used in simple scripts',
    author='Raychee Zhang',
    author_email='raychee.zhang@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities'
    ],
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
