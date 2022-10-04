from setuptools import setup, find_packages

setup(
    name='tennetConnector',

    version='0.3.12',

    description='A python API wrapper for TenneT',

    keywords='TenneT data api energy',
    packages=find_packages(),
    install_requires=['requests', 'lxml', 'pandas>=1.4.0'],

    include_package_data=True,
)
