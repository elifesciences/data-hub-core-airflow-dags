import os
from setuptools import (
    setup,
    find_packages
)

with open(os.path.join('requirements.txt'), 'r') as f:
    REQUIRED_PACKAGES = f.readlines()
PACKAGES = find_packages()

PACKAGES = [x for x in PACKAGES
            if x not in {'dags', 'tests'}]

setup(
    name='data-hub-core-airflow-dags',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=PACKAGES,
    include_package_data=True,
    description='data pipeline'
)
