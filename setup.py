from pip.req import parse_requirements
from os.path import dirname, join
from setuptools import (
    find_packages,
    setup,
)

with open(join(dirname(__file__), 'VERSION.txt'), 'rb') as f:
    version = f.read().decode('ascii').strip()

requirements = [str(ir.req) for ir in parse_requirements("requirements.txt", session=False)]

setup(
    name='rqalpha-mod-mongo-datasource',
    version=version,
    description='RQAlpha DataSource Mod supporting reading day or minute bar from mongodb',
    packages=find_packages(exclude=[]),
    author='BurdenBear',
    author_email='public@fxdayu.com',
    license='Apache License v2',
    package_data={'': ['*.*']},
    url='https://github.com/xingetouzi/rqalpha_mod_mongo_datasource',
    install_requires=requirements,
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)