from distutils.core import setup

setup(
    name='goes_mongo_inserter',
    version='1.0',
    packages=['goes_mongo_inserter.lib'],
    scripts=[
        'goes_mongo_inserter/gmi.py'
    ]
)
