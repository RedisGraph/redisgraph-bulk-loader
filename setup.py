
from setuptools import setup, find_packages
setup(
    name='redisgraph-bulk-loader',
    version='0.9dev',

    description='RedisGraph Bulk Import Tool',
    url='https://github.com/redisgraph/redisgraph-bulk-loader',
    packages=find_packages(),
    install_requires=['redis', 'click'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.0',
        'Topic :: Database'
    ]
)
