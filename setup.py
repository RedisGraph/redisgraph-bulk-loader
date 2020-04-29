from setuptools import setup, find_packages
setup(
    name='redisgraph-bulk-loader',
    python_requires='>=3',
    version='0.8.1',
    packages=find_packages(),
    install_requires=[
        'redis',
        'click'
    ],

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

    entry_points='''
        [console_scripts]
        redisgraph-bulk-loader=redisgraph_bulk_loader.bulk_insert:bulk_insert
    '''
)
