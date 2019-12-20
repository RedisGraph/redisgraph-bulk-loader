from setuptools import setup, find_packages
setup(
    name='redisgraph-bulk-loader',
    python_requires='>=3',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'redis',
        'click'
    ],
    entry_points='''
        [console_scripts]
        redisgraph-bulk-loader=redisgraph_bulk_loader.bulk_insert:bulk_insert
    '''
)