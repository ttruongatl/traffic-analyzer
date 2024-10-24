from setuptools import setup, find_packages

setup(
    name='traffic_analyzer',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
        'pathspec',
        'pytest',
        'azure-kusto-data',
        'azure-kusto-ingest',
        'azure.identity',
        'tqdm'

    ],
    entry_points='''
        [console_scripts]
        traffic-analyzer=traffic_analyzer.cli:cli
    ''',
)