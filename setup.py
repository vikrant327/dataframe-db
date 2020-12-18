from setuptools import setup

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='dataframe-db',
    url='https://github.com/vikrant327/dataframe-db',
    author='vikrant327',
    # Needed to actually package something
    packages=['dataframedb'],
    # Needed for dependencies
    install_requires=['sqlalchemy','pyodbc'],
    # *strongly* suggested for sharing
    version='1.0.0',
    # The license can be anything you like
    license='MIT',
    description='Create new table or append records to existing table from pandas database.',
)
