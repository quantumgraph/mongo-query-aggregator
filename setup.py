from setuptools import setup

setup(
    # This is the name of your PyPI-package.
    name='moquag',
    # Update the version number for new releases
    version='0.241',
    # The name of your scipt, and also the command you'll be using for
    # calling it
    # scripts=['moquag'],
    description='Mongo query aggregator using bulk operators',
    url='https://github.com/quantumgraph/mongo-query-aggregator',
    author='QuantumGraph',
    author_email='contact@quantumgraph.com',
    license='MIT',
    packages=['moquag'],
    install_requires=[
        'pymongo',
    ],
    zip_safe=False
)

# uploading command for package
# python setup.py sdist upload
