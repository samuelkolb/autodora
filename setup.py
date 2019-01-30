from setuptools import setup, find_packages

# Distribute: python setup.py sdist upload

setup(
    name='autodora',
    version='0.1.6',
    description='Automate experiments and explore your data',
    url='http://github.com/samuelkolb/pydora',
    author='Samuel Kolb',
    author_email='samuel.kolb@me.com',
    license='MIT',
    packages=find_packages(),
    zip_safe=False,
    install_requires=['matplotlib', 'peewee', 'pebble', 'numpy']
)
