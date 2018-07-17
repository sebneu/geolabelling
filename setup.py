from setuptools import setup, find_packages
#from pip.req import parse_requirements

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

install_reqs = parse_requirements("requirements.txt", session=False)

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='geo_tagger',
    version='0.1',
    packages=find_packages(),
    install_requires=reqs,
    license='GNU General Public License v3.0',
    author='Sebastian Neumaier',
    author_email='sebastian.neumaier@wu.ac.at',
    description='A service to add geographical identification metadata to CSV tables.'
)
