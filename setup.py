from setuptools import setup, find_packages
import sys, os

version = '0.0dev'

setup(
    name = 'riwo',
    version = version,
    description = "Reader/Writer universal tool.",
    packages = find_packages( exclude = [ 'ez_setup'] ),
    include_package_data = True,
    zip_safe = False,
    author = 'Bence Faludi',
    author_email = 'bence@ozmo.hu',
    license = 'GPL',
    install_requires = [
        'dm',
        'daprot',
        'xmlsquash',
        'sqlalchemy',
        'pip',
    ],
    test_suite = "riwo.tests",
    url = 'http://bfaludi.com'
)