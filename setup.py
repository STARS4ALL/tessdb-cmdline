from setuptools import setup, find_packages, Extension
import versioneer

# Default description in markdown
LONG_DESCRIPTION = open('README.md').read()

PKG_NAME     = 'tessdb-cmdline'
AUTHOR       = 'Rafael Gonzalez'
AUTHOR_EMAIL = 'rafael08@ucm.es'
DESCRIPTION  = 'tessdb command line tool to manage tessdb database',
LICENSE      = 'MIT'
KEYWORDS     = ['Light Pollution','Astronomy']
URL          = 'http://github.com/stars4all/tessdb-cmdline/'
DEPENDENCIES = [
    'jinja2',
    'tabulate',
    'geopy',
    'timezonefinder',
    'requests',
    'validators',
    'python-decouple'
]

CLASSIFIERS  = [
    'Environment :: Console',
    'Intended Audience :: Science/Research',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: SQL',
    'Topic :: Scientific/Engineering :: Astronomy',
    'Topic :: Scientific/Engineering :: Atmospheric Science',
    'Development Status :: 4 - Beta',
    'Natural Language :: English',
    'Natural Language :: Spanish',
]

PACKAGE_DATA  = {
    'tessutils': [
        'templates/*.j2',
    ],
}


SCRIPTS = [
    "scripts/tess",
    "scripts/tessdb_pause", 
    "scripts/tessdb_resume",
    "scripts/tessutils",
    "scripts/mongo-db", 
    "scripts/tess-db", 
    "scripts/cross-db", 
]

DATA_FILES  = []

setup(
    name             = PKG_NAME,
    version          = versioneer.get_version(),
    cmdclass         = versioneer.get_cmdclass(),
    author           = AUTHOR,
    author_email     = AUTHOR_EMAIL,
    description      = DESCRIPTION,
    long_description_content_type = "text/markdown",
    long_description = LONG_DESCRIPTION,
    license          = LICENSE,
    keywords         = KEYWORDS,
    url              = URL,
    classifiers      = CLASSIFIERS,
    packages         = find_packages("src"),
    package_dir      = {"": "src"},
    install_requires = DEPENDENCIES,
    scripts          = SCRIPTS,
    package_data     = PACKAGE_DATA,
    data_files       = DATA_FILES,
)
