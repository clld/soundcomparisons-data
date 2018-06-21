from setuptools import setup, find_packages


setup(
    name='pysoundcomparisons',
    version='0.1',
    license='CC-BY-4.0',
    description='programmatic access to clld/soundcomparisons-data',
    long_description='',
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
    author='Robert Forkel',
    author_email='forkel@shh.mpg.de',
    url='http://soundcomparisons.com',
    keywords='data',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'clldutils',
        'cdstarcat',
        'attrs',
        'pycldf>=1.0.6',
        'sqlalchemy',
        'pymysql',
    ],
    extras_require={
        'test': [
            'pytest',
        ],
        'dev': [],
    },
    entry_points={
        'console_scripts': [
            'soundcomparisons=pysoundcomparisons.__main__:main',
        ]
    },
)
