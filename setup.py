from setuptools import setup


def readme():
    with open("README.md") as f:
        return f.read()


setup(
    name="sqlthemall",
    description="Automatic import of JSON data into relational databases",
    long_description=readme(),
    classifiers=[
        "Topic :: Database",
        "Topic :: Utilities",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
    keywords="JSON,SQLAlchemy",
    url="http://github.com/chriskipp/sqlthemall",
    author="Christopher Kipp",
    author_email="christopher.kipp@web.de",
    license="GPLv3",
    packages=["sqlthemall"],
    scripts=["bin/sqlthemall"],
    install_requires=[
        "SQLalchemy >= 1.4",
        "alembic >= 1.5.8",
    ],
    extras_require={
        "ujson": ["ujson"],
    },
    include_package_data=True,
    zip_safe=False,
)
