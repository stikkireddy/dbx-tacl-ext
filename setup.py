from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf8") as fh:
    long_description = fh.read()

setup(
    name="dbx-tacl-ext",
    author="Sri Tikkireddy",
    author_email="sri.tikkireddy@databricks.com",
    description="Databricks Table ACL Extensions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=['tests', 'tests.*', 'notebooks']),
    use_scm_version={
        "local_scheme": "dirty-tag"
    },
    setup_requires=['setuptools_scm'],
    install_requires=[
        'requests>=2.17.3',
        'databricks-cli>=0.16.2',
        'setuptools==45',
        'python-decouple>=3.6',
        'sql-metadata>=2.5.0',
        'adlfs',
        'cloudpickle==2.0.0'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
