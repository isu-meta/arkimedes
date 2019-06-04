from setuptools import setup, find_packages

requirements = ["fuzzywuzzy==0.17.0", "lxml>=4.3.3, <5.0.0", "requests>=2.22.0, <3.0.0"]

setup(
    name="arkimedes",
    version="0.0.1",
    packages=find_packages(),
    scripts=["arkimedes.py"],
    install_requires=requirements,
)
