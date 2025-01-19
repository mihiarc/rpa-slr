from setuptools import setup, find_packages

setup(
    name="rpa_slr",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31.0",
        "pandas>=2.0.0",
        "pyarrow>=14.0.1",  # For parquet support
    ]
) 