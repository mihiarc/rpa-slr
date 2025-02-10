from setuptools import setup, find_packages

setup(
    name="county_level_tidal_flooding",
    version="0.2.0",
    description="NOAA High Tide Flooding Data Analysis",
    author="RPA",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.31.0",
        "pandas>=2.0.0",
        "pyarrow>=14.0.1",  # For parquet support
        "numpy>=1.24.0",    # For numerical operations
        "pyyaml>=6.0.0",    # For YAML configuration files
        "scipy>=1.10.0",    # For statistical analysis
        "matplotlib>=3.7.0", # For visualization support
    ],
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'black>=23.0.0',
            'flake8>=6.0.0',
            'mypy>=1.0.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'htf-analyze=analysis.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
) 