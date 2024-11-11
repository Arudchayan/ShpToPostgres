from setuptools import setup, find_packages

setup(
    name='shpextractor',  # Name of your package
    version='0.1.0',  # Version number
    packages=find_packages(),  # Automatically finds all the packages
    install_requires=[  # List of dependencies
        'fastapi',
        'uvicorn',
        'azure-storage-blob',
        'geopandas',
        'sqlalchemy',
        'geoalchemy2',
        'psycopg2',  # Required for PostgreSQL if you're using PostGIS
        'shapely',  # GeoPandas dependency
        'fiona',  # GeoPandas dependency for reading shapefiles
    ],
    include_package_data=True,  # To include non-Python files like README
    package_data={  # To include additional data files (e.g., README, LICENSE)
        '': ['README.md', 'LICENSE'],
    },
    classifiers=[  # Metadata to describe your package
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',  # Ensure compatibility with Python versions >=3.7
)
