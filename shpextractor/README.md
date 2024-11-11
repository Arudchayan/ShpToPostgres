# ShpExtractor

**ShpExtractor** is a FastAPI-based web service that allows you to download shapefiles from Azure Blob Storage, process them using GeoPandas, and upload them to a PostgreSQL/PostGIS database. This application provides a convenient way to work with geospatial data directly from the cloud storage to a spatially enabled database.

## Features

- Download shapefiles (in ZIP format) from Azure Blob Storage.
- Process the shapefiles in-memory using GeoPandas.
- Upload the shapefile data to a PostgreSQL/PostGIS database.
- Expose an API with FastAPI to handle shapefile processing requests.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [API Endpoints](#api-endpoints)
  - [Example Requests](#example-requests)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [License](#license)

## Installation

To run the ShpExtractor app, follow these steps:

### Prerequisites

- Python 3.7 or later
- PostgreSQL with PostGIS extension (for storing geospatial data)
- Azure Blob Storage account

### 1. Clone the repository

```bash
git clone https://github.com/Arudchayan/ShpToPostgres
cd shpextractor
