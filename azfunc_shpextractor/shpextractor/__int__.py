import azure.functions as func

import os
import tempfile
import fastapi
from fastapi import HTTPException, Body
from azure.storage.blob import BlobServiceClient
import io
from pydantic import BaseModel
import geopandas as gpd
from sqlalchemy import create_engine
import zipfile
import fiona

app = fastapi.FastAPI()



@app.get("/hello/{name}")
async def get_name(name: str):
    return {
        "name": name,
    }


class BlobRequest(BaseModel):
    connection_string: str
    container_name: str
    blob_name: str
    dbconn: str  


def download_blob_to_memory(connection_string: str, container_name: str, blob_name: str):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        download_stream = io.BytesIO()
        blob_client.download_blob().readinto(download_stream)
        download_stream.seek(0)
        
        return download_stream
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error downloading blob: {str(e)}")


@app.post("/process_shapefile")
async def process_shapefile(request: BlobRequest):
    try:
       
        zip_stream = download_blob_to_memory(
            request.connection_string, request.container_name, request.blob_name
        )

        
        with tempfile.TemporaryDirectory() as tmpdir:
            
            zip_file_path = os.path.join(tmpdir, "shapefile.zip")

            
            with open(zip_file_path, 'wb') as f:
                f.write(zip_stream.read())

            
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                
                zip_ref.extractall(tmpdir)

            
            shapefile_path = None
            for file_name in os.listdir(tmpdir):
                if file_name.endswith('.shp'):
                    shapefile_path = os.path.join(tmpdir, file_name)
                    break

            if not shapefile_path:
                raise HTTPException(status_code=400, detail="Shapefile not found in zip archive.")


            with fiona.open(shapefile_path) as src:

                gdf = gpd.read_file(shapefile_path)

        # Gather shapefile info
        shapefile_info = {
            "crs": str(gdf.crs),
            "num_features": len(gdf),
            "bounds": gdf.total_bounds.tolist(),
            "columns": gdf.columns.tolist(),
        }

        # Database connection string
        connection_string = request.dbconn
        engine = create_engine(connection_string)

        # Store the data in the PostGIS database
        gdf.to_postgis(name="he_regions", con=engine, if_exists='replace', index=False)

        return {"status": "success", "data": shapefile_info}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing shapefile: {str(e)}")