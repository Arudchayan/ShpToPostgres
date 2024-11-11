from fastapi import FastAPI, HTTPException, Body
from azure.storage.blob import BlobServiceClient
import io
from pydantic import BaseModel
import geopandas as gpd
from sqlalchemy import create_engine

app = FastAPI()

class BlobRequest(BaseModel):
    connection_string: str
    container_name: str
    blob_name: str
    dbconn: str  

@app.get("/")
def read_root():
    return {"message": "Welcome to the ShpExtractor app!"}    

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
        # Download the zip file to memory
        zip_stream = download_blob_to_memory(
            request.connection_string, request.container_name, request.blob_name
        )

        gdf = gpd.read_file(f"zip://{request.blob_name}", vfs=zip_stream)

        shapefile_info = {
            "crs": str(gdf.crs),  
            "num_features": len(gdf),  
            "bounds": gdf.total_bounds.tolist(),  
            "columns": gdf.columns.tolist(),  
        }

        connection_string = request.dbconn

        engine = create_engine(connection_string)

        gdf.to_postgis(name="he_regions", con=engine, if_exists='replace', index=False)

        return {"status": "success", "data": shapefile_info}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing shapefile: {str(e)}")
