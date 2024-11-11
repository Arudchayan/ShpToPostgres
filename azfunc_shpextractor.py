import logging
import io
from azure.storage.blob import BlobServiceClient
import geopandas as gpd
from sqlalchemy import create_engine
from azure.functions import HttpRequest, HttpResponse
from pydantic import BaseModel


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
        raise Exception(f"Error downloading blob: {str(e)}")

async def main(req: HttpRequest) -> HttpResponse:
    try:

        request_body = await req.get_json()
        blob_request = BlobRequest(**request_body)


        zip_stream = download_blob_to_memory(
            blob_request.connection_string, 
            blob_request.container_name, 
            blob_request.blob_name
        )

  
        gdf = gpd.read_file(f"zip://{blob_request.blob_name}", vfs=zip_stream)

        # Collect shapefile metadata
        shapefile_info = {
            "crs": str(gdf.crs),  
            "num_features": len(gdf),  
            "bounds": gdf.total_bounds.tolist(),  
            "columns": gdf.columns.tolist(),  
        }
        engine = create_engine(blob_request.dbconn)
        gdf.to_postgis(name="he_regions", con=engine, if_exists='replace', index=False)

        return HttpResponse(
            body=f'{{"status": "success", "data": {shapefile_info}}}',
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error processing shapefile: {str(e)}")
        return HttpResponse(
            body=f'{{"status": "error", "detail": "{str(e)}"}}',
            status_code=400,
            mimetype="application/json"
        )
