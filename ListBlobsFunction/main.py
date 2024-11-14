import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os

def main(req: func.HttpRequest, inputBlob: bytes) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Obtener la cadena de conexión del almacenamiento de Azure desde las configuraciones de entorno
    connection_string = os.getenv('AzureWebJobsStorage')

    # Crear el cliente del servicio Blob usando la cadena de conexión
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Nombre del contenedor
    container_name = "raw"

    # Obtener el cliente del contenedor
    container_client = blob_service_client.get_container_client(container_name)

    # Listar los blobs en el contenedor
    try:
        blob_list = container_client.list_blobs()
        blobs = [blob.name for blob in blob_list]
        # Devuelvo la lista de blobs como parte de la respuesta HTTP
        return func.HttpResponse("\n".join(blobs), status_code=200)
    except Exception as e:
        logging.error(f"Error al listar los blobs: {str(e)}")
        # Si hay un error, devuelvo un error 500
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


