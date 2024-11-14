import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Añadir la carpeta de la función actual a la ruta de Python
from procesar_ventas_function.transformaciones import procesar_ventas

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing the Ventas.xlsx file from the raw container.')

    # Obtener la cadena de conexión desde las configuraciones de entorno
    connection_string = os.getenv('AzureWebJobsStorage')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Nombre del contenedor y archivo específico
    container_name = "raw"
    file_name = "Ventas.xlsx"

    # Obtener el cliente del contenedor
    container_client = blob_service_client.get_container_client(container_name)

    try:
        # Intentamos obtener el blob "Ventas.xlsx" desde el contenedor "raw"
        blob_client = container_client.get_blob_client(file_name)

        # Descargar el contenido del blob en memoria
        file_data = blob_client.download_blob().readall()

        # Llamar a la función de procesamiento de ventas para leer y transformar el archivo
        df = procesar_ventas(file_data)

        # Convertir el DataFrame a Parquet
        table = pa.Table.from_pandas(df)
        parquet_file = f"/tmp/{file_name}.parquet"
        pq.write_table(table, parquet_file)

        # Subir el archivo Parquet al contenedor 'master'
        master_container = blob_service_client.get_container_client("master")
        master_blob_client = master_container.get_blob_client(file_name.replace(".xlsx", ".parquet"))
        
        with open(parquet_file, "rb") as f:
            master_blob_client.upload_blob(f, overwrite=True)

        return func.HttpResponse(f"File {file_name} processed and stored as Parquet.", status_code=200)
    
    except Exception as e:
        logging.error(f"Error processing {file_name}: {str(e)}")
        return func.HttpResponse(f"Error processing {file_name}: {str(e)}", status_code=500)
