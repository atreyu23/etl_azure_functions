import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Importar las funciones de procesamiento
from procesar_ventas_function.transformaciones import procesar_ventas, procesar_productos

# Mapeo de archivos y funciones de procesamiento
FILES_TO_PROCESS = [
    {"file_name": "Ventas.xlsx", "processor": procesar_ventas},
    {"file_name": "productos.csv", "processor": procesar_productos},
    # Puedes agregar más archivos y funciones aquí
]

def procesar_archivo(blob_service_client, container_name, file_info):
    try:
        # Obtener cliente del contenedor
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(file_info["file_name"])

        # Descargar el contenido del blob
        file_data = blob_client.download_blob().readall()

        # Llamar a la función de procesamiento específica
        df = file_info["processor"](file_data)

        # Convertir DataFrame a Parquet
        table = pa.Table.from_pandas(df)
        parquet_file_name = file_info["file_name"].replace(".xlsx", ".parquet").replace(".csv", ".parquet")
        parquet_file = f"/tmp/{parquet_file_name}"
        pq.write_table(table, parquet_file)

        # Subir el archivo Parquet al contenedor 'master'
        master_container_client = blob_service_client.get_container_client("master")
        master_blob_client = master_container_client.get_blob_client(parquet_file_name)
        
        with open(parquet_file, "rb") as f:
            master_blob_client.upload_blob(f, overwrite=True)

        logging.info(f"File {file_info['file_name']} processed and stored as Parquet.")
    
    except Exception as e:
        logging.error(f"Error processing {file_info['file_name']}: {str(e)}")
        raise

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Starting processing of files from the raw container.")

    # Obtener la cadena de conexión desde las configuraciones de entorno
    connection_string = os.getenv('AzureWebJobsStorage')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Nombre del contenedor de origen
    container_name = "raw"

    try:
        # Procesar cada archivo en la lista
        for file_info in FILES_TO_PROCESS:
            procesar_archivo(blob_service_client, container_name, file_info)

        return func.HttpResponse("All files processed and stored as Parquet.", status_code=200)
    
    except Exception as e:
        logging.error(f"Error processing files: {str(e)}")
        return func.HttpResponse(f"Error processing files: {str(e)}", status_code=500)

