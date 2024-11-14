import pandas as pd
import io
from io import BytesIO

def procesar_ventas(file_data):
    # Leer el archivo Excel en un DataFrame de pandas
    df = pd.read_excel(io.BytesIO(file_data))

    # Procesamiento específico de ventas
    df['cantidad'] = df['cantidad'].fillna(0)
    df['precio_unitario'] = df['precio_unitario'].fillna(0)
    return df

def procesar_productos(file_data):
    try:
        # Leer el archivo CSV en memoria con una codificación adecuada
        df = pd.read_csv(BytesIO(file_data), encoding="ISO-8859-1")

        # Eliminar duplicados
        df = df.drop_duplicates()

        # Retornar el DataFrame procesado
        return df

    except Exception as e:
        raise Exception(f"Error processing productos.csv: {str(e)}")

