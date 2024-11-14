import pandas as pd

def procesar_ventas(file_data):
    # Leer el archivo Excel en un DataFrame de pandas
    df = pd.read_excel(file_data, engine="openpyxl")  # Asegúrate de tener openpyxl para leer Excel

    # Transformaciones:
    # Sustituir los valores nulos en 'cantidad' y 'precio_unitario' por 0
    df['cantidad'].fillna(0, inplace=True)
    df['precio_unitario'].fillna(0, inplace=True)

    # Devolver el DataFrame transformado
    return df
