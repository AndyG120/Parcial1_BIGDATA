import requests
import boto3
import datetime
from bs4 import BeautifulSoup

# Configuración de AWS S3
BUCKET_NAME = "bparcial1-a"
s3_client = boto3.client("s3")

# URL base para descargar las páginas de resultados
URL_TEMPLATE = "https://casas.mitula.com.co/find?operationType=sell&propertyType=mitula_studio_apartment&geoId=mitula-CO-poblacion-0000014156&text=Bogot%C3%A1%2C++%28Cundinamarca%29&page={}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

def download_and_upload():
    # Obtener la fecha actual
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    folder_name = f"landing-casas-{today.replace('-', '')}"  # Formato landing-casas-YYYYMMDD

    for i in range(1, 11):  # Descargar las primeras 10 páginas
        url = URL_TEMPLATE.format(i)
        print(f"Descargando página {i}: {url}")

        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            file_name = f"{today}.html"
            s3_path = f"{folder_name}/pagina_{i}_{file_name}"  # Formato: landing-casas-YYYYMMDD/pagina_X_YYYY-MM-DD.html
            
            try:
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_path,
                    Body=response.text.encode("utf-8"),
                    ContentType="text/html"
                )
                print(f"Página {i} subida exitosamente a S3 en: {s3_path}")
            except Exception as e:
                print(f"Error al subir la página {i} a S3: {e}")
        else:
            print(f"Error {response.status_code} al descargar la página {i}")

    print("Proceso de descarga y subida a S3 completado.")

def lambda_handler(event, context):
    download_and_upload()
    return {
        'statusCode': 200,
        'body': 'Proceso completado exitosamente.'
    }