import requests
import boto3
import datetime

BUCKET_NAME = "bparcial1-a"
BASE_URL = "https://casas.mitula.com.co/find?operationType=sell&propertyType=mitula_studio_apartment&text=Bogot%C3%A1"

s3_client = boto3.client("s3")

def app(event, context):
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    s3_folder = f"{today}/"  # Carpeta dentro del bucket
    headers = {"User-Agent": "Mozilla/5.0"}
    saved_files = []

    for page in range(1, 11):  # Guardar 10 páginas
        url = f"{BASE_URL}/pag-{page}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            filename = f"pagina-{page}.html"
            s3_path = f"{s3_folder}{filename}"
            
            # Guardar cada página en S3
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_path,
                Body=response.text.encode("utf-8"),
                ContentType="text/html"
            )
            saved_files.append(f"s3://{BUCKET_NAME}/{s3_path}")

    return {
        "statusCode": 200,
        "body": f"Archivos guardados:\n" + "\n".join(saved_files)
    }