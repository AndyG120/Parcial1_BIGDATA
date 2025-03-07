import requests
import boto3
import datetime

BUCKET_NAME = "bparcial1-a"
BASE_URL = "https://casas.mitula.com.co/find?operationType=sell&propertyType=mitula_studio_apartment&text=Bogot%C3%A1"

s3_client = boto3.client("s3")

def app(event, context):
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"{today}.html"
    s3_path = f"{today}.html"

    headers = {"User-Agent": "Mozilla/5.0"}
    full_html = ""

    for page in range(1, 11):  # 10 páginas
        url = f"{BASE_URL}/pag-{page}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            full_html += response.text + "\n\n"

    # Guardar en S3
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_path,
        Body=full_html.encode("utf-8"),
        ContentType="text/html"
    )

    return {
        "statusCode": 200,
        "body": f"Archivo guardado en s3://{BUCKET_NAME}/{s3_path}"
    }