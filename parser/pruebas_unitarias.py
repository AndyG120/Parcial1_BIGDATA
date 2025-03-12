import pytest
import datetime
from parcial_p import extract_data, save_to_s3, DESTINATION_BUCKET
import boto3
from moto import mock_s3
from bs4 import BeautifulSoup

# HTML de prueba con cuatro propiedades
HTML_TEST = """
<html>
<body>
    <a class="listing listing-card" data-location="Bogotá, Centro" data-price="$300,000,000" data-floorarea="80 m²">
        <p data-test="bedrooms" content="3"></p>
        <p data-test="bathrooms" content="2"></p>
    </a>
    <a class="listing listing-card" data-location="Bogotá, Poblado" data-price="$500,000,000" data-floorarea="120 m²">
        <p data-test="bedrooms" content="4"></p>
        <p data-test="bathrooms" content="3"></p>
    </a>
    <a class="listing listing-card" data-location="Bogotá, Centro" data-price="$415,000,000" data-floorarea="55 m²">
        <p data-test="bedrooms" content="1"></p>
        <p data-test="bathrooms" content="1"></p>
    </a>
    <a class="listing listing-card" data-location="Bogotá, Ilarco" data-price="$420,000,000" data-floorarea="66 m²">
        <p data-test="bedrooms" content="1"></p>
        <p data-test="bathrooms" content="2"></p>
    </a>
</body>
</html>
"""

@pytest.fixture
def s3_mock():
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "bparcial1-b"
        s3.create_bucket(Bucket=bucket)
        yield s3

def decode_chunked(chunked_str: str) -> str:
    """
    Decodifica una cadena en formato chunked encoding y devuelve la concatenación de los chunks.
    """
    lines = chunked_str.split("\r\n")
    chunks = []
    i = 0
    while i < len(lines):
        if not lines[i]:
            i += 1
            continue
        try:
            size = int(lines[i], 16)
        except ValueError:
            i += 1
            continue
        if size == 0:
            break
        if i + 1 < len(lines):
            chunks.append(lines[i + 1])
        i += 2
    return "".join(chunks).strip()

def test_extract_data(s3_mock, monkeypatch):
    """Verifica que extract_data y save_to_s3 funcionan correctamente."""
    # 1. Prueba de extract_data
    fecha_descarga = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    data = extract_data(HTML_TEST)
    expected_data = [
        [fecha_descarga, "Bogotá, Centro", "300000000", "3", "2", 80],
        [fecha_descarga, "Bogotá, Poblado", "500000000", "4", "3", 120],
        [fecha_descarga, "Bogotá, Centro", "415000000", "1", "1", 55],
        [fecha_descarga, "Bogotá, Ilarco", "420000000", "1", "2", 66]
    ]
    assert data == expected_data, f"Datos extraídos incorrectos: {data}"
    print("assert 1 passed: extract_data devuelve los datos esperados.")

    # 2. Prueba de save_to_s3
    # Monkeypatch para reemplazar el cliente S3 global por el simulado
    monkeypatch.setattr("parcial_p.s3_client", s3_mock)

    filename = "test_file.csv"
    save_to_s3(data, filename)

    response = s3_mock.get_object(Bucket=DESTINATION_BUCKET, Key=filename)
    raw_content = response["Body"].read().decode("utf-8")
    content = decode_chunked(raw_content)

    header = "FechaDescarga,Barrio,Valor,NumHabitaciones,NumBanos,mts2"
    expected_content = header + "\n" + "\n".join([",".join(map(str, row)) for row in data])

    print("Contenido real:", repr(content))
    print("Contenido esperado:", repr(expected_content))

    assert content.strip() == expected_content.strip(), (
        f"El contenido no coincide:\nReal: {repr(content)}\nEsperado: {repr(expected_content)}"
    )
    print("assert 2 passed: save_to_s3 guarda el CSV con el contenido correcto.")