import base64
import pandas as pd
from google.cloud import storage
from io import BytesIO
from datetime import datetime

def consolidar_arquivos():
    # Inicializa o cliente do Google Cloud Storage
    client = storage.Client()
    # Nome do bucket e caminho dentro do bucket para os arquivos CSV
    bucket_name = 'jump-clean'
    # Lista para armazenar os DataFrames de cada arquivo CSV
    dataframes = []
    # Obtém uma lista de blobs no bucket com o prefixo especificado
    blobs = client.list_blobs(bucket_name)
    # Itera sobre os blobs
    for blob in blobs:
        if blob.name.endswith('.csv'):
            # Lê o arquivo CSV como uma string
            csv_data = blob.download_as_string()
            # Cria um DataFrame a partir da string CSV
            df = pd.read_csv(BytesIO(csv_data))
            # Adiciona o DataFrame à lista
            dataframes.append(df)
    # Concatena todos os DataFrames em um único DataFrame
    df_final = pd.concat(dataframes, ignore_index=True)
    return df_final

def save_storage(df_final):
    # Salvar DataFrame como CSV no Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket('jump-processed')
    blob = bucket.blob('jump_consolidado_v2.csv')
    # Converter DataFrame para CSV e fazer upload para o Cloud Storage
    data_frame_csv = df_final.to_csv(index=False)
    blob.upload_from_string(data_frame_csv)
    return print('csv salvo')


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    consolida = consolidar_arquivos()
    save = save_storage(consolida)
    print(save)
