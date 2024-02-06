# importando arquivos
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceExistsError
from io import StringIO
import os

#------------------------------------------------------------------------------------------------------
st.set_page_config(layout="wide") # Configuração da página larga
#------------------------------------------------------------------------------------------------------
# Uploading data

connection_string = "DefaultEndpointsProtocol=https;AccountName=beesexpansion0001;AccountKey=QBAsqeUnSwNe7hKHJwWrKfH1XE0LpERqc/N/x5jg51pKCvoOgaZw0NvIgxKwyciZ2JxnnjdBbu0b+ASt9jRAaA==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

container_name = 'expansionbees0001'
local_file_path = r'C:\Users\gabri\OneDrive\Área de Trabalho\DataID'
#blob_name = 'blob0001'

container_client = blob_service_client.get_container_client(container_name)

# Upload the file

container_client = blob_service_client.get_container_client(container_name)
try:
    container_client.create_container()
except ResourceExistsError:
    print(f"Container '{container_name}' already exists.")


# Iterate through the files and upload each one
# overwrite = False
# for root, dirs, files in os.walk(local_file_path):
#     for file in files:
#         file_path = os.path.join(root, file)
#         blob_path = os.path.relpath(file_path, local_file_path).replace(os.sep, '/')

#         try:
#             blob_client = container_client.get_blob_client(blob=blob_path)
#             with open(file_path, "rb") as data:
#                 blob_client.upload_blob(data, overwrite=overwrite)
#             print(f"Uploaded '{file_path}' to Blob '{blob_path}' in container '{container_name}'.")
#         except ResourceExistsError:
#             print(f'Blob "{blob_path}" already exists, skipping.')

def upload_files_to_blob_storage(local_file_path, container_client, overwrite=True):
    for root, dirs, files in os.walk(local_file_path):
        for file in files:
            file_path = os.path.join(root, file)
            blob_path = os.path.relpath(file_path, local_file_path).replace(os.sep, '/')

            try:
                blob_client = container_client.get_blob_client(blob=blob_path)
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=overwrite)
                print(f"Uploaded '{file_path}' to Blob '{blob_path}' in container '{container_client.container_name}'.")
            except ResourceExistsError:
                print(f'Blob "{blob_path}" already exists, skipping.')



#------------------------------------------------------------------------------------------------------
#### Mandar arquivos na pasta DataID para o Azure Blob Storage
upload_files_to_blob_storage(local_file_path, container_client, overwrite=True)

##### Tables
# Tabela teste
                
blob_name = 'export.csv'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content = blob_client.download_blob().content_as_text()
data = StringIO(blob_content)
df = pd.read_csv(data)


##### Imagens

logo = "logo.png"
blob_client_logo = blob_service_client.get_blob_client(container=container_name, blob=logo)
blob_content_logo = blob_client_logo.download_blob().readall()

#------------------------------------------------------------------------------------------------------

#### app
# Abas

abas = st.tabs(["In scope"])
aba0 = abas[0]


# Aba0
with aba0:
    colA = st.columns(1)
    colB = st.columns(1)

# Colunas

with colA[0]:
    st.image(blob_content_logo, use_column_width='always')
with colB[0]:
    st.markdown("""
    <style>
    .fonte-personalizada1 {
        font-size: 20px;
        font-style: bold;
    }
    </style>
    <div class="fonte-personalizada1">
        KPI's management - Indonesia
    </div>
    """, unsafe_allow_html=True)

# Teste



