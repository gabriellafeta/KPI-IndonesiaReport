# importando arquivos
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceExistsError
from io import StringIO
import os
import plotly.express as px

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

##### Tables from Blob
                
blob_name = 't1.csv'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content = blob_client.download_blob().content_as_text()
data_t1 = StringIO(blob_content)
df_t1 = pd.read_csv(data_t1)

blob_name = 't2.csv'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content = blob_client.download_blob().content_as_text()
data_t2 = StringIO(blob_content)
df_t2 = pd.read_csv(data_t2)

blob_name = 't3.csv'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content = blob_client.download_blob().content_as_text()
data_t3 = StringIO(blob_content)
df_t3 = pd.read_csv(data_t3)

blob_name = 't4.csv'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content = blob_client.download_blob().content_as_text()
data_t4 = StringIO(blob_content)
df_t4 = pd.read_csv(data_t4)

blob_name = 't5.csv'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content = blob_client.download_blob().content_as_text()
data_t5 = StringIO(blob_content)
df_t5 = pd.read_csv(data_t5)

##### Imagens

logo = "logo.png"
blob_client_logo = blob_service_client.get_blob_client(container=container_name, blob=logo)
blob_content_logo = blob_client_logo.download_blob().readall()

#------------------------------------------------------------------------------------------------------
## Manipulando tabelas

### Alias column for BDR

BDR_dict = {
    "5653270_SMC004": "Bram",
    "6389058_BDR001": "Harris",
    "6658562_BDR001": "Cheryl",
    "6421535_BDR001": "Christian",
    "6828128_BDR001": "Iwan Dwiarsono",
    "6713130_SMC007": "Dian",
    "6174675_BDR001": "Alvis"
}

df_t1['BDR_name'] = df_t1['BDR_ID'].map(BDR_dict)

### Tabelas para KPI 1 - N de visitas

df_t1_bram = df_t1[df_t1['BDR_name'] == 'Bram']
df_t1_harris = df_t1[df_t1['BDR_name'] == 'Harris']
df_t1_cheryl = df_t1[df_t1['BDR_name'] == 'Cheryl']
df_t1_christian = df_t1[df_t1['BDR_name'] == 'Christian']
df_t1_iwan = df_t1[df_t1['BDR_name'] == 'Iwan Dwiarsono']
df_t1_dian = df_t1[df_t1['BDR_name'] == 'Dian']
df_t1_alvis = df_t1[df_t1['BDR_name'] == 'Alvis']


#------------------------------------------------------------------------------------------------------
# Criando visualizações

max_date = df_t1['VISIT_DATE'].max()

##Gráfico de barras KPI 1 - N de visitas
###### All BDR's
df_t1['VISIT_DATE'] = pd.to_datetime(df_t1['VISIT_DATE'])
df_t1_sorted = df_t1.sort_values(by='VISIT_DATE')

start_date = df_t1_sorted['VISIT_DATE'].min()
end_date = start_date + pd.Timedelta(days=29)
df_t1_30_days = df_t1_sorted[(df_t1_sorted['VISIT_DATE'] >= start_date) & (df_t1_sorted['VISIT_DATE'] <= end_date)]
df_aggregated_t1 = df_t1_30_days.groupby('VISIT_DATE')['VISITED_STORES'].sum().reset_index()
kpi1_all_barplot = px.bar(df_aggregated_t1, x='VISIT_DATE', y='VISITED_STORES', color_discrete_sequence=['LightSalmon'])

# Layout
kpi1_all_barplot.update_layout(
    title='Visited Stores in the Last 30 Days for ALL BDRs',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_all_barplot.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi1_all_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)
### Parâmetros para graficos por BDR

max_date = df_t1['VISIT_DATE'].max()
min_date = df_t1['VISIT_DATE'].min()
date_range = pd.date_range(start=min_date, end=max_date)
dates_df = pd.DataFrame(date_range, columns=['VISIT_DATE'])

###### BRAM

df_t1_bram['VISIT_DATE'] = pd.to_datetime(df_t1_bram['VISIT_DATE'])

visits_per_day_bram = df_t1_bram.groupby(df_t1_bram['VISIT_DATE'].dt.date)['VISITED_STORES'].sum().reset_index()
visits_per_day_bram['VISIT_DATE'] = pd.to_datetime(visits_per_day_bram['VISIT_DATE'])

full_data = pd.merge(dates_df, visits_per_day_bram, on='VISIT_DATE', how='left').fillna(0)
full_data['VISIT_DATE'] = full_data['VISIT_DATE'].dt.date

kpi1_bram_barplot = px.bar(full_data, x='VISIT_DATE', y='VISITED_STORES', title='Number of Visits per Day for the Last 30 Days')

kpi1_bram_barplot.update_layout(
    title='Visited Stores in the Last 30 Days for Bram',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_bram_barplot.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside',
)

kpi1_bram_barplot.update_traces(marker_color='lightblue', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_bram_barplot.data[0].y]))

kpi1_bram_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Harris
df_t1_harris['VISIT_DATE'] = pd.to_datetime(df_t1_harris['VISIT_DATE'])

visits_per_day_harris = df_t1_harris.groupby(df_t1_harris['VISIT_DATE'].dt.date)['VISITED_STORES'].sum().reset_index()
visits_per_day_harris['VISIT_DATE'] = pd.to_datetime(visits_per_day_harris['VISIT_DATE'])

full_data_harris = pd.merge(dates_df, visits_per_day_harris, on='VISIT_DATE', how='left').fillna(0)
full_data_harris['VISIT_DATE'] = full_data_harris['VISIT_DATE'].dt.date

kpi1_harris_barplot = px.bar(full_data_harris, x='VISIT_DATE', y='VISITED_STORES', title='Number of Visits per Day for the Last 30 Days')

kpi1_harris_barplot.update_layout(
    title='Visited Stores in the Last 30 Days for Harris',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_harris_barplot.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside',
)

kpi1_harris_barplot.update_traces(marker_color='lightblue', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_harris_barplot.data[0].y]))

kpi1_harris_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Cheryl
df_t1_cheryl['VISIT_DATE'] = pd.to_datetime(df_t1_cheryl['VISIT_DATE'])

visits_per_day_cheryl = df_t1_cheryl.groupby(df_t1_cheryl['VISIT_DATE'].dt.date)['VISITED_STORES'].sum().reset_index()
visits_per_day_cheryl['VISIT_DATE'] = pd.to_datetime(visits_per_day_cheryl['VISIT_DATE'])

full_data_cheryl = pd.merge(dates_df, visits_per_day_cheryl, on='VISIT_DATE', how='left').fillna(0)
full_data_cheryl['VISIT_DATE'] = full_data_cheryl['VISIT_DATE'].dt.date

kpi1_cheryl_barplot = px.bar(full_data_cheryl, x='VISIT_DATE', y='VISITED_STORES', title='Number of Visits per Day for the Last 30 Days')

kpi1_cheryl_barplot.update_layout(
    title='Visited Stores in the Last 30 Days for Cheryl',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_cheryl_barplot.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside',
)

kpi1_cheryl_barplot.update_traces(marker_color='lightblue', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_cheryl_barplot.data[0].y]))

kpi1_cheryl_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Christian
df_t1_christian['VISIT_DATE'] = pd.to_datetime(df_t1_christian['VISIT_DATE'])

visits_per_day_christian = df_t1_christian.groupby(df_t1_christian['VISIT_DATE'].dt.date)['VISITED_STORES'].sum().reset_index()
visits_per_day_christian['VISIT_DATE'] = pd.to_datetime(visits_per_day_christian['VISIT_DATE'])

full_data_christian = pd.merge(dates_df, visits_per_day_christian, on='VISIT_DATE', how='left').fillna(0)
full_data_christian['VISIT_DATE'] = full_data_christian['VISIT_DATE'].dt.date

kpi1_christian_barplot = px.bar(full_data_christian, x='VISIT_DATE', y='VISITED_STORES', title='Number of Visits per Day for the Last 30 Days')

kpi1_christian_barplot.update_layout(
    title='Visited Stores in the Last 30 Days for Christian',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_christian_barplot.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside',
)

kpi1_christian_barplot.update_traces(marker_color='lightblue', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_christian_barplot.data[0].y]))

kpi1_christian_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Iwan Dwiarsono
df_t1_iwan['VISIT_DATE'] = pd.to_datetime(df_t1_iwan['VISIT_DATE'])

visits_per_day_iwan = df_t1_iwan.groupby(df_t1_iwan['VISIT_DATE'].dt.date)['VISITED_STORES'].sum().reset_index()
visits_per_day_iwan['VISIT_DATE'] = pd.to_datetime(visits_per_day_iwan['VISIT_DATE'])

full_data_iwan = pd.merge(dates_df, visits_per_day_iwan, on='VISIT_DATE', how='left').fillna(0)
full_data_iwan['VISIT_DATE'] = full_data_iwan['VISIT_DATE'].dt.date

kpi1_iwan_barplot = px.bar(full_data_iwan, x='VISIT_DATE', y='VISITED_STORES', title='Number of Visits per Day for the Last 30 Days')

kpi1_iwan_barplot.update_layout(
    title='Visited Stores in the Last 30 Days for Iwan Dwiarsono',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_iwan_barplot.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside',
)

kpi1_iwan_barplot.update_traces(marker_color='lightblue', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_iwan_barplot.data[0].y]))

kpi1_iwan_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Dian
df_t1_dian['VISIT_DATE'] = pd.to_datetime(df_t1_dian['VISIT_DATE'])

visits_per_day_dian = df_t1_dian.groupby(df_t1_dian['VISIT_DATE'].dt.date)['VISITED_STORES'].sum().reset_index()
visits_per_day_dian['VISIT_DATE'] = pd.to_datetime(visits_per_day_dian['VISIT_DATE'])

full_data_dian = pd.merge(dates_df, visits_per_day_dian, on='VISIT_DATE', how='left').fillna(0)
full_data_dian['VISIT_DATE'] = full_data_dian['VISIT_DATE'].dt.date

kpi1_dian_barplot = px.bar(full_data_dian, x='VISIT_DATE', y='VISITED_STORES', title='Number of Visits per Day for the Last 30 Days')

kpi1_dian_barplot.update_layout(
    title='Visited Stores in the Last 30 Days for Dian',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_dian_barplot.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside',
)

kpi1_dian_barplot.update_traces(marker_color='lightblue', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_dian_barplot.data[0].y]))

kpi1_dian_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)


###### Alvis
df_t1_alvis['VISIT_DATE'] = pd.to_datetime(df_t1_alvis['VISIT_DATE'])

visits_per_day_alvis = df_t1_alvis.groupby(df_t1_alvis['VISIT_DATE'].dt.date)['VISITED_STORES'].sum().reset_index()
visits_per_day_alvis['VISIT_DATE'] = pd.to_datetime(visits_per_day_alvis['VISIT_DATE'])

full_data_alvis = pd.merge(dates_df, visits_per_day_alvis, on='VISIT_DATE', how='left').fillna(0)
full_data_alvis['VISIT_DATE'] = full_data_alvis['VISIT_DATE'].dt.date

kpi1_alvis_barplot = px.bar(full_data_dian, x='VISIT_DATE', y='VISITED_STORES', title='Number of Visits per Day for the Last 30 Days')

kpi1_alvis_barplot.update_layout(
    title='Visited Stores in the Last 30 Days for Alvis',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_alvis_barplot.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside',
)

kpi1_alvis_barplot.update_traces(marker_color='lightblue', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_alvis_barplot.data[0].y]))

kpi1_alvis_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

########## KPI1 per BDR in last 30 days
df_aggregated_t1_BDR = df_t1_30_days.groupby('BDR_ID')['VISITED_STORES'].sum().reset_index()
df_aggregated_t1_BDR = df_aggregated_t1_BDR.sort_values(by='VISITED_STORES', ascending=False)
kpi1_all_barplot_bdr = px.bar(df_aggregated_t1_BDR, x='BDR_ID', y='VISITED_STORES', color_discrete_sequence=['LightSalmon'])

# Layout
kpi1_all_barplot_bdr.update_layout(
    title='Visited Stores in the Last 30 Days per BDR',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi1_all_barplot_bdr.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)


#------------------------------------------------------------------------------------------------------
#### App
# Abas

abas = st.tabs(["In-scope"])
aba0 = abas[0]


# Aba0
with aba0:
    colA = st.columns(1)
    colB = st.columns(1)
    colC = st.columns(1)
    colC_1 = st.columns(1)
    colD = st.columns(2)
    colE = st.columns(2)
    colF = st.columns(2)
    colG = st.columns(2)

# Colunas

with colA[0]:
    st.image(blob_content_logo, use_column_width='always')

with colB[0]:
    st.markdown("""
    <style>
    .fonte-personalizada1 {
        font-size: 30px;
        font-style: bold;
    }
    </style>
    <div class="fonte-personalizada1">
        KPI's management - Indonesia
    </div>
    """, unsafe_allow_html=True)

with colC[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        1.	Number of stores visited per day per BDR.
    </div>
    """, unsafe_allow_html=True)


with colC_1[0]:
    st.plotly_chart(kpi1_all_barplot_bdr, use_container_width=True)

with colD[0]:
    st.plotly_chart(kpi1_all_barplot)
with colD[1]:
    st.plotly_chart(kpi1_bram_barplot)

with colE[0]:
    st.plotly_chart(kpi1_harris_barplot)
with colE[1]:
    st.plotly_chart(kpi1_cheryl_barplot)

with colF[0]:
    st.plotly_chart(kpi1_christian_barplot)
with colF[1]:
    st.plotly_chart(kpi1_iwan_barplot)

with colG[0]:
    st.plotly_chart(kpi1_dian_barplot)
with colG[1]:
    st.plotly_chart(kpi1_alvis_barplot)





