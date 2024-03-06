# importando arquivos
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceExistsError
from io import StringIO
import os
import plotly.express as px
import plotly.graph_objects as go

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
# funções de uso geral

def formata_numero(valor, prefixo=''):
    if valor < 1000:
        return f'{prefixo}{valor:.2f}'
    elif valor < 1000000:
        return f'{prefixo}{valor / 1000:.2f} k IDR'
    elif valor < 1000000000:
        return f'{prefixo}{valor / 1000000:.2f} mi IDR'
    else:
        return f'{prefixo}{valor / 1000000000:.2f} bi IDR'
    

def formata_percentual(valor, sufixo='%'):
    return f'{valor*100:.1f}{sufixo}'     


def style_table(df, title):
    def format_as_percent(value):
        if isinstance(value, (int, float)):
            return '{:,.0f}%'.format(value * 100).replace(',', '.')
        return value

    # Apply styles to the DataFrame
    styler_pct = df.style.format(format_as_percent) \
        .set_table_styles([
            {'selector': 'thead th',
             'props': [('background-color', '#1f77b4'), ('color', 'black'), ('font-weight', 'bold')]},
            {'selector': 'td',
             'props': [('text-align', 'center')]}
        ], overwrite=False) \
        .applymap(lambda v: 'color: red' if isinstance(v, float) and v < 0 else 'color: black') \
        .background_gradient(cmap='RdYlGn', axis=None, low=0.3, high=0.7) \
        .set_caption(title) \
        .set_table_styles([
            {'selector': 'caption',
             'props': [('caption-side', 'top'), ('font-weight', 'bold'), ('font-size', '1.5em')]}
        ], overwrite=False)

    return styler_pct   

#------------------------------------------------------------------------------------------------------
#### Mandar arquivos na pasta DataID para o Azure Blob Storage
# upload_files_to_blob_storage(local_file_path, container_client, overwrite=True)

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

blob_name = 't6.csv'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content = blob_client.download_blob().content_as_text()
data_t6 = StringIO(blob_content)
df_t6 = pd.read_csv(data_t6)

##### Imagens

logo = "logo.png"
blob_client_logo = blob_service_client.get_blob_client(container=container_name, blob=logo)
blob_content_logo = blob_client_logo.download_blob().readall()

#------------------------------------------------------------------------------------------------------
## Manipulando tabelas

### Alias column for BDR

BDR_dict = {
    "6389058_BDR001": "Dinamis Artha Sukses",
    "6658562_BDR001": "RMS Jakarta Selatan",
    "6421535_BDR001": "RMS Depok",
    "6828128_BDR001": "RMS Bogor",
    "6713130_BDR001": "RMS Bekasi",
    "5653270_BDR001":"ASR",
    "6174675_BDR001": "CMP"
}

df_t1['BDR Name'] = df_t1['BDR_ID'].map(BDR_dict)
df_t2['BDR Name'] = df_t2['bdr_id'].map(BDR_dict)
df_t3['BDR Name'] = df_t3['bdr_id'].map(BDR_dict)
df_t4['BDR Name'] = df_t4['BDR_ID'].map(BDR_dict)
df_t5['BDR Name'] = df_t5['BDR_ID'].map(BDR_dict)
df_t6['BDR Name'] = df_t6['bdr_id'].map(BDR_dict)

df_t1 = df_t1[df_t1['BDR Name'].notnull()]
df_t2 = df_t2[df_t2['BDR Name'].notnull()]
df_t3 = df_t3[df_t3['BDR Name'].notnull()]
df_t4 = df_t4[df_t4['BDR Name'].notnull()]
df_t5 = df_t5[df_t5['BDR Name'].notnull()]
df_t6 = df_t6[df_t6['BDR Name'].notnull()]

# Mostrar apenas os últimos 30 dias

# df_t1['VISIT_DATE'] = pd.to_datetime(df_t1['VISIT_DATE'])
# latest_date_t1 = df_t1['VISIT_DATE'].max()
# date_limit_t1 = latest_date_t1 - pd.Timedelta(days=30)
# df_t1 = df_t1[df_t1['VISIT_DATE'] > date_limit_t1]

# df_t2['DATE'] = pd.to_datetime(df_t2['DATE'])
# latest_date_t2 = df_t2['DATE'].max()
# date_limit_t2 = latest_date_t2 - pd.Timedelta(days=30)
# df_t2 = df_t2[df_t2['DATE'] > date_limit_t2]

# df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
# latest_date_t3 = df_t3['DAY'].max()
# date_limit_t3 = latest_date_t3 - pd.Timedelta(days=30)
# df_t3 = df_t3[df_t3['DAY'] > date_limit_t3]

# df_t4['DATE'] = pd.to_datetime(df_t4['DATE'])
# latest_date_t4 = df_t4['DATE'].max()
# date_limit_t4 = latest_date_t4 - pd.Timedelta(days=30)
# df_t4 = df_t4[df_t4['DATE'] > date_limit_t4]

# df_t5['DATE'] = pd.to_datetime(df_t5['DATE'])
# latest_date_t5 = df_t5['DATE'].max()
# date_limit_t5 = latest_date_t5 - pd.Timedelta(days=30)
# df_t5 = df_t5[df_t5['DATE'] > date_limit_t5]

#### DFs do dia anterior
latest_date_t1 = df_t1['VISIT_DATE'].max()
latest_date_t2 = df_t2['DATE'].max()
latest_date_t3 = df_t3['DAY'].max()
latest_date_t4 = df_t4['DATE'].max()
latest_date_t5 = df_t5['DATE'].max()

df_t1_dm1 = df_t1[df_t1['VISIT_DATE'] != latest_date_t1]
df_t2_dm1 = df_t2[df_t2['DATE'] != latest_date_t2]
df_t3_dm1 = df_t3[df_t3['DAY'] != latest_date_t3]
df_t4_dm1 = df_t4[df_t4['DATE'] != latest_date_t4]
df_t5_dm1 = df_t5[df_t5['DATE'] != latest_date_t5]

df_t1_dm1['BDR Name'] = df_t1_dm1['BDR_ID'].map(BDR_dict)
df_t2_dm1['BDR Name'] = df_t2_dm1['bdr_id'].map(BDR_dict)
df_t3_dm1['BDR Name'] = df_t3_dm1['bdr_id'].map(BDR_dict)
df_t4_dm1['BDR Name'] = df_t4_dm1['BDR_ID'].map(BDR_dict)
df_t5_dm1['BDR Name'] = df_t5_dm1['BDR_ID'].map(BDR_dict)

df_t1_dm1 = df_t1_dm1[df_t1_dm1['BDR Name'].notnull()]
df_t2_dm1 = df_t2_dm1[df_t2_dm1['BDR Name'].notnull()]
df_t3_dm1 = df_t3_dm1[df_t3_dm1['BDR Name'].notnull()]
df_t4_dm1 = df_t4_dm1[df_t4_dm1['BDR Name'].notnull()]
df_t5_dm1 = df_t5_dm1[df_t5_dm1['BDR Name'].notnull()]

### Tabelas para KPI 1 - N de visitas

df_t1_bram = df_t1[df_t1['BDR Name'] == 'Dinamis Artha Sukses']
df_t1_harris = df_t1[df_t1['BDR Name'] == 'RMS Jakarta Selatan']
df_t1_cheryl = df_t1[df_t1['BDR Name'] == 'RMS Depok']
df_t1_christian = df_t1[df_t1['BDR Name'] == 'RMS Bogor']
df_t1_iwan = df_t1[df_t1['BDR Name'] == 'RMS Bekasi']
df_t1_dian = df_t1[df_t1['BDR Name'] == 'ASR']
df_t1_alvis = df_t1[df_t1['BDR Name'] == 'CMP']

# Data to csv for downloading button

csv_t1 = df_t1.to_csv(index=False).encode('utf-8')
csv_t2 = df_t2.to_csv(index=False).encode('utf-8')
csv_t3 = df_t3.to_csv(index=False).encode('utf-8')
csv_t4 = df_t4.to_csv(index=False).encode('utf-8')
csv_t5 = df_t5.to_csv(index=False).encode('utf-8')
csv_t6 = df_t6.to_csv(index=False).encode('utf-8')

#------------------------------------------------------------------------------------------------------
# Criando visualizações

max_date = df_t1['VISIT_DATE'].max()
max_date = pd.to_datetime(max_date)

##Gráfico de barras KPI 1 - N de visitas
###### All BDR's
df_t1['VISIT_DATE'] = pd.to_datetime(df_t1['VISIT_DATE'])
df_t1_sorted = df_t1.sort_values(by='VISIT_DATE')

start_date = max_date - pd.Timedelta(days=29)
end_date = max_date
df_aggregated_t1 = df_t1_sorted.groupby('VISIT_DATE')['VISITS'].sum().reset_index()
kpi1_all_barplot = px.bar(df_aggregated_t1, x='VISIT_DATE', y='VISITS', color_discrete_sequence=['#1a2634'])

# Layout
kpi1_all_barplot.update_layout(
    title='Visits in the Last 30 Days for ALL BDRs',
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

visits_per_day_bram = df_t1_bram.groupby(df_t1_bram['VISIT_DATE'].dt.date)['VISITS'].sum().reset_index()
visits_per_day_bram['VISIT_DATE'] = pd.to_datetime(visits_per_day_bram['VISIT_DATE'])

full_data = pd.merge(dates_df, visits_per_day_bram, on='VISIT_DATE', how='left').fillna(0)
full_data['VISIT_DATE'] = full_data['VISIT_DATE'].dt.date

kpi1_bram_barplot = px.bar(full_data, x='VISIT_DATE', y='VISITS', title='Number of Visits per Day for the Last 30 Days')

kpi1_bram_barplot.update_layout(
    title='Visits in the Last 30 Days for Dinamis Artha Sukses',
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

kpi1_bram_barplot.update_traces(marker_color='#1a2634', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_bram_barplot.data[0].y]))

kpi1_bram_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Harris
df_t1_harris['VISIT_DATE'] = pd.to_datetime(df_t1_harris['VISIT_DATE'])

visits_per_day_harris = df_t1_harris.groupby(df_t1_harris['VISIT_DATE'].dt.date)['VISITS'].sum().reset_index()
visits_per_day_harris['VISIT_DATE'] = pd.to_datetime(visits_per_day_harris['VISIT_DATE'])

full_data_harris = pd.merge(dates_df, visits_per_day_harris, on='VISIT_DATE', how='left').fillna(0)
full_data_harris['VISIT_DATE'] = full_data_harris['VISIT_DATE'].dt.date

kpi1_harris_barplot = px.bar(full_data_harris, x='VISIT_DATE', y='VISITS', title='Number of Visits per Day for the Last 30 Days')

kpi1_harris_barplot.update_layout(
    title='Visits in the Last 30 Days for RMS Jakarta Selatan',
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

kpi1_harris_barplot.update_traces(marker_color='#1a2634', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_harris_barplot.data[0].y]))

kpi1_harris_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Cheryl
df_t1_cheryl['VISIT_DATE'] = pd.to_datetime(df_t1_cheryl['VISIT_DATE'])

visits_per_day_cheryl = df_t1_cheryl.groupby(df_t1_cheryl['VISIT_DATE'].dt.date)['VISITS'].sum().reset_index()
visits_per_day_cheryl['VISIT_DATE'] = pd.to_datetime(visits_per_day_cheryl['VISIT_DATE'])

full_data_cheryl = pd.merge(dates_df, visits_per_day_cheryl, on='VISIT_DATE', how='left').fillna(0)
full_data_cheryl['VISIT_DATE'] = full_data_cheryl['VISIT_DATE'].dt.date

kpi1_cheryl_barplot = px.bar(full_data_cheryl, x='VISIT_DATE', y='VISITS', title='Number of Visits per Day for the Last 30 Days')

kpi1_cheryl_barplot.update_layout(
    title='Visits in the Last 30 Days for RMS Depok',
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

kpi1_cheryl_barplot.update_traces(marker_color='#1a2634', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_cheryl_barplot.data[0].y]))

kpi1_cheryl_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Christian
df_t1_christian['VISIT_DATE'] = pd.to_datetime(df_t1_christian['VISIT_DATE'])

visits_per_day_christian = df_t1_christian.groupby(df_t1_christian['VISIT_DATE'].dt.date)['VISITS'].sum().reset_index()
visits_per_day_christian['VISIT_DATE'] = pd.to_datetime(visits_per_day_christian['VISIT_DATE'])

full_data_christian = pd.merge(dates_df, visits_per_day_christian, on='VISIT_DATE', how='left').fillna(0)
full_data_christian['VISIT_DATE'] = full_data_christian['VISIT_DATE'].dt.date

kpi1_christian_barplot = px.bar(full_data_christian, x='VISIT_DATE', y='VISITS', title='Number of Visits per Day for the Last 30 Days')

kpi1_christian_barplot.update_layout(
    title='Visits in the Last 30 Days for RMS Bogor',
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

kpi1_christian_barplot.update_traces(marker_color='#1a2634', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_christian_barplot.data[0].y]))

kpi1_christian_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Iwan Dwiarsono
df_t1_iwan['VISIT_DATE'] = pd.to_datetime(df_t1_iwan['VISIT_DATE'])

visits_per_day_iwan = df_t1_iwan.groupby(df_t1_iwan['VISIT_DATE'].dt.date)['VISITS'].sum().reset_index()
visits_per_day_iwan['VISIT_DATE'] = pd.to_datetime(visits_per_day_iwan['VISIT_DATE'])

full_data_iwan = pd.merge(dates_df, visits_per_day_iwan, on='VISIT_DATE', how='left').fillna(0)
full_data_iwan['VISIT_DATE'] = full_data_iwan['VISIT_DATE'].dt.date

kpi1_iwan_barplot = px.bar(full_data_iwan, x='VISIT_DATE', y='VISITS', title='Number of Visits per Day for the Last 30 Days')

kpi1_iwan_barplot.update_layout(
    title='Visits in the Last 30 Days for RMS Bekasi',
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

kpi1_iwan_barplot.update_traces(marker_color='#1a2634', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_iwan_barplot.data[0].y]))

kpi1_iwan_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

###### Dian
df_t1_dian['VISIT_DATE'] = pd.to_datetime(df_t1_dian['VISIT_DATE'])

visits_per_day_dian = df_t1_dian.groupby(df_t1_dian['VISIT_DATE'].dt.date)['VISITS'].sum().reset_index()
visits_per_day_dian['VISIT_DATE'] = pd.to_datetime(visits_per_day_dian['VISIT_DATE'])

full_data_dian = pd.merge(dates_df, visits_per_day_dian, on='VISIT_DATE', how='left').fillna(0)
full_data_dian['VISIT_DATE'] = full_data_dian['VISIT_DATE'].dt.date

kpi1_dian_barplot = px.bar(full_data_dian, x='VISIT_DATE', y='VISITS', title='Number of Visits per Day for the Last 30 Days')

kpi1_dian_barplot.update_layout(
    title='Visits in the Last 30 Days for ASR',
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

kpi1_dian_barplot.update_traces(marker_color='#1a2634', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_dian_barplot.data[0].y]))

kpi1_dian_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)


###### Alvis
df_t1_alvis['VISIT_DATE'] = pd.to_datetime(df_t1_alvis['VISIT_DATE'])

visits_per_day_alvis = df_t1_alvis.groupby(df_t1_alvis['VISIT_DATE'].dt.date)['VISITS'].sum().reset_index()
visits_per_day_alvis['VISIT_DATE'] = pd.to_datetime(visits_per_day_alvis['VISIT_DATE'])

full_data_alvis = pd.merge(dates_df, visits_per_day_alvis, on='VISIT_DATE', how='left').fillna(0)
full_data_alvis['VISIT_DATE'] = full_data_alvis['VISIT_DATE'].dt.date

kpi1_alvis_barplot = px.bar(full_data_dian, x='VISIT_DATE', y='VISITS', title='Number of Visits per Day for the Last 30 Days')

kpi1_alvis_barplot.update_layout(
    title='Visits in the Last 30 Days for CMP',
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

kpi1_alvis_barplot.update_traces(marker_color='#1a2634', texttemplate='%{y}', textposition='outside',
    textfont=dict(color=["rgba(0,0,0,0)" if y == 0 else "rgba(0,0,0,1)" for y in kpi1_alvis_barplot.data[0].y]))

kpi1_alvis_barplot.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

########## KPI1 per BDR ALLD
df_aggregated_t1_BDR = df_t1_sorted.groupby('BDR Name')['VISITS'].sum().reset_index()
df_aggregated_t1_BDR = df_aggregated_t1_BDR.sort_values(by='VISITS', ascending=False)
kpi1_all_barplot_bdr = px.bar(df_aggregated_t1_BDR, x='BDR Name', y='VISITS', color_discrete_sequence=['#ffcc00'])

kpi1_all_barplot_bdr.update_layout(
    title='Visits ALLD per BDR',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

kpi1_all_barplot_bdr.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi1_all_barplot_bdr.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

########## KPI1 per BDR ALLD - Planned
df_aggregated_t1_BDR_p = df_t1_sorted.groupby('BDR Name')['PLANNED_VISITS'].sum().reset_index()
df_aggregated_t1_BDR_p = df_aggregated_t1_BDR_p.sort_values(by='PLANNED_VISITS', ascending=False)
kpi1_all_barplot_bdr_p = px.bar(df_aggregated_t1_BDR_p, x='BDR Name', y='PLANNED_VISITS', color_discrete_sequence=['#ffcc00'])

kpi1_all_barplot_bdr_p.update_layout(
    title='Visits Planned ALLD per BDR',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

kpi1_all_barplot_bdr_p.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi1_all_barplot_bdr_p.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

########## KPI1 per BDR in last latest DAY

df_tf_mtd = df_t1[df_t1['VISIT_DATE'] == max_date]
df_tf_mtd_agg = df_tf_mtd.groupby('BDR Name')['VISITS'].sum().reset_index()
df_tf_mtd_agg = df_tf_mtd_agg.sort_values(by='VISITS', ascending=False)
kpi1_all_barplot_bdr_mtd = px.bar(df_tf_mtd_agg, x='BDR Name', y='VISITS', color_discrete_sequence=['#ffcc00'])
formatted_max_date = max_date.strftime('%Y-%m-%d')

kpi1_all_barplot_bdr_mtd.update_layout(
    title=f'Visits on {formatted_max_date} per BDR',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

kpi1_all_barplot_bdr_mtd.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi1_all_barplot_bdr_mtd.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

#### Visits stacked

df_t1['visits_format'] = df_t1['VISITS'].apply(lambda x: f'{x:.0f}')
df_t1['VISIT_DATE'] = pd.to_datetime(df_t1['VISIT_DATE'])
df_t1_sort_new = df_t1.sort_values(by='VISIT_DATE', ascending=True)
df_t1_sort_new['FORMATTED_DATE'] = df_t1['VISIT_DATE'].dt.strftime('%d-%b-%Y')
df_t1_stacked = df_t1_sort_new.groupby(['FORMATTED_DATE', 'BDR Name'])['visits_format'].sum().reset_index()
df_t1_stacked['DATE_FOR_SORTING'] = pd.to_datetime(df_t1_stacked['FORMATTED_DATE'], format='mixed', errors='coerce')
df_t1_pivot = df_t1_stacked.pivot_table(index='DATE_FOR_SORTING', columns='BDR Name', values='visits_format', aggfunc='sum').fillna(0)

df_t1_pivot.index = df_t1_pivot.index.strftime('%d-%b-%Y')
visits_stacked = go.Figure()
colors = px.colors.sequential.Blues

blue_palette = ['#1a2634', '#203e5f', '#ffcc00', '#fee5b1', '#393e46', '#393e46', '#acdbdf']

for i, vendor in enumerate(df_t1_pivot.columns):
    text_labels = [f'{v}' if v != '0' else '' for v in df_t1_pivot[vendor]]

    visits_stacked.add_trace(go.Bar(
        x=df_t1_pivot.index, 
        y=df_t1_pivot[vendor], 
        name=vendor,
        marker_color=blue_palette[i % len(blue_palette)],  # Use the color palette
        text=text_labels,  # Use the prepared text labels
        textposition='outside'  # Position labels outside the bars
    ))

visits_stacked.update_layout(barmode='stack', title='Daily Visits by BDR', xaxis_title='', yaxis_title='')
for i, trace in enumerate(visits_stacked.data):
    trace.text = [f'{v}' if v != 0 else '' for v in df_t1_pivot[trace.name]]

# Customizing the figure's layout
visits_stacked.update_layout(
    barmode='stack',
    title='Daily Visits by BDR',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)


visits_stacked.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily Visits by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )

#### PLANNED VISITS STACKED
df_t1['visits_format_planned'] = df_t1['PLANNED_VISITS'].apply(lambda x: f'{x:.0f}')
df_t1['VISIT_DATE'] = pd.to_datetime(df_t1['VISIT_DATE'])
df_t1_sort_new = df_t1.sort_values(by='VISIT_DATE', ascending=True)
df_t1_sort_new['FORMATTED_DATE'] = df_t1['VISIT_DATE'].dt.strftime('%d-%b-%Y')
df_t1_stacked = df_t1_sort_new.groupby(['FORMATTED_DATE', 'BDR Name'])['visits_format_planned'].sum().reset_index()
df_t1_stacked['DATE_FOR_SORTING'] = pd.to_datetime(df_t1_stacked['FORMATTED_DATE'], format='mixed', errors='coerce')
df_t1_pivot_planned = df_t1_stacked.pivot_table(index='DATE_FOR_SORTING', columns='BDR Name', values='visits_format_planned', aggfunc='sum').fillna(0)

df_t1_pivot_planned.index = df_t1_pivot_planned.index.strftime('%d-%b-%Y')
visits_stacked_planned = go.Figure()
colors = px.colors.sequential.Blues

blue_palette = ['#1a2634', '#203e5f', '#ffcc00', '#fee5b1', '#393e46', '#393e46', '#acdbdf']

for i, vendor in enumerate(df_t1_pivot_planned.columns):
    text_labels = [f'{v}' if v != '0' else '' for v in df_t1_pivot[vendor]]

    visits_stacked_planned.add_trace(go.Bar(
        x=df_t1_pivot.index, 
        y=df_t1_pivot[vendor], 
        name=vendor,
        marker_color=blue_palette[i % len(blue_palette)],  # Use the color palette
        text=text_labels,  # Use the prepared text labels
        textposition='outside'  # Position labels outside the bars
    ))

visits_stacked_planned.update_layout(barmode='stack', title='Daily Visits by BDR', xaxis_title='', yaxis_title='')
for i, trace in enumerate(visits_stacked_planned.data):
    trace.text = [f'{v}' if v != 0 else '' for v in df_t1_pivot[trace.name]]

# Customizing the figure's layout
visits_stacked_planned.update_layout(
    barmode='stack',
    title='Daily Planned Visits by BDR',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)


visits_stacked_planned.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily Planned Visits by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )

#------------------------------------------------------------------------------------------------------
########## KPI 2
#### Agregado por dia

max_date_t2 = df_t2['DATE'].max()
max_date_t2 = pd.to_datetime(max_date_t2)

df_t2['DATE'] = pd.to_datetime(df_t2['DATE'])
df_t2_sorted = df_t2.sort_values(by='DATE')

start_date = max_date_t2 - pd.Timedelta(days=29)

df_aggregated_t2 = df_t2_sorted.groupby('DATE')['count_registered_stores'].sum().reset_index()
kpi2_barplot_dateagg = px.bar(df_aggregated_t2, x='DATE', y='count_registered_stores', color_discrete_sequence=['#1a2634'])

kpi2_barplot_dateagg.update_layout(
    title='Registered stores in Last 30 Days for ALL BDRs',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi2_barplot_dateagg.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi2_barplot_dateagg.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)



# KPI 2 per BDR

df_aggregated_t2_BDR = df_t2_sorted.groupby('BDR Name')['count_registered_stores'].sum().reset_index()
df_aggregated_t2_BDR_sorted = df_aggregated_t2_BDR.sort_values(by='count_registered_stores', ascending = False)
kpi2_all_barplot_bdr = px.bar(df_aggregated_t2_BDR_sorted, x='BDR Name', y='count_registered_stores', color_discrete_sequence=['#ffcc00'])
formatted_max_date_t2 = max_date_t2.strftime('%Y-%m-%d')

kpi2_all_barplot_bdr.update_layout(
    title='Registered Stores ALLD per BDR',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

kpi2_all_barplot_bdr.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi2_all_barplot_bdr.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

# KPI 2 in the latest day

df_tf2_mtd = df_t2[df_t2['DATE'] == max_date_t2]
df_tf2_mtd_agg = df_tf2_mtd.groupby('BDR Name')['count_registered_stores'].sum().reset_index()
df_tf2_mtd_agg = df_tf2_mtd_agg.sort_values(by='count_registered_stores', ascending=False)
kpi2_all_barplot_bdr_mtd = px.bar(df_tf2_mtd_agg, x='BDR Name', y='count_registered_stores', color_discrete_sequence=['#ffcc00'])
formatted_max_date_t2 = max_date_t2.strftime('%Y-%m-%d')

kpi2_all_barplot_bdr_mtd.update_layout(
    title=f'Registered Stores on {formatted_max_date_t2} per BDR',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

kpi2_all_barplot_bdr_mtd.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi2_all_barplot_bdr_mtd.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

#### Register stacked

df_t2['register_format'] = df_t2['count_registered_stores'].apply(lambda x: f'{x:.0f}')
df_t2['DATE'] = pd.to_datetime(df_t2['DATE'])
df_t2_sort_new = df_t2.sort_values(by='DATE', ascending=True)
df_t2_sort_new['FORMATTED_DATE'] = df_t2['DATE'].dt.strftime('%d-%b-%Y')
df_t2_stacked = df_t2_sort_new.groupby(['FORMATTED_DATE', 'BDR Name'])['register_format'].sum().reset_index()
df_t2_stacked['DATE_FOR_SORTING'] = pd.to_datetime(df_t2_stacked['FORMATTED_DATE'], format='%d-%b-%Y')
df_t2_pivot = df_t2_stacked.pivot(index='DATE_FOR_SORTING', columns='BDR Name', values='register_format').fillna(0)

df_t2_pivot.index = df_t2_pivot.index.strftime('%d-%b-%Y')
register_stacked = go.Figure()

blue_palette = ['#1a2634', '#203e5f', '#ffcc00', '#fee5b1', '#393e46', '#393e46', '#acdbdf']

for i, vendor in enumerate(df_t2_pivot.columns):
    text_labels = [f'{v}' if v != '0' else '' for v in df_t2_pivot[vendor]]

    register_stacked.add_trace(go.Bar(
        x=df_t2_pivot.index, 
        y=df_t2_pivot[vendor], 
        name=vendor,
        marker_color=blue_palette[i % len(blue_palette)],  # Use the color palette
        text=text_labels,  # Use the prepared text labels
        textposition='outside'  # Position labels outside the bars
    ))

register_stacked.update_layout(barmode='stack', title='Daily Registers by BDR', xaxis_title='', yaxis_title='')
for i, trace in enumerate(register_stacked.data):
    trace.text = [f'{v}' if v != 0 else '' for v in df_t2_pivot[trace.name]]

# Customizing the figure's layout
register_stacked.update_layout(
    barmode='stack',
    title='Daily Visits by BDR',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)


register_stacked.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily Registers by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )




#------------------------------------------------------------------------------------------------------
##### KPI 3.	Number & list of stores adopted (place order via apps) per day per BDR.

### Tratando a base
df_t3["TOTAL_ORDERS"] = df_t3["count_placed_orders_customer"] + df_t3["count_placed_orders_force"] + df_t3["count_placed_orders_grow"]
df_t3["bdr_id"] = df_t3["bdr_id"].fillna("TBD")

max_date_t3 = df_t3['DAY'].max()
max_date_t3 = pd.to_datetime(max_date_t3)

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_sorted = df_t3.sort_values(by='DAY')

df_t3_agg_bees = df_t3_sorted.groupby('BDR Name')['TOTAL_ORDERS'].sum().reset_index()
df_t3_agg_bees_sort = df_t3_agg_bees.sort_values(by='TOTAL_ORDERS', ascending=False)

# KPI 3 ORDERS - ALLD per BDR
kpi3_all_barplot_bdr = px.bar(df_t3_agg_bees_sort, x='BDR Name', y='TOTAL_ORDERS', color_discrete_sequence=['#ffcc00'])

kpi3_all_barplot_bdr.update_layout(
    title='BEES Orders ALLD per BDR',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

kpi3_all_barplot_bdr.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi3_all_barplot_bdr.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

# KPI 3 ORDERS - Latest Day

df_t3_mtd = df_t3[df_t3['DAY'] == max_date_t3]
df_t3_mtd_agg = df_t3_mtd.groupby('BDR Name')['TOTAL_ORDERS'].sum().reset_index()
df_t3_mtd_agg = df_t3_mtd_agg.sort_values(by='TOTAL_ORDERS', ascending=False)


kpi3_all_barplot_bdr_mtd = px.bar(df_t3_mtd_agg, x='BDR Name', y='TOTAL_ORDERS', color_discrete_sequence=['#ffcc00'])
formatted_max_date_t3 = max_date_t3.strftime('%Y-%m-%d')

kpi3_all_barplot_bdr_mtd.update_layout(
    title=f'BEES Orders on {formatted_max_date_t3} per BDR',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

kpi3_all_barplot_bdr_mtd.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi3_all_barplot_bdr_mtd.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

# Cumulative

df_t3_byday = df_t3.groupby('DAY')['TOTAL_ORDERS'].sum().reset_index()
df_t3_byday_sort = df_t3_byday.sort_values(by='DAY', ascending=True)
df_t3_byday_sort['CUMULATIVE_ORDERS'] = df_t3_byday_sort['TOTAL_ORDERS'].cumsum()

max_date_cum_t3 = df_t3_byday_sort['DAY'].max()
start_date_cum_t3 = df_t3_byday_sort['DAY'].min()
df_t3_last_30_days = df_t3_byday_sort[(df_t3_byday_sort['DAY'] >= start_date_cum_t3) & (df_t3['DAY'] <= max_date_cum_t3)]

date_range = pd.date_range(start=start_date_cum_t3, end=max_date_cum_t3)
date_range_df = pd.DataFrame(date_range, columns=['DAY'])
df_complete = date_range_df.merge(df_t3_byday_sort, on='DAY', how='left')

df_complete['TOTAL_ORDERS'] = df_complete['TOTAL_ORDERS'].fillna(0)

df_complete['CUMULATIVE_ORDERS'] = df_complete['TOTAL_ORDERS'].cumsum()
df_t3_last_30_days = df_complete[(df_complete['DAY'] >= start_date_cum_t3) & (df_complete['DAY'] <= max_date_cum_t3)]

kpi3_barplot_cum = px.bar(df_t3_last_30_days, x='DAY', y='CUMULATIVE_ORDERS', color_discrete_sequence=['#1a2634'])

kpi3_barplot_cum.update_layout(
    title='Cummulative BEES Orders in Last 30 Days for ALL BDRs',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi3_barplot_cum.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi3_barplot_cum.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)

####### Orders stacked
df_t3['Order_SUM'] = df_t3['count_placed_orders_customer'] + df_t3['count_placed_orders_force'] + df_t3['count_placed_orders_grow']

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_sort_new = df_t3.sort_values(by='DAY', ascending=True)
df_t3_sort_new['FORMATTED_DATE'] = df_t3['DAY'].dt.strftime('%d-%b-%Y')
df_t3_stacked = df_t3_sort_new.groupby(['FORMATTED_DATE', 'BDR Name'])['Order_SUM'].sum().reset_index()
df_t3_stacked['DATE_FOR_SORTING'] = pd.to_datetime(df_t3_stacked['FORMATTED_DATE'], format='%d-%b-%Y')
df_t3_pivot = df_t3_stacked.pivot(index='DATE_FOR_SORTING', columns='BDR Name', values='Order_SUM').fillna(0)

df_t3_pivot.index = df_t3_pivot.index.strftime('%d-%b-%Y')
order_stacked = go.Figure()

blue_palette = ['#1a2634', '#203e5f', '#ffcc00', '#fee5b1', '#393e46', '#393e46', '#acdbdf']

for i, vendor in enumerate(df_t3_pivot.columns):
    text_labels = [f'{v}' if v != '0' else '' for v in df_t3_pivot[vendor]]

    order_stacked.add_trace(go.Bar(
        x=df_t3_pivot.index, 
        y=df_t3_pivot[vendor], 
        name=vendor,
        marker_color=blue_palette[i % len(blue_palette)],  # Use the color palette
        text=text_labels,  # Use the prepared text labels
        textposition='outside'  # Position labels outside the bars
    ))

order_stacked.update_layout(barmode='stack', title='Daily Orders by BDR', xaxis_title='', yaxis_title='')
for i, trace in enumerate(order_stacked.data):
    trace.text = [f'{v}' if v != 0 else '' for v in df_t3_pivot[trace.name]]

order_stacked.update_layout(
    barmode='stack',
    title='Daily Orders by BDR',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)


order_stacked.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily Orders by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )

################### Orders Stacked by Channel

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_renamed = df_t3.rename(columns={
    'count_placed_orders_customer': 'Customer',
    'count_placed_orders_force': 'Force',
    'count_placed_orders_grow': 'Grow'
})

df_t3_sorted = df_t3_renamed.sort_values(by='DAY', ascending=True)

df_t3_order_empilhado = df_t3_sorted.groupby('DAY')[['Customer', 'Force', 'Grow']].sum().reset_index()

df_t3_order_empilhado['FORMATTED_DATE'] = df_t3_order_empilhado['DAY'].dt.strftime('%d-%b-%Y')

df_t3_order_empilhado = df_t3_order_empilhado.sort_values(by='DAY', ascending=True)

blue_scale = ['#1a2634', '#203e5f', '#ffcc00']

order_stacked_channel = px.bar(
    df_t3_order_empilhado, 
    x='FORMATTED_DATE', 
    y=['Customer', 'Force', 'Grow'],
    title='BEES Order Stacked by Channel',
    labels={'value': 'Orders', 'variable': 'Channel'},  # Keeps the axis labels
    color_discrete_sequence=blue_scale,
    text='value'
    )

order_stacked_channel.update_layout(
    xaxis=dict(tickangle=90, title=None, tickmode='linear'),  
    yaxis=dict(showgrid=False,showticklabels=False, title=None),
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    plot_bgcolor='white',
    height=600)

for trace in order_stacked_channel.data:
    non_zero_text = [t if t != 0 else '' for t in trace.y]

    trace.update(
        text=non_zero_text,
        texttemplate='%{text}',  # Since we've already formatted text, we use '%{text}'
        textposition='outside'
    )

################### Buyers Stacked by Channel

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_renamed_buyers = df_t3.rename(columns={
    'count_buyers_customer': 'Customer',
    'count_buyers_force': 'Force',
    'count_buyers_grow': 'Grow'
})

df_t3_sorted_buyer = df_t3_renamed_buyers.sort_values(by='DAY', ascending=True)

df_t3_order_empilhado_buyer = df_t3_sorted_buyer.groupby('DAY')[['Customer', 'Force', 'Grow']].sum().reset_index()

df_t3_order_empilhado_buyer['FORMATTED_DATE'] = df_t3_order_empilhado_buyer['DAY'].dt.strftime('%d-%b-%Y')

df_t3_order_empilhado_buyer = df_t3_order_empilhado_buyer.sort_values(by='DAY', ascending=True)

blue_scale = ['#1a2634', '#203e5f', '#ffcc00']

buyer_stacked_channel = px.bar(
    df_t3_order_empilhado_buyer, 
    x='FORMATTED_DATE', 
    y=['Customer', 'Force', 'Grow'],
    title='BEES Buyers Stacked by Channel',
    labels={'value': 'Orders', 'variable': 'Channel'},  # Keeps the axis labels
    color_discrete_sequence=blue_scale,
    text='value'
    )

buyer_stacked_channel.update_layout(
    xaxis=dict(tickangle=90, title=None, tickmode='linear'),  
    yaxis=dict(showgrid=False,showticklabels=False, title=None),
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    plot_bgcolor='white',
    height=600)

for trace in buyer_stacked_channel.data:
    non_zero_text = [t if t != 0 else '' for t in trace.y]

    trace.update(
        text=non_zero_text,
        texttemplate='%{text}',  # Since we've already formatted text, we use '%{text}'
        textposition='outside'
    )


#------------------------------------------------------------------------------------------------------
####### KPI 4.	Sales value per day per BDR and Count of orders 

df_t3['gmv_placed_customer'] = pd.to_numeric(df_t3['gmv_placed_customer'], errors='coerce').fillna(0)
df_t3['gmv_placed_force'] = pd.to_numeric(df_t3['gmv_placed_force'], errors='coerce').fillna(0)
df_t3['gmv_placed_grow'] = pd.to_numeric(df_t3['gmv_placed_grow'], errors='coerce').fillna(0)

df_t3['TOTAL_SALES'] = df_t3['gmv_placed_customer'] + df_t3['gmv_placed_force'] + df_t3['gmv_placed_grow']
df_t3['TOTAL_SALES'] = pd.to_numeric(df_t3['TOTAL_SALES'], errors='coerce').fillna(0)

df_t3_sales = df_t3.groupby('BDR Name')['TOTAL_SALES'].sum().reset_index()
df_t3_sales_notnull = df_t3_sales[(df_t3_sales['TOTAL_SALES'] != 0)]
df_t3_sales_notnull.dropna(subset=['TOTAL_SALES'], inplace=True)

df_t3_sales_notnull_sort = df_t3_sales_notnull.sort_values(by='TOTAL_SALES', ascending=False)

df_t3_sales_notnull_sort['TOTAL_SALES'] = df_t3_sales_notnull_sort['TOTAL_SALES'].fillna(0).round(1)

def custom_format(value):
    if value >= 1e6:  # If the value is in millions
        value = value / 1e6
        return f'{value:.2f}M IDR'
    elif value >= 1e3:  # If the value is in thousands
        value = value / 1e3
        return f'{value:.2f}K IDR'
    else:  # If the value is less than a thousand
        return f'{value:.2f} IDR'

# Apply the formatting function to your sales data
formatted_sales = df_t3_sales_notnull_sort['TOTAL_SALES'].apply(custom_format)

kpi4_all_barplot_bdr = px.bar(df_t3_sales_notnull_sort, x='BDR Name', y='TOTAL_SALES', color_discrete_sequence=['#ffcc00'], text=formatted_sales)

kpi4_all_barplot_bdr.update_layout(
    title='BEES Sales ALLD per BDR',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

kpi4_all_barplot_bdr.update_traces(
    hovertemplate="<b>%{x}</b><br>Total Sales: %{y:.2s}<extra></extra>",  # Use .2s for smart formatting
    textposition='outside'  # This positions the text on top of the bars  # Place the text above the bars
)

kpi4_all_barplot_bdr.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

# GVM Stacked per Channel

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_renamed_gmv = df_t3.rename(columns={
    'gmv_placed_customer': 'Customer',
    'gmv_placed_force': 'Force',
    'gmv_placed_grow': 'Grow'
})

df_t3_sorted_gmv = df_t3_renamed_gmv.sort_values(by='DAY', ascending=True)
df_t3_gmv_empilhado = df_t3_sorted_gmv.groupby('DAY')[['Customer', 'Force', 'Grow']].sum().reset_index()
df_t3_gmv_empilhado['FORMATTED_DATE'] = df_t3_gmv_empilhado['DAY'].dt.strftime('%d-%b-%Y')
df_t3_gmv_empilhado = df_t3_gmv_empilhado.sort_values(by='DAY', ascending=True)

blue_scale = ['#1a2634', '#203e5f', '#ffcc00']

gmv_stacked_channel = px.bar(
    df_t3_gmv_empilhado, 
    x='FORMATTED_DATE', 
    y=['Customer', 'Force', 'Grow'],
    title='BEES GMV Stacked by Channel',
    labels={'value': 'Orders', 'variable': 'Channel'},  # Keeps the axis labels
    color_discrete_sequence=blue_scale,
    text='value'
    )

gmv_stacked_channel.update_layout(
    xaxis=dict(tickangle=90, title=None, tickmode='linear'),  
    yaxis=dict(showgrid=False, showticklabels=False, title=None),
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    plot_bgcolor='white',
    height=600)

for trace in gmv_stacked_channel.data:
    formatted_text = [custom_format(value) if value != 0 else '' for value in trace.y]
    trace.update(
        hoverinfo='text',
        hovertext=[f"<b>{x}</b><br>{trace.name}: {custom_format(y)}" for x, y in zip(df_t3_gmv_empilhado['FORMATTED_DATE'], trace.y)],
        text=formatted_text,
        texttemplate='%{text}',  # Use the formatted text
        textposition='outside'
    )

gmv_stacked_channel.update_layout(
    xaxis=dict(tickangle=90, title=None),  
    yaxis=dict(showgrid=False, title=None),
    showlegend=True,
    plot_bgcolor='white')

##### GMV Stacked by BDR
df_t3['TOTAL_SALES'] = df_t3['TOTAL_SALES'] = df_t3['gmv_placed_customer'] + df_t3['gmv_placed_force'] + df_t3['gmv_placed_grow']

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_sort_new_gmv = df_t3.sort_values(by='DAY', ascending=True)
df_t3_sort_new_gmv['FORMATTED_DATE'] = df_t3['DAY'].dt.strftime('%d-%b-%Y')
df_t3_stacked_gmvbdr = df_t3_sort_new_gmv.groupby(['FORMATTED_DATE', 'BDR Name'])['TOTAL_SALES'].sum().reset_index()
df_t3_stacked_gmvbdr['DATE_FOR_SORTING'] = pd.to_datetime(df_t3_stacked_gmvbdr['FORMATTED_DATE'], format='%d-%b-%Y')
df_t3_pivot_gmvbdr = df_t3_stacked_gmvbdr.pivot(index='DATE_FOR_SORTING', columns='BDR Name', values='TOTAL_SALES').fillna(0)

df_t3_pivot_gmvbdr.index = df_t3_pivot_gmvbdr.index.strftime('%d-%b-%Y')

gmvbdr_stacked = go.Figure()

blue_palette = ['#1a2634', '#203e5f', '#ffcc00', '#fee5b1', '#393e46', '#393e46', '#acdbdf']

for i, vendor in enumerate(df_t3_pivot_gmvbdr.columns):
    gmvbdr_stacked.add_trace(go.Bar(
        x=df_t3_pivot_gmvbdr.index, 
        y=df_t3_pivot_gmvbdr[vendor],
        name=vendor,
        marker_color=blue_palette[i % len(blue_palette)],
        text=[custom_format(v) if v != 0 else '' for v in df_t3_pivot_gmvbdr[vendor]],
        textposition='outside',
        hoverinfo='text',
    ))

for trace in gmvbdr_stacked.data:
    formatted_text = [custom_format(value) if value != 0 else '' for value in trace.y]
    trace.update(
        hoverinfo='text',
        hovertext=[f"<b>{trace.name}</b><br>{custom_format(y)}" for y in trace.y],
        text=formatted_text,
        texttemplate='%{text}',
        textposition='outside'
    )

gmvbdr_stacked.update_layout(barmode='stack', title='Daily GMV by BDR', xaxis_title='', yaxis_title='')

for trace in gmvbdr_stacked.data:
    formatted_text = [custom_format(value) if value != 0 else '' for value in trace.y]
    trace.update(
        hoverinfo='text',
        hovertext=[f"<b>{trace.name}</b><br>{custom_format(y)}" for y in trace.y],
        text=formatted_text,
        texttemplate='%{text}',
        textposition='outside'
    )

gmvbdr_stacked.update_layout(
    barmode='stack',
    title='Daily GMV by BDR',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)

gmvbdr_stacked.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily GMV by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )


#------------------------------------------------------------------------------------------------------
####### KPI - 5.	No of BDR tasks completed and task effectiveness 

df_t4_grouped = df_t4.groupby('BDR Name')[['TOTAL_TASKS', 'COMPLETED_TASKS', 'EFFECTIVED_TASKS']].sum().reset_index()
df_t4_grouped['TASK_EFFECTIVNESS'] = (df_t4_grouped['EFFECTIVED_TASKS'] / df_t4_grouped['TOTAL_TASKS']) * 100
df_t4_grouped['TASK_EFFECTIVNESS'] = df_t4_grouped['TASK_EFFECTIVNESS'].apply(lambda x: f"{x:.2f}%")
df_t4_grouped_sort = df_t4_grouped.sort_values(by='TOTAL_TASKS', ascending=False)

cols_t4 = ['TOTAL_TASKS', 'COMPLETED_TASKS', 'EFFECTIVED_TASKS']

def style_table(df, columns, font_size='10pt'):
    def format_with_dots(value):
        if isinstance(value, (int, float)):
            return '{:,.0f}'.format(value).replace(',', '.')
        return value

    styler = df.style.format(format_with_dots, subset=columns)\
        .set_table_styles([
            {'selector': 'thead th',
             'props': [('background-color', '#1a2634'), ('color', 'white'), ('font-weight', 'bold')]},
            {'selector': 'td',
             'props': [('text-align', 'center')]},
            {'selector': 'table, th, td',
             'props': [('font-size', font_size)]}  # Setting the font size for the table
        ])

    styler = styler.set_properties(**{'background-color': 'white'}, subset=pd.IndexSlice[df.index[-1], :])

    return styler

df_t4_grouped_sort.set_index(df_t4_grouped_sort.columns[0], inplace=True)
df_t4_grouped_sort.fillna(0, inplace=True)
df_estilizado_t4 = style_table(df_t4_grouped_sort, cols_t4)
html_t4 = df_estilizado_t4.to_html()
#------------------------------------------------------------------------------------------------------
###### KPI 6.	No of GPS check in and GPS quality
df_t5_grouped = df_t5.groupby('BDR Name')[['GPS', 'GPS_QUALITY']].mean().reset_index()
df_t5_grouped[['GPS', 'GPS_QUALITY']] = df_t5_grouped[['GPS', 'GPS_QUALITY']].applymap(lambda x: f"{x * 100:.2f}%")
df_t5_grouped_sort = df_t5_grouped.sort_values(by='GPS', ascending=False)

cols_t5 = ['GPS', 'GPS_QUALITY']

df_t5_grouped_sort.set_index(df_t5_grouped_sort.columns[0], inplace=True)
df_estilizado_t5 = style_table(df_t5_grouped_sort, cols_t5)
html_t5 = df_estilizado_t5.to_html()

###### Joined Force kpi

df_joined = df_t4_grouped_sort.join(df_t5_grouped_sort, how='outer', lsuffix='_t4', rsuffix='_t5')
df_joined_sort = df_joined.sort_values(by='TOTAL_TASKS', ascending=False)
df_joined_sort.columns = df_joined_sort.columns.str.replace('_', ' ')
df_estilizado_joined = style_table(df_joined_sort, df_joined_sort.columns, font_size='10pt')
force_html = df_estilizado_joined.to_html()

force_csv = df_joined_sort.to_csv(index=False).encode('utf-8')

###### GPS table by day

df_t5['DATE'] = pd.to_datetime(df_t5['DATE'])
df_t5_sort_gps = df_t5.sort_values(by='DATE', ascending=True)
df_t5_sort_gps['FORMATTED_DATE'] = df_t5['DATE'].dt.strftime('%d-%b-%Y')
df_t5['GPS'] = df_t5['GPS'].astype(float)

pivot_df_tgps = df_t5_sort_gps.pivot_table(
    index='BDR Name', 
    columns='DATE', 
    values='GPS', 
    aggfunc='mean'
)

pivot_df_tgps = pivot_df_tgps.reindex(sorted(pivot_df_tgps.columns), axis=1)
pivot_df_tgps.columns = [date.strftime('%d-%b-%Y') for date in pivot_df_tgps.columns]

pivot_df_tgps_formatted = pivot_df_tgps.applymap(lambda x: f"{x * 100:.0f}%" if pd.notnull(x) else "0%")
all_columns_B = pivot_df_tgps_formatted.columns.tolist()
gps_table = style_table(pivot_df_tgps_formatted, all_columns_B)

gps_table_html = gps_table.to_html()
gpsday_csv = pivot_df_tgps_formatted.to_csv(index=False).encode('utf-8')
###### GPS Quality table by day

df_t5['DATE'] = pd.to_datetime(df_t5['DATE'])
df_t5_sort_gpsq = df_t5.sort_values(by='DATE', ascending=True)
df_t5_sort_gpsq['FORMATTED_DATE'] = df_t5['DATE'].dt.strftime('%d-%b-%Y')
df_t5['GPS_QUALITY'] = df_t5['GPS_QUALITY'].astype(float)

pivot_df_tgpsq = df_t5_sort_gpsq.pivot_table(
    index='BDR Name', 
    columns='DATE', 
    values='GPS_QUALITY', 
    aggfunc='mean'
)

pivot_df_tgpsq = pivot_df_tgpsq.reindex(sorted(pivot_df_tgpsq.columns), axis=1)
pivot_df_tgpsq.columns = [date.strftime('%d-%b-%Y') for date in pivot_df_tgpsq.columns]

pivot_df_tgpsq_formatted = pivot_df_tgpsq.applymap(lambda x: f"{x * 100:.0f}%" if pd.notnull(x) else "0%")
all_columns_C = pivot_df_tgpsq_formatted.columns.tolist()
gpsq_table = style_table(pivot_df_tgpsq_formatted, all_columns_C)

gpsq_table_html = gpsq_table.to_html()
gpsqday_csv = pivot_df_tgpsq_formatted.to_csv(index=False).encode('utf-8')
###### Task table by day

df_t4['DATE'] = pd.to_datetime(df_t4['DATE'])
df_t4_sort_eff = df_t4.sort_values(by='DATE', ascending=True)
df_t4_sort_eff['FORMATTED_DATE'] = df_t4['DATE'].dt.strftime('%d-%b-%Y')
df_t4['TASK_EFFECTIVENESS'] = df_t4['TASK_EFFECTIVENESS'].astype(float)

pivot_df_teff = df_t4_sort_eff.pivot_table(
    index='BDR Name', 
    columns='DATE', 
    values='TASK_EFFECTIVENESS', 
    aggfunc='mean'
)

pivot_df_teff = pivot_df_teff.reindex(sorted(pivot_df_teff.columns), axis=1)
pivot_df_teff.columns = [date.strftime('%d-%b-%Y') for date in pivot_df_teff.columns]

pivot_df_teff_formatted = pivot_df_teff.applymap(lambda x: f"{x:.0%}" if pd.notnull(x) else "0%")
all_columns_A = pivot_df_teff_formatted.columns.tolist()
taskeffect_table = style_table(pivot_df_teff_formatted, all_columns_A)

taskeffect_table_html = taskeffect_table.to_html()
taskday_csv = pivot_df_teff_formatted.to_csv(index=False).encode('utf-8')
############### Tasks stacked

df_t4['DATE'] = pd.to_datetime(df_t4['DATE'])
df_t4_sort = df_t4.sort_values(by='DATE', ascending=True)
df_t4_sort['FORMATTED_DATE'] = df_t4['DATE'].dt.strftime('%d-%b-%Y')
df_t4_stacked_bar = df_t4_sort.groupby(['FORMATTED_DATE', 'BDR Name'])['TOTAL_TASKS'].sum().reset_index()
df_t4_stacked_bar['DATE_FOR_SORTING'] = pd.to_datetime(df_t4_stacked_bar['FORMATTED_DATE'], format='%d-%b-%Y')
df_t4_pivot = df_t4_stacked_bar.pivot(index='DATE_FOR_SORTING', columns='BDR Name', values='TOTAL_TASKS').fillna(0)

df_t4_pivot.index = df_t4_pivot.index.strftime('%d-%b-%Y')
tasks_stacked = go.Figure()

blue_palette = ['#1a2634', '#203e5f', '#ffcc00', '#fee5b1', '#393e46', '#393e46', '#acdbdf']

for i, vendor in enumerate(df_t4_pivot.columns):
    text_labels = [f'{v}' if v != '0' else '' for v in df_t4_pivot[vendor]]

    tasks_stacked.add_trace(go.Bar(
        x=df_t4_pivot.index, 
        y=df_t4_pivot[vendor], 
        name=vendor,
        marker_color=blue_palette[i % len(blue_palette)],  # Use the color palette
        text=text_labels,  # Use the prepared text labels
        textposition='outside'  # Position labels outside the bars
    ))

tasks_stacked.update_layout(barmode='stack', title='Daily Tasks by BDR', xaxis_title='', yaxis_title='')
for i, trace in enumerate(tasks_stacked.data):
    trace.text = [f'{int(v)}' if v != 0 else '' for v in df_t4_pivot[trace.name]]

# Customizing the figure's layout
tasks_stacked.update_layout(
    barmode='stack',
    title='Daily Tasks by BDR',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)

tasks_stacked.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily Tasks by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )
#------------------------------------------------------------------------------------------------------
##### Metricas consolidadas

### Register
sum_register = df_aggregated_t2_BDR['count_registered_stores'].sum()
sum_register_dm1 = df_t2_dm1['count_registered_stores'].sum()
diff_register = sum_register - sum_register_dm1
diff_register = int(diff_register)

### Orders
sum_orders = df_t3["TOTAL_ORDERS"].sum()
df_t3_dm1['total_orders'] = df_t3_dm1['count_placed_orders_customer'] + df_t3_dm1['count_placed_orders_force'] + df_t3_dm1['count_placed_orders_grow']
sum_orders_dm1 = df_t3_dm1['total_orders'].sum()
diff_orders = sum_orders - sum_orders_dm1
diff_orders = int(diff_orders)

### Sales Value

df_t3_dm1['gmv_placed_customer'] = pd.to_numeric(df_t3_dm1['gmv_placed_customer'], errors='coerce').fillna(0)
df_t3_dm1['gmv_placed_force'] = pd.to_numeric(df_t3_dm1['gmv_placed_force'], errors='coerce').fillna(0)
df_t3_dm1['gmv_placed_grow'] = pd.to_numeric(df_t3_dm1['gmv_placed_grow'], errors='coerce').fillna(0)

sum_sales = df_t3_sales_notnull_sort['TOTAL_SALES'].sum()
df_t3_dm1['total_sales_dm1'] = df_t3_dm1['gmv_placed_customer'] + df_t3_dm1['gmv_placed_force'] + df_t3_dm1['gmv_placed_grow']
sum_sales_dm1 = df_t3_dm1['total_sales_dm1'].sum()
diff_total_sales = sum_sales - sum_sales_dm1
diff_total_sales = int(diff_total_sales)

diff_total_sales_format = custom_format(diff_total_sales)
sum_sales_format = custom_format(sum_sales)


### Sales Metric per channel
#Custumer
sum_customer = df_t3['gmv_placed_customer'].sum()
sum_customer_dm1 = df_t3_dm1['gmv_placed_customer'].sum()
diff_customer_sales = sum_customer - sum_customer_dm1

diff_customer_sales = custom_format(diff_customer_sales)
sum_customer = custom_format(sum_customer)

#Force
sum_force = df_t3['gmv_placed_force'].sum()
sum_force_dm1 = df_t3_dm1['gmv_placed_force'].sum()
diff_force_sales = sum_force - sum_force_dm1

diff_force_sales = custom_format(diff_force_sales)
sum_force = custom_format(sum_force)

#Grow
sum_grow = df_t3['gmv_placed_grow'].sum()
sum_grow_dm1 = df_t3_dm1['gmv_placed_grow'].sum()
diff_grow_sales = sum_grow - sum_grow_dm1

diff_grow_sales = custom_format(diff_grow_sales)
sum_grow = custom_format(sum_grow)

# Metrics Visits

sum_visits = df_t1['VISITS'].sum()
sum_visits_dm1 = df_t1_dm1['VISITS'].sum()
diff_visits = sum_visits - sum_visits_dm1
diff_visits = int(diff_visits)

sum_visitsp = df_t1['PLANNED_VISITS'].sum()
sum_visitsp_dm1 = df_t1_dm1['PLANNED_VISITS'].sum()
diff_visitsp = sum_visitsp - sum_visitsp_dm1
diff_visitsp = int(diff_visitsp)

#------------------------------------------------------------------------------------------------------
##################################################### Per Segment

# Visits per segment
df_register_segment_full = df_t2.groupby('segment')['count_registered_stores'].sum().reset_index()
df_register_segment_full_sort = df_register_segment_full.sort_values(by='count_registered_stores', ascending=False)
register_persegment = px.bar(df_register_segment_full_sort, x='segment', y='count_registered_stores', color_discrete_sequence=['#ffcc00'])

register_persegment.update_layout(
    title='Registered Stores ALLD per Segment',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

register_persegment.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

register_persegment.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

# Visits per segment latestday
df_tf2_mtd = df_t2[df_t2['DATE'] == max_date_t2]
df_tf2_mtd_agg2 = df_tf2_mtd.groupby('segment')['count_registered_stores'].sum().reset_index()
df_tf2_mtd_agg2 = df_tf2_mtd_agg2.sort_values(by='count_registered_stores', ascending=False)
register_persegment_mtd = px.bar(df_tf2_mtd_agg2, x='segment', y='count_registered_stores', color_discrete_sequence=['#ffcc00'])
formatted_max_date_t2 = max_date_t2.strftime('%Y-%m-%d')

register_persegment_mtd.update_layout(
    title=f'Registered Stores on {formatted_max_date_t2} per Segment',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

register_persegment_mtd.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

register_persegment_mtd.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

# Register Daily stacked by segment

df_t2['DATE'] = pd.to_datetime(df_t2['DATE'])
df_t2_sort_new = df_t2.sort_values(by='DATE', ascending=True)
df_t2_sort_new['FORMATTED_DATE'] = df_t2_sort_new['DATE'].dt.strftime('%d-%b-%Y')

df_t2_stacked2 = df_t2_sort_new.groupby(['FORMATTED_DATE', 'segment'])['count_registered_stores'].sum().reset_index()
df_t2_stacked2['DATE_FOR_SORTING'] = pd.to_datetime(df_t2_stacked2['FORMATTED_DATE'], format='%d-%b-%Y')

df_t2_pivot_seg = df_t2_stacked2.pivot_table(index='DATE_FOR_SORTING', columns='segment', values='count_registered_stores', aggfunc='sum').fillna(0)

df_t2_pivot_seg.index = df_t2_pivot_seg.index.strftime('%d-%b-%Y')
register_stacked_seg = go.Figure()

blue_palette_seg = ['#1a2634', '#203e5f', '#ffcc00', '#fee5b1', '#393e46', '#393e46', '#acdbdf', '#c7b198']

for i, vendor in enumerate(df_t2_pivot_seg.columns):
    text_labels = [f'{v:.0f}' if v != 0 else '' for v in df_t2_pivot_seg[vendor]]

    register_stacked_seg.add_trace(go.Bar(
        x=df_t2_pivot_seg.index, 
        y=df_t2_pivot_seg[vendor], 
        name=vendor,
        marker_color=blue_palette_seg[i % len(blue_palette_seg)],  # Use the color palette
        text=text_labels,  # Use the prepared text labels
        textposition='outside'  # Position labels outside the bars
    ))

register_stacked_seg.update_layout(barmode='stack', title='Daily Registers by Segment', xaxis_title='', yaxis_title='')
for i, trace in enumerate(register_stacked_seg.data):
    trace.text = [f'{v}' if v != 0 else '' for v in df_t2_pivot_seg[trace.name]]

register_stacked_seg.update_layout(
    barmode='stack',
    title='Daily Visits by Segment',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)


register_stacked_seg.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily Registers by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )

############ Orders per Segment

df_t3["TOTAL_ORDERS"] = df_t3["count_placed_orders_customer"] + df_t3["count_placed_orders_force"] + df_t3["count_placed_orders_grow"]

max_date_t3 = df_t3['DAY'].max()
max_date_t3 = pd.to_datetime(max_date_t3)

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_sorted = df_t3.sort_values(by='DAY')

df_t3_agg_bees_seg = df_t3_sorted.groupby('store_segment')['TOTAL_ORDERS'].sum().reset_index()
df_t3_agg_bees_sort_seg = df_t3_agg_bees_seg.sort_values(by='TOTAL_ORDERS', ascending=False)

# KPI 3 ORDERS - ALLD per BDR
orders_seg = px.bar(df_t3_agg_bees_sort_seg, x='store_segment', y='TOTAL_ORDERS', color_discrete_sequence=['#ffcc00'])

orders_seg.update_layout(
    title='BEES Orders ALLD per Segment',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

orders_seg.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

orders_seg.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

## Order pr segment latest day

df_t3_mtd = df_t3[df_t3['DAY'] == max_date_t3]
df_t3_mtd_agg_seg = df_t3_mtd.groupby('store_segment')['TOTAL_ORDERS'].sum().reset_index()
df_t3_mtd_agg_seg = df_t3_mtd_agg_seg.sort_values(by='TOTAL_ORDERS', ascending=False)


orders_seg_mtd = px.bar(df_t3_mtd_agg_seg, x='store_segment', y='TOTAL_ORDERS', color_discrete_sequence=['#ffcc00'])
formatted_max_date_t3 = max_date_t3.strftime('%Y-%m-%d')

orders_seg_mtd.update_layout(
    title=f'BEES Orders on {formatted_max_date_t3} per BDR',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

orders_seg_mtd.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

orders_seg_mtd.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

# Segment stacked day Orders

df_t3['Order_SUM'] = df_t3['count_placed_orders_customer'] + df_t3['count_placed_orders_force'] + df_t3['count_placed_orders_grow']

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_sort_new = df_t3.sort_values(by='DAY', ascending=True)
df_t3_sort_new['FORMATTED_DATE'] = df_t3['DAY'].dt.strftime('%d-%b-%Y')
df_t3_stacked_seg = df_t3_sort_new.groupby(['FORMATTED_DATE', 'store_segment'])['Order_SUM'].sum().reset_index()
df_t3_stacked_seg['DATE_FOR_SORTING'] = pd.to_datetime(df_t3_stacked['FORMATTED_DATE'], format='%d-%b-%Y')

df_t3_pivot_seg = df_t3_stacked_seg.pivot_table(
    index='DATE_FOR_SORTING', 
    columns='store_segment', 
    values='Order_SUM', 
    aggfunc='sum'
).fillna(0)

df_t3_pivot_seg.index = df_t3_pivot_seg.index.strftime('%d-%b-%Y')
order_stacked_seg = go.Figure()

blue_palette_seg = ['#1a2634', '#203e5f', '#ffcc00', '#fee5b1', '#393e46', '#393e46', '#acdbdf', '#c7b198']

for i, vendor in enumerate(df_t3_pivot_seg.columns):
    text_labels = [f'{v}' if v != '0' else '' for v in df_t3_pivot_seg[vendor]]

    order_stacked_seg.add_trace(go.Bar(
        x=df_t3_pivot_seg.index, 
        y=df_t3_pivot_seg[vendor], 
        name=vendor,
        marker_color=blue_palette_seg[i % len(blue_palette_seg)],  # Use the color palette
        text=text_labels,  # Use the prepared text labels
        textposition='outside'  # Position labels outside the bars
    ))

order_stacked_seg.update_layout(barmode='stack', title='Daily Orders by Segment', xaxis_title='', yaxis_title='')
for i, trace in enumerate(order_stacked_seg.data):
    trace.text = [f'{v}' if v != 0 else '' for v in df_t3_pivot_seg[trace.name]]

order_stacked_seg.update_layout(
    barmode='stack',
    title='Daily Orders by BDR',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)


order_stacked_seg.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily Orders by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )


################ Sales per Segment

df_t3['gmv_placed_customer'] = pd.to_numeric(df_t3['gmv_placed_customer'], errors='coerce').fillna(0)
df_t3['gmv_placed_force'] = pd.to_numeric(df_t3['gmv_placed_force'], errors='coerce').fillna(0)
df_t3['gmv_placed_grow'] = pd.to_numeric(df_t3['gmv_placed_grow'], errors='coerce').fillna(0)

df_t3['TOTAL_SALES'] = df_t3['gmv_placed_customer'] + df_t3['gmv_placed_force'] + df_t3['gmv_placed_grow']
df_t3['TOTAL_SALES'] = pd.to_numeric(df_t3['TOTAL_SALES'], errors='coerce').fillna(0)

df_t3_sales_seg = df_t3.groupby('store_segment')['TOTAL_SALES'].sum().reset_index()
df_t3_sales_notnull_seg = df_t3_sales_seg[(df_t3_sales_seg['TOTAL_SALES'] != 0)]
df_t3_sales_notnull_seg.dropna(subset=['TOTAL_SALES'], inplace=True)

df_t3_sales_notnull_sort_seg = df_t3_sales_notnull_seg.sort_values(by='TOTAL_SALES', ascending=False)

df_t3_sales_notnull_sort_seg['TOTAL_SALES'] = df_t3_sales_notnull_sort_seg['TOTAL_SALES'].fillna(0).round(1)

def custom_format(value):
    if value >= 1e6:  # If the value is in millions
        value = value / 1e6
        return f'{value:.2f}M IDR'
    elif value >= 1e3:  # If the value is in thousands
        value = value / 1e3
        return f'{value:.2f}K IDR'
    else:  # If the value is less than a thousand
        return f'{value:.2f} IDR'

# Apply the formatting function to your sales data
formatted_sales_seg = df_t3_sales_notnull_sort_seg['TOTAL_SALES'].apply(custom_format)

sales_seg = px.bar(df_t3_sales_notnull_sort_seg, x='store_segment', y='TOTAL_SALES', color_discrete_sequence=['#ffcc00'], text=formatted_sales_seg)

sales_seg.update_layout(
    title='BEES Sales ALLD per Segment',
    xaxis=dict(tickmode='linear', title=''),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white'  # Set background color to white for a clean look
)

sales_seg.update_traces(
    hovertemplate="<b>%{x}</b><br>Total Sales: %{y:.2s}<extra></extra>",  # Use .2s for smart formatting
    textposition='outside'  # This positions the text on top of the bars  # Place the text above the bars
)

sales_seg.update_layout( # Adjust the width to fit within the column
    height=500  # You can also adjust the height if necessary
)

### Sales by day Seg

df_t3['TOTAL_SALES'] = df_t3['TOTAL_SALES'] = df_t3['gmv_placed_customer'] + df_t3['gmv_placed_force'] + df_t3['gmv_placed_grow']

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_sort_new_gmv = df_t3.sort_values(by='DAY', ascending=True)
df_t3_sort_new_gmv['FORMATTED_DATE'] = df_t3['DAY'].dt.strftime('%d-%b-%Y')
df_t3_stacked_gmvbdr_seg = df_t3_sort_new_gmv.groupby(['FORMATTED_DATE', 'store_segment'])['TOTAL_SALES'].sum().reset_index()
df_t3_stacked_gmvbdr_seg['DATE_FOR_SORTING'] = pd.to_datetime(df_t3_stacked_gmvbdr_seg['FORMATTED_DATE'], format='%d-%b-%Y')

df_t3_pivot_gmvbdr_seg = df_t3_stacked_gmvbdr_seg.pivot_table(
    index='DATE_FOR_SORTING', columns='store_segment', values='TOTAL_SALES', aggfunc='sum').fillna(0)

df_t3_pivot_gmvbdr_seg.index = df_t3_pivot_gmvbdr_seg.index.strftime('%d-%b-%Y')

sales_stacked_seg = go.Figure()

for i, vendor in enumerate(df_t3_pivot_gmvbdr_seg.columns):
    sales_stacked_seg.add_trace(go.Bar(
        x=df_t3_pivot_gmvbdr_seg.index, 
        y=df_t3_pivot_gmvbdr_seg[vendor],
        name=vendor,
        marker_color=blue_palette_seg[i % len(blue_palette_seg)],
        text=[custom_format(v) if v != 0 else '' for v in df_t3_pivot_gmvbdr_seg[vendor]],
        textposition='outside',
        hoverinfo='text',
    ))

for trace in sales_stacked_seg.data:
    formatted_text = [custom_format(value) if value != 0 else '' for value in trace.y]
    trace.update(
        hoverinfo='text',
        hovertext=[f"<b>{trace.name}</b><br>{custom_format(y)}" for y in trace.y],
        text=formatted_text,
        texttemplate='%{text}',
        textposition='outside'
    )

sales_stacked_seg.update_layout(barmode='stack', title='Daily GMV by BDR', xaxis_title='', yaxis_title='')

for trace in sales_stacked_seg.data:
    formatted_text = [custom_format(value) if value != 0 else '' for value in trace.y]
    trace.update(
        hoverinfo='text',
        hovertext=[f"<b>{trace.name}</b><br>{custom_format(y)}" for y in trace.y],
        text=formatted_text,
        texttemplate='%{text}',
        textposition='outside'
    )

sales_stacked_seg.update_layout(
    barmode='stack',
    title='Daily GMV by BDR',
    xaxis_title='',
    yaxis_title='',
    xaxis_tickangle=-90,
    yaxis={'visible': False, 'showticklabels': False},
    plot_bgcolor='rgba(0,0,0,0)',
    xaxis={'showgrid': False},
)

sales_stacked_seg.update_layout(
    xaxis=dict(
        tickmode='linear',
        dtick=1,  # Set the interval between ticks to 1 day
        tickangle=-90,  # Rotate labels by 90 degrees
        type='category'  # This ensures that all categories (dates) are displayed
    ),
    yaxis=dict(
        showticklabels=False,  # Hide Y-axis labels
        showgrid=False,  # Hide grid lines
    ),
    plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
    barmode='stack',
    title='Daily GMV by BDR',
    showlegend=True,
    legend=dict(
        orientation='h',
        yanchor='top',
        y=-0.2,  # You might need to adjust this value to fit your chart
        xanchor='center',
        x=0.5  # Center the legend on the x-axis
    ),
    margin=dict(b=50),
    height=600
    )

########### Force KPI's per segment

## Table by Segment
df_t4_grouped_seg = df_t4.groupby('segment')[['TOTAL_TASKS', 'COMPLETED_TASKS', 'EFFECTIVED_TASKS']].sum().reset_index()
df_t4_grouped_seg['TASK_EFFECTIVNESS'] = (df_t4_grouped_seg['EFFECTIVED_TASKS'] / df_t4_grouped_seg['TOTAL_TASKS']) * 100
df_t4_grouped_seg['TASK_EFFECTIVNESS'] = df_t4_grouped_seg['TASK_EFFECTIVNESS'].apply(lambda x: f"{x:.2f}%")
df_t4_grouped_sort_seg = df_t4_grouped_seg.sort_values(by='TOTAL_TASKS', ascending=False)

cols_t4_seg = ['TOTAL_TASKS', 'COMPLETED_TASKS', 'EFFECTIVED_TASKS']
df_t4_grouped_sort_seg.set_index(df_t4_grouped_sort_seg.columns[0], inplace=True)
df_t4_grouped_sort_seg.fillna(0, inplace=True)

df_t5_grouped_seg = df_t5.groupby('segment')[['GPS', 'GPS_QUALITY']].mean().reset_index()
df_t5_grouped_seg[['GPS', 'GPS_QUALITY']] = df_t5_grouped_seg[['GPS', 'GPS_QUALITY']].applymap(lambda x: f"{x * 100:.2f}%")
df_t5_grouped_sort_seg = df_t5_grouped_seg.sort_values(by='GPS', ascending=False)

cols_t5_seg = ['GPS', 'GPS_QUALITY']

df_joined_seg = df_t4_grouped_sort_seg.join(df_t5_grouped_sort_seg, how='outer', lsuffix='_t4', rsuffix='_t5')
df_joined_sort_seg = df_joined_seg.sort_values(by='TOTAL_TASKS', ascending=False)
df_joined_sort_seg.columns = df_joined_sort_seg.columns.str.replace('_', ' ')
df_estilizado_joined_seg = style_table(df_joined_sort_seg, df_joined_sort_seg.columns, font_size='10pt')
force_html_seg = df_estilizado_joined_seg.to_html()

force_csv_seg = df_joined_sort_seg.to_csv(index=False).encode('utf-8')



#------------------------------------------------------------------------------------------------------
#### App
# Abas

abas = st.tabs(["By BDR", "By Segment"])
aba0 = abas[0]
aba1 = abas[1]

# Aba0
with aba0:
    colA = st.columns(1)
    colB = st.columns(1)
    colH = st.columns(1)
    colH_2 = st.columns(1)
    colH_1 = st.columns(1)
    colH_3 = st.columns(1)
    colI = st.columns(1)
    colI_1 = st.columns(1)
    colJ = st.columns(1)
    colK = st.columns(1)
    colK_1 = st.columns(1)
    colK_2 = st.columns(1)
    colK_3 = st.columns(1)
    colK_4 = st.columns(1)
    colK_5 = st.columns(1)
    colK_6 = st.columns(1)
    colK_7 = st.columns(1)
    colL = st.columns(1)
    colL_1 = st.columns(4)
    colM = st.columns(1)
    colN = st.columns(1)
    colN_1 = st.columns(1)
    colN_2 = st.columns(1)
    colN_3 = st.columns(1)
    colO = st.columns(1)
    colP = st.columns(1)
    colP_1 = st.columns(3)
    colQ = st.columns(2)
    colR = st.columns(1)
    colS = st.columns(1)
    colS_1 = st.columns(1)
    colS_2 = st.columns(1)
    colS_3 = st.columns(1)
    colC_1 = st.columns(1)
    colC_2 = st.columns(1)
    colC_3 = st.columns(1)
    colD = st.columns(2)
    colE = st.columns(2)
    colF = st.columns(2)
    colG = st.columns(2)
    colG_1 = st.columns(1)
    colG_3 = st.columns(1)
    colT = st.columns(1)

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

with colC_1[0]:
    st.metric(label="Total Visits", value=sum_visits, delta=diff_visits)
    st.plotly_chart(kpi1_all_barplot_bdr, use_container_width=True)

with colC_2[0]:
    st.plotly_chart(kpi1_all_barplot_bdr_mtd, use_container_width=True)

with colC_3[0]:
    st.plotly_chart(visits_stacked, use_container_width=True)

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

with colG_1[0]:
    st.download_button(
    label="Download data as CSV",
    data=csv_t1,
    file_name='data.csv',
    mime='text/csv',
    key="download_button_1"
)

with colH[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        1.	Number of stores registered by day per BDR.
    </div>
    """, unsafe_allow_html=True)

with colH_1[0]:
    st.metric(label="Total Registers", value=sum_register, delta=diff_register)
    st.plotly_chart(kpi2_all_barplot_bdr, use_container_width=True)

with colH_3[0]:
    st.plotly_chart(kpi2_all_barplot_bdr_mtd, use_container_width=True)

with colI[0]:
    st.plotly_chart(register_stacked, use_container_width=True)

with colI_1[0]:
    st.download_button(
    label="Download data as CSV",
    data=csv_t2,
    file_name='data.csv',
    mime='text/csv',
    key="download_button_2")

with colJ[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        2.	Number of stores adopted (place order via apps) per day per BDR.
    </div>
    """, unsafe_allow_html=True)

with colK_1[0]:
    st.metric(label="Total Orders", value=sum_orders, delta=diff_orders)
    st.plotly_chart(kpi3_all_barplot_bdr, use_container_width=True)

with colK_2[0]:
    st.plotly_chart(kpi3_all_barplot_bdr_mtd, use_container_width=True)
    
with colK_3[0]:
    st.plotly_chart(kpi3_barplot_cum, use_container_width=True)

with colK_5[0]:
    st.plotly_chart(order_stacked, use_container_width=True)

with colK_6[0]:
    st.plotly_chart(order_stacked_channel, use_container_width=True)

with colK_7[0]:
    st.plotly_chart(buyer_stacked_channel, use_container_width=True)

with colK_4[0]:
    st.download_button(
    label="Download data as CSV",
    data=csv_t3,
    file_name='data.csv',
    mime='text/csv',
    key="download_button_3")

with colL[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        3.	Sales value per BDR 
    </div>
    """, unsafe_allow_html=True)

with colL_1[0]:
    st.metric(label="Total Sales", value=sum_sales_format, delta=diff_total_sales_format)

with colL_1[1]:
    st.metric(label="Total Customer", value=sum_customer, delta=diff_customer_sales)

with colL_1[2]:
    st.metric(label="Total Force", value=sum_force, delta=diff_force_sales)

with colL_1[3]:
    st.metric(label="Total Grow", value=sum_grow, delta=diff_grow_sales)


with colM[0]:
    st.plotly_chart(kpi4_all_barplot_bdr, use_container_width=True)

with colN[0]:
    st.plotly_chart(gmv_stacked_channel, use_container_width=True)
    st.markdown("""
    <style>
    .fonte-personalizada3 {
        font-size: 10px;
        font-style: italic
    }
    </style>
    <div class="fonte-personalizada3">
        To see values hover over the bars.
    </div>
    """, unsafe_allow_html=True)

with colN_1[0]:
    st.plotly_chart(gmvbdr_stacked, use_container_width=True)

with colN_3[0]:
    st.download_button(
    label="Download data as CSV",
    data=csv_t3,
    file_name='data.csv',
    mime='text/csv',
    key="download_button_4")

with colO[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        4.	Force KPI's
    </div>
    """, unsafe_allow_html=True)

with colP[0]:
    st.markdown(f"""
    <div style="display: table; margin: auto;">
        {force_html}
    """, unsafe_allow_html=True)

with colP_1[0]:
    st.markdown(f"""
    <div style="text-align: center; margin-top: 20px;">  <!-- Adjust margin-top as needed -->
        <a href="data:text/csv;base64,{force_csv}" download="data.csv">
            <button>
                This table as CSV
            </button>
        </a>
    </div>
    """, unsafe_allow_html=True)

with colP_1[1]:
    st.markdown(f"""
    <div style="text-align: center; margin-top: 20px;">  <!-- Adjust margin-top as needed -->
        <a href="data:text/csv;base64,{csv_t4}" download="data.csv">
            <button>
                Raw Tasks as CSV
            </button>
        </a>
    </div>
    """, unsafe_allow_html=True)

with colP_1[2]:
    st.markdown(f"""
    <div style="text-align: center; margin-top: 20px;">  <!-- Adjust margin-top as needed -->
        <a href="data:text/csv;base64,{csv_t5}" download="data.csv">
            <button>
                Raw GPS as CSV
            </button>
        </a>
    </div>
    """, unsafe_allow_html=True)

with colS[0]:
    st.plotly_chart(tasks_stacked, use_container_width=True)

with colS_1[0]:
    st.markdown("""
    <style>
    .fonte-personalizada4 {
        font-size: 20px;
        font-style: bold
    }
    </style>
    <div class="fonte-personalizada4">
        Task Effect by day
    </div>
    """, unsafe_allow_html=True)
    st.markdown(taskeffect_table_html, unsafe_allow_html=True)
    st.download_button(
    label="This table as CSV",
    data=taskday_csv,
    file_name='data.csv',
    mime='text/csv',
    key="download_button_8"
)

with colS_2[0]:
    st.markdown("""
    <style>
    .fonte-personalizada4 {
        font-size: 20px;
        font-style: bold
    }
    </style>
    <div class="fonte-personalizada4">
        GPS by day
    </div>
    """, unsafe_allow_html=True)
    st.markdown(gps_table_html, unsafe_allow_html=True)
    st.download_button(
    label="This table as CSV",
    data=gpsday_csv,
    file_name='data.csv',
    mime='text/csv',
    key="download_button_9"
)

with colS_3[0]:
    st.markdown("""
    <style>
    .fonte-personalizada4 {
        font-size: 20px;
        font-style: bold
    }
    </style>
    <div class="fonte-personalizada4">
        GPS Quality by day
    </div>
    """, unsafe_allow_html=True)
    st.markdown(gpsq_table_html, unsafe_allow_html=True)
    st.download_button(
    label="This table as CSV",
    data=gpsqday_csv,
    file_name='data.csv',
    mime='text/csv',
    key="download_button_10"
)

with colT[0]:
    st.plotly_chart(visits_stacked_planned, use_container_width=True)
    st.markdown("""
    <style>
    .fonte-personalizada3 {
        font-size: 10px;
        font-style: italic
    }
    </style>
    <div class="fonte-personalizada3">
        Planned Visits: Count of visits if STATUS = "OPEN", "PENDING" or "NOT_COMPLETED".
    </div>
    """, unsafe_allow_html=True)

with colG_3[0]:
    st.markdown("""
    <style>
    .fonte-personalizada4 {
        font-size: 20px;
        font-style: bold
    }
    </style>
    <div class="fonte-personalizada4">
        Planned Visits
    </div>
    """, unsafe_allow_html=True)
    st.metric(label="Total Planned Visits", value=sum_visitsp, delta=diff_visitsp)
    st.plotly_chart(kpi1_all_barplot_bdr_p, use_container_width=True)


#---------------------------------------------------------------------------------------------------
# Aba1
with aba1:
    colAn = st.columns(1)
    colBn = st.columns(1)
    colCn = st.columns(1)
    colDn = st.columns(1)
    colEn = st.columns(1)
    colFn = st.columns(1)
    colFn_1 = st.columns(1)
    colGn = st.columns(1)
    colGn_1 = st.columns(1)
    colHn = st.columns(1)

with colAn[0]:
    st.image(blob_content_logo, use_column_width='always')

with colBn[0]:
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

with colCn[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        1.	Number of stores registered by day per segment.
    </div>
    """, unsafe_allow_html=True)
    st.plotly_chart(register_persegment, use_container_width=True)

with colDn[0]:
    st.plotly_chart(register_persegment_mtd, use_container_width=True)

with colEn[0]:
    st.plotly_chart(register_stacked_seg, use_container_width=True)

with colFn[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        2.	Number of stores adopted (place order via apps) per day per Segment.
    </div>
    """, unsafe_allow_html=True)

with colFn_1[0]:
    st.plotly_chart(orders_seg, use_container_width=True)
    st.plotly_chart(orders_seg_mtd, use_container_width=True)
    st.plotly_chart(order_stacked_seg, use_container_width=True)

with colGn[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        3.	Sales value per Segment
    </div>
    """, unsafe_allow_html=True)

with colGn_1[0]:
    st.plotly_chart(sales_seg, use_container_width=True)
    st.plotly_chart(sales_stacked_seg, use_container_width=True)

with colHn[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        4.	Force KPI's per Segment
    </div>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <div style="display: table; margin: auto;">
        {force_html_seg}
    """, unsafe_allow_html=True)