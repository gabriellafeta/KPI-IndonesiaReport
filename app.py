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
        return f'{prefixo}{valor / 1000:.2f} k PHP'
    elif valor < 1000000000:
        return f'{prefixo}{valor / 1000000:.2f} mi PHP'
    else:
        return f'{prefixo}{valor / 1000000000:.2f} bi PHP'
    

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
             'props': [('background-color', 'yellow'), ('color', 'black'), ('font-weight', 'bold')]},
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
    "6389058_BDR001": "Dinamis Artha Sukses",
    "6658562_BDR001": "RMS Jakarta Selatan",
    "6421535_BDR001": "RMS Depok",
    "6828128_BDR001": "RMS Bogor",
    "6713130_BDR001": "RMS Bekasi",
    "5653270_BDR001":"ASR",
    "6174675_BDR001": "CMP"
}

df_t1['BDR_name'] = df_t1['BDR_ID'].map(BDR_dict)
df_t2['BDR_name'] = df_t2['bdr_id'].map(BDR_dict)
df_t3['BDR_name'] = df_t3['bdr_id'].map(BDR_dict)
df_t4['BDR_name'] = df_t4['BDR_ID'].map(BDR_dict)
df_t5['BDR_name'] = df_t5['BDR_ID'].map(BDR_dict)

df_t1 = df_t1[df_t1['BDR_name'].notnull()]
df_t2 = df_t2[df_t2['BDR_name'].notnull()]
df_t3 = df_t3[df_t3['BDR_name'].notnull()]
df_t4 = df_t4[df_t4['BDR_name'].notnull()]
df_t5 = df_t5[df_t5['BDR_name'].notnull()]

### Tabelas para KPI 1 - N de visitas

df_t1_bram = df_t1[df_t1['BDR_name'] == 'Dinamis Artha Sukses']
df_t1_harris = df_t1[df_t1['BDR_name'] == 'RMS Jakarta Selatan']
df_t1_cheryl = df_t1[df_t1['BDR_name'] == 'RMS Depok']
df_t1_christian = df_t1[df_t1['BDR_name'] == 'RMS Bogor']
df_t1_iwan = df_t1[df_t1['BDR_name'] == 'RMS Bekasi']
df_t1_dian = df_t1[df_t1['BDR_name'] == 'ASR']
df_t1_alvis = df_t1[df_t1['BDR_name'] == 'CMP']

# Data to csv for downloading button

csv_t1 = df_t1.to_csv(index=False).encode('utf-8')
csv_t2 = df_t2.to_csv(index=False).encode('utf-8')
csv_t3 = df_t3.to_csv(index=False).encode('utf-8')
csv_t4 = df_t4.to_csv(index=False).encode('utf-8')
csv_t5 = df_t5.to_csv(index=False).encode('utf-8')
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
df_aggregated_t1 = df_t1_sorted.groupby('VISIT_DATE')['VISITED_STORES'].sum().reset_index()
kpi1_all_barplot = px.bar(df_aggregated_t1, x='VISIT_DATE', y='VISITED_STORES', color_discrete_sequence=['lightblue'])

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
    title='Visited Stores in the Last 30 Days for Dinamis Artha Sukses',
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
    title='Visited Stores in the Last 30 Days for RMS Jakarta Selatan',
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
    title='Visited Stores in the Last 30 Days for RMS Depok',
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
    title='Visited Stores in the Last 30 Days for RMS Bogor',
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
    title='Visited Stores in the Last 30 Days for RMS Bekasi',
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
    title='Visited Stores in the Last 30 Days for ASR',
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
    title='Visited Stores in the Last 30 Days for CMP',
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

########## KPI1 per BDR ALLD
df_aggregated_t1_BDR = df_t1_sorted.groupby('BDR_name')['VISITED_STORES'].sum().reset_index()
df_aggregated_t1_BDR = df_aggregated_t1_BDR.sort_values(by='VISITED_STORES', ascending=False)
kpi1_all_barplot_bdr = px.bar(df_aggregated_t1_BDR, x='BDR_name', y='VISITED_STORES', color_discrete_sequence=['LightSalmon'])

kpi1_all_barplot_bdr.update_layout(
    title='Visited Stores ALLD per BDR',
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

########## KPI1 per BDR in last latest DAY

df_tf_mtd = df_t1[df_t1['VISIT_DATE'] == max_date]
df_tf_mtd_agg = df_tf_mtd.groupby('BDR_name')['VISITED_STORES'].sum().reset_index()
df_tf_mtd_agg = df_tf_mtd_agg.sort_values(by='VISITED_STORES', ascending=False)
kpi1_all_barplot_bdr_mtd = px.bar(df_tf_mtd_agg, x='BDR_name', y='VISITED_STORES', color_discrete_sequence=['LightSalmon'])
formatted_max_date = max_date.strftime('%Y-%m-%d')

kpi1_all_barplot_bdr_mtd.update_layout(
    title=f'Visited Stores on {formatted_max_date} per BDR',
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

df_t1['VISIT_DATE'] = pd.to_datetime(df_t1['VISIT_DATE'])
df_t1['FORMATTED_DATE'] = df_t1['VISIT_DATE'].dt.strftime('%d-%b')
df_t1_stacked = df_t1.groupby(['FORMATTED_DATE', 'BDR_name'])['VISITED_STORES'].sum().reset_index()
df_t1_pivot = df_t1_stacked.pivot(index='FORMATTED_DATE', columns='BDR_name', values='VISITED_STORES').fillna(0)

visits_stacked = go.Figure()
colors = px.colors.sequential.Blues

blue_palette = ['#1f77b4', '#aec7e8', '#c6dbef', '#6baed6', '#2171b5', '#4c78a8', '#9ecae1']

for i, vendor in enumerate(df_t1_pivot.columns):
    visits_stacked.add_trace(go.Bar(
        x=df_t1_pivot.index,
        y=df_t1_pivot[vendor],
        name=vendor,
        marker_color=blue_palette[i % len(blue_palette)],  # Use the color palette
        text=[f'{v:.0f}' if v != 0 else '' for v in df_t1_pivot[vendor]],  # Format labels with zero decimal places
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
#------------------------------------------------------------------------------------------------------
########## KPI 2
#### Agregado por dia

max_date_t2 = df_t2['DATE'].max()
max_date_t2 = pd.to_datetime(max_date_t2)

df_t2['DATE'] = pd.to_datetime(df_t2['DATE'])
df_t2_sorted = df_t2.sort_values(by='DATE')

start_date = max_date_t2 - pd.Timedelta(days=29)

df_aggregated_t2 = df_t2_sorted.groupby('DATE')['count_registered_stores'].sum().reset_index()
kpi2_barplot_dateagg = px.bar(df_aggregated_t2, x='DATE', y='count_registered_stores', color_discrete_sequence=['lightblue'])

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

df_aggregated_t2_BDR = df_t2_sorted.groupby('BDR_name')['count_registered_stores'].sum().reset_index()
df_aggregated_t2_BDR_sorted = df_aggregated_t2_BDR.sort_values(by='count_registered_stores', ascending = False)
kpi2_all_barplot_bdr = px.bar(df_aggregated_t2_BDR_sorted, x='BDR_name', y='count_registered_stores', color_discrete_sequence=['LightSalmon'])
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
df_tf2_mtd_agg = df_tf2_mtd.groupby('BDR_name')['count_registered_stores'].sum().reset_index()
df_tf2_mtd_agg = df_tf2_mtd_agg.sort_values(by='count_registered_stores', ascending=False)
kpi2_all_barplot_bdr_mtd = px.bar(df_tf2_mtd_agg, x='BDR_name', y='count_registered_stores', color_discrete_sequence=['LightSalmon'])
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

#------------------------------------------------------------------------------------------------------
##### KPI 3.	Number & list of stores adopted (place order via apps) per day per BDR.

### Tratando a base
df_t3["TOTAL_ORDERS"] = df_t3["count_placed_orders_customer"] + df_t3["count_placed_orders_force"] + df_t3["count_placed_orders_grow"]
df_t3["bdr_id"] = df_t3["bdr_id"].fillna("TBD")

max_date_t3 = df_t3['DAY'].max()
max_date_t3 = pd.to_datetime(max_date_t3)

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])
df_t3_sorted = df_t3.sort_values(by='DAY')

df_t3_agg_bees = df_t3_sorted.groupby('bdr_id')['TOTAL_ORDERS'].sum().reset_index()
df_t3_agg_bees_sort = df_t3_agg_bees.sort_values(by='TOTAL_ORDERS', ascending=False)

# KPI 3 ORDERS - ALLD per BDR
kpi3_all_barplot_bdr = px.bar(df_t3_agg_bees_sort, x='bdr_id', y='TOTAL_ORDERS', color_discrete_sequence=['LightSalmon'])

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
df_t3_mtd_agg = df_t3_mtd.groupby('bdr_id')['TOTAL_ORDERS'].sum().reset_index()
df_t3_mtd_agg = df_t3_mtd_agg.sort_values(by='TOTAL_ORDERS', ascending=False)


kpi3_all_barplot_bdr_mtd = px.bar(df_t3_mtd_agg, x='bdr_id', y='TOTAL_ORDERS', color_discrete_sequence=['LightSalmon'])
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
start_date_cum_t3 = max_date_cum_t3 - pd.Timedelta(days=29)
df_t3_last_30_days = df_t3_byday_sort[(df_t3_byday_sort['DAY'] >= start_date_cum_t3) & (df_t3['DAY'] <= max_date_cum_t3)]

date_range = pd.date_range(start=start_date_cum_t3, end=max_date_cum_t3)
date_range_df = pd.DataFrame(date_range, columns=['DAY'])
df_complete = date_range_df.merge(df_t3_byday_sort, on='DAY', how='left')

df_complete['TOTAL_ORDERS'] = df_complete['TOTAL_ORDERS'].fillna(0)

df_complete['CUMULATIVE_ORDERS'] = df_complete['TOTAL_ORDERS'].cumsum()
df_t3_last_30_days = df_complete[(df_complete['DAY'] >= start_date_cum_t3) & (df_complete['DAY'] <= max_date_cum_t3)]

kpi3_barplot_cum = px.bar(df_t3_last_30_days, x='DAY', y='CUMULATIVE_ORDERS', color_discrete_sequence=['lightblue'])

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

#------------------------------------------------------------------------------------------------------
####### KPI 4.	Sales value per day per BDR and Count of orders 

df_t3['gmv_placed_customer'] = pd.to_numeric(df_t3['gmv_placed_customer'], errors='coerce').fillna(0)
df_t3['gmv_placed_force'] = pd.to_numeric(df_t3['gmv_placed_force'], errors='coerce').fillna(0)
df_t3['gmv_placed_grow'] = pd.to_numeric(df_t3['gmv_placed_grow'], errors='coerce').fillna(0)

df_t3['TOTAL_SALES'] = df_t3['gmv_placed_customer'] + df_t3['gmv_placed_force'] + df_t3['gmv_placed_grow']
df_t3['TOTAL_SALES'] = pd.to_numeric(df_t3['TOTAL_SALES'], errors='coerce').fillna(0)

df_t3_sales = df_t3.groupby('bdr_id')['TOTAL_SALES'].sum().reset_index()
df_t3_sales_notnull = df_t3_sales[(df_t3_sales['TOTAL_SALES'] != 0)]
df_t3_sales_notnull.dropna(subset=['TOTAL_SALES'], inplace=True)

df_t3_sales_notnull_sort = df_t3_sales_notnull.sort_values(by='TOTAL_SALES', ascending=False)

df_t3_sales_notnull_sort['TOTAL_SALES'] = df_t3_sales_notnull_sort['TOTAL_SALES'].fillna(0).round(1)

def custom_format(value):
    if value >= 1e6:  # If the value is in millions
        value = value / 1e6
        return f'{value:.2f}M PHP'
    elif value >= 1e3:  # If the value is in thousands
        value = value / 1e3
        return f'{value:.2f}K PHP'
    else:  # If the value is less than a thousand
        return f'{value:.2f} PHP'

# Apply the formatting function to your sales data
formatted_sales = df_t3_sales_notnull_sort['TOTAL_SALES'].apply(custom_format)

kpi4_all_barplot_bdr = px.bar(df_t3_sales_notnull_sort, x='bdr_id', y='TOTAL_SALES', color_discrete_sequence=['LightSalmon'], text=formatted_sales)

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

# Grafico empilhado
df_t3_sales_empilhado = df_t3.groupby('bdr_id')[['gmv_placed_customer', 'gmv_placed_force', 'gmv_placed_grow']].sum().reset_index()

df_t3_sales_empilhado['total_gmv'] = df_t3_sales_empilhado[['gmv_placed_customer', 'gmv_placed_force', 'gmv_placed_grow']].sum(axis=1)
df_t3_sales_empilhado_sorted = df_t3_sales_empilhado.sort_values('total_gmv', ascending=False)

# Create the stacked bar plot
kpi4_all_stacked_barplot_bdr = px.bar(
    df_t3_sales_empilhado_sorted, 
    x='bdr_id', 
    y=['gmv_placed_customer', 'gmv_placed_force', 'gmv_placed_grow'],
    title='BEES Sales Stacked per BDR',
    labels={'value': 'GMV', 'variable': 'Category'},  # Keeps the axis labels
    color_discrete_map={
        'gmv_placed_customer': 'lightblue',
        'gmv_placed_force': 'lightcoral',
        'gmv_placed_grow': '#D3D3D3'
    })

kpi4_all_stacked_barplot_bdr.update_traces(
    hovertemplate="<b>%{x}</b><br>%{data.name}: %{y:PHP,.2s}<extra></extra>")

kpi4_all_stacked_barplot_bdr.update_layout(
    xaxis=dict(tickangle=90, title=None),  
    yaxis=dict(showgrid=False, title=None),
    showlegend=True,
    plot_bgcolor='white')

#------------------------------------------------------------------------------------------------------
####### KPI - 5.	No of BDR tasks completed and task effectiveness 

df_t4_grouped = df_t4.groupby('BDR_ID')[['TOTAL_TASKS', 'COMPLETED_TASKS', 'EFFECTIVED_TASKS']].sum().reset_index()
df_t4_grouped['TASK_EFFECTIVNESS'] = (df_t4_grouped['EFFECTIVED_TASKS'] / df_t4_grouped['TOTAL_TASKS']) * 100
df_t4_grouped['TASK_EFFECTIVNESS'] = df_t4_grouped['TASK_EFFECTIVNESS'].apply(lambda x: f"{x:.2f}%")
df_t4_grouped_sort = df_t4_grouped.sort_values(by='TOTAL_TASKS', ascending=False)

cols_t4 = ['TOTAL_TASKS', 'COMPLETED_TASKS', 'EFFECTIVED_TASKS']

def style_table(df, columns):
    def format_with_dots(value):
        if isinstance(value, (int, float)):
            return '{:,.0f}'.format(value).replace(',', '.')
        return value

    styler = df.style.format(format_with_dots, subset=columns)\
        .set_table_styles([
            {'selector': 'thead th',
             'props': [('background-color', 'yellow'), ('color', 'black'), ('font-weight', 'bold')]},
            {'selector': 'td',
             'props': [('text-align', 'center')]}
        ])

    # Aplica estilos específicos para a última linha
    styler = styler.set_properties(**{'background-color': 'white'}, subset=pd.IndexSlice[df.index[-1], :])

    return styler

df_t4_grouped_sort.set_index(df_t4_grouped_sort.columns[0], inplace=True)
df_estilizado_t4 = style_table(df_t4_grouped_sort, cols_t4)
html_t4 = df_estilizado_t4.to_html()
#------------------------------------------------------------------------------------------------------
###### KPI 6.	No of GPS check in and GPS quality
df_t5_grouped = df_t5.groupby('BDR_ID')[['GPS', 'GPS_QUALITY']].mean().reset_index()
df_t5_grouped[['GPS', 'GPS_QUALITY']] = df_t5_grouped[['GPS', 'GPS_QUALITY']].applymap(lambda x: f"{x:.2f}%")
df_t5_grouped_sort = df_t5_grouped.sort_values(by='GPS', ascending=False)

cols_t5 = ['GPS', 'GPS_QUALITY']

df_t5_grouped_sort.set_index(df_t5_grouped_sort.columns[0], inplace=True)
df_estilizado_t5 = style_table(df_t5_grouped_sort, cols_t5)
html_t5 = df_estilizado_t5.to_html()
#------------------------------------------------------------------------------------------------------
#### App
# Abas

abas = st.tabs(["KPI's"])
aba0 = abas[0]


# Aba0
with aba0:
    colA = st.columns(1)
    colB = st.columns(1)
    colC = st.columns(1)
    colC_1 = st.columns(1)
    colC_2 = st.columns(1)
    colC_3 = st.columns(1)
    colD = st.columns(2)
    colE = st.columns(2)
    colF = st.columns(2)
    colG = st.columns(2)
    colG_1 = st.columns(1)
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
    colL = st.columns(1)
    colM = st.columns(1)
    colN = st.columns(1)
    colO = st.columns(1)
    colP = st.columns(1)
    colQ = st.columns(2)
    colR = st.columns(2)

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
        2.	Number of stores registered by day per BDR.
    </div>
    """, unsafe_allow_html=True)

with colH_2[0]:
    st.markdown("""
    <style>
    .fonte-personalizada3 {
        font-size: 10px;
        font-style: italic
    }
    </style>
    <div class="fonte-personalizada3">
        Up to 23/02/2024 There were a considerable amount of null values in BDR columns.
        For Those Values delivery_center_id was considered instead.
    </div>
    """, unsafe_allow_html=True)

with colH_1[0]:
    st.plotly_chart(kpi2_all_barplot_bdr, use_container_width=True)

with colH_3[0]:
    st.plotly_chart(kpi2_all_barplot_bdr_mtd, use_container_width=True)

with colI[0]:
    st.plotly_chart(kpi2_barplot_dateagg, use_container_width=True)

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
        3.	Number of stores adopted (place order via apps) per day per BDR.
    </div>
    """, unsafe_allow_html=True)

with colK[0]:
    st.markdown("""
    <style>
    .fonte-personalizada3 {
        font-size: 10px;
        font-style: italic
    }
    </style>
    <div class="fonte-personalizada3">
        Up to 23/02/2024 There were a considerable amount of null values in BDR columns.
        Null values treated as "TBD".
    </div>
    """, unsafe_allow_html=True)

with colK_1[0]:
    st.plotly_chart(kpi3_all_barplot_bdr, use_container_width=True)

with colK_2[0]:
    st.plotly_chart(kpi3_all_barplot_bdr_mtd, use_container_width=True)
    
with colK_3[0]:
    st.plotly_chart(kpi3_barplot_cum, use_container_width=True)

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
        4.	Sales value per BDR 
    </div>
    """, unsafe_allow_html=True)

with colM[0]:
    st.plotly_chart(kpi4_all_barplot_bdr, use_container_width=True)

with colN[0]:
    st.plotly_chart(kpi4_all_stacked_barplot_bdr, use_container_width=True)
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
        5.	Tasks and Task Effectivness per BDR 
    </div>
    """, unsafe_allow_html=True)

with colP[0]:
    st.markdown(html_t4, unsafe_allow_html=True)

with colQ[0]:
    st.markdown("""
    <style>
    .fonte-personalizada2 {
        font-size: 20px;
        font-style: bold;
        text-decoration: underline; /* This line adds the underline */
    }
    </style>
    <div class="fonte-personalizada2">
        6.	GPS check in and GPS quality
    </div>
    """, unsafe_allow_html=True)

with colR[0]:
    st.markdown(html_t5, unsafe_allow_html=True)