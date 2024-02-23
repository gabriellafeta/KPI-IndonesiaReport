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

########## KPI1 per BDR ALLD
df_aggregated_t1_BDR = df_t1_sorted.groupby('BDR_ID')['VISITED_STORES'].sum().reset_index()
df_aggregated_t1_BDR = df_aggregated_t1_BDR.sort_values(by='VISITED_STORES', ascending=False)
kpi1_all_barplot_bdr = px.bar(df_aggregated_t1_BDR, x='BDR_ID', y='VISITED_STORES', color_discrete_sequence=['LightSalmon'])

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
df_tf_mtd_agg = df_tf_mtd.groupby('BDR_ID')['VISITED_STORES'].sum().reset_index()
df_tf_mtd_agg = df_tf_mtd_agg.sort_values(by='VISITED_STORES', ascending=False)
kpi1_all_barplot_bdr_mtd = px.bar(df_tf_mtd_agg, x='BDR_ID', y='VISITED_STORES', color_discrete_sequence=['LightSalmon'])
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

df_t2_sorted['BDR_TEMP'] = df_t2_sorted.apply(lambda row: row['delivery_center_id'].split('_')[0] if pd.isnull(row['bdr_id']) else row['bdr_id'], axis=1)
df_aggregated_t2_BDR = df_t2_sorted.groupby('BDR_TEMP')['count_registered_stores'].sum().reset_index()
df_aggregated_t2_BDR_sorted = df_aggregated_t2_BDR.sort_values(by='count_registered_stores', ascending = False)
kpi2_all_barplot_bdr = px.bar(df_aggregated_t2_BDR_sorted, x='BDR_TEMP', y='count_registered_stores', color_discrete_sequence=['LightSalmon'])
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
df_tf2_mtd['BDR_TEMP'] = df_tf2_mtd.apply(lambda row: row['delivery_center_id'].split('_')[0] if pd.isnull(row['bdr_id']) else row['bdr_id'], axis=1)
df_tf2_mtd_agg = df_tf2_mtd.groupby('BDR_TEMP')['count_registered_stores'].sum().reset_index()
df_tf2_mtd_agg = df_tf2_mtd_agg.sort_values(by='count_registered_stores', ascending=False)
kpi2_all_barplot_bdr_mtd = px.bar(df_tf2_mtd_agg, x='BDR_TEMP', y='count_registered_stores', color_discrete_sequence=['LightSalmon'])
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

df_t3['DAY'] = pd.to_datetime(df_t3['DAY'])

df_t3 = df_t3.sort_values(by="DAY")

df_t3["Cummulative Orders"] = df_t3["TOTAL_ORDERS"].cumsum()
max_date_t3 = df_t3['DAY'].max()

# Find the maximum date (most recent date) in 'DAY'
max_date_t3 = df_t3['DAY'].max()

# Calculate the start date for the last 30 days
start_date_t3_cum = max_date_t3 - pd.Timedelta(days=29)

# Filter the DataFrame to include only the last 30 days
df_agg_t3_cum = df_t3[(df_t3['DAY'] >= start_date_t3_cum) & (df_t3['DAY'] <= max_date_t3)]

kpi3_barplot_dateagg_cum = px.bar(df_agg_t3_cum, x= 'DAY', y='Cummulative Orders', color_discrete_sequence=['lightblue'])

kpi3_barplot_dateagg_cum.update_layout(
    title='Cummulative BEES Orders per day for ALL BDRs',
    xaxis=dict(tickmode='linear', title='', tickangle=90),
    showlegend=False,
    yaxis=dict(showgrid=False, showticklabels=False, title=''),  # Hide Y-axis grid lines and tick labels
    plot_bgcolor='white',
    margin=dict(t=50)  # Set background color to white for a clean look
)

kpi3_barplot_dateagg_cum.update_traces(
    texttemplate='%{y}',  # Use the Y value for the text
    textposition='outside'  # Place the text above the bars
)

kpi3_barplot_dateagg_cum.update_layout(
    width=500,  # Adjust the width to fit within the column
    height=400  # You can also adjust the height if necessary
)





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
    st.plotly_chart(kpi3_barplot_dateagg_cum, use_container_width=True)
