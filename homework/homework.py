"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import os
import glob

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    # --- 1. Carga y consolidación ---
    input_path = 'files/input/'
    all_zip_files = glob.glob(os.path.join(input_path, "*.csv.zip"))

    df_list = []
    for file in all_zip_files:
        # Leer directamente, ahora sí con encabezado
        df_temp = pd.read_csv(file, compression='zip', sep=',')
        df_list.append(df_temp)

    df = pd.concat(df_list, ignore_index=True)

    # Limpiar nombres de columnas
    df.columns = [col.strip().lower().replace('.', '_') for col in df.columns]



    # --- 2. Limpieza de Nombres de Columnas del DF principal ---
    # Limpiar nombres: quitar coma inicial, minúsculas, reemplazar puntos por guiones bajos
    # df.columns = [col.lstrip(',').strip().lower().replace('.', '_') for col in df.columns]


    #expected_cols = ['id', 'age', 'job', 'marital', 'education', 'default', 'housing']
    #missing_cols = [col for col in expected_cols if col not in df.columns]
    #print("Faltan las siguientes columnas:", missing_cols)

    # --- 3. Creación y Limpieza del DataFrame 'client' ---
    # Se seleccionan las columnas usando los nombres ya limpios.
    client_df = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()

    
    # Se renombran a los nombres finales requeridos por el test.
    #client_df.rename(columns={
    #    'id': 'client_id',
    #    'default': 'credit_default',
    #    'housing': 'mortgage'
    #}, inplace=True)

    # Se aplican las transformaciones de valores.
    client_df['job'] = client_df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    client_df['education'] = client_df['education'].str.replace('.', '_', regex=False)
    client_df['education'] = client_df['education'].replace('unknown', pd.NA)
    client_df['credit_default'] = (client_df['credit_default'] == 'yes').astype(int)
    client_df['mortgage'] = (client_df['mortgage'] == 'yes').astype(int)

    # --- 4. Creación y Limpieza del DataFrame 'campaign' ---
    campaign_df = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'month', 'day']].copy()

    #campaign_df.rename(columns={
    #    'id': 'client_id',
    #    'campaign': 'number_contacts',
    #    'duration': 'contact_duration',
    #    'previous': 'previous_campaign_contacts',
    #    'poutcome': 'previous_outcome',
    #    'y': 'campaign_outcome'
    #}, inplace=True)
    
    campaign_df['previous_outcome'] = (campaign_df['previous_outcome'] == 'success').astype(int)
    campaign_df['campaign_outcome'] = (campaign_df['campaign_outcome'] == 'yes').astype(int)
    
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    month_num = campaign_df['month'].map(month_map)
    day_str = campaign_df['day'].astype(str).str.zfill(2)
    campaign_df['last_contact_date'] = '2022-' + month_num + '-' + day_str
    campaign_df.drop(columns=['month', 'day'], inplace=True)

    # --- 5. Creación y Limpieza del DataFrame 'economics' ---
    economics_df = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    
    #economics_df.rename(columns={
    #    'id': 'client_id',
    #    'euribor3m': 'euribor_three_months'
    #}, inplace=True)

    # --- 6. Guardado de los archivos de salida ---
    output_path = 'files/output/'
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    client_df.to_csv(os.path.join(output_path, 'client.csv'), index=False)
    campaign_df.to_csv(os.path.join(output_path, 'campaign.csv'), index=False)
    economics_df.to_csv(os.path.join(output_path, 'economics.csv'), index=False)


if __name__ == "__main__":
    clean_campaign_data()
