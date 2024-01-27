from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
import os
import requests
import pandas as pd
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Obtengo los valores de las variables de entorno
load_dotenv()
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
REDSHIFT_DB_USER = os.getenv("REDSHIFT_DB_USER")
REDSHIFT_DB_PASSWORD = os.getenv("REDSHIFT_DB_PASSWORD")
REDSHIFT_DB_HOST = os.getenv("REDSHIFT_DB_HOST")
REDSHIFT_DB_PORT = os.getenv("REDSHIFT_DB_PORT")
REDSHIFT_DB_NAME = os.getenv("REDSHIFT_DB_NAME")
REDSHIFT_DB_SCHEMA = os.getenv("REDSHIFT_DB_SCHEMA")
REDSHIFT_DB_TABLE = os.getenv("REDSHIFT_DB_TABLE")

def obtener_noticias(api_key):
    url = "https://newsapi.org/v2/everything"
    parametros = {
        'domains': 'wsj.com',
        'apiKey': api_key
    }

    response = requests.get(url, params=parametros)

    if response.status_code == 200:
        datos_noticias = response.json()

        for article in datos_noticias.get('articles', []):
            if 'source' in article:
                del article['source']

        return datos_noticias['articles']
    else:
        print(f"Error al obtener noticias. Código de estado: {response.status_code}")
        return None

def insertar_en_redshift(**kwargs):
    ti = kwargs['ti']
    noticias = ti.xcom_pull(task_ids='obtener_noticias')

    if noticias:
        df = pd.DataFrame(noticias)

        cadena_conexion = f'dbname={REDSHIFT_DB_NAME} user={REDSHIFT_DB_USER} password={REDSHIFT_DB_PASSWORD} host={REDSHIFT_DB_HOST} port={REDSHIFT_DB_PORT}'
        conn = psycopg2.connect(cadena_conexion)

        total_insertados = 0
        for index, row in df.iterrows():
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {REDSHIFT_DB_SCHEMA}.{REDSHIFT_DB_TABLE} WHERE (author = %s OR author IS NULL) AND title = %s AND description = %s", (row['author'], row['title'], row['description']))
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"INSERT INTO {REDSHIFT_DB_SCHEMA}.{REDSHIFT_DB_TABLE} VALUES ({', '.join(['%s']*len(row))})", tuple(row))
                total_insertados += 1
            cursor.close()

        conn.commit()
        conn.close()

        print(f"Se insertaron {total_insertados} registros en la tabla {REDSHIFT_DB_SCHEMA}.{REDSHIFT_DB_TABLE}")
    else:
        print("No se encontraron noticias para insertar")

def verificar_y_enviar_correo():
    # Lógica para verificar si se insertan nuevos registros con author igual a 'wsj'
    cadena_conexion = f'dbname={REDSHIFT_DB_NAME} user={REDSHIFT_DB_USER} password={REDSHIFT_DB_PASSWORD} host={REDSHIFT_DB_HOST} port={REDSHIFT_DB_PORT}'
    conn = psycopg2.connect(cadena_conexion)
    cursor = conn.cursor()

    # Consulta para obtener la cantidad de nuevos registros con author 'wsj' que se insertaron en el dia
    cursor.execute(f"SELECT COUNT(*) FROM {REDSHIFT_DB_SCHEMA}.{REDSHIFT_DB_TABLE} WHERE author = 'wsj' AND DATE(ultima_actualizacion) = CURRENT_DATE")
    count_new_wsj_records = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    if count_new_wsj_records > 0:
        enviar_correo_electronico()

def enviar_correo_electronico():
    # Configuración del servidor SMTP de Outlook
    smtp_server = "smtp-mail.outlook.com"
    smtp_port = 587
    smtp_username = "leandro.arial@hotmail.com"
    smtp_password = ""  # Por cuestiones de seguridad queda vacio este campo

    # Configuración del correo electrónico
    from_address = "leandro.arial@hotmail.com"
    to_address = "leandro.arial@hotmail.com"  # Cambiado a la dirección de correo deseada
    subject = "Alerta: Nuevo registro en Redshift con author 'wsj'"

    # Cuerpo del correo electrónico
    body = "Hay una nueva noticia de wsj"

    # Crear el objeto MIMEMultipart
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject

    # Agregar el cuerpo del correo electrónico
    msg.attach(MIMEText(body, 'plain'))

    # Conectar al servidor SMTP y enviar el correo electrónico
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        text = msg.as_string()
        server.sendmail(from_address, to_address, text)

    print("Correo electrónico enviado con éxito.")

default_args = {
    'owner': 'Leandro',
    'start_date': datetime(2024, 1, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_noticias',
    default_args=default_args,
    description='DAG para obtener y almacenar noticias en Redshift',
    schedule_interval='0 15 * * *',
)

tarea_obtener_noticias = PythonOperator(
    task_id='obtener_noticias',
    python_callable=obtener_noticias,
    op_kwargs={'api_key': NEWS_API_KEY},
    dag=dag,
)

tarea_insertar_en_redshift = PythonOperator(
    task_id='insertar_en_redshift',
    python_callable=insertar_en_redshift,
    provide_context=True,
    dag=dag,
)

tarea_verificar_correo = PythonOperator(
    task_id='verificar_y_enviar_correo',
    python_callable=verificar_y_enviar_correo,
    dag=dag,
)

tarea_obtener_noticias >> tarea_insertar_en_redshift >> tarea_verificar_correo
