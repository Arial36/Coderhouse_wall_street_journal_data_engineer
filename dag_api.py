from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import psycopg2

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

        usuario = 'leandro_mar_b_coderhouse'
        contraseña = '5pp1EmI79G'
        host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
        puerto = '5439'
        nombre_base_datos = 'data-engineer-database'
        nombre_esquema = 'leandro_mar_b_coderhouse'
        nombre_tabla = 'wall_street_journal'

        cadena_conexion = f'dbname={nombre_base_datos} user={usuario} password={contraseña} host={host} port={puerto}'
        conn = psycopg2.connect(cadena_conexion)

        total_insertados = 0
        for index, row in df.iterrows():
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {nombre_esquema}.{nombre_tabla} WHERE (author = %s OR author IS NULL) AND title = %s AND description = %s", (row['author'], row['title'], row['description']))
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"INSERT INTO {nombre_esquema}.{nombre_tabla} VALUES ({', '.join(['%s']*len(row))})", tuple(row))
                total_insertados += 1
            cursor.close()

        conn.commit()
        conn.close()

        print(f"Se insertaron {total_insertados} registros en la tabla {nombre_esquema}.{nombre_tabla}")
    else:
        print("No se encontraron noticias para insertar")


default_args = {
    'owner': 'Leandro',
    'start_date': datetime(2024, 1, 11),
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
    op_kwargs={'api_key': '6f02a6d17cbe4d7bbc6424314cbb36ee'},
    dag=dag,
)


tarea_insertar_en_redshift = PythonOperator(
    task_id='insertar_en_redshift',
    python_callable=insertar_en_redshift,
    provide_context=True,
    dag=dag,
)


tarea_obtener_noticias >> tarea_insertar_en_redshift