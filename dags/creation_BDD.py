from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

USER = "airflow"
PASSWORD = "airflow"
HOST = "postgres"  
PORT = "5432"
DATABASES = ["bronze", "silver", "gold"]

def create_databases():
    conn = psycopg2.connect(dbname='postgres', user=USER, password=PASSWORD, host=HOST, port=PORT)
    conn.autocommit = True
    cursor = conn.cursor()

    for db_name in DATABASES:
        try:
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(db_name)))
            print(f"Base de données '{db_name}' créée avec succès.")
        except psycopg2.errors.DuplicateDatabase:
            print(f"La base de données '{db_name}' existe déjà.")

    cursor.close()
    conn.close()

def create_tables_bronze():
    conn = psycopg2.connect(dbname='bronze', user=USER, password=PASSWORD, host=HOST, port=PORT)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            ID_CLIENT VARCHAR NOT NULL,
            NOM VARCHAR,
            PRENOM VARCHAR,
            CIN VARCHAR,
            CARTE_SEJOUR VARCHAR,
            VILLE VARCHAR,
            NATIONALITE VARCHAR NOT NULL,
            TELEPHONE VARCHAR,
            ID_INTERMEDIAIRE INT NOT NULL,
            DATE_CREATION TIMESTAMP NOT NULL,
            PRIMARY KEY (ID_CLIENT)
        );
    """)


    conn.commit()
    cursor.close()
    conn.close()
    print("Tables créées avec succès dans la base de données 'bronze'.")

def create_tables_silver():
    conn = psycopg2.connect(dbname='silver', user=USER, password=PASSWORD, host=HOST, port=PORT)
    cursor = conn.cursor()


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS controles (
            ID_CONTROLE FLOAT,
            DATA TEXT,
            DIM_CONTROLE VARCHAR,
            DATE_CONTROLE TIMESTAMP
        );
    """)

    table_names = [
        "df_comp_nom", "df_comp_prenom", "df_comp_cin", "df_comp_carte_sejour", "df_comp_ville", "df_comp_telephone",
        "df_val_nom", "df_val_prenom", "df_val_cin", "df_val_carte_sejour", "df_val_ville", "df_val_telephone",
        "df_co_cin_natio", "df_co_cin_carteSejour", "df_co_nom_prenom", "df_un_cin_nom_prenom",
        "df_un_carte_nom_prenom", "df_un_tele_nom_prenom"
    ]

    for table_name in table_names:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ID_CLIENT VARCHAR NOT NULL,
                RESULT INT NOT NULL,
                ID_CONTROLE INT NOT NULL
            );
        """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Tables créées avec succès dans la base de données 'silver'.")


def create_tables_gold():

    conn = psycopg2.connect(dbname='gold', user=USER, password=PASSWORD, host=HOST, port=PORT)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            ID_CLIENT VARCHAR NOT NULL,
            NOM VARCHAR,
            PRENOM VARCHAR,
            CIN VARCHAR,
            CARTE_SEJOUR VARCHAR,
            VILLE VARCHAR,
            NATIONALITE VARCHAR NOT NULL,
            TELEPHONE VARCHAR,
            ID_INTERMEDIAIRE INT NOT NULL,
            DATE_CREATION TIMESTAMP NOT NULL,
            PRIMARY KEY (ID_CLIENT)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS intermediaire (
            id_intermidiaire INT NOT NULL,
            nom VARCHAR NOT NULL,
            prenom VARCHAR NOT NULL,
            ville VARCHAR NOT NULL,
            pays VARCHAR NOT NULL,
            region VARCHAR NOT NULL,
            type_intermidiaire VARCHAR NOT NULL,
            classe_intermidiaire VARCHAR NOT NULL,
            PRIMARY KEY (id_intermidiaire)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS controles (
            ID_CONTROLE FLOAT,
            DATA TEXT,
            DIM_CONTROLE VARCHAR,
            DATE_CONTROLE TIMESTAMP
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS resultat_final (
            id SERIAL PRIMARY KEY,
            result INT,
            id_controle INT,
            id_client VARCHAR  NOT NULL
        );
    """)


    conn.commit()

    cursor.close()
    conn.close()
    print("Tables créées avec succès dans la base de données 'gold'.")



def load_parquet_to_postgres():
    try:
        engine_bronze = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/bronze')
        engine_gold = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/gold')
        
        print("Tentative de chargement de dataset_intermidiaire.parquet...")
        intermediaire_df = pd.read_parquet('/opt/airflow/data/dataset_intermidiaire.parquet')
        print("Chargement réussi : dataset_intermidiaire.parquet")

        print("Tentative de chargement de dataset_clients.parquet...")
        clients_df = pd.read_parquet('/opt/airflow/data/dataset_clients.parquet')
        print("Chargement réussi : dataset_clients.parquet")

        clients_df.columns = [col.lower() for col in clients_df.columns]
        intermediaire_df.columns = [col.lower() for col in intermediaire_df.columns]

        clients_df.to_sql('clients', engine_bronze, if_exists='append', index=False)

        intermediaire_df.to_sql('intermediaire', engine_gold, if_exists='append', index=False)
        clients_df.to_sql('clients', engine_gold, if_exists='append', index=False)


        print("Données chargées avec succès dans les tables PostgreSQL.")
    except Exception as e:
        print(f"Erreur lors du chargement des données : {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 28),
    'retries': 1,
}

dag = DAG(
    'create_databases_and_tables',
    default_args=default_args,
    description='DAG pour créer la base de données et les tables',
    schedule_interval=None,
)

create_db_task = PythonOperator(
    task_id='create_db_task',
    python_callable=create_databases,
    dag=dag,
)

create_tables_bronze_task = PythonOperator(
    task_id='create_tables_bronze',
    python_callable=create_tables_bronze,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_parquet_to_postgres_task',
    python_callable=load_parquet_to_postgres,
    dag=dag,
)

create_tables_silver_task = PythonOperator(
    task_id='create_tables_silver',
    python_callable=create_tables_silver,
    dag=dag,
)

create_tables_gold_task = PythonOperator(
    task_id='create_tables_gold',
    python_callable=create_tables_gold,
    dag=dag,
)

create_db_task >> create_tables_bronze_task >> create_tables_silver_task >> create_tables_gold_task >> load_data_task