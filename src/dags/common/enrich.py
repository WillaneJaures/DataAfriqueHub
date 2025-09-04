import pandas as pd
import os
import sqlite3
from datetime import datetime
import logging


#check if file exists
DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")
CLEAN_DATA_DIR = os.path.join(DATA_DIR, "clean_data")
ENRICH_DATA_DIR = os.path.join(DATA_DIR, "enrich_data")


#check if the file exists and is accessible
def check_file(file_name: str):
    if not os.path.isfile(file_name):
        raise FileNotFoundError(f"Le fichier {file_name} n'existe pas.")
    else:
        print(f"Le fichier {file_name} trouvé.")
    return True


#fonction enrichessement des donnees
def enrich_clients(file_name, process_data: datetime = None) -> pd.DataFrame:
    check_file(file_name)
    if process_data is None:
        date = datetime.now()
    
    try:
        #enrichissement des données
        df = pd.read_csv(file_name, parse_dates= ['date'])
        print(f"Fichier {file_name} chargé avec succès.")
        print(f"type de la colonne date : {df['date'].dtype}")

        df['full_name'] = df['firstname'] + ' ' + df['lastname']
        df['month'] = df['date'].dt.month
        df['year'] = df['date'].dt.year


        #creation du repertoire de destination 
        output_dir = os.path.join(ENRICH_DATA_DIR, "clients", str(process_data.year), str(process_data.month))
        os.makedirs(output_dir, exist_ok=True)

        #sauvegarde du fichier nettoyé
        enrich_file_path = os.path.join(output_dir, f"{process_data.day}.csv")
        df.to_csv(enrich_file_path, index=False)
        print(f"Fichier enrichi sauvegardé dans {enrich_file_path}")

        return df
    except Exception as e:
        logging.error(f"Erreur lors de l'enrichissement des données : {e}")
        raise


def enrich_products(file_name, process_data: datetime = None) -> pd.DataFrame:
    check_file(file_name)
    if process_data is None:
        date = datetime.now()
    
    try:
        #enrichissement des données
        df = pd.read_csv(file_name, parse_dates= ['date'])
        print(f"Fichier {file_name} chargé avec succès.")
        df['month'] = df['date'].dt.month
        df['year'] = df['date'].dt.year


        #creation du repertoire de destination 
        output_dir = os.path.join(ENRICH_DATA_DIR, "products", str(process_data.year), str(process_data.month))
        os.makedirs(output_dir, exist_ok=True)

        #sauvegarde du fichier nettoyé
        enrich_file_path = os.path.join(output_dir, f"{process_data.day}.csv")
        df.to_csv(enrich_file_path, index=False)
        print(f"Fichier enrichi sauvegardé dans {enrich_file_path}")

        return df
    except Exception as e:
        logging.error(f"Erreur lors de l'enrichissement des données : {e}")
        raise

def enrich_orders(file_name, process_data: datetime = None) -> pd.DataFrame:
    check_file(file_name)
    if process_data is None:
        date = datetime.now()
    
    try:
        #enrichissement des données
        df = pd.read_csv(file_name, parse_dates= ['order_date'])
        print(f"Fichier {file_name} chargé avec succès.")
        df['month'] = df['order_date'].dt.month
        df['year'] = df['order_date'].dt.year


        #creation du repertoire de destination 
        output_dir = os.path.join(ENRICH_DATA_DIR, "orders", str(process_data.year), str(process_data.month))
        os.makedirs(output_dir, exist_ok=True)

        #sauvegarde du fichier nettoyé
        enrich_file_path = os.path.join(output_dir, f"{process_data.day}.csv")
        df.to_csv(enrich_file_path, index=False)
        print(f"Fichier enrichi sauvegardé dans {enrich_file_path}")

        return df
    except Exception as e:
        logging.error(f"Erreur lors de l'enrichissement des données : {e}")
        raise

if __name__=="__main__":
    #enrich_clients(os.path.join(CLEAN_DATA_DIR, "clients/2024/5/10.csv"), datetime.strptime("2024-05-10", "%Y-%m-%d"))
    #enrich_products(os.path.join(CLEAN_DATA_DIR, "products/2024/5/10.csv"), datetime.strptime("2024-05-10", "%Y-%m-%d"))
    enrich_orders(os.path.join(CLEAN_DATA_DIR, "orders/2024/5/3.csv"), datetime.strptime("2024-05-03", "%Y-%m-%d"))
