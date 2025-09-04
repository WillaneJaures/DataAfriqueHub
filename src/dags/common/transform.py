import pandas as pd
import os
import sqlite3
from datetime import datetime
from typing import List, Tuple
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




#fonction de transformation des données

def transform_clients(file_path: str, process_data: datetime = None) -> pd.DataFrame:
    check_file(file_path)
    if process_data is None:
        date = datetime.now()
    
    try:
        df = pd.read_csv(file_path)
        print(f"Fichier {file_path} chargé avec succès.")

        #nettoyage des données
        df.drop_duplicates(inplace=True)
        df.columns  = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df.dropna(inplace=True)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')


        #creation du repertoire de destination 
        output_dir = os.path.join(CLEAN_DATA_DIR, "clients", str(process_data.year), str(process_data.month))
        os.makedirs(output_dir, exist_ok=True)

        #sauvegarde du fichier nettoyé
        clean_file_path = os.path.join(output_dir, f"{process_data.day}.csv")
        df.to_csv(clean_file_path, index=False, date_format='%Y-%m-%d')
        print(f"Fichier nettoyé sauvegardé dans {clean_file_path}")

        return df
    except Exception as e:
        logging.error(f"Erreur lors de la transformation des données : {e}")
        raise
    




def transform_products(file_path: str, process_data: datetime = None) -> pd.DataFrame:
    check_file(file_path)
    if process_data is None:
        date = datetime.now()
    
    try:
        df = pd.read_csv(file_path)
        print(f"Fichier {file_path} chargé avec succès.")

        #nettoyage des données
        df.drop_duplicates(inplace=True)
        df.columns  = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df.dropna(inplace=True)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')


        #creation du repertoire de destination 
        output_dir = os.path.join(CLEAN_DATA_DIR, "products", str(process_data.year), str(process_data.month))
        os.makedirs(output_dir, exist_ok=True)

        #sauvegarde du fichier nettoyé
        clean_file_path = os.path.join(output_dir, f"{process_data.day}.csv")
        df.to_csv(clean_file_path, index=False, date_format='%Y-%m-%d')
        print(f"Fichier nettoyé sauvegardé dans {clean_file_path}")

        return df
    except Exception as e:
        logging.error(f"Erreur lors de la transformation des données : {e}")
        raise


def transform_orders(file_path: str, process_data: datetime = None) -> pd.DataFrame:
    check_file(file_path)
    if process_data is None:
        date = datetime.now()
    
    try:
        df = pd.read_csv(file_path)
        print(f"Fichier {file_path} chargé avec succès.")

        #nettoyage des données
        df.drop_duplicates(inplace=True)
        df.columns  = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df.dropna(inplace=True)
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')


        #creation du repertoire de destination 
        output_dir = os.path.join(CLEAN_DATA_DIR, "orders", str(process_data.year), str(process_data.month))
        os.makedirs(output_dir, exist_ok=True)

        #sauvegarde du fichier nettoyé
        clean_file_path = os.path.join(output_dir, f"{process_data.day}.csv")
        df.to_csv(clean_file_path, index=False, date_format='%Y-%m-%d')
        print(f"Fichier nettoyé sauvegardé dans {clean_file_path}")

        return df
    except Exception as e:
        logging.error(f"Erreur lors de la transformation des données : {e}")
        raise

if __name__ == "__main__":
    #transform_clients(os.path.join(RAW_DATA_DIR, "clients", "2024", "5", "10.csv"), datetime.strptime("2024-05-10", "%Y-%m-%d"))
    #transform_products(os.path.join(RAW_DATA_DIR, "products", "2024", "5", "10.csv"), datetime.strptime("2024-05-10", "%Y-%m-%d"))
    transform_orders(os.path.join(RAW_DATA_DIR, "orders", "2024", "5", "3.csv"), datetime.strptime("2024-05-03", "%Y-%m-%d"))
        


