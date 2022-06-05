import os
import shutil
import sys
import sqlite3

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from etl import etl
from db import db_setup


class PipelineRunner():
    db_path: str
    invalide_data_path: str
    loaded_data_path:str

    def __init__(self, db_path:str, invalide_data_path:str, loaded_data_path:str):
        self.db_path = db_path
        self.invalide_data_path = invalide_data_path
        self.loaded_data_path = loaded_data_path


    def create_database(self):
        conn= sqlite3.connect(self.db_path, check_same_thread=False)
        db_setup.create_spotify_db(conn)
        conn.close()


    def run_spotify_pipeline(self, src_path:str):
        print('Start extraction.')
        try:
            df = etl.extract(src_path)
        except Exception as e:
            print(e)
            print('Extraction faied')
        print('Finished extraction.')
        print('Start validation.')
        df = etl.validate_data(df, invalid_data_path=self.invalide_data_path)
        try:
            df = etl.validate_data(df, invalid_data_path=self.invalide_data_path)
        except Exception as e:
            print(e)
            print('Validation faied')
        print('Finished validateion.')
        print('Start transformation.')
        try:
            df = etl.transform(df)
        except Exception as e:
            print(e)
            print('Transformation faied')
        print('Finished transformation.')
        print('Start load.')
        try:
            conn= sqlite3.connect(self.db_path, check_same_thread=False)
            etl.load(df, db_conn=conn)
            self.copy_loaded_file(src_path)
            conn.close()
        except Exception as e:
            conn.close()
            print(e)
            print('Load faied')
        print('Finished load.')
        

    def copy_invalide_file(self, src_path:str):
        target = f"{self.invalide_data_path}/{ os.path.basename(src_path)}"
        shutil.copyfile(src_path, target)
        print(f'Transfared {src_path} to {target}')

    def copy_loaded_file(self, src_path:str):
        target = f"{self.loaded_data_path}/{ os.path.basename(src_path)}"
        shutil.copyfile(src_path, target)
        print(f'Transfared {src_path} to {target}')