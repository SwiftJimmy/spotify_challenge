import os
import shutil
import sqlite3
from libs.etl import etl
from libs.db import db_setup


class PipelineRunner():
    """
    PipelineRunner orchestrates the data livecycle.
    """
    db_path: str
    invalide_data_path: str
    loaded_data_path:str

    def __init__(self, db_path:str, invalide_data_path:str, loaded_data_path:str):
        self.db_path = db_path
        self.invalide_data_path = invalide_data_path
        self.loaded_data_path = loaded_data_path


    def create_database(self):
        """
        Creates the database.
        """
        conn= sqlite3.connect(self.db_path, check_same_thread=False)
        db_setup.create_spotify_db(conn)
        conn.close()


    def run_spotify_pipeline(self, src_path:str):
        """
        Runs the data etl pipeline.
        Parameters:
            src_path (str): File source path
        """
        print('Start extraction.')
        try:
            df = etl.extract(src_path)
        except Exception as e:
            print(e)
            print('Extraction faied')
        print('Finished extraction.')
        print('Start validation.')
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
        """
        Copies a file to the invalide folder and deletes the source. 
        Parameters:
            src_path (str): File source path
        """
        target = f"{self.invalide_data_path}/{ os.path.basename(src_path)}"
        shutil.copyfile(src_path, target)
        os.remove(src_path)
        print(f'Transfared {src_path} to {target}')


    def copy_loaded_file(self, src_path:str):
        """
        Copies a file to the loaded folder and deletes the source. 
        Parameters:
            src_path (str): File source path
        """
        target = f"{self.loaded_data_path}/{ os.path.basename(src_path)}"
        shutil.copyfile(src_path, target)
        os.remove(src_path)
        print(f'Transfared {src_path} to {target}')
