import os
import shutil
import sqlite3
from libs.etl import etl
from libs.models.pipelinerunner import PipelineRunner


class SpotifyPipelineRunner(PipelineRunner):
    """
    PipelineRunner orchestrates the data livecycle.
    """
    db_path: str
    db_schema_path: str
    invalide_data_path: str
    loaded_data_path:str
    

    def __init__(self, db_path:str, invalide_data_path:str, loaded_data_path:str, db_schema_path:str  ):
        self.db_path = db_path
        self.invalide_data_path = invalide_data_path
        self.loaded_data_path = loaded_data_path
        self.db_schema_path = db_schema_path


    def create_database(self):
        """
        Creates the database.
        """
        conn= sqlite3.connect(self.db_path, check_same_thread=False)
        with open(self.db_schema_path, 'r') as sql_schema_file:
            sql_script = sql_schema_file.read()
        conn.cursor().executescript(sql_script)
        conn.commit()
        conn.close()
        


    def run_pipeline(self, src_path:str):
        """
        Runs the data etl pipeline.
        Parameters:
            src_path (str): File source path
        """
        print('Start extraction.')
        df = etl.extract(src_path)
        print('Finished extraction.')

        print('Start validation.')
        df = etl.validate_data(df, invalid_data_path=self.invalide_data_path)
        print('Finished validateion.')

        print('Start transformation.')
        df = etl.transform(df)
        print('Finished transformation.')

        print('Start load.')
        conn= sqlite3.connect(self.db_path, check_same_thread=False)
        etl.load(df, db_conn=conn)
        self.copy_file(src_path, self.loaded_data_path)   
        conn.close()
        print('Finished load.')
        

    def on_error(self, src_path:str):
        """
        Copy the faulty file on error 
        Parameters:
            src_path (str): File source path
        """
        faulty_file = self.copy_file(src_path, self.invalide_data_path)
        print(f"Data processing failed for file {faulty_file}")


    def copy_file(self, src_path:str, target_path):
        """
        Copies a file to the target folder and deletes the source. 
        Parameters:
            src_path (str): File source path
            target_path (str): Target path
        """
        target = f"{target_path}/{ os.path.basename(src_path)}"
        shutil.copyfile(src_path, target)
        os.remove(src_path)
        print(f'Transfared {src_path} to {target}')
        return target



