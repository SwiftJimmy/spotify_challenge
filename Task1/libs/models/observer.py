from datetime import datetime
from watchdog.events import FileSystemEventHandler
from libs.models.spotify_pipelinerunner import SpotifyPipelineRunner
from libs.models.pipelinerunner import PipelineRunner

class PipelineHandler(FileSystemEventHandler):
    """
    PipelineHandler used to observe the ingestion folder for incomming files.
    """
    pipeline_runner:SpotifyPipelineRunner


    def __init__(self, pipeline_runner:PipelineRunner):
        print('PipelineHandler is running.')
        print('Waiting for new files to arrive in data/ingest')
        try:
            self.pipeline_runner = pipeline_runner
            self.pipeline_runner.create_database()
        except Exception as e:
            print(e)
            print(f"Database creation faild")
        
        

    def on_created(self, event):
        """
        Called when a file is created in the ingestion folder.
        Parameters:
            event (): Incomming event
        """
        print(f"{datetime.now()} - New file in pipeline: {event.src_path}")
        try:             
            self.pipeline_runner.run_pipeline(src_path=event.src_path)   
        except Exception as e:
            print(e)
            self.pipeline_runner.on_error(src_path=event.src_path)
