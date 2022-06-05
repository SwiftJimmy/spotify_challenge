from datetime import datetime
from watchdog.events import FileSystemEventHandler
from libs.models.pipelinerunner import PipelineRunner

class PipelineHandler(FileSystemEventHandler):
    """
    PipelineHandler used to observe the ingestion folder for incomming files.
    """
    pipeline_runner:PipelineRunner


    def __init__(self, pipeline_runner:PipelineRunner):
        print('PipelineHandler is running.')
        print('Waiting for new files to arrive in data/ingest')

        self.pipeline_runner = pipeline_runner
        self.pipeline_runner.create_database()
        

    def on_created(self, event):
        """
        Called when a file is created in the ingestion folder.
        Parameters:
            event (): Incomming event
        """
        print(f"{datetime.now()} - New file in pipeline: {event.src_path}")

        if (event.src_path.lower().endswith(('.json', '.txt'))):
            self.pipeline_runner.run_spotify_pipeline(src_path=event.src_path)
        else:
            self.pipeline_runner.copy_invalide_file(src_path=event.src_path)
