from watchdog.observers import Observer
from libs.models.pipelinerunner import PipelineRunner
from libs.models.observer import PipelineHandler
import time

DB_PATH = r'./database/spotify.db'
INGEST_PATH = r'./data/ingest'
INVALIDE_DATA_PATH = r'./data/invalide'
FINISHED_DATA_PATH = r'./data/loaded'

def main():
    pipeline_runner = PipelineRunner(   db_path=DB_PATH, 
                                        invalide_data_path=INVALIDE_DATA_PATH, 
                                        loaded_data_path= FINISHED_DATA_PATH)
    event_handler = PipelineHandler(pipeline_runner)
    observer = Observer()
    observer.schedule(event_handler, path=INGEST_PATH, recursive=False)
    observer.start()

    try:
        while True:
            # Set the thread sleep time
            time.sleep(1)
    except KeyboardInterrupt:
        print('Stop')
        observer.stop()
    observer.join()


if __name__ == '__main__':
    main()