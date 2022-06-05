from watchdog.observers import Observer
from libs.etl.etl import extract
from libs.models.observer import PipelineHandler

def main():
    event_handler = PipelineHandler()
    observer = Observer()
    observer.schedule(event_handler, path='./data/', recursive=False)
    observer.start()

    while True:
        try:
            pass
        except KeyboardInterrupt:
            observer.stop()



if __name__ == '__main__':
    main()