from watchdog.events import FileSystemEventHandler

from libs.etl.etl import extract, transform, load

class PipelineHandler(FileSystemEventHandler):
    def on_created(self, event):
        extract(event.src_path)
        transform()
        load()
        return super().on_created(event)
    

