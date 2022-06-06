class PipelineRunner:
    """
    Base pipeline runner that you can override methods from.
    """

    def create_database(self):
        """
        Creates the database.
        """

    def run_pipeline(self, src_path):
        """
        Runs the data etl pipeline.
        Parameters:
            src_path (str): File source path
        """