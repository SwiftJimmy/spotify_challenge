# spotify_challenge

## Overview
The following describes the 5 main folders of the repository:

1. The content of folder **data** represents the data life cycle during the etl job. 
As soon as the pipeline has been started, all files moved in the **ingest** subfolder getting processed.
If an error occurs during the processing of the data, they will be moved to the **invalid** folder. The corrupted data can be the entire file or just individual records.
The data moves to the **loaded** folder, once they have been successfully processed and loaded into the database.

2. The sqlite database is created in the folder **database**

3. The code for the data pipeline is located in the **Task1** folder. 
After all requirements have been installed, the **main.py** file can be executed.
This starts a FileSystemEventHandler which observs the **data/ingest** for new incomming files.
So just copy the sample data, or put your dataset into the ata/ingest folder

4. In the **Task2** folder you will find all the solutions for the three tasks from the assignment Task2.
For each task a corresponding README file was created, which describes the solution in more detail.

5. In the **Task3** folder you will find all the solutions for the assignment Task3.
