# spotify_challenge

## Overview
The following describes the 5 main folders of the repository:

1. The content of folder **data** represents the data life cycle during the etl job. 
As soon as the pipeline has been started, all new files, stored in the **ingest** subfolder getting processed. If an error occurs during the data processing, they will be moved to the **invalid** folder. The corrupted data can be either the entire file or just individual records. The data moves to the **loaded** folder, once they have been successfully processed and loaded into the database.

2. The sqlite database is created in the folder **database**

3. The code for the data pipeline is located in the **Task1** folder. After all requirements have been installed, the **main.py** file can be executed. This starts a FileSystemEventHandler which observs the **data/ingest** folder for new incomming files. So just copy the sample data, or put your own dataset into the ata/ingest folder

4. In folder **Task2** you will find all the solutions for the three tasks from the assignment Task2. You will also find a corresponding README file for each task, which describes the solutions in more detail.

5. In folder **Task3** you will find all the solutions for the assignment Task3.
