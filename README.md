## Overview
The following describes the 5 main folders of the repository and their usage:

1. The content of the folder **data** represents the data life cycle during the etl job. 
As soon as the pipeline has been started, all new files, stored in the **ingest** subfolder, are getting processed. If an error occurs during the data processing, the data will be moved to the **invalid** folder. The corrupted data can either be the entire file or just individual records. The data moves to the **loaded** folder once it has been successfully processed.

2. The sqlite database is created in the folder **database**. You will also find a database schema image.

3. The code for the data pipeline is located in the **Task1** folder. After all requirements have been installed, the **main.py** file can be executed. This starts a FileSystemEventHandler which observs the **data/ingest** folder for new incomming files. So just copy the sample data, or put your own dataset into the data/ingest folder. In order to stop the FileSystemEventHandler enter **Control + c** in the terminal. (Don't forget to stop the running FileSystemEventHandler before executing task 2/3 or use another terminal.)

4. In folder **Task2** you will find the solutions for the three sub tasks of assignment Task2. You will also find a corresponding README file for each task, which describes the solutions in more detail. 

5. In folder **Task3** you will find the solutions for the assignment Task3.


### Database Schema
Here is an overview of the database schema! 

![Logo](https://github.com/SwiftJimmy/spotify_challenge/blob/main/database/db_schema.jpg)
