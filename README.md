### Problem Statement

Design an ingestion pipeline to fetch Mutual Fund data from the AMFI website, model, and store that data into a database for further processing.

### Checklist

1. Pipeline can do a full fetch and load
2. Initial fetch and loading into database takes less than 30 seconds (more than 3,00,000 entries in less than 30 seconds)
3. Pipeline supports parallel processing using processes to crawl website for data
4. Pipeline uses queue system to pass data for DB insertion
5. Deals with failures and logs them into log files
6. Secondary indexes created for better query performance
7. Normalise scheme_name for faster and better queries

### Flow

1. First a pool of multiple processes is created if --inital-fetch=True and each process then starts to crawl the website from a date range. If inital-fetch = False, then no pool is created.
2. This data is then fetched and cleaned up.
3. Cleaned up data is sent to a RabbitMQ link
4. Receiver.py is actively consuming the queue and inserts the data into MongoDB when it gets it.

### Tools and frameworks

1. MongoDB
2. RabbitMQ
3. Multiprocessing

### Steps to Run Code

1. Download RabbitMQ docker image and run the command ```docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.9-management```
2. Run ```pip install -r requirements.txt```
2. Run ```python receiver.py ```
3. Run ```python main.py --initial-fetch=True```

### Scope of improvement

1. Dockerize the application and create a deployable solution
2. Transform data for more readability and analysis
3. Scripts to automate calling of python scripts
4. Better error handling and script to read log files and analyse it