# SQS-to-Postgres
Objective:
1. read JSON data containing user login behavior from an AWS SQS Queue , that is made available via a custom localStack image that has the data pre loaded.
2. Find a way for data analysts to identify duplicate values in those fields.
3. write each record to a Postgres database that is made available via a custom postgres image that has the tables pre created.

This README file will help you run the Take home test. For the most part I commented on the code file(fetch-rewards-data-engineering-TH.py).

•	Package Installation:  

In order to running the code, apart from python being installed in the system, following packages needs to be installed:

•	pip install awscli-local - for importing boto3.
•	pip install docker - for using the docker image provided for reading the JSON data and wrting record to a Postgres DB. 
•	pip install psycopg2 - for using as an adaptor to connect between python and Postgres.  

•	Docker YAML File Setup:

I also added a sample docker-compose.yml file to setting up the test environment. Execute the following command to run it: docker-compose up -d

To check if the container is running: docker-compose ps

Run docker-compose down to Stop and remove the containers.  


•	Running the code:

I modularized the code into 3 custom functions, one for reading the data, one for identifying the duplicates, and other one for writing the data into a POSTGRES database.

I also created a main function as well to do the essential task. To run the code just execute the following command in CMD: python3 fetch-rewards-data-engineering-TH.py (or python based on your version). 

Make sure python is added to your environmental variable path.

