#pip install awscli-local for importing boto3
#pip install docker for using the docker image provided for reading the JSON data and wrting record to a Postgres DB 
#pip install psycopg2 for using as an adaptor to connect between python and Postgres   


import json
import gzip
import boto3
import docker
import psycopg2

#Read JSON data from an AWS SQS Queue and returns a list of JSON objects.

def read_data():

	# Pull the docker image from the location provided
    docker.from_env().images.pull('fetchdocker/data-takehome-localstack')

    # Run the image
    docker.from_env().containers.run('fetchdocker/data-takehome-localstack', detach=True)

    # Data path within the docker image
    fetch_data = '/tmp/data/sample_data.json.gz'
	
	# Open the JSON data file in the docker image
	with gzip.open(fetch_data, 'rb') as f:
    fetch_data = json.load(f)
	
	# Create a boto3 SQS client.
	sqs = boto3.client('sqs', endpoint_url='http://localhost:4566/000000000000/login-queue')
	
	# Create an SQS queue in localStack.
	fetch_queue = 'fetch-queue'
	sqs.create_queue(QueueName=fetch_queue)

	# Send the JSON data to the SQS queue.
	message = {'Body': json.dumps(fetch_data)}
	sqs.send_message(QueueUrl=fetch-queue, MessageBody=json.dumps(message))

	# Receive the JSON data from the SQS queue.
	response = sqs.receive_message(QueueUrl=fetch-queue)
	message = response['Messages'][0]
	
	data_returned = json.loads(message['Body'])
	
	return data_returned


def identify_duplicates(data_returned):
	# Create a dictionary to store the unique records.
    unique_records = {}

    # Iterate over the JSON data.
    for record in data_returned:
        # Get the masked fields from the record.
        masked_fields = [
            "masked_ip",
            "masked_device_id",
        ]

        # Create a fingerprint for the record.
        fingerprint = 0
        for j in masked_fields:
            fingerprint += hash(record[j])

        # Check if the key is already in the dictionary.
        if fingerprint in unique_records:
            # The record is a duplicate.
            unique_records[fingerprint].append(record)
        else:
            # The record is not a duplicate.
            unique_records[fingerprint] = [record]

    # Get the list of duplicate records.
    duplicates = []
    for records in unique_records.values():
        if len(records) > 1:
            duplicates.append(records)

    return duplicates
	
	
def write_data_to_postgres(data_returned):

	# Pull the docker image from the location provided
    docker.from_env().images.pull('fetchdocker/data-takehome-postgres')

    # Run the image
    docker.from_env().containers.run('fetchdocker/data-takehome-postgres', detach=True)

	#connect to the Postgres Docker
	
	conn = psycopg2.connect(
			
            dbname='postgres',
            user='postgres',
            password='postgres',
            host='fetchdocker/data-takehome-postgres',
            port='5432'
        )
		
	# Iterate over the JSON data.
    for record in data_returned:
        # Insert the record into the table.
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO records (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (record['user_id'], record['device_type'], record['masked_ip'], record['masked_device_id'], record['locale'], record['app_version'], record['create_date']),
        )
		
	conn.commit()
	
	
def main():

	read_data = read_data()
	
	write_data_to_postgres(read_data)
	
if __name__="__main__":
	main()
	
	
	
	
	
	
	
	