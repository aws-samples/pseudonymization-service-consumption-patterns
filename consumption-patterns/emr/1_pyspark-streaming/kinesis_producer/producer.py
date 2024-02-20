from kiner.producer import KinesisProducer
import json
from random import randrange
import datetime
import time
import sys

"""
    Requirements: pip3 install kiner
	Title: Python Kinesis Producer
	Description: 
		This code is responsible to write batch of records to a given Kinesis Data Stream.
		The code picks up three inputs to identify batch size and parallelism, then it creates the example records and writes them to a queue until the batch size is reached.
        Once the queue threshold is reached, the batch is written to Kinesis Data Stream

	Example usage: python3 producer.py <KINESIS_DATA_STREAM_NAME> <BATCH> <THREADS_NUM>
"""

kinesis_stream = sys.argv[1]
batch_size = int(sys.argv[2])
threads = int(sys.argv[3])

MAX_RANGE = 40 * 1000 * 1000
MAX_DEPTS = 10

p = KinesisProducer(
    kinesis_stream, batch_size=batch_size, max_retries=5, threads=threads
)


def create_record():
    return {
        "dept_id": str(randrange(MAX_DEPTS)),
        "vin": str(randrange(MAX_RANGE)),
        "example_ts": datetime.datetime.now().isoformat(),
    }


records_sent = 0
start_time = time.time()

while True:
    records = []

    for i in range(batch_size):
        records.append(create_record())

    for r in records:
        p.put_record(json.dumps(r))

    records_sent += batch_size

    elapsed_time = time.time() - start_time

    if elapsed_time >= 60.0:
        records_per_second = records_sent / elapsed_time
        print(f"Records sent per minute: {records_per_second:.2f}")
        records_sent = 0
        start_time = time.time()
