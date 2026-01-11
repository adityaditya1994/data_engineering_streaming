import boto3
import json
import time
import random
import uuid
from datetime import datetime
from config import CONFIG

def get_kinesis_client():
    return boto3.client('kinesis', region_name=CONFIG['region'])

def load_sample_data(filepath='../EMR/Pyspark/sample_clickstream.json'):
    with open(filepath, 'r') as f:
        return json.load(f)

def generate_record(base_record):
    """
    Takes a sample record and updates timestamp/ids to make it unique and current.
    """
    record = base_record.copy()
    record['timestamp'] = int(time.time())
    # Add slight randomization to values
    if 'temperature' in record:
        record['temperature'] = round(record['temperature'] + random.uniform(-0.5, 0.5), 1)
    if 'humidity' in record:
        record['humidity'] = round(record['humidity'] + random.uniform(-1, 1), 1)
    
    # Add a unique ID for tracking
    record['uuid'] = str(uuid.uuid4())
    return record

def run_producer(target_throughput_kb_sec=100):
    client = get_kinesis_client()
    stream_name = CONFIG['stream_name']
    sample_data = load_sample_data()
    
    print(f"Starting producer on stream: {stream_name}")
    print(f"Target throughput: {target_throughput_kb_sec} KB/sec")
    
    batch_size = 50  # Records per PutRecords call
    total_bytes_sent = 0
    start_time = time.time()
    
    try:
        while True:
            records_batch = []
            batch_bytes = 0
            
            # Create a batch
            for _ in range(batch_size):
                # Pick a random template from sample data
                template = random.choice(sample_data)
                record = generate_record(template)
                data_json = json.dumps(record) + '\n'
                
                entry = {
                    'Data': data_json,
                    'PartitionKey': str(record.get('sensor_id', 'default'))
                }
                records_batch.append(entry)
                batch_bytes += len(data_json.encode('utf-8'))
            
            # Send batch
            response = client.put_records(
                StreamName=stream_name,
                Records=records_batch
            )
            
            failed_count = response['FailedRecordCount']
            if failed_count > 0:
                print(f"Warning: {failed_count} records failed to put.")
            
            total_bytes_sent += batch_bytes
            
            # Throttle to meet target throughput
            # 100 KB/s
            current_time = time.time()
            elapsed = current_time - start_time
            expected_bytes = elapsed * (target_throughput_kb_sec * 1024)
            
            if total_bytes_sent > expected_bytes:
                sleep_time = (total_bytes_sent - expected_bytes) / (target_throughput_kb_sec * 1024)
                if sleep_time > 0:
                   time.sleep(sleep_time)
            
            # Log every 5 seconds
            if int(elapsed) % 5 == 0 and int(elapsed) > 0:
                 rate = total_bytes_sent / 1024 / elapsed
                 print(f"Sent {total_bytes_sent/1024:.2f} KB in {elapsed:.0f}s. Avg Rate: {rate:.2f} KB/sec")
                 
    except KeyboardInterrupt:
        print("\nStopping producer...")

if __name__ == "__main__":
    run_producer()
