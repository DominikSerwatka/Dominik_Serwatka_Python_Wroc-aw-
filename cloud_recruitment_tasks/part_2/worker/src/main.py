import time
import boto3
import os

sqs = boto3.client('sqs', endpoint_url=os.getenv('AWS_ENDPOINT_URL'), region_name=os.getenv('AWS_DEFAULT_REGION'),
                   aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                   aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
s3 = boto3.client('s3', endpoint_url=os.getenv('AWS_ENDPOINT_URL'), region_name=os.getenv('AWS_DEFAULT_REGION'),
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

worker_queue_url = os.getenv('WORKER_QUEUE_URL')
debts_bucket_name = os.getenv('DEBTS_BUCKET_NAME')


def process_file_from_s3(bucket_name, file_key):
    # Pobieranie pliku z S3
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    # Zakładamy, że plik jest w formacie CSV
    data = obj['Body'].read().decode('utf-8')
    # Tutaj można przetworzyć dane, np. optymalizacja długów
    print("Przetworzono dane:", data)
    # Można też zapisać wyniki przetworzenia z powrotem do S3...


if __name__ == "__main__":
    while True:
        response = sqs.receive_message(QueueUrl=worker_queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=20)
        messages = response.get('Messages', [])
        if messages:
            for message in messages:
                # Przetwarzanie otrzymanego identyfikatora pliku
                debts_id = message['Body']
                print(f"Odbieranie pliku dla debtsId: {debts_id}")
                process_file_from_s3(debts_bucket_name, f"{debts_id}.csv")

                # Usuwanie wiadomości z kolejki po przetworzeniu
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(QueueUrl=worker_queue_url, ReceiptHandle=receipt_handle)
        else:
            print("Brak nowych wiadomości.")
        time.sleep(5)
