import datetime
import os
import boto3
import random
import uuid
import redis
from botocore.exceptions import ClientError

class S3FileUploader:
    def __init__(self):
        # Configurações AWS
        self.s3_client = boto3.client('s3')
        self.sns_client = boto3.client('sns')
        self.sqs_client = boto3.client('sqs')
        
        # Configurações de ambiente
        self.bucket_name = os.environ.get('S3_BUCKET_NAME', 'bucket-projeto-final-aws-ada')
        self.sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
        self.sqs_queue_url = os.environ.get('SQS_QUEUE_URL')
        
        # Configuração do Redis (Elasticache)
        try:
            redis_host = os.environ.get('REDIS_HOST', 'meucluster-wdul6z.serverless.use1.cache.amazonaws.com')
            redis_port = int(os.environ.get('REDIS_PORT', 6379))
            self.redis_client = redis.Redis(
                host=redis_host, 
                port=redis_port,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=False
            )
        except Exception as e:
            print(f"Erro ao conectar ao Redis: {e}")
            self.redis_client = None

    def generate_random_file(self, min_lines=100, max_lines=1000):
        num_lines = random.randint(min_lines, max_lines)
        filename = f"{uuid.uuid4()}.txt"
        
        with open(filename, 'w') as f:
            for _ in range(num_lines):
                f.write(f"Linha aleatória: {random.random()}\n")
        
        return filename, num_lines

    def upload_to_s3(self, filename):
        try:
            self.s3_client.upload_file(filename, self.bucket_name, filename)
            return True
        except ClientError as e:
            print(f"Erro no upload para S3: {e}")
            return False

    def send_sns_notification(self, filename, num_lines):
        if not self.sns_topic_arn:
            print("SNS Topic ARN não configurado")
            return

        message = f"""
        Novo Relatório Gerado
        -------------------
        Nome do Arquivo: {filename}
        Número de Linhas: {num_lines}
        Timestamp: {datetime.datetime.now()}
        
        Detalhes Adicionais:
        - Bucket S3: {self.bucket_name}
        - Processo: Geração Automática de Arquivo
        """
        
        try:
            response = self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Message=message,
                Subject='Novo Arquivo Gerado no Sistema'
            )
            print(f"Notificação enviada. MessageId: {response['MessageId']}")
        except Exception as e:
            print(f"Erro ao enviar notificação SNS: {e}")

    def send_sqs_message(self, filename, num_lines):
        if not self.sqs_queue_url:
            print("SQS Queue URL não configurado")
            return

        try:
            message_body = f"{filename}|{num_lines}"
            self.sqs_client.send_message(
                QueueUrl=self.sqs_queue_url,
                MessageBody=message_body
            )
        except Exception as e:
            print(f"Erro ao enviar mensagem SQS: {e}")

    def cache_file_info(self, filename, num_lines):
        if not self.redis_client:
            print("Cliente Redis não disponível")
            return

        try:
            self.redis_client.hset(f"file:{filename}", 
                                 mapping={"name": filename, "lines": num_lines})
        except Exception as e:
            print(f"Erro ao armazenar no Redis: {e}")

    def process_file(self):
        try:
            filename, num_lines = self.generate_random_file()
            
            if self.upload_to_s3(filename):
                self.send_sns_notification(filename, num_lines)
                self.send_sqs_message(filename, num_lines)
                self.cache_file_info(filename, num_lines)
            
            # Remove arquivo local após processamento
            if os.path.exists(filename):
                os.remove(filename)
                
            print(f"Arquivo {filename} processado com sucesso!")
        except Exception as e:
            print(f"Erro durante o processamento: {e}")

def lambda_handler(event, context):
    uploader = S3FileUploader()
    uploader.process_file()
    return {
        'statusCode': 200,
        'body': 'Processamento concluído'
    }

if __name__ == "__main__":
    uploader = S3FileUploader()
    uploader.process_file()