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
        redis_host = os.environ.get('REDIS_HOST', 'meucluster-wdul6z.serverless.use1.cache.amazonaws.com')
        redis_port = int(os.environ.get('REDIS_PORT', 6379))
        socket_timeout=5,  
        socket_connect_timeout=5  
        self.redis_client = redis.Redis(host=redis_host, port=redis_port)

    def generate_random_file(self, min_lines=100, max_lines=1000):
        """
        Gera um arquivo de texto com número aleatório de linhas
        """
        num_lines = random.randint(min_lines, max_lines)
        filename = f"{uuid.uuid4()}.txt"
        
        with open(filename, 'w') as f:
            for _ in range(num_lines):
                f.write(f"Linha aleatória: {random.random()}\n")
        
        return filename, num_lines

    def upload_to_s3(self, filename):
        """
        Upload do arquivo para o S3
        """
        try:
            self.s3_client.upload_file(filename, self.bucket_name, filename)
            return True
        except ClientError as e:
            print(f"Erro no upload para S3: {e}")
            return False

    def send_sns_notification(self, filename, num_lines):
        """
        Envia notificação via SNS com mais detalhes
        """
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
        
        # Enviar mensagem
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
        """
        Envia mensagem para fila SQS
        """
        message_body = f"{filename}|{num_lines}"
        self.sqs_client.send_message(
            QueueUrl=self.sqs_queue_url,
            MessageBody=message_body
        )

    def cache_file_info(self, filename, num_lines):
        """
        Armazena informações do arquivo no Redis
        """
        self.redis_client.hset(f"file:{filename}", {
            "name": filename,
            "lines": num_lines
        })

    def process_file(self):
        """
        Processo completo de geração, upload e notificação
        """
        filename, num_lines = self.generate_random_file()
        
        if self.upload_to_s3(filename):
            self.send_sns_notification(filename, num_lines)
            self.send_sqs_message(filename, num_lines)
            self.cache_file_info(filename, num_lines)
            
            # Remove arquivo local após upload
            os.remove(filename)
            
            print(f"Arquivo {filename} processado com sucesso!")

def lambda_handler(event, context):
    """
    Handler da Lambda para processamento
    """
    uploader = S3FileUploader()
    uploader.process_file()
    return {
        'statusCode': 200,
        'body': 'Processamento concluído'
    }

# Execução local para testes
if __name__ == "__main__":
    uploader = S3FileUploader()
    uploader.process_file()
    print("Processamento concluído. Encerrando o programa.")
    exit(0)