#python publisher_1.py
import pika
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes

def load_private_key():
    with open("../keys/private_key.pem", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    return private_key

def publish_message(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='my_queue')

    private_key = load_private_key()

    signature = private_key.sign(
        message.encode(),  # mensagem em bytes
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

    #assinatura como uma tupla
    channel.basic_publish(
        exchange='',
        routing_key='my_queue',
        body=message.encode() + b'|' + signature  # separa a mensagem e a assinatura
    )
    
    print(f"Mensagem enviada: {message}")

    connection.close()

if __name__ == "__main__":
    publish_message("Olá, RabbitMQ!")
