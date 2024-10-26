#python consumer_1.py
import pika
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes

def load_public_key():
    with open("../keys/public_key.pem", "rb") as key_file:
        public_key = serialization.load_pem_public_key(key_file.read())
    return public_key

def callback(ch, method, properties, body):
    message, signature = body.rsplit(b'|', 1)

    print(f"Mensagem recebida: {message.decode()}")

    public_key = load_public_key()

    try:
        public_key.verify(
            signature,
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        print("Assinatura verificada com sucesso.")
    except Exception as e:
        print(f"Falha na verificação da assinatura: {e}")

def consume_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='my_queue')

    channel.basic_consume(queue='my_queue', on_message_callback=callback, auto_ack=True)
    
    print("Aguardando mensagens...")

    channel.start_consuming()

if __name__ == "__main__":
    consume_message()
