#python consumer/consumer_recommendation.py
import pika
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes

def load_public_key():
    with open('../SD_TRABALHO1/keys/public_key_recommendation.pem', "rb") as key_file:
        public_key = serialization.load_pem_public_key(key_file.read())
    return public_key

def callback(ch, method, properties, body):
    message, signature = body.rsplit(b'|', 1)

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
        print(f"Mensagem recebida: {message.decode('utf-8')}")
        
    except Exception as e:
        print(f"Falha na verificação da assinatura: {e}")

def consume_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Exchange do tipo 'fanout'
    channel.exchange_declare(exchange='recommendation_exchange', exchange_type='fanout')

    # fila temporária exclusiva para este consumidor
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Vincular a fila temporária ao exchange
    channel.queue_bind(exchange='recommendation_exchange', queue=queue_name)

    print(f"Aguardando mensagens em {queue_name}...")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

if __name__ == "__main__":
    consume_message()
