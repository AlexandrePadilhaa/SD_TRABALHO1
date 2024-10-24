#python consumer_1.py
import pika

def callback(ch, method, properties, body):
    print(f"Mensagem recebida: {body.decode()}")

def consume_message():

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()


    channel.queue_declare(queue='my_queue')


    channel.basic_consume(queue='my_queue', on_message_callback=callback, auto_ack=True)
    
    print("Aguardando mensagens...")


    channel.start_consuming()

if __name__ == "__main__":
    consume_message()
