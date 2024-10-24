#python publisher_1.py
import pika

def publish_message(message):

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()


    channel.queue_declare(queue='my_queue')


    channel.basic_publish(exchange='', routing_key='my_queue', body=message)
    
    print(f"Mensagem enviada: {message}")


    connection.close()

if __name__ == "__main__":
    publish_message("Olá, RabbitMQ!")
