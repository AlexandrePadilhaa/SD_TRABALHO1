#python publisher/publisher_inclusion.py
import pika
import csv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes

def load_private_key():
    with open("../SD_TRABALHO1/keys/private_key_inclusion.pem", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    return private_key

def include_movie(movie):
    with open("../SD_TRABALHO1/movies/movies.csv", mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([movie['genre'], movie['title']])
        return f"Movie {movie['title']} included in the catalogue"
    
    return f"Error including {movie['title']} in the catalogue"

def publish_message(movie):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # exchange do tipo 'fanout'
    channel.exchange_declare(exchange='movie_exchange', exchange_type='fanout')

    private_key = load_private_key()

    message = include_movie(movie)

    signature = private_key.sign(
        message.encode('utf-8'),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

    #mensagem no exchange
    channel.basic_publish(
        exchange='movie_exchange',  
        routing_key='',  
        body=message.encode('utf-8') + b'|' + signature 
    )
    
    print(f"Mensagem enviada: {message}")

    connection.close()

if __name__ == "__main__":
    movie = {'genre': 'Fantasy', 'title': 'Harry Potter 2'}
    publish_message(movie)
