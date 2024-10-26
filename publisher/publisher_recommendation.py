#python publisher_recommendation.py
import pika
import csv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes

def load_private_key():
    with open("../SD_TRABALHO1/keys/private_key_recommendation.pem", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    return private_key

def load_recommendations(genre: str):
    movies = []
    with open("../SD_TRABALHO1/movies/movies.csv", "r", encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            movie_genre, movie_name = row
            if movie_genre.lower() == genre.lower():
                movies.append(movie_name)
                
    movies = ', '.join(movies)            
    return f'{genre} movies recommendations: {movies}'

def publish_recommendation(genre):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='my_queue')

    private_key = load_private_key()
    
    recommendations = load_recommendations(genre)

    signature = private_key.sign(
        recommendations.encode('utf-8'),  # mensagem em bytes
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
        body=recommendations.encode('utf-8') + b'|' + signature  # separa a mensagem e a assinatura
    )
    
    print(f"Mensagem enviada: {recommendations}")

    connection.close()

if __name__ == "__main__":
    publish_recommendation('fantasy')
