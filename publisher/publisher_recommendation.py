#python publisher/publisher_recommendation.py
import random
import time
import pika
import csv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
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
    with open("../SD_TRABALHO1/movies/catalogue.csv", "r", encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            #print(row)
            movie_genre, movie_name = row[:2] 
            if movie_genre.lower() == genre.lower():
                movies.append(movie_name)
                
    movies = ', '.join(movies)            
    return f'{genre} movie recommendations: {movies}'

def publish_recommendation(genre):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Exchange do tipo 'fanout' para recomendações
    channel.exchange_declare(exchange='recommendation_exchange', exchange_type='fanout')

    private_key = load_private_key()
    
    recommendations = load_recommendations(genre)

    signature = private_key.sign(
        recommendations.encode('utf-8'),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

    # Publicar a recomendação no exchange
    channel.basic_publish(
        exchange='recommendation_exchange', 
        routing_key='',  # Sem chave de roteamento no fanout
        body=recommendations.encode('utf-8') + b'|' + signature 
    )
    
    print(f"Message sent: {recommendations}")

    connection.close()

def get_genres_from_csv(file_path):
    genres = set()  
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  

        for row in reader:
            genre = row[0].strip()  
            genres.add(genre) 

    return list(genres)

def simulate_recommendation_cycle(duration_in_seconds, interval_in_seconds):
    genres = get_genres_from_csv('../SD_TRABALHO1/movies/movies.csv')

    start_time = time.time()
    elapsed_time = 0

    while elapsed_time < duration_in_seconds:
        genre = random.choice(genres)

        print(f"Publicando recomendação para o gênero: {genre}")
        publish_recommendation(genre)

        time.sleep(interval_in_seconds)

        elapsed_time = time.time() - start_time

if __name__ == "__main__":
    #publish_recommendation('Romance')
    simulate_recommendation_cycle(20, 2)
