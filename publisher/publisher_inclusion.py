#python publisher/publisher_inclusion.py
import pika
import csv
import time
import json
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
    with open("../SD_TRABALHO1/movies/catalogue.csv", mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([movie['genre'], movie['title']])
        return f"Movie {movie['title']} included in the catalogue"
    
    return f"Error including {movie['title']} in the catalogue"

def update_inclusion_list(reader,included_movies):
    # deleting movies already included in the catalogue
    updated_list = [x for x in reader if x not in included_movies]
    with open('../SD_TRABALHO1/movies/movies_to_include.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Genre','Movie'])
        writer.writerows(updated_list)
    
def publish_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # exchange do tipo 'fanout'
    channel.exchange_declare(exchange='movie_exchange', exchange_type='fanout')

    private_key = load_private_key()
    
    start_time = time.time()

    included_movies = []
    
    with open('../SD_TRABALHO1/movies/movies_to_include.csv', "r" , encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader) # header
        
        for row in reader:
            
            if time.time() - start_time > 30:
                print('30 seconds past: Stoping execution')
                break 
            
            movie_genre, movie_name = row
            included_movies.append(row)
        
            message = include_movie({'genre': movie_genre, 'title': movie_name})

            signature = private_key.sign(
                message.encode('utf-8'),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            
            # usando json para garantir a integridade dos dados
            data = json.dumps({'message' : message,
                       'signature' : signature.hex()} # em hex porque o json não aceita bytes
                      ).encode('utf-8') 

            #mensagem no exchange
            channel.basic_publish(
                exchange='movie_exchange',  
                routing_key='',  
                body=data
            )
                
            print(f"Message sent: {message}")
            time.sleep(3)

        update_inclusion_list(reader,included_movies)
    
    connection.close()

if __name__ == "__main__":
    publish_message()
