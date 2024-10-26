import os
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# Caminhos dos arquivos de chave
PRIVATE_KEY_PATH = "keys/private_key"
PUBLIC_KEY_PATH = "keys/public_key"

def generate_keys(private_key_path,public_key_path):
    if os.path.exists(private_key_path) or os.path.exists(public_key_path):
        print("As chaves já existem")
        return

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    public_key = private_key.public_key()

    with open(private_key_path, "wb") as private_file:
        private_file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            )
        )

    with open(public_key_path, "wb") as public_file:
        public_file.write(
            public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
        )

    print(f"Chaves RSA geradas e salvas em '{PRIVATE_KEY_PATH}' e '{PUBLIC_KEY_PATH}'")

if __name__ == "__main__":

    if not os.path.exists("keys"):
        os.makedirs("keys")

    generate_keys(f'{PRIVATE_KEY_PATH}_inclusion.pem', f'{PUBLIC_KEY_PATH}_inclusion.pem')
    generate_keys(f'{PRIVATE_KEY_PATH}_recommendation.pem', f'{PUBLIC_KEY_PATH}_recommendation.pem')
