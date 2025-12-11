import os
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

# Debe coincidir con el generador
MAGIC_SEPARATOR = b"||__SECRET_PAYLOAD__||"

def obtener_secreto(ruta_cert: str, password=None) -> str:
    """
    Lee un archivo PEM Híbrido y extrae el secreto oculto al final.
    El password se ignora porque estos PEMs se generan sin contraseña (-nodes).
    """
    if not os.path.exists(ruta_cert):
        raise FileNotFoundError(f"No se encuentra el archivo: {ruta_cert}")

    # 1. Leer archivo completo
    with open(ruta_cert, "rb") as f:
        contenido_total = f.read()

    # 2. Separar PEM del Secreto
    if MAGIC_SEPARATOR not in contenido_total:
        raise ValueError("El archivo no tiene un secreto inyectado (Formato incorrecto).")
    
    # Partimos el archivo en dos trozos usando el separador
    partes = contenido_total.split(MAGIC_SEPARATOR, 1)
    pem_data = partes[0]        # Lo de arriba (Key + Cert)
    encrypted_secret = partes[1] # Lo de abajo (Secreto)

    # 3. Cargar la Clave Privada desde la parte PEM
    # Usamos password=None porque el generador usó NoEncryption
    try:
        private_key = serialization.load_pem_private_key(
            pem_data,
            password=None, 
            backend=default_backend()
        )
    except Exception as e:
        raise ValueError(f"Error leyendo la clave privada del PEM: {e}")

    # 4. Desencriptar el secreto
    try:
        decrypted_bytes = private_key.decrypt(
            encrypted_secret,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return decrypted_bytes.decode('utf-8')
    except Exception as e:
        raise ValueError(f"Fallo al desencriptar. ¿Es el archivo correcto? {e}")

# Prueba rápida si ejecutas este fichero
if __name__ == "__main__":
    try:
        fichero = input("Archivo a leer [certServ.pem]: ").strip() or "certServ.pem"
        secreto = obtener_secreto(fichero)
        print(f"Secreto recuperado: {secreto}")
    except Exception as e:
        print(f"Error: {e}")