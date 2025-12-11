import os
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

# Debe coincidir con el del generador
MAGIC_SEPARATOR = b"||__SECRET_PAYLOAD__||"

def obtener_secreto(ruta_archivo: str, password: str) -> str:
    """
    Desencripta un archivo .p12 híbrido (Método Canguro) y devuelve el secreto oculto.
    
    Args:
        ruta_archivo (str): Ruta al archivo .p12
        password (str): Contraseña para desbloquear el certificado
        
    Returns:
        str: El secreto desencriptado (API Key, Token, etc.)
        
    Raises:
        FileNotFoundError: Si el archivo no existe.
        ValueError: Si la contraseña es incorrecta o el formato no es válido.
    """
    
    if not os.path.exists(ruta_archivo):
        raise FileNotFoundError(f"No se encuentra el archivo: {ruta_archivo}")

    # 1. Leer archivo binario
    with open(ruta_archivo, "rb") as f:
        contenido_total = f.read()

    # 2. Buscar el separador y cortar
    if MAGIC_SEPARATOR not in contenido_total:
        raise ValueError("El archivo no tiene el formato híbrido esperado (Falta el separador).")

    p12_bytes, encrypted_payload = contenido_total.split(MAGIC_SEPARATOR, 1)

    try:
        # 3. Desbloquear el contenedor PKCS#12
        # Esto valida la contraseña y nos da la Clave Privada
        private_key, certificate, _ = pkcs12.load_key_and_certificates(
            p12_bytes,
            password.encode(),
            backend=default_backend()
        )
    except ValueError:
        raise ValueError("Contraseña incorrecta.")

    # 4. Desencriptar el Payload usando la Clave Privada
    try:
        decrypted_bytes = private_key.decrypt(
            encrypted_payload,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return decrypted_bytes.decode('utf-8')
    except Exception as e:
        raise ValueError(f"Error al desencriptar el payload: {e}")