# generar_certificado.py

"""Generador de certificado X.509 en formato PEM que incluye un secreto oculto.
El secreto se cifra usando la clave pública del certificado y se almacena"""

import os
import datetime
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend

# MARCA DE SEPARACIÓN (Protocolo Canguro)
MAGIC_SEPARATOR = b"||__SECRET_PAYLOAD__||"

def pedir_dato(mensaje, defecto):
    """Ayuda para pedir datos con un valor por defecto"""
    valor = input(f"{mensaje} [{defecto}]: ").strip()
    return valor if valor else defecto

def generar_certificado_hibrido():
    print("--- 🛠️ GENERADOR DE CERTIFICADO PEM + SECRETO ---")
    
    # 1. PEDIR EL SECRETO
    secreto = input(">> Introduce el SECRETO a proteger (API Key, Token...): ").strip()
    if not secreto:
        print("El secreto es obligatorio.")
        return

    nombre_fichero = input("Nombre del archivo de salida").strip()
    if not nombre_fichero:
        raise ValueError("El nombre del archivo es obligatorio.")
    nombre_fichero = nombre_fichero if nombre_fichero.endswith(".pem") else nombre_fichero + ".pem"

    print("\n--- DATOS DEL CERTIFICADO X.509 ---")
    C  = pedir_dato("Country Name (C)", "ES")
    ST = pedir_dato("State/Province (ST)", "Comunidad Valenciana")
    L  = pedir_dato("Locality (L)", "Alicante")
    O  = pedir_dato("Organization (O)", "UA")
    OU = pedir_dato("Organizational Unit (OU)", "SD")
    CN = pedir_dato("Common Name (CN)", "localhost")

    print("\nGenerando criptografía... ⏳")

    # 2. GENERAR CLAVE PRIVADA (RSA 2048)
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

    # 3. CREAR EL CERTIFICADO
    subject = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, C),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, ST),
        x509.NameAttribute(NameOID.LOCALITY_NAME, L),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, O),
        x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, OU),
        x509.NameAttribute(NameOID.COMMON_NAME, CN),
    ])

    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        subject
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=365)
    ).sign(
        private_key, hashes.SHA256(), default_backend()
    )

    # 4. SERIALIZAR A PEM
    # Clave Privada SIN contraseña (equivale a -nodes)
    pem_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    # Certificado Público
    pem_cert = cert.public_bytes(serialization.Encoding.PEM)

    # 5. ENCRIPTAR EL SECRETO (Usando la parte pública del propio cert)
    public_key = cert.public_key()
    encrypted_secret = public_key.encrypt(
        secreto.encode(),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

    # 6. GUARDAR TODO EN UN SOLO ARCHIVO
    with open(nombre_fichero, "wb") as f:
        f.write(pem_key)         # Clave Privada
        f.write(pem_cert)        # Certificado
        f.write(MAGIC_SEPARATOR) # Separador
        f.write(encrypted_secret)# Secreto Oculto

    print(f"\nÉXITO. Archivo generado: '{nombre_fichero}'")
    print("Este archivo sirve para HTTPS y contiene tu secreto oculto.")

if __name__ == "__main__":
    generar_certificado_hibrido()