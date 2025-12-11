''' 
Este programa genera un archivo .p12 (PKCS#12) con CUALQUIER secreto 
pegado al final de forma segura (Método Canguro Genérico).
'''

import getpass
import os
import datetime
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.backends import default_backend

# MARCA DE SEPARACIÓN (Importante para que el lector sepa dónde cortar)
MAGIC_SEPARATOR = b"||__SECRET_PAYLOAD__||"

def generar_identidad_generica():
    print("--- GENERADOR DE IDENTIDAD SEGURA (CANGURO / ONE-FILE) ---")
    
    # 1. SOLICITUD DE DATOS GENÉRICOS
    nombre_archivo = input("Nombre para el archivo (sin extensión): ").strip()
    if not nombre_archivo:
        print("El nombre es obligatorio.")
        return

    secreto = input("Introduce el DATO SECRETO a proteger (API Key, Token, Password...): ").strip()
    if not secreto:
        print("El secreto no puede estar vacío.")
        return

    password = getpass.getpass(f"Crea una contraseña para proteger '{nombre_archivo}.p12': ").strip()
    if not password:
        print("La contraseña es obligatoria.")
        return

    print("\nGenerando criptografía... (esto puede tardar un segundo)")

    # 2. Generar Clave Privada RSA (2048 bits)
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    # 3. Crear Certificado Autofirmado (Contenedor público)
    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, f"{nombre_archivo}_User"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Security Module"),
    ])
    
    cert = x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(
        private_key.public_key()
    ).serial_number(x509.random_serial_number()).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=3650) # Valido 10 años
    ).sign(private_key, hashes.SHA256(), default_backend())

    # 4. Empaquetar Clave + Cert en formato estándar P12
    p12_data = pkcs12.serialize_key_and_certificates(
        name=nombre_archivo.encode('utf-8'),
        key=private_key,
        cert=cert,
        cas=None,
        encryption_algorithm=serialization.BestAvailableEncryption(password.encode())
    )

    # 5. ENCRIPTAR EL SECRETO (Usando la parte pública del certificado)
    public_key = cert.public_key()
    encrypted_secret = public_key.encrypt(
        secreto.encode(),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

    # 6. FUSIÓN: Escribir P12 + Separador + Secreto Encriptado
    filename_completo = f"{nombre_archivo}.p12"
    
    with open(filename_completo, "wb") as f:
        f.write(p12_data)       # Identidad estándar
        f.write(MAGIC_SEPARATOR) # Frontera
        f.write(encrypted_secret) # Carga útil oculta

    print("\n✅ GENERADO CON ÉXITO.")
    print(f"Archivo: {filename_completo}")
    print("----------------------------------------------------------------")
    print("Este archivo contiene tu identidad digital y el secreto encriptado.")
    print("Para leerlo, tu programa necesitará buscar la marca separadora." + f" ({MAGIC_SEPARATOR.decode()})")

if __name__ == "__main__":
    generar_identidad_generica()