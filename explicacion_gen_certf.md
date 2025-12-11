# Implementación de Seguridad: Certificado Híbrido PEM (X.509 + Payload)

## Descripción Técnica
Para cumplir con los requisitos de despliegue y seguridad, se ha diseñado un mecanismo de persistencia basado en un **Certificado X.509 Híbrido en formato PEM**. Este método extiende la funcionalidad de un archivo de certificado estándar, permitiéndole actuar simultáneamente como identidad para conexiones SSL/TLS y como contenedor cifrado de credenciales sensibles (API Keys).

El resultado es un único archivo (`certServ.pem`) que cumple estrictamente con el formato OpenSSL estándar, pero que aloja datos adicionales mediante la técnica de inyección de payload (Método Canguro).

## Estructura del Archivo
El archivo `certServ.pem` generado no es un simple archivo de texto, sino una estructura binaria compuesta compuesta por cuatro segmentos secuenciales:

1.  **Clave Privada RSA:** Bloque PEM (`-----BEGIN PRIVATE KEY-----`) que contiene la clave privada de 2048 bits. Siguiendo las especificaciones del entorno (comando `-nodes`), esta clave se almacena sin cifrado de transporte.
2.  **Certificado Público:** Bloque PEM (`-----BEGIN CERTIFICATE-----`) que contiene la identidad pública (Organización: UA, Localidad: Alicante, etc.) firmada digitalmente.
3.  **Separador Binario:** Una secuencia de bytes única (`||__SECRET_PAYLOAD__||`) que actúa como frontera lógica invisible para los lectores estándar.
4.  **Payload Cifrado:** La API Key del servicio externo (OpenWeather), cifrada matemáticamente mediante RSA utilizando la Clave Pública del propio certificado.

## Modelo de Seguridad
El sistema implementa un modelo de seguridad basado en **Criptografía Asimétrica (RSA)** para proteger el secreto en reposo:

1.  **Confidencialidad:** La API Key no se almacena en texto plano. Está cifrada con la Clave Pública, lo que garantiza que solo la Clave Privada correspondiente puede revertir el proceso.
2.  **Integridad:** El secreto viaja inseparablemente unido a la identidad criptográfica que lo protege.
3.  **Dependencia:** Para recuperar el secreto, el sistema requiere acceso de lectura al archivo `certServ.pem`. Al no usar contraseña (requisito `-nodes`), la seguridad delega en los permisos del sistema de archivos, evitando la necesidad de interacción humana (input de contraseña) durante el arranque del servicio.

## Funcionamiento en el Módulo EV_W
El flujo de recuperación de credenciales en tiempo de ejecución es el siguiente:

1.  **Localización:** Al iniciar, el módulo busca el archivo `certServ.pem` en la raíz del proyecto.
2.  **Extracción Híbrida:** El script de lectura (`descifrar_certf.py`) carga el archivo completo en modo binario y localiza el **Separador Binario**.
3.  **Segmentación:** El archivo se divide en memoria: la parte superior se procesa como un objeto criptográfico estándar y la parte inferior como datos cifrados.
4.  **Descifrado RSA:** Se utiliza la Clave Privada (extraída de la parte superior) para descifrar el payload (parte inferior), obteniendo la API Key limpia.
5.  **Uso:** La API Key descifrada se inyecta en la configuración del entorno para realizar las peticiones a OpenWeather, sin haber sido expuesta nunca en disco en texto plano.