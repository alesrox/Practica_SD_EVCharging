# Implementación de Seguridad: Contenedor Híbrido PKCS#12

## Descripción Técnica
Para garantizar la seguridad y portabilidad de las credenciales, se ha implementado un mecanismo de persistencia basado en un **contenedor PKCS#12 híbrido**. Este método aprovecha la estructura binaria de los archivos para anexar datos arbitrarios al final del archivo (EOF) sin corromper la integridad del certificado estándar.

El resultado es un único archivo (`identidad.p12`) que actúa simultáneamente como identidad criptográfica del cliente y como almacén seguro de secretos (API Keys).

## Estructura del Archivo
El archivo generado se compone de tres segmentos binarios concatenados secuencialmente:

1.  **Contenedor PKCS#12 Estándar:** Bloque inicial que aloja el Certificado X.509 y la Clave Privada RSA. Este bloque está cifrado con algoritmos estándar (AES) y protegido por la contraseña del usuario.
2.  **Separador Binario:** Una secuencia de bytes predefinida (`||__SECRET_PAYLOAD__||`) que actúa como delimitador lógico.
3.  **Payload Cifrado:** La API Key del servicio externo (OpenWeather), cifrada mediante RSA utilizando la Clave Pública del certificado anterior.

## Modelo de Seguridad
La seguridad del sistema se basa en una cadena de dependencia criptográfica que impide el acceso no autorizado al secreto, incluso si el archivo es sustraído:

1.  El **Payload** (API Key) solo puede descifrarse con la **Clave Privada**.
2.  La **Clave Privada** reside dentro del bloque PKCS#12.
3.  El bloque PKCS#12 solo puede desbloquearse con la **Contraseña del Usuario**.

Por tanto, la seguridad del secreto es matemáticamente equivalente a la fortaleza de la contraseña del usuario y del algoritmo RSA-2048.

## Funcionamiento en el Módulo EV_W
En el contexto de esta práctica, el flujo de ejecución es el siguiente:

1.  **Inicio:** Al ejecutar `ev_w.py`, el sistema solicita la contraseña del certificado por consola.
2.  **Lectura y Corte:** El script carga el archivo `identidad.p12` en memoria y lo divide en dos partes utilizando el separador binario como referencia.
3.  **Autenticación:** Se utiliza la contraseña proporcionada para desbloquear la primera parte (el contenedor PKCS#12) y extraer la Clave Privada en tiempo de ejecución.
4.  **Descifrado:** Se utiliza la Clave Privada extraída para descifrar la segunda parte (el payload), recuperando la API Key en texto plano solo en la memoria RAM.
5.  **Conexión:** El módulo utiliza la API Key recuperada para iniciar las peticiones HTTPS al servicio de OpenWeather.