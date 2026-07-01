#!/bin/bash

# Ejecutar desde el directorio del proyecto

echo "Deteniendo entorno"
docker compose down --remove-orphans
echo "OK"