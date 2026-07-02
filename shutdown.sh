#!/bin/bash

echo "Deteniendo entorno"
docker compose down --remove-orphans
echo "OK"