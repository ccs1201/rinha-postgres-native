#!/bin/bash

# Parar e remover containers existentes
docker-compose -f docker-compose-payment-processor.yml down --remove-orphans
docker-compose -f docker-compose.yml down --remove-orphans
docker container prune -f

# Build da aplicação
mvn clean -Pnative spring-boot:build-image