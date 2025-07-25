# Rinha de Backend 2025 - CCS1201

## Tecnologias utilizadas

- **Java 21**
- **Spring-boot 3.5.3**
- **postgres**
- **Nginx**
- **Docker**

## Como rodar

1. Suba o docker-compose dos Payment Processors primeiro (conforme instruções do repositório oficial).
2. Depois, suba este compose:
   ```sh
   docker compose up
   ```
3. O backend ficará disponível na porta **9999**.

## Abordagem KISS

Receba o maior número de requisições possível e depois se vira maluco...

Filas recebem o ingress;
Filas estocam os pagamento a serem processados;
E Filas orquestram a pesrsistência, fila fila fila :()

Brincadeiras a parte foi utilizada uma estratégia realmente simples, recebo a requisição inicio o processamento
numa nova Thread (Virtual), com um executor para coordenar e fazer o papel de fila interna, com isso liberando o servidor web
para responder novas requisições, após confirmação do payment processor a transação é armazenada postgres. Um fallback com 
re-enfileiramento e retry de 3x. Simples assim!
A consistência eventual é garantida, mas isso não significa que você terá o saldo realtime rsrsrs. 

## Repositório do código-fonte

[GitHub /ccs1201](https://github.com/ccs1201/rinha-postgres-native)
