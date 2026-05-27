# 📦 Hub de Logística e Rastreamento com RabbitMQ

Aplicação web completa de **mensageria distribuída** simulando um hub de logística para e-commerce. Quando um pedido sai para entrega, **vários microsserviços reagem em paralelo**, de forma desacoplada, usando o RabbitMQ como broker de mensagens.

> **Stack:** Node.js · Express · amqplib · Socket.IO · HTML/CSS/JS · Nginx · RabbitMQ 3 (management) · Docker · Docker Compose

---

##  Como executar

### Pré-requisitos

- Docker
- Docker Compose (v2 já vem embutido no Docker Desktop)

### 1) Subir tudo (modo padrão)

```bash
docker compose up --build
```

Isto sobe **6 containers**:

- `rabbitmq` (broker + painel)
- `backend` (API REST + Socket.IO)
- `frontend` (Nginx)
- `consumer-rastreamento`
- `consumer-notificacao`
- `consumer-estoque`

### 2) Abrir as interfaces

| Serviço         | URL                                                   | Credenciais       |
| --------------- | ------------------------------------------------------| ----------------- |
| Dashboard       | [http://localhost:8080](http://localhost:8080)        | —                 |
| RabbitMQ Admin  | [http://localhost:15672](http://localhost:15672)      | `guest` / `guest` |
| Backend API     | [http://localhost:3000](http://localhost:3000)        | —                 |

### 3) Comandos úteis

```bash
# Subir em background
docker compose up --build -d

# Ver logs de todos os serviços
docker compose logs -f

# Ver log só de um serviço específico
docker compose logs -f backend
docker compose logs -f consumer-rastreamento

# Parar e remover containers (mantém o volume do RabbitMQ)
docker compose down

# Parar e remover TUDO (incluindo volumes - APAGA MENSAGENS PERSISTIDAS)
docker compose down -v

# Subir apenas o RabbitMQ + backend (para o Teste 1 - desacoplamento)
docker compose up rabbitmq backend frontend

# Iniciar os consumidores depois
docker compose up -d consumer-rastreamento consumer-notificacao consumer-estoque

# Reiniciar só um serviço
docker compose restart consumer-rastreamento

# Escalar consumidores (ex.: 3 instâncias de notificação para processar mais rápido)
docker compose up -d --scale consumer-notificacao=3
```

---

## Endpoints da API

| Método | Endpoint              | Descrição                                          |
| ------ | --------------------- | -------------------------------------------------- |
| POST   | `/api/pedidos`        | Cria 1 pedido aleatório e publica na exchange      |
| POST   | `/api/pedidos/lote`   | Body: `{ "quantidade": 10 }` – cria N pedidos      |
| GET    | `/api/filas`          | Métricas atuais das 3 filas                        |
| GET    | `/api/health`         | Health check                                       |

### Exemplo de payload publicado

```json
{
  "pedidoId": 1023,
  "cliente": "Carlos",
  "produto": "Smartphone",
  "cidade": "Videira",
  "status": "SAIU_PARA_ENTREGA",
  "timestamp": "2026-05-22T10:30:00.000Z"
}
```

### Teste rápido via cURL

```bash
# 1 pedido
curl -X POST http://localhost:3000/api/pedidos

# 10 pedidos de uma vez
curl -X POST http://localhost:3000/api/pedidos/lote \
     -H "Content-Type: application/json" \
     -d '{"quantidade": 10}'

# Estado das filas
curl http://localhost:3000/api/filas
```

---

## Testes obrigatórios

### Teste 1 — Desacoplamento

**Objetivo:** mostrar que mensagens ficam armazenadas até os consumidores estarem disponíveis.

```bash
# 1. Subir RabbitMQ + backend + frontend (SEM consumidores)
docker compose up -d rabbitmq backend frontend

# 2. No dashboard (http://localhost:8080), clicar em "Enviar 5 mensagens"
#    OU via curl:
curl -X POST http://localhost:3000/api/pedidos/lote \
     -H "Content-Type: application/json" -d '{"quantidade":5}'

# 3. Abrir http://localhost:15672 -> aba "Queues" -> ver "Ready: 5" em cada fila

# 4. Subir os consumidores
docker compose up -d consumer-rastreamento consumer-notificacao consumer-estoque

# 5. Ver no dashboard as mensagens sendo consumidas em tempo real
```

**Resultado esperado:** as 5 mensagens permanecem armazenadas nas 3 filas (15 mensagens totais no broker) até os consumidores subirem. No momento em que eles iniciam, processam tudo na sequência.

### Teste 2 — Monitoramento em tempo real

```bash
docker compose up -d
```

No dashboard, clique em **"Disparar lote de 20"** e observe:

- Os cards das filas pulsando a cada mensagem processada.
- Os contadores subindo.
- O log à direita mostrando cada evento dos 3 consumidores.
- O painel do RabbitMQ (`:15672`) com os gráficos de "Message rates" subindo.

### Teste 3 — Persistência

```bash
# 1. Disparar 20 mensagens
curl -X POST http://localhost:3000/api/pedidos/lote \
     -H "Content-Type: application/json" -d '{"quantidade":20}'

# 2. Parar os consumidores ANTES que terminem (deixar mensagens enfileiradas)
docker compose stop consumer-rastreamento consumer-notificacao consumer-estoque

# 3. Verificar no painel admin que ainda há mensagens nas filas

# 4. REINICIAR o RabbitMQ
docker compose restart rabbitmq

# 5. Verificar no painel: as mensagens AINDA ESTÃO LÁ (persistência funcionou)

# 6. Subir os consumidores novamente -> processam tudo
docker compose start consumer-rastreamento consumer-notificacao consumer-estoque
```

**Resultado esperado:** as mensagens sobrevivem ao reinício do broker porque:

- As **filas** foram declaradas `durable: true`.
- As **mensagens** foram publicadas com `persistent: true`.
- O **volume** `rabbitmq_data` mantém os dados em disco do host.

---

## Painel do RabbitMQ

Após acessar [http://localhost:15672](http://localhost:15672) com `guest` / `guest`, você verá:

### Aba **Exchanges**

| Nome                 | Tipo  |
| -------------------- | ----- |
| `logistica.eventos`  | topic |
| `logistica.status`   | topic |
| _(amq.\* default)_   | —     |

### Aba **Queues**

| Nome                  | Durable | Bindings           | Mensagens (Ready) | Consumers |
| --------------------- | ------- | ------------------ | ----------------- | --------- |
| `rastreamento.queue`  | ✓       | logistica.eventos / pedido.* | 0       | 1         |
| `notificacao.queue`   | ✓       | logistica.eventos / pedido.* | 0       | 1         |
| `estoque.queue`       | ✓       | logistica.eventos / pedido.* | 0       | 1         |

Ao clicar numa fila você ainda vê **Message rates** (gráfico ao vivo), **Get messages** (para inspecionar uma mensagem) e **Bindings**.

---
```bash
docker compose logs backend
curl http://localhost:3000/api/health
```

Se for problema de CORS no navegador, lembre-se: o frontend é servido pelo **Nginx em :8080** e o Nginx **faz proxy** de `/api` e `/socket.io` para o backend. Não acesse o `index.html` direto pelo `file://` — use sempre `http://localhost:8080`.

### Quero limpar as mensagens persistidas

```bash
docker compose down -v   # remove o volume rabbitmq_data
docker compose up -d
```

### Quero escalar um consumidor para processar mais rápido

```bash
docker compose up -d --scale consumer-rastreamento=3
```

O RabbitMQ distribui mensagens em **round-robin** entre as instâncias daquela fila — é o padrão "competing consumers".

---

## Objetivo final

Este projeto demonstra de ponta a ponta:

Comunicação **assíncrona** entre serviços
**Desacoplamento** real entre produtor e consumidores
**Persistência** de mensagens (sobrevivem a restart)
**Monitoramento em tempo real** (painel próprio + painel RabbitMQ)
**Containers** isolados, escaláveis horizontalmente
Integração **Frontend ⇄ Backend ⇄ Filas**
