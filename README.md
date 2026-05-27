# 📦 Hub de Logística e Rastreamento com RabbitMQ

Aplicação web completa de **mensageria distribuída** simulando um hub de logística para e-commerce. Quando um pedido sai para entrega, **vários microsserviços reagem em paralelo**, de forma desacoplada, usando o RabbitMQ como broker de mensagens.

> **Stack:** Node.js · Express · amqplib · Socket.IO · HTML/CSS/JS · Nginx · RabbitMQ 3 (management) · Docker · Docker Compose

---

## 🗺️ Índice

1. [Arquitetura](#-arquitetura)
2. [Conceitos de mensageria](#-conceitos-de-mensageria-aplicados)
3. [Como o RabbitMQ funciona dentro do sistema](#-como-o-rabbitmq-funciona-dentro-do-sistema)
4. [Estrutura do projeto](#-estrutura-do-projeto)
5. [Como executar](#-como-executar)
6. [Endpoints da API](#-endpoints-da-api)
7. [Testes obrigatórios](#-testes-obrigatórios)
8. [Logs esperados](#-logs-esperados)
9. [Painel do RabbitMQ](#-painel-do-rabbitmq)
10. [Troubleshooting](#-troubleshooting)

---

## 🏗️ Arquitetura

```
                       ┌──────────────────────────┐
                       │       FRONTEND           │
                       │  HTML/CSS/JS + Nginx     │
                       │   (porta 8080)           │
                       └─────────────┬────────────┘
                                     │  HTTP (REST)
                                     │  WebSocket (Socket.IO)
                                     ▼
                       ┌──────────────────────────┐
                       │       BACKEND API        │
                       │ Node + Express + Socket.IO│
                       │  (produtor RabbitMQ)     │
                       │       (porta 3000)       │
                       └─────────────┬────────────┘
                                     │ publish (AMQP)
                                     │ routing key: pedido.saiu_para_entrega
                                     ▼
                ┌──────────────────────────────────────┐
                │  EXCHANGE: logistica.eventos (topic) │
                └─────┬──────────────┬──────────────┬──┘
                      │              │              │
              bind: pedido.*   bind: pedido.*  bind: pedido.*
                      │              │              │
                      ▼              ▼              ▼
              ┌──────────────┐ ┌────────────┐ ┌────────────┐
              │ rastreamento │ │ notificacao│ │  estoque   │
              │   .queue     │ │   .queue   │ │   .queue   │
              │  (durable)   │ │  (durable) │ │  (durable) │
              └──────┬───────┘ └─────┬──────┘ └─────┬──────┘
                     │               │              │
                     ▼               ▼              ▼
              ┌──────────────┐ ┌────────────┐ ┌────────────┐
              │  CONSUMER 1  │ │ CONSUMER 2 │ │ CONSUMER 3 │
              │ Rastreamento │ │ Notificacao│ │  Estoque   │
              │ (container)  │ │ (container)│ │ (container)│
              └──────┬───────┘ └─────┬──────┘ └─────┬──────┘
                     │               │              │
                     └───────────────┴──────────────┘
                                     │
                       publish em "logistica.status"
                                     │
                                     ▼
                       ┌──────────────────────────┐
                       │ Backend (status listener)│ ──► Socket.IO ──► Dashboard
                       └──────────────────────────┘
```

**Resumo do fluxo:**

1. O **frontend** dispara um `POST /api/pedidos` no backend.
2. O **backend** monta o JSON do pedido e **publica** na exchange `logistica.eventos` (tipo `topic`) com a routing key `pedido.saiu_para_entrega`.
3. A exchange roteia a mensagem para **3 filas duráveis**, uma para cada microsserviço.
4. Os **3 consumidores rodam em paralelo**, em processos/containers independentes, e processam suas mensagens.
5. Cada consumidor publica um evento de status na exchange `logistica.status`.
6. O backend escuta essa exchange e **emite por Socket.IO** para o dashboard, que se atualiza em tempo real.

---

## 💡 Conceitos de mensageria aplicados

| Conceito                       | Como aparece no projeto                                                                                                                                   |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Comunicação assíncrona**     | O backend não conhece os consumidores. Apenas publica na exchange e segue a vida. Os consumidores processam quando puderem.                               |
| **Desacoplamento**             | Frontend, backend e os 3 consumidores são **containers independentes**, com suas próprias responsabilidades e ciclos de vida.                             |
| **Filas e consumidores**       | Três filas (`rastreamento.queue`, `notificacao.queue`, `estoque.queue`), cada uma com seu consumidor dedicado.                                            |
| **Processamento paralelo**     | Os 3 consumidores processam **ao mesmo tempo**, em containers distintos.                                                                                  |
| **Persistência de mensagens**  | Filas declaradas como `durable: true` + mensagens publicadas com `persistent: true`. O volume do RabbitMQ é mapeado em disco (volume Docker `rabbitmq_data`). |
| **Monitoramento em tempo real**| Painel web próprio (Socket.IO) e o painel admin do RabbitMQ em `:15672`.                                                                                  |
| **Containers Docker**          | Tudo orquestrado com `docker-compose.yml` (6 containers).                                                                                                 |
| **Roteamento por tópico**      | Exchange tipo `topic` com routing key `pedido.*`, permitindo expandir tipos de evento no futuro (ex.: `pedido.cancelado`, `pedido.entregue`).             |

---

## 🐇 Como o RabbitMQ funciona dentro do sistema

### Topologia declarada (idempotente em todos os serviços)

```js
// Exchange principal de eventos
channel.assertExchange("logistica.eventos", "topic", { durable: true });

// Exchange para que consumidores reportem status ao backend
channel.assertExchange("logistica.status",  "topic", { durable: true });

// 3 filas duráveis (sobrevivem a restart do broker)
channel.assertQueue("rastreamento.queue", { durable: true });
channel.assertQueue("notificacao.queue",  { durable: true });
channel.assertQueue("estoque.queue",      { durable: true });

// Bindings: pedido.* roteia para todas as 3 filas
channel.bindQueue("rastreamento.queue", "logistica.eventos", "pedido.*");
channel.bindQueue("notificacao.queue",  "logistica.eventos", "pedido.*");
channel.bindQueue("estoque.queue",      "logistica.eventos", "pedido.*");
```

### Por que `topic` e não `fanout`?

Topic permite **filtrar por padrão da routing key**. Hoje todos consomem `pedido.*`, mas amanhã você pode criar um consumidor que só escuta `pedido.cancelado.*` ou `pedido.internacional.*` sem mudar nada no produtor.

### Garantias de entrega

- `prefetch(1)` em cada consumidor → ele só pega a próxima mensagem **depois** de dar `ack` na anterior. Isso evita sobrecarga e garante distribuição justa.
- `channel.ack(msg)` é feito **só depois** do processamento completo. Se o consumidor cair no meio, o RabbitMQ **reenfileira** a mensagem.
- `persistent: true` + `durable: true` → mensagens vão para disco. Reiniciar o container do RabbitMQ não perde as mensagens.

### Fluxo de status (caminho de volta)

Para a dashboard atualizar "ao vivo", cada consumidor, após processar uma mensagem, **publica** um evento em `logistica.status`. O backend cria uma fila **exclusiva temporária** (que existe só enquanto ele estiver vivo) ligada a essa exchange e, ao receber qualquer evento, emite via Socket.IO para o navegador.

---

## 📁 Estrutura do projeto

```
logistica-rabbitmq-web/
│
├── docker-compose.yml              # Orquestração de todos os containers
├── README.md                       # Este arquivo
│
├── backend/
│   ├── Dockerfile
│   ├── package.json
│   ├── server.js                   # Express + Socket.IO + bootstrap
│   ├── rabbitmq/
│   │   ├── connection.js           # Conexão singleton + topologia
│   │   ├── publisher.js            # Publica eventos na exchange
│   │   └── statusListener.js       # Recebe status -> Socket.IO
│   ├── routes/
│   │   └── pedidos.js              # POST /api/pedidos, /lote, GET /filas
│   └── socket/
│       └── socketHandler.js        # Setup do Socket.IO + snapshot 2s
│
├── frontend/
│   ├── Dockerfile                  # Nginx servindo HTML + proxy /api e /socket.io
│   ├── nginx.conf
│   ├── index.html                  # Dashboard
│   ├── style.css                   # Tema dark moderno
│   └── app.js                      # Lógica + Socket.IO client
│
└── consumers/
    ├── rastreamento/               # Atualiza rastreamento e mapa
    │   ├── Dockerfile
    │   ├── package.json
    │   └── index.js
    ├── notificacao/                # Simula email/SMS ao cliente
    │   ├── Dockerfile
    │   ├── package.json
    │   └── index.js
    └── estoque/                    # Baixa estoque do CD
        ├── Dockerfile
        ├── package.json
        └── index.js
```

---

## ▶️ Como executar

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

| Serviço            | URL                                                   | Credenciais       |
| ------------------ | ----------------------------------------------------- | ----------------- |
| 🖥️ Dashboard       | [http://localhost:8080](http://localhost:8080)        | —                 |
| 🐇 RabbitMQ Admin  | [http://localhost:15672](http://localhost:15672)      | `guest` / `guest` |
| 🔌 Backend API     | [http://localhost:3000](http://localhost:3000)        | —                 |

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

## 🌐 Endpoints da API

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

## 🧪 Testes obrigatórios

### ✅ Teste 1 — Desacoplamento

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

### ✅ Teste 2 — Monitoramento em tempo real

```bash
docker compose up -d
```

No dashboard, clique em **"Disparar lote de 20"** e observe:

- Os cards das filas pulsando a cada mensagem processada.
- Os contadores subindo.
- O log à direita mostrando cada evento dos 3 consumidores.
- O painel do RabbitMQ (`:15672`) com os gráficos de "Message rates" subindo.

### ✅ Teste 3 — Persistência

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

## 📜 Logs esperados

### Backend (produtor)

```
[RabbitMQ] Tentando conectar em amqp://guest:guest@rabbitmq:5672 ...
[RabbitMQ] Conectado e topologia declarada com sucesso.
[StatusListener] Ouvindo eventos de status na fila amq.gen-xyz...
============================================
  Backend rodando em http://localhost:3000
  Socket.IO ativo no mesmo endereço
============================================
[Socket.IO] Cliente conectado: kJg2...
[Publisher] >> Pedido 1001 publicado (rk=pedido.saiu_para_entrega) | ack=true
[StatusListener] << [rastreamento] pedido 1001 - RASTREAMENTO_ATUALIZADO
[StatusListener] << [notificacao]  pedido 1001 - CLIENTE_NOTIFICADO
[StatusListener] << [estoque]      pedido 1001 - ESTOQUE_ATUALIZADO
```

### Consumer Rastreamento

```
[rastreamento] Conectando ao RabbitMQ amqp://guest:guest@rabbitmq:5672 ...
[rastreamento] Aguardando mensagens na fila "rastreamento.queue"...
[rastreamento] >> Recebido pedido 1001 (Carlos - Videira)
[rastreamento] ✅ Rastreamento atualizado: pedido 1001 -> lat=-26.5421, lng=-50.1283 (em 1342ms)
```

### Consumer Notificação

```
[notificacao] Aguardando mensagens na fila "notificacao.queue"...
[notificacao] >> Recebido pedido 1001 (Carlos)
[notificacao] 📧 EMAIL enviado ao cliente "Carlos" sobre o pedido 1001 (em 982ms)
```

### Consumer Estoque

```
[estoque] Aguardando mensagens na fila "estoque.queue"...
[estoque] >> Recebido pedido 1001 (Smartphone)
[estoque] 📦 Saída registrada no CD para pedido 1001. Estoque agora: 999 (em 1104ms)
```

---

## 🐇 Painel do RabbitMQ

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

## 🛠️ Troubleshooting

### Os consumidores reiniciam várias vezes no início

Normal nos primeiros segundos: eles tentam conectar enquanto o RabbitMQ termina de subir. O healthcheck do compose já faz o `backend` aguardar, mas os consumidores também têm retry interno (até 30 tentativas).

### O dashboard mostra "Desconectado"

Confira se o backend está saudável:

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

## 🎯 Objetivo final

Este projeto demonstra de ponta a ponta:

✅ Comunicação **assíncrona** entre serviços
✅ **Desacoplamento** real entre produtor e consumidores
✅ **Persistência** de mensagens (sobrevivem a restart)
✅ **Monitoramento em tempo real** (painel próprio + painel RabbitMQ)
✅ **Containers** isolados, escaláveis horizontalmente
✅ Integração **Frontend ⇄ Backend ⇄ Filas**

> Construído com ❤️ usando Node.js, RabbitMQ e Docker.
