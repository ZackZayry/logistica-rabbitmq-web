/**
 * server.js
 * ----------------------------------------------------------------------
 * Ponto de entrada do backend.
 *  - Sobe o Express + Socket.IO
 *  - Conecta no RabbitMQ
 *  - Inicia o listener da exchange de status (para repassar p/ Socket.IO)
 *  - Expõe a API REST
 * ----------------------------------------------------------------------
 */

const express = require("express");
const http = require("http");
const cors = require("cors");
const path = require("path");

const { connect } = require("./rabbitmq/connection");
const { iniciarListenerStatus } = require("./rabbitmq/statusListener");
const { inicializarSocket } = require("./socket/socketHandler");
const pedidosRoutes = require("./routes/pedidos");

const PORT = process.env.PORT || 3000;

async function main() {
  const app = express();
  const httpServer = http.createServer(app);

  // Middlewares
  app.use(cors());
  app.use(express.json());

  // Servir arquivos estáticos do frontend se alguém acessar direto a 3000
  // (opcional, frontend "oficial" roda no Nginx em 8080)
  app.use(express.static(path.join(__dirname, "../frontend")));

  // 1) Conecta no RabbitMQ
  await connect();

  // 2) Socket.IO precisa estar disponível para as rotas
  const io = inicializarSocket(httpServer);
  app.set("io", io);

  // 3) Listener da exchange de status para repassar via Socket.IO
  await iniciarListenerStatus(io);

  // 4) Rotas
  app.use("/api", pedidosRoutes);

  // Rota raiz informativa
  app.get("/", (_req, res) => {
    res.json({
      servico: "Hub Logística - Backend",
      versao: "1.0.0",
      endpoints: [
        "POST /api/pedidos",
        "POST /api/pedidos/lote  (body: { quantidade })",
        "GET  /api/filas",
        "GET  /api/health",
      ],
      websocket: "Socket.IO disponível neste mesmo host",
    });
  });

  httpServer.listen(PORT, () => {
    console.log("============================================");
    console.log(`  Backend rodando em http://localhost:${PORT}`);
    console.log("  Socket.IO ativo no mesmo endereço");
    console.log("============================================");
  });
}

main().catch((err) => {
  console.error("[server] Erro fatal:", err);
  process.exit(1);
});
