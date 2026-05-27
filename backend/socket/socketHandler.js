/**
 * socket/socketHandler.js
 * ----------------------------------------------------------------------
 * Inicializa o Socket.IO e registra os listeners de conexão.
 * O servidor emite eventos para o frontend:
 *   - "pedido:criado"        -> quando um pedido é publicado pelo produtor
 *   - "status:atualizacao"   -> quando um consumidor reporta processamento
 *   - "filas:snapshot"       -> snapshot periódico do tamanho das filas
 * ----------------------------------------------------------------------
 */

const { Server } = require("socket.io");
const { getChannel, QUEUE_RASTREAMENTO, QUEUE_NOTIFICACAO, QUEUE_ESTOQUE } =
  require("../rabbitmq/connection");

function inicializarSocket(httpServer) {
  const io = new Server(httpServer, {
    cors: { origin: "*" }, // permite o frontend (em outra porta) se conectar
  });

  io.on("connection", (socket) => {
    console.log(`[Socket.IO] Cliente conectado: ${socket.id}`);

    socket.emit("conexao:ok", { mensagem: "Conectado ao backend" });

    socket.on("disconnect", () => {
      console.log(`[Socket.IO] Cliente desconectou: ${socket.id}`);
    });
  });

  // A cada 2 segundos, lemos o estado das filas e emitimos para o frontend.
  // Isto é o que faz o dashboard ter os medidores "ao vivo".
  setInterval(async () => {
    try {
      const channel = getChannel();
      const snapshot = {};
      for (const nome of [
        QUEUE_RASTREAMENTO,
        QUEUE_NOTIFICACAO,
        QUEUE_ESTOQUE,
      ]) {
        const info = await channel.checkQueue(nome);
        snapshot[nome] = {
          mensagens: info.messageCount,
          consumidores: info.consumerCount,
        };
      }
      io.emit("filas:snapshot", snapshot);
    } catch {
      // ignora se o canal ainda não estiver pronto
    }
  }, 2000);

  return io;
}

module.exports = { inicializarSocket };
