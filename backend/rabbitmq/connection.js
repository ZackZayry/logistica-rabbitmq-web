/**
 * connection.js
 * ----------------------------------------------------------------------
 * Responsável por criar e manter a conexão única (singleton) com o
 * RabbitMQ. Faz reconexão automática e tenta várias vezes na inicial,
 * já que o container do RabbitMQ pode demorar a ficar pronto.
 * ----------------------------------------------------------------------
 */

const amqp = require("amqplib");

const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

// ---------------------------------------------------------------------
// Constantes de topologia (exchanges/filas/routing keys)
// ---------------------------------------------------------------------

// Exchange principal por onde os eventos de pedido são publicados.
// Usamos TOPIC para permitir roteamento flexível com routing keys.
const EXCHANGE_EVENTOS = "logistica.eventos";

// Exchange para que os consumidores publiquem o STATUS de processamento
// de volta. O backend escuta isso e propaga via Socket.IO para o
// frontend, atualizando a dashboard em tempo real.
const EXCHANGE_STATUS = "logistica.status";

// Filas (uma por consumidor). Cada uma é DURÁVEL.
const QUEUE_RASTREAMENTO = "rastreamento.queue";
const QUEUE_NOTIFICACAO = "notificacao.queue";
const QUEUE_ESTOQUE = "estoque.queue";

// Routing key usada para mensagens de pedido em entrega.
const ROUTING_KEY_ENTREGA = "pedido.saiu_para_entrega";

let connection = null;
let channel = null;

/**
 * Conecta no RabbitMQ com retry. Retorna o canal pronto para uso.
 */
async function connect(maxRetries = 20, delayMs = 3000) {
  let attempt = 0;
  while (attempt < maxRetries) {
    try {
      console.log(`[RabbitMQ] Tentando conectar em ${RABBITMQ_URL} ...`);
      connection = await amqp.connect(RABBITMQ_URL);

      connection.on("error", (err) => {
        console.error("[RabbitMQ] Erro de conexão:", err.message);
      });
      connection.on("close", () => {
        console.warn("[RabbitMQ] Conexão encerrada. Tentando reconectar...");
        channel = null;
        connection = null;
        setTimeout(() => connect().catch(() => {}), 3000);
      });

      channel = await connection.createChannel();

      // -------- Declaração da topologia (idempotente) --------
      // Exchange dos eventos (topic, durável)
      await channel.assertExchange(EXCHANGE_EVENTOS, "topic", {
        durable: true,
      });

      // Exchange de status (topic, durável)
      await channel.assertExchange(EXCHANGE_STATUS, "topic", {
        durable: true,
      });

      // Cria as filas duráveis e faz o binding na exchange de eventos.
      // Cada fila recebe TODA mensagem com routing key "pedido.*".
      for (const queue of [
        QUEUE_RASTREAMENTO,
        QUEUE_NOTIFICACAO,
        QUEUE_ESTOQUE,
      ]) {
        await channel.assertQueue(queue, { durable: true });
        await channel.bindQueue(queue, EXCHANGE_EVENTOS, "pedido.*");
      }

      console.log("[RabbitMQ] Conectado e topologia declarada com sucesso.");
      return channel;
    } catch (err) {
      attempt++;
      console.error(
        `[RabbitMQ] Falha ao conectar (tentativa ${attempt}/${maxRetries}): ${err.message}`
      );
      if (attempt >= maxRetries) throw err;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

function getChannel() {
  if (!channel) {
    throw new Error("Canal RabbitMQ ainda não está pronto.");
  }
  return channel;
}

module.exports = {
  connect,
  getChannel,
  EXCHANGE_EVENTOS,
  EXCHANGE_STATUS,
  QUEUE_RASTREAMENTO,
  QUEUE_NOTIFICACAO,
  QUEUE_ESTOQUE,
  ROUTING_KEY_ENTREGA,
};
