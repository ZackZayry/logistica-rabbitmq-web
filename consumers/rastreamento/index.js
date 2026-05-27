/**
 * Consumer: RASTREAMENTO
 * ----------------------------------------------------------------------
 *  - Escuta a fila "rastreamento.queue"
 *  - Atualiza rastreamento, mapa e simula movimentação da entrega
 *  - Reporta o resultado na exchange "logistica.status"
 *    para o backend repassar ao dashboard via Socket.IO.
 * ----------------------------------------------------------------------
 */

const amqp = require("amqplib");

const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

const EXCHANGE_EVENTOS = "logistica.eventos";
const EXCHANGE_STATUS = "logistica.status";
const QUEUE = "rastreamento.queue";
const SERVICO = "rastreamento";

async function startConsumer(maxRetries = 30, delay = 3000) {
  let attempt = 0;
  while (attempt < maxRetries) {
    try {
      console.log(`[${SERVICO}] Conectando ao RabbitMQ ${RABBITMQ_URL} ...`);
      const conn = await amqp.connect(RABBITMQ_URL);
      const channel = await conn.createChannel();

      // Topologia (idempotente)
      await channel.assertExchange(EXCHANGE_EVENTOS, "topic", { durable: true });
      await channel.assertExchange(EXCHANGE_STATUS, "topic", { durable: true });
      await channel.assertQueue(QUEUE, { durable: true });
      await channel.bindQueue(QUEUE, EXCHANGE_EVENTOS, "pedido.*");

      // Quality of service: 1 mensagem por vez (justo entre múltiplos workers)
      await channel.prefetch(1);

      console.log(`[${SERVICO}] Aguardando mensagens na fila "${QUEUE}"...`);

      channel.consume(QUEUE, async (msg) => {
        if (!msg) return;
        const pedido = JSON.parse(msg.content.toString());

        console.log(
          `[${SERVICO}] >> Recebido pedido ${pedido.pedidoId} (${pedido.cliente} - ${pedido.cidade})`
        );

        // Simula processamento (atualizando mapa, etc) - 800ms a 2s
        const delayProc = 800 + Math.random() * 1200;
        await sleep(delayProc);

        // Simula coordenadas (mock de mapa/logística)
        const lat = -27 + Math.random() * 2;
        const lng = -51 + Math.random() * 2;

        console.log(
          `[${SERVICO}] ✅ Rastreamento atualizado: pedido ${pedido.pedidoId} -> lat=${lat.toFixed(4)}, lng=${lng.toFixed(4)} (em ${delayProc.toFixed(0)}ms)`
        );

        // Publica status para o backend repassar via Socket.IO
        const evento = {
          servico: SERVICO,
          pedidoId: pedido.pedidoId,
          cliente: pedido.cliente,
          cidade: pedido.cidade,
          status: "RASTREAMENTO_ATUALIZADO",
          detalhe: `Veículo em lat ${lat.toFixed(3)}, lng ${lng.toFixed(3)}`,
          duracaoMs: Math.round(delayProc),
          timestamp: new Date().toISOString(),
        };
        channel.publish(
          EXCHANGE_STATUS,
          `status.${SERVICO}`,
          Buffer.from(JSON.stringify(evento)),
          { persistent: false }
        );

        // ACK: avisa o broker que a mensagem foi processada com sucesso
        channel.ack(msg);
      });

      // Loop terminou: conectado e consumindo. Mantém o processo vivo.
      conn.on("close", () => {
        console.error(`[${SERVICO}] Conexão fechada. Reconectando em 3s...`);
        setTimeout(() => startConsumer(), 3000);
      });
      return;
    } catch (err) {
      attempt++;
      console.error(
        `[${SERVICO}] Falha ao conectar (tentativa ${attempt}/${maxRetries}): ${err.message}`
      );
      await sleep(delay);
    }
  }
  console.error(`[${SERVICO}] Não foi possível conectar. Encerrando.`);
  process.exit(1);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

startConsumer();
