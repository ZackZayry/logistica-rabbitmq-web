/**
 * Consumer: NOTIFICACAO
 * ----------------------------------------------------------------------
 *  - Escuta a fila "notificacao.queue"
 *  - Simula envio de e-mail/SMS ao cliente avisando que o pedido saiu
 *    para entrega
 *  - Reporta status na exchange "logistica.status"
 * ----------------------------------------------------------------------
 */

const amqp = require("amqplib");

const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

const EXCHANGE_EVENTOS = "logistica.eventos";
const EXCHANGE_STATUS = "logistica.status";
const QUEUE = "notificacao.queue";
const SERVICO = "notificacao";

async function startConsumer(maxRetries = 30, delay = 3000) {
  let attempt = 0;
  while (attempt < maxRetries) {
    try {
      console.log(`[${SERVICO}] Conectando ao RabbitMQ ${RABBITMQ_URL} ...`);
      const conn = await amqp.connect(RABBITMQ_URL);
      const channel = await conn.createChannel();

      await channel.assertExchange(EXCHANGE_EVENTOS, "topic", { durable: true });
      await channel.assertExchange(EXCHANGE_STATUS, "topic", { durable: true });
      await channel.assertQueue(QUEUE, { durable: true });
      await channel.bindQueue(QUEUE, EXCHANGE_EVENTOS, "pedido.*");
      await channel.prefetch(1);

      console.log(`[${SERVICO}] Aguardando mensagens na fila "${QUEUE}"...`);

      channel.consume(QUEUE, async (msg) => {
        if (!msg) return;
        const pedido = JSON.parse(msg.content.toString());

        console.log(
          `[${SERVICO}] >> Recebido pedido ${pedido.pedidoId} (${pedido.cliente})`
        );

        const delayProc = 500 + Math.random() * 1500;
        await sleep(delayProc);

        const canal = Math.random() > 0.5 ? "EMAIL" : "SMS";

        console.log(
          `[${SERVICO}] 📧 ${canal} enviado ao cliente "${pedido.cliente}" sobre o pedido ${pedido.pedidoId} (em ${delayProc.toFixed(0)}ms)`
        );

        const evento = {
          servico: SERVICO,
          pedidoId: pedido.pedidoId,
          cliente: pedido.cliente,
          cidade: pedido.cidade,
          status: "CLIENTE_NOTIFICADO",
          detalhe: `${canal} enviado para ${pedido.cliente}`,
          duracaoMs: Math.round(delayProc),
          timestamp: new Date().toISOString(),
        };
        channel.publish(
          EXCHANGE_STATUS,
          `status.${SERVICO}`,
          Buffer.from(JSON.stringify(evento)),
          { persistent: false }
        );

        channel.ack(msg);
      });

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
  process.exit(1);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

startConsumer();
