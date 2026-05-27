/**
 * Consumer: ESTOQUE
 * ----------------------------------------------------------------------
 *  - Escuta a fila "estoque.queue"
 *  - Atualiza estoque e registra saída do centro de distribuição
 *  - Reporta status na exchange "logistica.status"
 * ----------------------------------------------------------------------
 */

const amqp = require("amqplib");

const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

const EXCHANGE_EVENTOS = "logistica.eventos";
const EXCHANGE_STATUS = "logistica.status";
const QUEUE = "estoque.queue";
const SERVICO = "estoque";

// "Banco" em memória só para mostrar evolução do estoque nos logs
let estoqueAtual = 1000;

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
          `[${SERVICO}] >> Recebido pedido ${pedido.pedidoId} (${pedido.produto || "item"})`
        );

        const delayProc = 600 + Math.random() * 1000;
        await sleep(delayProc);

        estoqueAtual -= 1;

        console.log(
          `[${SERVICO}] 📦 Saída registrada no CD para pedido ${pedido.pedidoId}. Estoque agora: ${estoqueAtual} (em ${delayProc.toFixed(0)}ms)`
        );

        const evento = {
          servico: SERVICO,
          pedidoId: pedido.pedidoId,
          cliente: pedido.cliente,
          cidade: pedido.cidade,
          status: "ESTOQUE_ATUALIZADO",
          detalhe: `Saída registrada no CD. Estoque atual: ${estoqueAtual}`,
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
