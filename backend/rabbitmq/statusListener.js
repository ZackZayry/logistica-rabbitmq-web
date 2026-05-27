/**
 * statusListener.js
 * ----------------------------------------------------------------------
 * O backend cria uma fila EXCLUSIVA (uma fila temporária somente sua)
 * ligada à exchange "logistica.status". Cada consumidor publica nessa
 * exchange para reportar que processou uma mensagem. O backend então
 * propaga esses eventos para o frontend via Socket.IO.
 *
 * Esta é a "ponte" entre os consumidores RabbitMQ e a UI em tempo real.
 * ----------------------------------------------------------------------
 */

const { getChannel, EXCHANGE_STATUS } = require("./connection");

async function iniciarListenerStatus(io) {
  const channel = getChannel();

  // Fila anônima exclusiva: gerada pelo RabbitMQ, somente este consumer
  // a usa e ela é deletada quando ele desconecta.
  const q = await channel.assertQueue("", { exclusive: true });

  // Liga essa fila à exchange de status para receber TODOS os eventos
  // (routing key "#" no topic exchange = pega tudo).
  await channel.bindQueue(q.queue, EXCHANGE_STATUS, "#");

  console.log(
    `[StatusListener] Ouvindo eventos de status na fila ${q.queue}`
  );

  channel.consume(
    q.queue,
    (msg) => {
      if (!msg) return;
      try {
        const evento = JSON.parse(msg.content.toString());
        // Emite para todos os clientes Socket.IO conectados
        io.emit("status:atualizacao", evento);

        // Log no console do backend
        console.log(
          `[StatusListener] << [${evento.servico}] pedido ${evento.pedidoId} - ${evento.status}`
        );
      } catch (err) {
        console.error("[StatusListener] Falha ao processar mensagem:", err);
      }
    },
    { noAck: true } // status events não precisam de ack
  );
}

module.exports = { iniciarListenerStatus };
