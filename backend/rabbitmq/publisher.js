/**
 * publisher.js
 * ----------------------------------------------------------------------
 * Publica eventos de pedido na exchange "logistica.eventos".
 * As mensagens são PERSISTENTES (gravadas em disco no RabbitMQ),
 * para sobreviverem a reinicializações do broker.
 * ----------------------------------------------------------------------
 */

const {
  getChannel,
  EXCHANGE_EVENTOS,
  ROUTING_KEY_ENTREGA,
} = require("./connection");

/**
 * Publica uma mensagem de pedido em entrega.
 * @param {object} pedido - objeto contendo dados do pedido.
 */
function publicarPedido(pedido) {
  const channel = getChannel();

  const payload = Buffer.from(JSON.stringify(pedido));

  // persistent:true -> a mensagem vai para disco do RabbitMQ
  const ok = channel.publish(
    EXCHANGE_EVENTOS,
    ROUTING_KEY_ENTREGA,
    payload,
    {
      persistent: true,
      contentType: "application/json",
      timestamp: Date.now(),
    }
  );

  console.log(
    `[Publisher] >> Pedido ${pedido.pedidoId} publicado (rk=${ROUTING_KEY_ENTREGA}) | ack=${ok}`
  );
  return ok;
}

module.exports = { publicarPedido };
