/**
 * routes/pedidos.js
 * ----------------------------------------------------------------------
 * Endpoints HTTP para criar pedidos e disparar eventos no RabbitMQ.
 *
 *   POST /api/pedidos        -> Cria 1 pedido aleatório (ou customizado)
 *   POST /api/pedidos/lote   -> Cria N pedidos de uma só vez (default 5)
 *   GET  /api/health         -> Health check
 *   GET  /api/filas          -> Métricas das filas (mensagens enfileiradas)
 * ----------------------------------------------------------------------
 */

const express = require("express");
const { publicarPedido } = require("../rabbitmq/publisher");
const {
  getChannel,
  QUEUE_RASTREAMENTO,
  QUEUE_NOTIFICACAO,
  QUEUE_ESTOQUE,
} = require("../rabbitmq/connection");

const router = express.Router();

// Dados fictícios para gerar pedidos plausíveis
const CLIENTES = [
  "Carlos",
  "Mariana",
  "João",
  "Ana",
  "Pedro",
  "Beatriz",
  "Lucas",
  "Fernanda",
  "Rafael",
  "Juliana",
];
const CIDADES = [
  "Videira",
  "São Paulo",
  "Rio de Janeiro",
  "Curitiba",
  "Porto Alegre",
  "Belo Horizonte",
  "Florianópolis",
  "Salvador",
];
const PRODUTOS = [
  "Smartphone",
  "Notebook",
  "Fone Bluetooth",
  "Smart TV",
  "Cafeteira",
  "Tênis Esportivo",
  "Câmera DSLR",
  "Drone",
];

let proximoId = 1000;

function gerarPedido(dadosCustom = {}) {
  proximoId += 1;
  return {
    pedidoId: proximoId,
    cliente:
      dadosCustom.cliente ||
      CLIENTES[Math.floor(Math.random() * CLIENTES.length)],
    produto:
      dadosCustom.produto ||
      PRODUTOS[Math.floor(Math.random() * PRODUTOS.length)],
    cidade:
      dadosCustom.cidade ||
      CIDADES[Math.floor(Math.random() * CIDADES.length)],
    status: "SAIU_PARA_ENTREGA",
    timestamp: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------
// POST /api/pedidos
// Gera 1 pedido e publica no RabbitMQ
// ---------------------------------------------------------------
router.post("/pedidos", (req, res) => {
  try {
    const pedido = gerarPedido(req.body || {});
    publicarPedido(pedido);

    // Emite no socket o evento "novo pedido criado pelo produtor"
    req.app.get("io").emit("pedido:criado", pedido);

    res.status(201).json({
      sucesso: true,
      mensagem: "Pedido publicado na fila",
      pedido,
    });
  } catch (err) {
    console.error("[API] Erro ao publicar pedido:", err);
    res.status(500).json({ sucesso: false, erro: err.message });
  }
});

// ---------------------------------------------------------------
// POST /api/pedidos/lote
// Gera N pedidos (body: { quantidade: 10 })
// ---------------------------------------------------------------
router.post("/pedidos/lote", (req, res) => {
  try {
    const quantidade = Math.min(
      Math.max(parseInt(req.body?.quantidade, 10) || 5, 1),
      100
    );

    const pedidos = [];
    for (let i = 0; i < quantidade; i++) {
      const p = gerarPedido();
      publicarPedido(p);
      pedidos.push(p);
      req.app.get("io").emit("pedido:criado", p);
    }

    res.status(201).json({
      sucesso: true,
      mensagem: `${quantidade} pedidos publicados na fila`,
      pedidos,
    });
  } catch (err) {
    console.error("[API] Erro ao publicar lote:", err);
    res.status(500).json({ sucesso: false, erro: err.message });
  }
});

// ---------------------------------------------------------------
// GET /api/filas
// Retorna o tamanho atual de cada fila (mensagens não consumidas)
// Útil para mostrar no dashboard.
// ---------------------------------------------------------------
router.get("/filas", async (_req, res) => {
  try {
    const channel = getChannel();
    const filas = [
      QUEUE_RASTREAMENTO,
      QUEUE_NOTIFICACAO,
      QUEUE_ESTOQUE,
    ];
    const resultado = {};
    for (const nome of filas) {
      const info = await channel.checkQueue(nome);
      resultado[nome] = {
        mensagens: info.messageCount,
        consumidores: info.consumerCount,
      };
    }
    res.json(resultado);
  } catch (err) {
    res.status(500).json({ erro: err.message });
  }
});

// ---------------------------------------------------------------
// GET /api/health
// ---------------------------------------------------------------
router.get("/health", (_req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

module.exports = router;
