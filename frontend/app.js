/**
 * app.js
 * ----------------------------------------------------------------------
 * Frontend:
 *   - Conecta no Socket.IO do backend
 *   - Recebe eventos em tempo real e atualiza a dashboard
 *   - Dispara HTTP para criar pedidos
 * ----------------------------------------------------------------------
 */

// O frontend (em 8080) chama o backend (em 3000) via proxy do Nginx ('/api', '/socket.io').
// Por isso usamos URLs RELATIVAS - sem porta - e o Nginx encaminha.

const socket = io({ transports: ["websocket", "polling"] });

// ----- Estado local -----
const state = {
  publicados: 0,
  processadas: { rastreamento: 0, notificacao: 0, estoque: 0 },
  filas: {
    "rastreamento.queue": { mensagens: 0, consumidores: 0 },
    "notificacao.queue":  { mensagens: 0, consumidores: 0 },
    "estoque.queue":      { mensagens: 0, consumidores: 0 },
  },
  pedidos: [], // últimos 30
  logs:    [], // últimos 100
};

// ---------- Helpers de DOM ----------
const $ = (id) => document.getElementById(id);

function setText(id, value) { $(id).textContent = value; }

function renderMetricas() {
  setText("m-publicados", state.publicados);

  const totalProc =
    state.processadas.rastreamento +
    state.processadas.notificacao +
    state.processadas.estoque;
  setText("m-processadas", totalProc);

  const totalConsumidores =
    state.filas["rastreamento.queue"].consumidores +
    state.filas["notificacao.queue"].consumidores +
    state.filas["estoque.queue"].consumidores;
  setText("m-consumidores", totalConsumidores);

  const totalFila =
    state.filas["rastreamento.queue"].mensagens +
    state.filas["notificacao.queue"].mensagens +
    state.filas["estoque.queue"].mensagens;
  setText("m-em-fila", totalFila);
}

function renderFilas() {
  const mapa = [
    ["rastreamento.queue", "rastreamento"],
    ["notificacao.queue",  "notificacao"],
    ["estoque.queue",      "estoque"],
  ];
  let maxMsgs = 1;
  for (const [qname] of mapa) {
    maxMsgs = Math.max(maxMsgs, state.filas[qname].mensagens);
  }
  for (const [qname, key] of mapa) {
    const f = state.filas[qname];
    setText(`q-${key}-msg`,  f.mensagens);
    setText(`q-${key}-cons`, f.consumidores);
    setText(`q-${key}-done`, state.processadas[key]);

    const badge = $(`b-${key}`);
    badge.classList.toggle("online", f.consumidores > 0);

    const pct = Math.min(100, (f.mensagens / Math.max(10, maxMsgs)) * 100);
    $(`bar-${key}`).style.width = `${pct}%`;
  }
}

function renderPedidos() {
  const ul = $("orders-list");
  if (!state.pedidos.length) {
    ul.innerHTML = '<div class="empty">Nenhum pedido ainda. Clique em "Gerar pedido" 👆</div>';
    return;
  }
  ul.innerHTML = state.pedidos
    .map((p) => {
      const chip = p.completos >= 3
        ? '<span class="chip done">✓ Concluído</span>'
        : `<span class="chip">${p.completos}/3 serviços</span>`;
      return `
        <div class="order-item">
          <span class="order-id">#${p.pedidoId}</span>
          <div class="order-info">
            <h4>${escapeHtml(p.cliente)} · ${escapeHtml(p.cidade)}</h4>
            <small>${escapeHtml(p.produto || "produto")} · ${formatTime(p.timestamp)}</small>
          </div>
          ${chip}
        </div>
      `;
    })
    .join("");
}

function renderLogs() {
  const ul = $("log-list");
  if (!state.logs.length) {
    ul.innerHTML = '<div class="empty">Aguardando eventos...</div>';
    return;
  }
  ul.innerHTML = state.logs
    .map(
      (l) => `
      <div class="log-item ${l.tipo}">
        <span class="ts">${l.ts}</span>
        <span>${escapeHtml(l.texto)}</span>
      </div>`
    )
    .join("");
}

function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function formatTime(iso) {
  try {
    return new Date(iso).toLocaleTimeString("pt-BR");
  } catch {
    return iso;
  }
}

function addLog(tipo, texto) {
  state.logs.unshift({
    tipo,
    texto,
    ts: new Date().toLocaleTimeString("pt-BR"),
  });
  if (state.logs.length > 100) state.logs.length = 100;
  renderLogs();
}

function pulseCard(servico) {
  const map = {
    rastreamento: '[data-queue="rastreamento.queue"]',
    notificacao:  '[data-queue="notificacao.queue"]',
    estoque:      '[data-queue="estoque.queue"]',
  };
  const el = document.querySelector(map[servico]);
  if (!el) return;
  el.classList.remove("pulse");
  void el.offsetWidth; // força reflow para reanimar
  el.classList.add("pulse");
}

// ---------- SOCKET.IO ----------
socket.on("connect", () => {
  $("conn-status").classList.remove("disconnected");
  $("conn-status").classList.add("connected");
  $("conn-status").querySelector(".label").textContent = "Conectado";
  addLog("producer", "🔌 Socket conectado ao backend");
});

socket.on("disconnect", () => {
  $("conn-status").classList.remove("connected");
  $("conn-status").classList.add("disconnected");
  $("conn-status").querySelector(".label").textContent = "Desconectado";
  addLog("producer", "❌ Socket desconectado");
});

// Quando o backend publica um novo pedido
socket.on("pedido:criado", (p) => {
  state.publicados += 1;
  state.pedidos.unshift({ ...p, completos: 0 });
  if (state.pedidos.length > 30) state.pedidos.length = 30;
  renderMetricas();
  renderPedidos();
  addLog(
    "producer",
    `📤 Pedido #${p.pedidoId} publicado: ${p.cliente} (${p.cidade})`
  );
});

// Atualização de status vinda de algum consumidor
socket.on("status:atualizacao", (ev) => {
  state.processadas[ev.servico] = (state.processadas[ev.servico] || 0) + 1;

  // Marcar progresso do pedido
  const pedido = state.pedidos.find((p) => p.pedidoId === ev.pedidoId);
  if (pedido) {
    pedido.completos = (pedido.completos || 0) + 1;
  }

  pulseCard(ev.servico);
  renderMetricas();
  renderPedidos();
  renderFilas();

  const icons = { rastreamento: "🗺️", notificacao: "📧", estoque: "📦" };
  addLog(
    ev.servico,
    `${icons[ev.servico] || "•"} [${ev.servico.toUpperCase()}] pedido #${ev.pedidoId} → ${ev.status} (${ev.duracaoMs}ms)`
  );
});

// Snapshot periódico das filas (a cada 2s)
socket.on("filas:snapshot", (snap) => {
  for (const q of Object.keys(snap)) {
    state.filas[q] = snap[q];
  }
  renderMetricas();
  renderFilas();
});

// ---------- BOTÕES ----------
$("btn-1").addEventListener("click", () => {
  fetch("/api/pedidos", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" })
    .catch(err => addLog("producer", `❌ Erro: ${err.message}`));
});

$("btn-5").addEventListener("click", () => {
  fetch("/api/pedidos/lote", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ quantidade: 5 }),
  }).catch(err => addLog("producer", `❌ Erro: ${err.message}`));
});

$("btn-20").addEventListener("click", () => {
  fetch("/api/pedidos/lote", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ quantidade: 20 }),
  }).catch(err => addLog("producer", `❌ Erro: ${err.message}`));
});

$("btn-limpar-pedidos").addEventListener("click", () => {
  state.pedidos = [];
  renderPedidos();
});
$("btn-limpar-log").addEventListener("click", () => {
  state.logs = [];
  renderLogs();
});

// Render inicial
renderMetricas();
renderFilas();
renderPedidos();
renderLogs();
