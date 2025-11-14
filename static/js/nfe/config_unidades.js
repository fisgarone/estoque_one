// Variável global para edição
let produtoEditando = null;

// Descobre a BASE pela URL atual, garantindo barra no final.
// Se você está em /nfe/config  ou /nfe/config/, a BASE vira /nfe/config/
const BASE = (function () {
  let p = window.location.pathname || "/";
  return p.endsWith("/") ? p : (p + "/");
})();

// Endpoints SEM hardcode de prefixo
const EP = {
  salvar: BASE + "api/salvar",
  pendentes: BASE + "api/pendentes",
  configurados: BASE + "api/configurados",
  // se tiver um recálculo futuramente: BASE + "recalcular-todos"
};

document.addEventListener('DOMContentLoaded', function() {
  // 1. Tabs
  const tabs = document.querySelectorAll('.tab[data-tab]');
  tabs.forEach(tab => {
    tab.addEventListener('click', function() {
      tabs.forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));

      this.classList.add('active');
      const tabId = this.getAttribute('data-tab');
      document.getElementById(`panel-${tabId}`).classList.add('active');

      if (tabId === 'todos') {
        carregarTodos();
      }
    });
  });

  // 2. Botão atualizar (reload simples)
  const btnAtualizar = document.getElementById('btn-atualizar');
  if (btnAtualizar) btnAtualizar.addEventListener('click', () => window.location.reload());

  // 3. Modal
  window.editarConfiguracao = editarConfiguracao;
  window.fecharModal = fecharModal;
  window.salvarEdicao = salvarEdicao;
  window.salvarConfiguracao = salvarConfiguracao;
});

// Utilitário: tenta parsear JSON; se vier HTML (erro), mostra texto
async function parseJSONorThrow(response) {
  const ct = (response.headers.get('content-type') || '').toLowerCase();
  if (ct.includes('application/json')) {
    return await response.json();
  } else {
    const txt = await response.text();
    throw new Error(`HTTP ${response.status} — resposta não-JSON:\n` + txt.slice(0, 500));
  }
}

// -------- Função para salvar nova configuração --------
async function salvarConfiguracao(codigo, nome, unidade, volume, pacote) {
  if (!volume || !pacote) {
    alert('Por favor, preencha todos os campos'); return;
  }

  const payload = {
    codigo_fornecedor: String(codigo || '').trim(),
    nome: String(nome || '').trim(),
    unidade_compra: String(unidade || '').trim(),
    qtd_por_volume: Number(volume || 1),
    qtd_por_pacote: Number(pacote || 1)
  };

  try {
    const response = await fetch(EP.salvar, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const data = await parseJSONorThrow(response);

    if (response.ok && data.status === 'success') {
      alert('Configuração salva com sucesso!');
      window.location.reload();
    } else {
      throw new Error(data.message || `Falha ao salvar (HTTP ${response.status})`);
    }
  } catch (error) {
    console.error('Erro:', error);
    alert('Erro ao salvar: ' + error.message);
  }
}

// -------- Modal: abrir/fechar/confirmar edição --------
function editarConfiguracao(codigo, nome, unidade, volume, pacote) {
  produtoEditando = { codigo, nome, unidade, volume, pacote };
  document.getElementById('modal-nome').value = nome;
  document.getElementById('modal-codigo').value = codigo;
  document.getElementById('modal-unidade').value = unidade;
  document.getElementById('modal-volume').value = volume;
  document.getElementById('modal-pacote').value = pacote;
  document.getElementById('modal-edicao').style.display = 'flex';
}

function fecharModal() {
  document.getElementById('modal-edicao').style.display = 'none';
  produtoEditando = null;
}

async function salvarEdicao() {
  const novoVolume = document.getElementById('modal-volume').value;
  const novoPacote = document.getElementById('modal-pacote').value;

  if (!novoVolume || !novoPacote) {
    alert('Por favor, preencha todos os campos'); return;
  }
  await salvarConfiguracao(
    produtoEditando.codigo,
    produtoEditando.nome,
    produtoEditando.unidade,
    novoVolume,
    novoPacote
  );
  fecharModal();
}

// -------- Carregar aba "Todos" --------
async function carregarTodos() {
  try {
    const [rPend, rConf] = await Promise.all([
      fetch(EP.pendentes),
      fetch(EP.configurados)
    ]);
    const pendentes = await parseJSONorThrow(rPend);
    const configurados = await parseJSONorThrow(rConf);

    const todos = [...pendentes, ...configurados];
    const container = document.getElementById('lista-todos');
    container.innerHTML = '';

    todos.forEach(item => {
      const card = document.createElement('div');
      card.className = 'product-card';
      const isConfigurado = ('qtd_por_volume' in item);

      card.innerHTML = `
        <div class="product-header">
          <h3 class="product-title">
            <i class="fas ${isConfigurado ? 'fa-check-circle' : 'fa-exclamation-circle'}"
               style="color: ${isConfigurado ? 'var(--success)' : 'var(--warning)'};"></i>
            ${item.nome}
            <span class="product-badge">${item.unidade_compra}</span>
          </h3>
        </div>
        <p class="product-code">Código: ${item.codigo_fornecedor}</p>
        ${isConfigurado ? `
          <div class="product-meta">
            <div class="meta-item">
              <i class="fas fa-box-open"></i>
              <span>Volume: <span class="meta-value">${item.qtd_por_volume} ${item.unidade_compra}</span></span>
            </div>
            <div class="meta-item">
              <i class="fas fa-boxes"></i>
              <span>Pacote: <span class="meta-value">${item.qtd_por_pacote} ${item.unidade_compra}</span></span>
            </div>
          </div>
          <div class="form-actions">
            <button class="btn btn-secondary" onclick="editarConfiguracao(
              '${item.codigo_fornecedor}',
              '${item.nome}',
              '${item.unidade_compra}',
              '${item.qtd_por_volume}',
              '${item.qtd_por_pacote}'
            )">
              <i class="fas fa-edit"></i> Editar
            </button>
          </div>
        ` : `
          <div class="form-group">
            <label class="form-label">Quantidade por Volume</label>
            <input type="number" class="form-input" id="volume-todos-${item.codigo_fornecedor}" placeholder="Ex: 10">
          </div>
          <div class="form-group">
            <label class="form-label">Quantidade por Pacote</label>
            <input type="number" class="form-input" id="pacote-todos-${item.codigo_fornecedor}" placeholder="Ex: 50">
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" onclick="salvarConfiguracao(
              '${item.codigo_fornecedor}',
              '${item.nome}',
              '${item.unidade_compra}',
              document.getElementById('volume-todos-${item.codigo_fornecedor}').value,
              document.getElementById('pacote-todos-${item.codigo_fornecedor}').value
            )">
              <i class="fas fa-save"></i> Salvar
            </button>
          </div>
        `}
      `;
      container.appendChild(card);
    });

  } catch (error) {
    console.error('Erro ao carregar todos os itens:', error);
    alert('Erro ao carregar dados: ' + error.message);
  }
}
