/* Dashboard ML - JavaScript principal (versão real, sem dados fictícios) */

class MLDashboard {
  constructor() {
    this.charts = {};
    this.currentData = null;
    this.refreshInterval = null;
    this.init();
  }

  init() {
    this.loadDashboardData();   // cards e gráficos de categorias/status
    this.initCharts();
    this.setupEventListeners();
    this.startAutoRefresh();
    this.loadRecentAds();       // tabela
    this.loadPerformance();     // gráfico de performance (novo)
    this.toggleIASection(false);
  }

  // --------------------- Carregamento de dados (cards + gráficos parciais) ---------------------
  async loadDashboardData() {
    try {
      const resp = await fetch('/vendas/ml/anuncios/api/dashboard-data', { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      this.currentData = await resp.json();
      this.updateStats();
      this.updateCharts();   // atualiza categorias/status
    } catch (error) {
      console.error('Erro ao carregar dados do dashboard:', error);
      this.showNotification('Erro ao carregar dados', 'error');
    }
  }

  updateStats() {
    if (!this.currentData) return;

    this.animateCounter('.stat-card.primary h3', Number(this.currentData.total_anuncios || 0));
    this.animateCounter('.stat-card.success h3', Number(this.currentData.anuncios_ativos || 0));
    this.animateCounter('.stat-card.warning h3', Number(this.currentData.total_vendido || 0));

    const receitaEl = document.querySelector('.stat-card.accent h3');
    if (receitaEl) {
      const val = Number(this.currentData.receita_total || 0);
      receitaEl.textContent = `R$ ${val.toFixed(2).replace('.', ',')}`;
    }
  }

  animateCounter(selector, finalValue) {
    const el = document.querySelector(selector);
    if (!el) return;
    const isNumber = typeof finalValue === 'number' && !Number.isNaN(finalValue);
    if (!isNumber) { el.textContent = finalValue || '0'; return; }

    const startValue = 0, duration = 800;
    const startTime = performance.now();
    const step = (now) => {
      const p = Math.min((now - startTime) / duration, 1);
      const val = Math.floor(startValue + (finalValue - startValue) * p);
      el.textContent = val.toLocaleString('pt-BR');
      if (p < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  }

  // --------------------------------- Gráficos ---------------------------------
  initCharts() {
    this.initCategoriesChart();
    this.initStatusChart();
    this.initPerformanceChart();
  }

  initCategoriesChart() {
    const ctx = document.getElementById('categoriasChart');
    if (!ctx) return;
    this.charts.categorias = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: [],
        datasets: [{ data: [], backgroundColor: ['#00bfff','#ff3e80','#00c896','#ffaa00','#8b5cf6'], borderWidth: 0, hoverOffset: 10 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom', labels: { padding: 20, usePointStyle: true } } },
        animation: { animateRotate: true, duration: 800 }
      }
    });
  }

  initStatusChart() {
    const ctx = document.getElementById('statusChart');
    if (!ctx) return;
    this.charts.status = new Chart(ctx, {
      type: 'pie',
      data: {
        labels: [],
        datasets: [{ data: [], backgroundColor: ['#00c896','#ffaa00','#ff3e5f'], borderWidth: 0, hoverOffset: 8 }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom', labels: { padding: 20, usePointStyle: true } } },
        animation: { animateRotate: true, duration: 800 }
      }
    });
  }

  initPerformanceChart() {
    const ctx = document.getElementById('performanceChart');
    if (!ctx) return;
    this.charts.performance = new Chart(ctx, {
      type: 'line',
      data: {
        labels: [],
        datasets: [
          { label: 'Vendas',  data: [], borderColor: '#00bfff', backgroundColor: 'rgba(0,191,255,0.1)', borderWidth: 3, fill: true, tension: 0.4 },
          { label: 'Receita', data: [], borderColor: '#ff3e80', backgroundColor: 'rgba(255,62,128,0.1)', borderWidth: 3, fill: true, tension: 0.4 },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        plugins: { legend: { position: 'top', labels: { padding: 20, usePointStyle: true } } },
        scales: { x: { ticks: {} }, y: { ticks: {} } },
        animation: { duration: 800 }
      }
    });
  }

  updateCharts() {
    if (!this.currentData) return;

    // Categorias (top_categorias = [[categoria, qtd], ...])
    if (this.charts.categorias && Array.isArray(this.currentData.top_categorias)) {
      const labels = this.currentData.top_categorias.map(x => (x?.[0] || 'Sem categoria'));
      const data   = this.currentData.top_categorias.map(x => Number(x?.[1] || 0));
      this.charts.categorias.data.labels = labels;
      this.charts.categorias.data.datasets[0].data = data;
      this.charts.categorias.update('active');
    }

    // Status (status_distribution = [[status, qtd], ...])
    if (this.charts.status && Array.isArray(this.currentData.status_distribution)) {
      const map = { active: 'Ativo', paused: 'Pausado', closed: 'Finalizado' };
      const labels = this.currentData.status_distribution.map(s => map[s?.[0]] || (s?.[0] || ''));
      const data   = this.currentData.status_distribution.map(s => Number(s?.[1] || 0));
      this.charts.status.data.labels = labels;
      this.charts.status.data.datasets[0].data = data;
      this.charts.status.update('active');
    }

    // Performance agora é carregada via API dedicada (loadPerformance)
  }

  // ------------------------- Tabela: anúncios recentes -------------------------
  async loadRecentAds() {
    try {
      const resp = await fetch('/vendas/ml/anuncios/api/recentes?limit=10', { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const ads = await resp.json();
      this.renderRecentAds(Array.isArray(ads) ? ads : []);
    } catch (error) {
      console.error('Erro ao carregar anúncios recentes:', error);
      this.renderRecentAds([]);
    }
  }

  renderRecentAds(ads) {
    const tbody = document.getElementById('adsTableBody');
    if (!tbody) return;

    const frag = document.createDocumentFragment();
    const fmtBR = (n) =>
      Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    const makeCell = (html) => {
      const td = document.createElement('td');
      td.innerHTML = html;
      return td;
    };

    if (!Array.isArray(ads) || ads.length === 0) {
      const tr = document.createElement('tr');
      tr.appendChild(makeCell(`<td colspan="8" style="text-align:center;color:#888;">Sem anúncios para exibir.</td>`));
      frag.appendChild(tr);
      tbody.replaceChildren(frag);
      return;
    }

    for (const ad of ads) {
      const tr = document.createElement('tr');

      const id        = ad.id || ad.mlb || ad.codigo || '';
      const titulo    = ad.titulo || ad.title || '';
      const cat       = ad.categoria || ad.category || '';
      const precoNum  = ad.preco ?? ad.price ?? null;
      const preco     = (precoNum != null) ? `R$ ${fmtBR(precoNum)}` : '';
      const statusRaw = (ad.status || '').toString().toLowerCase();
      const vendidos  = ad.vendidos ?? ad.sold ?? ad.sales ?? 0;
      const atualizado= ad.atualizado || ad.updated_at || ad.updated || ad.data_atualizacao || '';

      // IMAGEM: usa EXCLUSIVAMENTE as duas colunas definidas (sem heurística)
      const imgUrl = (ad && typeof ad.url_imagem_principal === 'string' && ad.url_imagem_principal.trim())
        ? ad.url_imagem_principal.trim()
        : ((ad && typeof ad.miniatura === 'string' && ad.miniatura.trim())
            ? ad.miniatura.trim()
            : null);

      const img = document.createElement('img');
      img.loading = 'lazy';
      img.decoding = 'async';
      img.width = 50;
      img.height = 50;
      img.style.objectFit = 'cover';
      img.style.borderRadius = '8px';

      if (imgUrl) {
        img.src = imgUrl;
        img.alt = titulo || 'Anúncio';
        img.onerror = function () {
          if (!this.dataset.errorHandled) {
            this.dataset.errorHandled = '1';
            this.removeAttribute('src'); // não tenta de novo
            this.alt = 'Sem imagem';
            this.style.background = '#f0f0f0 url("data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 width=%2750%27 height=%2750%27 viewBox=%270 0 24 24%27 fill=%27%23bbb%27%3E%3Cpath d=%27M19 3H5c-1.1 0-2 .9-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2zM8 7a2 2 0 1 1 0 4 2 2 0 0 1 0-4zm11 12H5l4.5-6 3.5 4.5 2.5-3L19 19z%27/%3E%3C/svg%3E") center/24px no-repeat';
          }
        };
      } else {
        img.alt = 'Sem imagem';
        img.style.background = '#f0f0f0 url("data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 width=%2750%27 height=%2750%27 viewBox=%270 0 24 24%27 fill=%27%23bbb%27%3E%3Cpath d=%27M19 3H5c-1.1 0-2 .9-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2zM8 7a2 2 0 1 1 0 4 2 2 0 0 1 0-4zm11 12H5l4.5-6 3.5 4.5 2.5-3L19 19z%27/%3E%3C/svg%3E") center/24px no-repeat';
      }

      const imgTd = document.createElement('td');
      imgTd.appendChild(img);
      tr.appendChild(imgTd);

      tr.appendChild(makeCell(`<strong>${(titulo || '').substring(0, 80)}${(titulo || '').length > 80 ? '...' : ''}</strong>`));
      tr.appendChild(makeCell(`${cat || ''}`));
      tr.appendChild(makeCell(`<strong>${preco || ''}</strong>`));

      const statusLabel = statusRaw === 'active' ? 'Ativo'
                        : statusRaw === 'paused' ? 'Pausado'
                        : statusRaw === 'closed' ? 'Finalizado'
                        : (statusRaw || '');
      const statusClass = ['active','paused','closed'].includes(statusRaw) ? statusRaw : 'unknown';
      tr.appendChild(makeCell(`<span class="status-badge status-${statusClass}">${statusLabel}</span>`));

      tr.appendChild(makeCell(`<span class="success">${vendidos}</span>`));
      const updatedBR = atualizado ? new Date(atualizado).toLocaleDateString('pt-BR') : '';
      tr.appendChild(makeCell(`${updatedBR}`));

      tr.appendChild(makeCell(`
        <div style="display:flex;gap:5px;">
          <button onclick="verAnuncio('${id}')" class="action-btn primary" title="Ver"><i class="ri-eye-line"></i></button>
          <button onclick="editarAnuncio('${id}')" class="action-btn secondary" title="Editar"><i class="ri-edit-line"></i></button>
        </div>
      `));

      frag.appendChild(tr);
    }

    tbody.replaceChildren(frag);
  }

  // ------------------------- Performance: carrega do backend -------------------------
  async loadPerformance() {
    try {
      const sel = document.getElementById('periodoFilter');
      const days = sel ? parseInt(sel.value, 10) : 30;
      const resp = await fetch(`/vendas/ml/anuncios/api/performance?days=${days}`, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      if (!this.charts.performance) return;

      this.charts.performance.data.labels = Array.isArray(data.labels) ? data.labels : [];
      this.charts.performance.data.datasets[0].data = Array.isArray(data.vendas)  ? data.vendas  : [];
      this.charts.performance.data.datasets[1].data = Array.isArray(data.receita) ? data.receita : [];
      this.charts.performance.update('active');
    } catch (error) {
      console.error('Erro ao carregar performance:', error);
      if (this.charts.performance) {
        this.charts.performance.data.labels = [];
        this.charts.performance.data.datasets[0].data = [];
        this.charts.performance.data.datasets[1].data = [];
        this.charts.performance.update('active');
      }
    }
  }

  // ------------------------------- Event Listeners -------------------------------
  setupEventListeners() {
    const periodoFilter = document.getElementById('periodoFilter');
    if (periodoFilter) periodoFilter.addEventListener('change', () => this.loadPerformance());

    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
      searchInput.addEventListener('keyup', (e) => {
        if (e.key === 'Enter' && window.filtrarAnuncios) window.filtrarAnuncios();
      });
    }
  }

  // -------------------------------- Auto-refresh --------------------------------
  startAutoRefresh() { this.refreshInterval = setInterval(() => this.loadDashboardData(), 300000); } // 5 min
  stopAutoRefresh()  { if (this.refreshInterval) clearInterval(this.refreshInterval); }

  // -------------------------------- Utilitários --------------------------------
  showNotification(message, type = 'success') {
    const n = document.getElementById('notification');
    const m = document.getElementById('notification-message');
    if (n && m) {
      m.textContent = message;
      n.className = `notification ${type}`;
      n.classList.add('show');
      setTimeout(() => n.classList.remove('show'), 3000);
    }
  }

  toggleIASection(show) {
    const el = document.getElementById('aiInsights');
    if (el) el.style.display = show ? 'block' : 'none';
  }
}

/* ========================= Boot ========================= */
document.addEventListener('DOMContentLoaded', () => { window.mlDashboard = new MLDashboard(); });

/* ================== Ações de UI globais ================= */
function novoAnuncioIA() { window.location.href = '/vendas/ml/anuncios/novo'; }
function sincronizarML() { mlDashboard.loadDashboardData().then(() => mlDashboard.showNotification('Dados sincronizados!')); }
function exportarRelatorio(){ mlDashboard.showNotification('Exportando relatório...', 'info'); }
function atualizarGrafico(tipo) { mlDashboard.updateCharts(); mlDashboard.showNotification(`Gráfico ${tipo} atualizado`, 'info'); }
function exportarGrafico(tipo) {
  const chart = mlDashboard.charts[tipo]; if (!chart) return;
  const url = chart.toBase64Image(); const a = document.createElement('a'); a.download = `grafico-${tipo}.png`; a.href = url; a.click();
}
function atualizarPerformance() { mlDashboard.loadPerformance(); }
function verAnuncio(id)    { if (id) window.location.href = `/vendas/ml/anuncios/ver/${id}`; }
function editarAnuncio(id) { if (id) window.location.href = `/vendas/ml/anuncios/editar/${id}`; }
function filtrarAnuncios() { /* filtro server-side se necessário */ }
function verTodosAnuncios(){ window.location.href = '/vendas/ml/anuncios/listar'; }
function gerarInsights()   { mlDashboard.showNotification('Gere uma chave de IA para habilitar insights.', 'info'); }
function otimizarPrecos()  { mlDashboard.showNotification('Recurso dependente de IA.', 'info'); }
function reativarAnuncios(){ window.location.href = '/vendas/ml/anuncios/listar?status=paused'; }
function criarAnuncioCategoria(categoria) { window.location.href = `/vendas/ml/anuncios/novo?categoria=${encodeURIComponent(categoria)}`; }
window.addEventListener('beforeunload', () => mlDashboard.stopAutoRefresh());
