// /static/js/estoque/dashboard_estoque.js

document.addEventListener('DOMContentLoaded', function() {
    let custoChart = null;
    let evolucaoChart = null;

    function carregarCards(periodo = 90) {
  fetch(`/estoque/api/cards_info?periodo=${periodo}`)
    .then(response => response.json())
    .then(data => {
      const elCusto = document.getElementById('custo-total');
      const elQtd   = document.getElementById('qtd-total');
      const elTot   = document.getElementById('total-produtos'); // <- NOVO

      if (elCusto) elCusto.textContent = data.custo_total;
      if (elQtd)   elQtd.textContent   = data.qtd_total;
      if (elTot && data.total_produtos !== undefined) elTot.textContent = data.total_produtos; // <- NOVO
    })
    .catch(error => console.error('Erro ao carregar cards de resumo:', error));
}


    function carregarGraficoCusto(periodo = 90) {
        const chartEl = document.getElementById('custoPorProdutoChart');
        if (!chartEl) return;

        fetch(`/estoque/api/custo_por_produto?periodo=${periodo}`)
            .then(response => response.json())
            .then(data => {
                const ctx = chartEl.getContext('2d');
                if (custoChart) {
                    custoChart.destroy();
                }
                custoChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: data.labels,
                        datasets: [{
                            label: 'Custo',
                            data: data.data,
                            backgroundColor: '#00bfff',
                            borderRadius: 8, // Cantos arredondados
                            borderSkipped: false,
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: { display: false },
                            tooltip: {
                                callbacks: {
                                    label: function(context) {
                                        let label = context.dataset.label || '';
                                        if (label) {
                                            label += ': ';
                                        }
                                        if (context.parsed.y !== null) {
                                            label += new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(context.parsed.y);
                                        }
                                        return label;
                                    }
                                }
                            }
                        },
                        scales: {
                            y: {
                                beginAtZero: true,
                                grid: {
                                    display: false // Remove as linhas de grade do fundo
                                },
                                ticks: {
                                    callback: function(value, index, ticks) {
                                        return 'R$ ' + (value / 1000) + 'k';
                                    }
                                }
                            },
                            x: {
                                grid: {
                                    display: false // Remove as linhas de grade do fundo
                                }
                            }
                        }
                    }
                });
            })
            .catch(error => console.error('Erro ao carregar gráfico de custo:', error));
    }

    function carregarAlertas(periodo = 90) {
        const alertasContainer = document.getElementById('alertas-lista');
        if (!alertasContainer) return;
        fetch(`/estoque/api/produtos_alerta?periodo=${periodo}`)
            .then(response => response.json())
            .then(data => {
                alertasContainer.innerHTML = '';
                if (data.length === 0) {
                    alertasContainer.innerHTML = '<p class="no-alerts">Nenhum produto precisando de atenção.</p>';
                    return;
                }
                data.forEach(alerta => {
                    const card = `<a href="/estoque/produto/${alerta.id}" class="alert-card ${alerta.status.includes('Crítico') ? 'critical' : 'warning'}"><div class="alert-icon"><i class="ri-error-warning-line"></i></div><div class="alert-details"><strong>${alerta.nome}</strong><span>${alerta.status}</span></div><div class="alert-info"><span>Estoque: ${alerta.quantidade}</span><span>Reposição: ${alerta.ponto_reposicao}</span></div></a>`;
                    alertasContainer.insertAdjacentHTML('beforeend', card);
                });
            })
            .catch
                if (window.__fisgar_fitGrid__) { setTimeout(() => window.__fisgar_fitGrid__.fitAll(), 50); }(error => console.error('Erro ao carregar alertas:', error));
    }

    function carregarGraficoEvolucao() {
        const chartEl = document.getElementById('evolucaoEstoqueChart');
        if (!chartEl) return;
        fetch('/estoque/api/evolucao_estoque')
            .then(response => response.json())
            .then(data => {
                if (data.error) { console.error('Erro da API de evolução:', data.error); return; }
                const ctx = chartEl.getContext('2d');
                if (evolucaoChart) { evolucaoChart.destroy(); }
                evolucaoChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: data.labels,
                        datasets: [{
                            label: 'Valor do Estoque (R$)',
                            data: data.data,
                            borderColor: '#00c896',
                            backgroundColor: 'rgba(0, 200, 150, 0.1)',
                            fill: true,
                            tension: 0.4
                        }]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } },
                        scales: { y: { ticks: { callback: value => 'R$ ' + (value / 1000).toFixed(0) + 'k' } } }
                    }
                });
            })
            .catch(error => console.error('Erro ao carregar gráfico de evolução:', error));
    }

    function carregarGiroEstoque(periodo = 90) {
        const el = document.getElementById('giro-estoque');
        if (!el) return;
        el.textContent = '...';
        fetch(`/estoque/api/giro_estoque?periodo=${periodo}`)
            .then(res => res.json())
            .then(data => { el.textContent = data.giro_estoque !== undefined ? data.giro_estoque.toFixed(2) : 'N/A'; })
            .catch(() => { el.textContent = 'Erro'; });
    }

    function carregarProdutosSemGiro(periodo = 90) {
        const el = document.getElementById('produtos-sem-giro');
        if (!el) return;
        el.textContent = '...';
        fetch(`/estoque/api/produtos_sem_giro?periodo=${periodo}`)
            .then(res => res.json())
            .then(data => { el.textContent = data.produtos_sem_giro !== undefined ? data.produtos_sem_giro : 'N/A'; })
            .catch(() => { el.textContent = 'Erro'; });
    }

    // --- NOVA FUNÇÃO PARA O VALOR PARADO ---
    function carregarValorParado(periodo = 90) {
        const el = document.getElementById('valor-parado');
        if (!el) return;
        el.textContent = '...';
        fetch(`/estoque/api/valor_parado?periodo=${periodo}`)
            .then(res => res.json())
            .then(data => { el.textContent = data.valor_parado_formatado !== undefined ? data.valor_parado_formatado : 'N/A'; })
            .catch(() => { el.textContent = 'Erro'; });
    }

    function atualizarDashboard() {
        const periodo = document.getElementById('periodo-filtro').value;
        carregarCards(periodo);
        carregarGraficoCusto(periodo);
        carregarAlertas(periodo);
        carregarGiroEstoque(periodo);
        carregarProdutosSemGiro(periodo);
        carregarValorParado(periodo); // <-- Chamada da nova função
        carregarGraficoEvolucao();

  if (window.__fisgar_fitGrid__) { setTimeout(() => window.__fisgar_fitGrid__.fitAll(), 120); }
}

    if (document.querySelector('.dashboard-container')) {
        document.getElementById('periodo-filtro').addEventListener('change', atualizarDashboard);
        atualizarDashboard();
    }
});

window.atualizarDashboard = atualizarDashboard;


// ===================== FISGARONE – AUTO-FIT DE ALTURA PARA GRIDSTACK =====================
(function() {
  const RETRY_MS = 120;
  let grid = null;

  function debounce(fn, wait) {
    let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
  }

  function getGridInstance() {
    try {
      if (grid) return grid;
      const el = document.querySelector('.grid-stack');
      if (!el || !window.GridStack) return null;
      grid = GridStack.get(el) || GridStack.init(null, el);
      return grid;
    } catch (e) { return null; }
  }

  function pxToCells(px, cellH, vMargin) {
    const ch = Number(cellH) || 70;
    const vm = Number(vMargin) || 0;
    return Math.max(1, Math.ceil((px + vm) / (ch + vm)));
  }

  function fitItem(itemEl) {
    const g = getGridInstance();
    if (!g || !itemEl) return;
    const content = itemEl.querySelector('.grid-stack-item-content');
    if (!content) return;
    // Altura desejada é o scrollHeight do conteúdo
    const desiredPx = Math.max(content.scrollHeight, content.offsetHeight, 1);
    const hCells = pxToCells(desiredPx, g.opts.cellHeight, g.opts.verticalMargin);
    const current = parseInt(itemEl.getAttribute('gs-h') || itemEl.dataset.gsH || "1", 10);
    if (hCells !== current) {
      try {
        g.update(itemEl, {h: hCells});
      } catch (e) { /* ignore */ }
    }
  }

  function fitAll() {
    const g = getGridInstance();
    if (!g) return;
    document.querySelectorAll('.grid-stack-item').forEach(fitItem);
  }

  function observeContentResize() {
    // Refita quando a viewport muda
    window.addEventListener('resize', debounce(fitAll, 150));

    // Refita quando conteúdos internos mudarem (tabelas, listas, alertas, charts)
    const observer = new MutationObserver(debounce(fitAll, 60));
    observer.observe(document.querySelector('.grid-stack'), { childList: true, subtree: true });
  }

  function bootstrap() {
    const g = getGridInstance();
    if (!g) { return setTimeout(bootstrap, RETRY_MS); }
    fitAll();
    observeContentResize();
    // Expor para outras partes do app chamarem após render (charts, fetch, etc.)
    window.__fisgar_fitGrid__ = { fitAll, fitItem };
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bootstrap);
  } else {
    bootstrap();
  }
})();
