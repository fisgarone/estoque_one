/* ===================================================================
   ML Dashboard JavaScript - Versão Corporativa Completa
   Sistema: FisgarOne ERP - Design Corporativo

   ATUALIZADO: Validado para integração com fisgarone.db
   Todos os endpoints de API agora extraem dados de fisgarone.db
   Gráficos e visualizações alimentados por dados reais do banco global
   =================================================================== */

const BASE = "/vendas/ml";
const AI_BASE = "/vendas/ml/api/ai";

// Estado centralizado da aplicação
const state = {
    page: 1,
    pageSize: 50,
    filtros: {conta: "", status: "", start: "", end: "", q: "", sku: "", mlb: ""},
    refreshTimer: null,
    chart: null,
    topChart: null,
    pieChart: null,
    metric: "bruto",
    currentOrder: null,
    pieMetric: "vendas",
    isLoading: false,
    currentTheme: 'light'
};

/* ===================================================================
   CONTROLE DE TEMA
   =================================================================== */

function initializeTheme() {
    const savedTheme = localStorage.getItem('ml-dashboard-theme');
    const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    const theme = savedTheme || (systemPrefersDark ? 'dark' : 'light');
    setTheme(theme);
}

function setTheme(theme) {
    state.currentTheme = theme;
    document.documentElement.classList.toggle('dark-theme', theme === 'dark');
    localStorage.setItem('ml-dashboard-theme', theme);
    updateChartsForTheme();
}

function toggleTheme() {
    const newTheme = state.currentTheme === 'light' ? 'dark' : 'light';
    setTheme(newTheme);
    showToast(`Tema alterado para ${newTheme === 'dark' ? 'escuro' : 'claro'}`, 'success');
}

function updateChartsForTheme() {
    setTimeout(() => {
        if (state.chart) state.chart.update('none');
        if (state.topChart) state.topChart.update('none');
        if (state.pieChart) state.pieChart.update('none');
    }, 100);
}

/* ===================================================================
   FUNÇÕES HELPER
   =================================================================== */

const fmtMoney = (v) => (v === null || v === undefined || isNaN(Number(v))) ? "—" : Number(v).toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL"
});
const fmtPct = (v) => (v === null || v === undefined || isNaN(Number(v))) ? "—" : `${(Number(v) * 100).toFixed(1)}%`;
const fmtNumber = (v) => (v === null || v === undefined || isNaN(Number(v))) ? "—" : Number(v).toLocaleString("pt-BR");
const toISO = (d) => d.toISOString().split('T')[0];

function buildQuery(extra = {}) {
    const f = {...state.filtros, ...extra};
    const p = new URLSearchParams();
    Object.entries(f).forEach(([k, v]) => {
        if (v) {
            if (k === 'conta') {
                p.set(k, v.toUpperCase());
            } else {
                p.set(k, v);
            }
        }
    });
    return `?${p.toString()}`;
}

function setLoading(element, isLoading) {
    if (element) {
        element.style.opacity = isLoading ? '0.5' : '1';
        element.style.pointerEvents = isLoading ? 'none' : 'auto';
        if (isLoading) {
            element.classList.add('loading');
        } else {
            element.classList.remove('loading');
        }
    }
}

function showToast(message, type = 'success') {
    if (typeof window.showNotification === 'function') {
        window.showNotification(message, type);
    } else {
        const notification = document.createElement('div');
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 1rem 1.5rem;
            border-radius: 8px;
            color: white;
            font-weight: 600;
            z-index: 10000;
            animation: slideIn 0.3s ease-out;
            ${type === 'success' ? 'background: #059669;' :
            type === 'error' ? 'background: #dc2626;' :
                type === 'warning' ? 'background: #d97706;' :
                    'background: #1e40af;'}
        `;
        notification.textContent = message;
        document.body.appendChild(notification);
        setTimeout(() => notification.remove(), 5000);
    }
}

/* ===================================================================
   FUNÇÕES AUXILIARES
   =================================================================== */

function getStatusBadgeClass(status) {
    if (!status) return 'secondary';
    const statusLower = status.toLowerCase();
    if (statusLower.includes('entreg') || statusLower.includes('concluíd') || statusLower.includes('approved') || statusLower.includes('paid')) {
        return 'success';
    } else if (statusLower.includes('pendente') || statusLower.includes('process') || statusLower.includes('pending') || statusLower.includes('handling')) {
        return 'warning';
    } else if (statusLower.includes('cancel') || statusLower.includes('rejeitad') || statusLower.includes('rejected') || statusLower.includes('cancelled')) {
        return 'danger';
    } else {
        return 'secondary';
    }
}

function openOrderAnalyticalModal(orderData) {
    /**
     * Abre modal analítico com dados do pedido
     * orderData extraído de fisgarone.db via API
     */
    if (typeof analyticalModal !== 'undefined') {
        analyticalModal.open(orderData);
    } else {
        const profitMargin = ((orderData.lucro_real_rs || 0) / (orderData.bruto_rs || 1)) * 100;
        showToast(`📦 Pedido: ${orderData.id_pedido_ml}\n💰 Valor: ${fmtMoney(orderData.bruto_rs)}\n💸 Lucro: ${fmtMoney(orderData.lucro_real_rs)}\n📊 Margem: ${profitMargin.toFixed(1)}%`, 'info');
    }
}

function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showToast('ID copiado para a área de transferência!', 'success');
    }).catch(() => {
        showToast('Erro ao copiar para a área de transferência', 'error');
    });
}

function trackOrder(orderId) {
    window.open(`https://www.mercadolivre.com.br/rastreio/${orderId}`, '_blank');
    showToast(`Abrindo rastreamento do pedido ${orderId}`, 'info');
}

function contactBuyer(orderId) {
    window.open(`https://www.mercadolivre.com.br/mensagens/${orderId}`, '_blank');
    showToast(`Abrindo mensagens para o pedido ${orderId}`, 'info');
}

/* ===================================================================
   CARREGAMENTO DE DADOS

   ATUALIZADO: Todas as funções extraem dados de fisgarone.db via API
   =================================================================== */

async function loadFilters() {
    /**
     * Carrega filtros disponíveis de fisgarone.db
     * Extrai contas e status distintos da tabela vendas_ml
     */
    try {
        const res = await fetch(`${BASE}/api/filters`);
        if (!res.ok) throw new Error(`Falha ao carregar filtros: ${res.statusText}`);
        const data = await res.json();

        const contaSelect = document.getElementById("f-conta");
        const statusSelect = document.getElementById("f-status");

        const contasUnicas = [...new Set((data.contas || []).map(c => c.toUpperCase()))];

        contaSelect.innerHTML = '<option value="">Todas as Contas</option>';
        contasUnicas.forEach(c => {
            contaSelect.innerHTML += `<option value="${c}">${c}</option>`;
        });

        statusSelect.innerHTML = '<option value="">Todos os Status</option>';
        (data.status || []).forEach(s => {
            statusSelect.innerHTML += `<option value="${s}">${s}</option>`;
        });

        const now = new Date();
        const start = new Date(now.getFullYear(), now.getMonth(), 1);
        document.getElementById("f-start").value = toISO(start);
        document.getElementById("f-end").value = toISO(now);

        showToast('Filtros carregados com sucesso!', 'success');

    } catch (error) {
        console.error("Erro ao carregar filtros:", error);
        showToast('Erro crítico ao carregar filtros iniciais', 'error');
    }
}

async function loadOverview() {
    /**
     * Carrega KPIs principais de fisgarone.db
     * Extrai: pedidos, bruto, taxas, lucro real, repasse
     */
    const container = document.getElementById("ml-kpis");
    setLoading(container, true);

    try {
        const res = await fetch(`${BASE}/api/overview${buildQuery()}`);
        if (!res.ok) throw new Error('Falha ao carregar KPIs');
        const d = await res.json();

        document.getElementById("kpi-bruto").textContent = fmtMoney(d.bruto);
        document.getElementById("kpi-pedidos").textContent = fmtNumber(d.pedidos);
        document.getElementById("kpi-repasse").textContent = fmtMoney(d.repasse_real);
        document.getElementById("kpi-taxas").textContent = fmtMoney((d.taxa_total || 0) + (d.frete_net || 0));
        document.getElementById("kpi-lucro").textContent = fmtMoney(d.lucro_real);
        document.getElementById("kpi-lucro-pct").textContent = fmtPct(d.lucro_percent_medio);
        document.getElementById("kpi-div").textContent = fmtNumber(d.divergencias);

    } catch (error) {
        console.error("Erro ao carregar overview:", error);
        showToast('Erro ao carregar KPIs principais', 'error');
    } finally {
        setLoading(container, false);
    }
}

async function loadTrends() {
    /**
     * Carrega tendências diárias de fisgarone.db
     * Gráfico de linha: Faturamento Bruto vs Lucro Real
     */
    const container = document.getElementById("chart-trend")?.closest('.card');
    setLoading(container, true);
    try {
        const res = await fetch(`${BASE}/api/trends${buildQuery()}`);
        if (!res.ok) throw new Error('Falha ao carregar tendências');
        const d = await res.json();
        const ctx = document.getElementById("chart-trend")?.getContext('2d');
        if (!ctx) return;

        const labels = d.map(x => new Date(x.dia + 'T00:00:00').toLocaleDateString('pt-BR', {timeZone: 'UTC'}));
        if (state.chart) state.chart.destroy();

        state.chart = new Chart(ctx, {
            type: "line",
            data: {
                labels,
                datasets: [
                    {
                        label: "Faturamento Bruto",
                        data: d.map(x => x.bruto),
                        borderColor: '#1e40af',
                        backgroundColor: 'rgba(30, 64, 175, 0.1)',
                        tension: 0.3,
                        fill: true
                    },
                    {
                        label: "Lucro Real",
                        data: d.map(x => x.lucro),
                        borderColor: '#059669',
                        backgroundColor: 'rgba(5, 150, 105, 0.1)',
                        tension: 0.3,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function (value) {
                                return 'R$ ' + value.toLocaleString('pt-BR');
                            }
                        }
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        callbacks: {
                            label: function (context) {
                                let label = context.dataset.label || '';
                                if (label) label += ': ';
                                if (context.parsed.y !== null) label += fmtMoney(context.parsed.y);
                                return label;
                            }
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error("Erro ao carregar tendências:", error);
        showToast('Erro ao carregar gráfico de tendências', 'error');
    } finally {
        setLoading(container, false);
    }
}

async function loadDaily() {
    /**
     * Carrega dados diários de fisgarone.db
     * Mini cards com barras de tendência
     */
    const container = document.getElementById("mini-cards");
    setLoading(container, true);
    try {
        const res = await fetch(`${BASE}/api/daily${buildQuery()}`);
        if (!res.ok) throw new Error('Falha ao carregar dados diários');
        const data = await res.json();

        container.innerHTML = "";
        let prevValue = null;

        data.forEach((d, index) => {
            const currentValue = state.metric === "bruto" ? (d.bruto || 0) : (d.unidades || 0);
            let barColor = '#6b7280';
            let trendClass = 'trend-eq';

            if (prevValue !== null && prevValue !== 0) {
                const change = ((currentValue - prevValue) / prevValue) * 100;
                if (change > 5) {
                    barColor = '#059669';
                    trendClass = 'trend-up';
                } else if (change < -5) {
                    barColor = '#d97706';
                    trendClass = 'trend-down';
                }
            }

            const isToday = new Date().toDateString() === new Date(d.dia + 'T00:00:00').toDateString();
            const dayCardClass = `day-card ${trendClass} ${isToday ? 'today' : ''}`;

            const el = document.createElement("div");
            el.className = dayCardClass;
            el.innerHTML = `
                <div class="day-bar" style="height:${Math.min(100, (currentValue / (Math.max(...data.map(i => state.metric === 'bruto' ? i.bruto : i.unidades)) || 1)) * 100)}%; background-color:${barColor};"></div>
                <div class="day-label">${new Date(d.dia + 'T00:00:00').getDate()}</div>
                <div class="day-value">${state.metric === "bruto" ? fmtMoney(currentValue) : fmtNumber(currentValue)}</div>
            `;
            container.appendChild(el);
            prevValue = currentValue;
        });
    } catch (error) {
        console.error("Erro ao carregar dados diários:", error);
        showToast('Erro ao carregar cards diários', 'error');
    } finally {
        setLoading(container, false);
    }
}

async function loadOrders(page = 1) {
    /**
     * Carrega lista de pedidos paginada de fisgarone.db
     * Exibe cards com informações detalhadas de cada pedido
     * Inclui: ID, Data, Conta, SKU, Faturamento, Lucro, Margem
     */
    state.page = page;
    const container = document.getElementById("orders-grid")?.closest('.card');
    setLoading(container, true);
    try {
        const skuFilter = document.getElementById("f-sku").value;
        const mlbFilter = document.getElementById("f-mlb").value;

        const res = await fetch(`${BASE}/api/orders${buildQuery({
            page,
            page_size: state.pageSize,
            sku: skuFilter,
            mlb: mlbFilter
        })}`);
        if (!res.ok) throw new Error('Falha ao carregar pedidos');
        const d = await res.json();

        const grid = document.getElementById("orders-grid");
        grid.innerHTML = "";
        grid.className = 'orders-grid-advanced';

        if (!d.items || d.items.length === 0) {
            grid.innerHTML = '<div class="empty-msg">Nenhum pedido encontrado com os filtros aplicados.</div>';
            updatePagination(d.page, d.total, d.page_size);
            return;
        }

        d.items.forEach(row => {
            const dataFormatada = row.data_venda_iso ? new Date(row.data_venda_iso + 'T00:00:00').toLocaleDateString('pt-BR', {timeZone: 'UTC'}) : 'N/A';
            const statusClass = getStatusBadgeClass(row.status_pedido);
            const profitMargin = ((row.lucro_real_rs || 0) / (row.bruto_rs || 1)) * 100;
            const marginClass = profitMargin >= 20 ? 'positive' : profitMargin >= 10 ? '' : 'negative';

            const orderCard = document.createElement('div');
            orderCard.className = 'order-card-advanced';
            orderCard.innerHTML = `
            <div class="order-badge badge-${statusClass}">
                ${row.status_pedido || 'N/A'}
            </div>
            <div class="order-header">
                <div class="order-id">${row.id_pedido_ml}</div>
                <div class="order-date">${dataFormatada}</div>
            </div>
            <div class="order-details">
                <div class="detail-item">
                    <div class="detail-label">Conta</div>
                    <div class="detail-value">${row.conta || '—'}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">SKU</div>
                    <div class="detail-value">${row.sku || '—'}</div>
                </div>
            </div>
            <div class="order-metrics">
                <div class="metric-item">
                    <div class="metric-value">${fmtMoney(row.bruto_rs || 0)}</div>
                    <div class="metric-label">Faturamento</div>
                </div>
                <div class="metric-item">
                    <div class="metric-value ${marginClass}">${fmtMoney(row.lucro_real_rs || 0)}</div>
                    <div class="metric-label">Lucro (${profitMargin.toFixed(1)}%)</div>
                </div>
            </div>
            <div class="order-actions">
                <button class="order-action-btn analytical" onclick="event.stopPropagation(); openOrderAnalyticalModal(${JSON.stringify(row).replace(/"/g, '&quot;')})">
                    <i class="ri-bar-chart-line"></i>
                    Análise Avançada
                </button>
                <button class="order-action-btn" onclick="event.stopPropagation(); copyToClipboard('${row.id_pedido_ml}')">
                    <i class="ri-file-copy-line"></i>
                    Copiar ID
                </button>
            </div>
        `;

            orderCard.addEventListener('click', () => {
                openOrderAnalyticalModal(row);
            });

            grid.appendChild(orderCard);
        });

        updatePagination(d.page, d.total, d.page_size);

    } catch (error) {
        console.error("Erro ao carregar pedidos:", error);
        showToast('Erro ao carregar lista de pedidos', 'error');
    } finally {
        setLoading(container, false);
    }
}

function updatePagination(page, total, pageSize) {
    const totalPages = Math.ceil(total / pageSize);
    const pageInfo = document.getElementById("page-info");
    const prevBtn = document.getElementById("prev-page");
    const nextBtn = document.getElementById("next-page");

    if (pageInfo) pageInfo.textContent = `Página ${page} de ${totalPages} • ${total} pedidos`;
    if (prevBtn) {
        prevBtn.disabled = page <= 1;
        prevBtn.style.opacity = page <= 1 ? '0.5' : '1';
    }
    if (nextBtn) {
        nextBtn.disabled = page >= totalPages;
        nextBtn.style.opacity = page >= totalPages ? '0.5' : '1';
    }
}

/* ===================================================================
   EVENTOS E INICIALIZAÇÃO
   =================================================================== */

function applyFilters() {
    /**
     * Aplica filtros e recarrega todos os dados de fisgarone.db
     * Atualiza: KPIs, Tendências, Cards Diários, Lista de Pedidos
     */
    if (state.isLoading) {
        showToast('Aguarde a atualização anterior terminar', 'warning');
        return;
    }

    state.isLoading = true;
    showToast('Aplicando filtros...', 'info');

    state.filtros = {
        conta: document.getElementById("f-conta").value,
        status: document.getElementById("f-status").value,
        start: document.getElementById("f-start").value,
        end: document.getElementById("f-end").value,
        q: document.getElementById("f-q").value,
    };

    document.getElementById("f-sku").value = "";
    document.getElementById("f-mlb").value = "";

    const allPromises = [
        loadOverview(),
        loadTrends(),
        loadDaily(),
        loadOrders(1)
    ];

    Promise.allSettled(allPromises).then((results) => {
        state.isLoading = false;
        const failed = results.filter(r => r.status === 'rejected').length;
        if (failed === 0) {
            showToast('Dashboard atualizado com sucesso!', 'success');
        } else {
            showToast(`Dashboard atualizado com ${failed} erros`, 'warning');
        }
    });
}

function bindEventListeners() {
    // Botão de tema
    const themeToggle = document.createElement('button');
    themeToggle.className = 'btn primary outline';
    themeToggle.innerHTML = '<i class="ri-moon-line"></i> Tema';
    themeToggle.id = 'theme-toggle';
    themeToggle.addEventListener('click', toggleTheme);

    // Adicionar botão de tema ao header se existir
    const header = document.querySelector('.ml-header');
    if (header) {
        header.querySelector('.ml-title-wrap').appendChild(themeToggle);
    }

    // Filtros principais
    document.getElementById("btn-aplicar").addEventListener("click", applyFilters);
    document.getElementById("f-q").addEventListener("keyup", e => e.key === 'Enter' && applyFilters());

    // Exportação
    document.getElementById("btn-export-csv").addEventListener("click", () => {
        showToast('Iniciando exportação CSV...', 'info');
        window.open(`${BASE}/export${buildQuery({format: 'csv'})}`);
    });

    document.getElementById("btn-export-xlsx").addEventListener("click", () => {
        showToast('Iniciando exportação Excel...', 'info');
        window.open(`${BASE}/export${buildQuery({format: 'xlsx'})}`);
    });

    // Paginação
    document.getElementById("prev-page").addEventListener("click", () => {
        if (state.page > 1) loadOrders(state.page - 1);
    });

    document.getElementById("next-page").addEventListener("click", () => {
        loadOrders(state.page + 1);
    });

    // Atualização automática
    state.refreshTimer = setInterval(() => {
        if (!state.isLoading && document.visibilityState === 'visible') {
            loadOverview();
            loadTrends();
        }
    }, 120000);
}

// Inicialização
document.addEventListener("DOMContentLoaded", async () => {
    console.log('🚀 Inicializando ML Dashboard Corporativo (fisgarone.db)...');

    try {
        initializeTheme();
        await loadFilters();
        bindEventListeners();
        await applyFilters();

        console.log('✅ ML Dashboard Corporativo inicializado com sucesso!');
        console.log('📊 Todos os dados são extraídos de fisgarone.db');
        showToast('Sistema carregado e pronto para uso!', 'success');

    } catch (error) {
        console.error('❌ Erro na inicialização do dashboard:', error);
        showToast('Erro na inicialização do sistema', 'error');
    }
});

// Cleanup
window.addEventListener('beforeunload', () => {
    if (state.refreshTimer) clearInterval(state.refreshTimer);
});

// Exportar funções globais
window.copyOrderId = copyToClipboard;
window.trackOrder = trackOrder;
window.contactBuyer = contactBuyer;
window.openOrderAnalyticalModal = openOrderAnalyticalModal;
window.loadOrders = loadOrders;
window.applyFilters = applyFilters;

if (typeof showNotification === 'undefined') {
    window.showNotification = showToast;
}

console.log('📊 ML Dashboard JavaScript Corporativo carregado! (Integrado com fisgarone.db)');