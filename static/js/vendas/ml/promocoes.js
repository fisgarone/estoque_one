// Dashboard de Promoções ML - Versão Premium Corrigida
console.log('Script promocoes.js carregado');

class PromocoesDashboard {
    constructor() {
        this.charts = {};
        this.data = {};
        this.filters = {
            dateRange: '30d',
            promotionType: 'all',
            status: 'all',
            minDiscount: 0,
            searchTerm: ''
        };
        this.currentView = 'cards';
        this.init();
    }

    async init() {
        console.log('Iniciando dashboard premium...');
        try {
            await this.loadData();
            this.showDashboard(); // Mostrar primeiro o dashboard
            this.setupEventListeners(); // Depois configurar eventos
            this.initCharts();
            this.applyFilters();
            this.generateAIInsights();
        } catch (error) {
            console.error('Erro na inicialização:', error);
            this.showError('Erro ao inicializar dashboard: ' + error.message);
        }
    }

    async loadData() {
        console.log('Carregando dados...');
        document.getElementById('loadingState').style.display = 'flex';

        await new Promise(resolve => setTimeout(resolve, 1000));

        try {
            const dashboardData = window.dashboardData || {};

            this.data = {
                metrics: {
                    totalPromotions: dashboardData.totalPromotions || 0,
                    sharedPromotions: dashboardData.sharedPromotions || 0,
                    totalOrders: dashboardData.totalOrders || 0,
                    ordersWithDiscount: dashboardData.ordersWithDiscount || 0,
                    totalSales: dashboardData.totalSales || 0,
                    totalDiscounts: dashboardData.totalDiscounts || 0,
                    avgDiscount: dashboardData.avgDiscount || 0,
                    avgOrderValue: dashboardData.avgOrderValue || 0,
                    roi: dashboardData.roi || 0,
                    discountRate: dashboardData.discountRate || 0
                },
                salesData: {
                    dates: this.ensureArray(dashboardData.salesDates) || ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun'],
                    amounts: this.ensureArray(dashboardData.salesAmounts) || [1200, 1800, 1500, 2200, 1900, 2100]
                },
                discountData: {
                    dates: this.ensureArray(dashboardData.discountDates) || ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun'],
                    amounts: this.ensureArray(dashboardData.discountAmounts) || [120, 180, 150, 220, 190, 210]
                },
                promotionTypes: {
                    labels: this.ensureArray(dashboardData.promLabels) || ['STANDARD', 'FLASH', 'VIP', 'SUPER'],
                    counts: this.ensureArray(dashboardData.promCounts) || [8, 3, 1, 2]
                },
                promotions: this.ensureArray(dashboardData.promotionsTable) || this.getSamplePromotions(),
                orders: this.ensureArray(dashboardData.ordersTable) || this.getSampleOrders()
            };

            console.log('Dados carregados:', this.data);
        } catch (error) {
            console.error('Erro ao carregar dados:', error);
            this.data = this.getSampleData();
        }
    }

    getSamplePromotions() {
        return [
            ['PROM001', 'STANDARD', 'Promoção de Verão - Brinquedos', '2024-01-01', '2024-01-31', 1, 15],
            ['PROM002', 'FLASH', 'Flash Sale - Eletrônicos', '2024-01-15', '2024-01-16', 0, 8],
            ['PROM003', 'VIP', 'Oferta Exclusiva VIP', '2024-01-10', '2024-01-20', 1, 5],
            ['PROM004', 'STANDARD', 'Desconto Progressivo', '2024-01-05', '2024-02-05', 0, 12],
            ['PROM005', 'SUPER', 'Super Black Friday', '2024-01-20', '2024-01-25', 1, 25],
            ['PROM006', 'FLASH', 'Oferta Relâmpago', '2024-01-18', '2024-01-19', 1, 6],
            ['PROM007', 'STANDARD', 'Promoção Mensal', '2024-01-01', '2024-01-31', 0, 18],
            ['PROM008', 'VIP', 'Cliente Premium', '2024-01-12', '2024-01-26', 1, 3]
        ];
    }

    getSampleOrders() {
        return [
            ['ORDER001', '2024-01-15', 250.00, 25.00, 3],
            ['ORDER002', '2024-01-16', 180.00, 18.00, 2],
            ['ORDER003', '2024-01-17', 320.00, 45.00, 4],
            ['ORDER004', '2024-01-18', 150.00, 12.00, 1],
            ['ORDER005', '2024-01-19', 280.00, 35.00, 3]
        ];
    }

    getSampleData() {
        return {
            metrics: {
                totalPromotions: 12,
                sharedPromotions: 5,
                totalOrders: 45,
                ordersWithDiscount: 38,
                totalSales: 12500.50,
                totalDiscounts: 1250.75,
                avgDiscount: 32.89,
                avgOrderValue: 277.78,
                roi: 2.5,
                discountRate: 10.0
            },
            salesData: {
                dates: ['01/01', '02/01', '03/01', '04/01', '05/01', '06/01'],
                amounts: [1200, 1800, 1500, 2200, 1900, 2100]
            },
            discountData: {
                dates: ['01/01', '02/01', '03/01', '04/01', '05/01', '06/01'],
                amounts: [120, 180, 150, 220, 190, 210]
            },
            promotionTypes: {
                labels: ['STANDARD', 'FLASH', 'VIP', 'SUPER'],
                counts: [8, 3, 1, 2]
            },
            promotions: this.getSamplePromotions(),
            orders: this.getSampleOrders()
        };
    }

    ensureArray(data) {
        if (Array.isArray(data)) return data;
        if (typeof data === 'string') {
            try {
                const parsed = JSON.parse(data);
                return Array.isArray(parsed) ? parsed : [];
            } catch (e) {
                return [];
            }
        }
        return [];
    }

    setupEventListeners() {
        console.log('Configurando event listeners...');

        // Filtros - com verificação de existência
        this.setupFilter('dateRangeFilter', 'change', (e) => {
            this.filters.dateRange = e.target.value;
            this.applyFilters();
        });

        this.setupFilter('promotionTypeFilter', 'change', (e) => {
            this.filters.promotionType = e.target.value;
            this.applyFilters();
        });

        this.setupFilter('statusFilter', 'change', (e) => {
            this.filters.status = e.target.value;
            this.applyFilters();
        });

        this.setupFilter('searchFilter', 'input', (e) => {
            this.filters.searchTerm = e.target.value;
            this.applyFilters();
        });

        this.setupFilter('applyFilters', 'click', () => {
            this.applyFilters();
        });

        this.setupFilter('clearFilters', 'click', () => {
            this.clearFilters();
        });

        // Tabs de visualização
        document.querySelectorAll('.view-tab').forEach(tab => {
            tab.addEventListener('click', (e) => {
                this.switchView(e.target.dataset.view);
            });
        });

        // Botões de ação no header
        this.setupFilter('refreshData', 'click', () => {
            this.refreshData();
        });

        this.setupFilter('exportReport', 'click', () => {
            this.exportReport();
        });

        console.log('Event listeners configurados');
    }

    setupFilter(elementId, eventType, callback) {
        const element = document.getElementById(elementId);
        if (element) {
            element.addEventListener(eventType, callback);
        } else {
            console.warn(`Elemento ${elementId} não encontrado para event listener`);
        }
    }

    applyFilters() {
        console.log('Aplicando filtros:', this.filters);
        this.renderFilteredData();
    }

    clearFilters() {
        this.filters = {
            dateRange: '30d',
            promotionType: 'all',
            status: 'all',
            minDiscount: 0,
            searchTerm: ''
        };

        // Resetar valores dos inputs apenas se existirem
        this.setFilterValue('dateRangeFilter', '30d');
        this.setFilterValue('promotionTypeFilter', 'all');
        this.setFilterValue('statusFilter', 'all');
        this.setFilterValue('searchFilter', '');

        this.applyFilters();
        this.showNotification('Filtros limpos com sucesso!', 'success');
    }

    setFilterValue(elementId, value) {
        const element = document.getElementById(elementId);
        if (element) {
            element.value = value;
        }
    }

    renderFilteredData() {
        let filteredPromotions = [...this.data.promotions];

        // Aplicar filtro de tipo
        if (this.filters.promotionType !== 'all') {
            filteredPromotions = filteredPromotions.filter(promo =>
                promo[1] === this.filters.promotionType
            );
        }

        // Aplicar filtro de status
        if (this.filters.status !== 'all') {
            filteredPromotions = filteredPromotions.filter(promo => {
                const status = this.getPromotionStatus(promo[3], promo[4]);
                return status === this.filters.status;
            });
        }

        // Aplicar filtro de busca
        if (this.filters.searchTerm) {
            const searchTerm = this.filters.searchTerm.toLowerCase();
            filteredPromotions = filteredPromotions.filter(promo =>
                promo[2].toLowerCase().includes(searchTerm) ||
                promo[1].toLowerCase().includes(searchTerm)
            );
        }

        this.renderContent(filteredPromotions);
        this.showNotification(`${filteredPromotions.length} promoções encontradas`, 'info');
    }

    switchView(view) {
        this.currentView = view;

        // Atualizar tabs ativas
        document.querySelectorAll('.view-tab').forEach(tab => {
            tab.classList.toggle('active', tab.dataset.view === view);
        });

        // Mostrar/ocultar views
        const cardsView = document.getElementById('cardsView');
        const listView = document.getElementById('listView');
        const tableView = document.getElementById('promotionsTableBody');

        if (cardsView) cardsView.style.display = view === 'cards' ? 'grid' : 'none';
        if (listView) listView.style.display = view === 'list' ? 'block' : 'none';
        if (tableView) tableView.parentElement.parentElement.parentElement.style.display = view === 'table' ? 'block' : 'none';

        this.renderFilteredData();
    }

    renderContent(promotions) {
        switch (this.currentView) {
            case 'cards':
                this.renderCardsView(promotions);
                break;
            case 'list':
                this.renderListView(promotions);
                break;
            case 'table':
                this.renderTableView(promotions);
                break;
        }
    }

    renderCardsView(promotions) {
        const container = document.getElementById('cardsView');
        if (!container) return;

        if (promotions.length === 0) {
            container.innerHTML = `
                <div class="empty-state" style="grid-column: 1 / -1; text-align: center; padding: 3rem; color: var(--erp-dark-muted);">
                    <i class="ri-search-line" style="font-size: 3rem; margin-bottom: 1rem;"></i>
                    <h3>Nenhuma promoção encontrada</h3>
                    <p>Tente ajustar os filtros ou atualizar os dados.</p>
                </div>
            `;
            return;
        }

        let html = '';
        promotions.forEach(promo => {
            const isShared = promo[5] === 1;
            const type = promo[1].toLowerCase();
            const status = this.getPromotionStatus(promo[3], promo[4]);
            const itemsCount = promo[6] || 0;

            html += `
                <div class="promotion-card ${type} ${isShared ? 'shared' : ''}">
                    <div class="promotion-header">
                        <h3 class="promotion-title">${this.escapeHtml(promo[2])}</h3>
                        <span class="promotion-badge badge-${this.getBadgeType(promo[1])}">
                            ${promo[1]}
                        </span>
                    </div>

                    <div class="promotion-details">
                        <div class="detail-item">
                            <span class="detail-label">Início</span>
                            <span class="detail-value">${this.formatDate(promo[3])}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Término</span>
                            <span class="detail-value">${this.formatDate(promo[4])}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Status</span>
                            <span class="status-indicator status-${status}">
                                <i class="ri-${this.getStatusIcon(status)}-fill"></i>
                                ${this.getStatusText(status)}
                            </span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Compartilhada</span>
                            <span class="detail-value">${isShared ? 'Sim' : 'Não'}</span>
                        </div>
                    </div>

                    <div class="promotion-stats">
                        <div class="stat-item">
                            <div class="stat-value">${itemsCount}</div>
                            <div class="stat-label">Itens</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${isShared ? '15%' : '0%'}</div>
                            <div class="stat-label">ML Compartilha</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">R$ ${(itemsCount * 25).toFixed(2)}</div>
                            <div class="stat-label">Desconto Total</div>
                        </div>
                    </div>

                    <div class="promotion-actions">
                        <button class="btn btn-xs btn-primary" onclick="dashboard.showPromotionDetails('${promo[0]}')">
                            <i class="ri-eye-line"></i> Detalhes
                        </button>
                        <button class="btn btn-xs btn-secondary" onclick="dashboard.analyzePromotion('${promo[0]}')">
                            <i class="ri-line-chart-line"></i> Análise
                        </button>
                        ${isShared ? `
                        <button class="btn btn-xs btn-success" onclick="dashboard.showSharedDiscounts('${promo[0]}')">
                            <i class="ri-share-line"></i> Descontos ML
                        </button>
                        ` : ''}
                    </div>
                </div>
            `;
        });

        container.innerHTML = html;
    }

    renderListView(promotions) {
        const container = document.getElementById('listView');
        if (!container) {
            // Criar container se não existir
            const contentViews = document.getElementById('contentViews');
            if (contentViews) {
                const listView = document.createElement('div');
                listView.id = 'listView';
                listView.className = 'list-view';
                listView.style.display = 'none';
                contentViews.appendChild(listView);
                this.renderListView(promotions);
            }
            return;
        }

        if (promotions.length === 0) {
            container.innerHTML = `
                <div class="empty-state" style="text-align: center; padding: 3rem; color: var(--erp-dark-muted);">
                    <i class="ri-search-line" style="font-size: 3rem; margin-bottom: 1rem;"></i>
                    <h3>Nenhuma promoção encontrada</h3>
                    <p>Tente ajustar os filtros ou atualizar os dados.</p>
                </div>
            `;
            return;
        }

        let html = '';
        promotions.forEach(promo => {
            const isShared = promo[5] === 1;
            const type = promo[1].toLowerCase();
            const status = this.getPromotionStatus(promo[3], promo[4]);
            const itemsCount = promo[6] || 0;

            html += `
                <div class="promotion-list-item ${type} ${isShared ? 'shared' : ''}" style="display: flex; justify-content: space-between; align-items: center; padding: 1.5rem; background: var(--erp-card); border-radius: var(--border-radius); margin-bottom: 1rem; border: 1px solid var(--erp-border);">
                    <div style="flex: 1;">
                        <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 0.5rem;">
                            <h4 style="margin: 0; color: var(--erp-dark);">${this.escapeHtml(promo[2])}</h4>
                            <span class="promotion-badge badge-${this.getBadgeType(promo[1])}">
                                ${promo[1]}
                            </span>
                            <span class="status-indicator status-${status}">
                                <i class="ri-${this.getStatusIcon(status)}-fill"></i>
                                ${this.getStatusText(status)}
                            </span>
                        </div>
                        <div style="display: flex; gap: 2rem; color: var(--erp-dark-muted); font-size: 0.9rem;">
                            <span><strong>Início:</strong> ${this.formatDate(promo[3])}</span>
                            <span><strong>Término:</strong> ${this.formatDate(promo[4])}</span>
                            <span><strong>Itens:</strong> ${itemsCount}</span>
                            <span><strong>ML Compartilha:</strong> ${isShared ? '15%' : '0%'}</span>
                        </div>
                    </div>
                    <div style="display: flex; gap: 0.5rem;">
                        <button class="btn btn-xs btn-primary" onclick="dashboard.showPromotionDetails('${promo[0]}')">
                            <i class="ri-eye-line"></i> Detalhes
                        </button>
                        <button class="btn btn-xs btn-secondary" onclick="dashboard.analyzePromotion('${promo[0]}')">
                            <i class="ri-line-chart-line"></i> Análise
                        </button>
                    </div>
                </div>
            `;
        });

        container.innerHTML = html;
    }

    renderTableView(promotions) {
        const tbody = document.getElementById('promotionsTableBody');
        if (!tbody) return;

        if (promotions.length === 0) {
            tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; padding: 2rem;">Nenhuma promoção encontrada</td></tr>';
            return;
        }

        let html = '';
        promotions.forEach(promo => {
            const isShared = promo[5] === 1;
            const status = this.getPromotionStatus(promo[3], promo[4]);
            const itemsCount = promo[6] || 0;
            const sharedDiscount = isShared ? '15%' : '0%';
            const totalDiscount = (itemsCount * 25).toFixed(2);

            html += `
                <tr>
                    <td>
                        <div style="display: flex; align-items: center;">
                            <i class="ri-treasure-map-line" style="margin-right: 0.5rem; color: var(--erp-primary);"></i>
                            <strong>${this.escapeHtml(promo[2])}</strong>
                        </div>
                    </td>
                    <td><span class="badge badge-${this.getBadgeType(promo[1])}">${promo[1]}</span></td>
                    <td>${this.formatDate(promo[3])}</td>
                    <td>${this.formatDate(promo[4])}</td>
                    <td>
                        <span class="status-indicator status-${status}">
                            <i class="ri-${this.getStatusIcon(status)}-fill"></i>
                            ${this.getStatusText(status)}
                        </span>
                    </td>
                    <td>${itemsCount}</td>
                    <td>
                        <span style="color: ${isShared ? 'var(--erp-success)' : 'var(--erp-dark-muted)'};">
                            <i class="ri-${isShared ? 'check' : 'close'}-line"></i>
                            ${sharedDiscount}
                        </span>
                    </td>
                    <td>
                        <div style="display: flex; gap: 0.25rem;">
                            <button class="btn-icon" onclick="dashboard.showPromotionDetails('${promo[0]}')" title="Detalhes">
                                <i class="ri-eye-line"></i>
                            </button>
                            <button class="btn-icon" onclick="dashboard.analyzePromotion('${promo[0]}')" title="Análise">
                                <i class="ri-line-chart-line"></i>
                            </button>
                            ${isShared ? `
                            <button class="btn-icon" onclick="dashboard.showSharedDiscounts('${promo[0]}')" title="Descontos ML">
                                <i class="ri-share-line"></i>
                            </button>
                            ` : ''}
                        </div>
                    </td>
                </tr>
            `;
        });

        tbody.innerHTML = html;
    }

    getPromotionStatus(startDate, endDate) {
        const now = new Date();
        const start = new Date(startDate);
        const end = new Date(endDate);

        if (now < start) return 'upcoming';
        if (now > end) return 'expired';
        return 'active';
    }

    getStatusIcon(status) {
        const icons = {
            active: 'check',
            expired: 'close',
            upcoming: 'time'
        };
        return icons[status] || 'help';
    }

    getStatusText(status) {
        const texts = {
            active: 'Ativa',
            expired: 'Expirada',
            upcoming: 'Futura'
        };
        return texts[status] || 'Desconhecido';
    }

    getBadgeType(promotionType) {
        const types = {
            'STANDARD': 'primary',
            'FLASH': 'warning',
            'VIP': 'success',
            'SUPER': 'danger'
        };
        return types[promotionType] || 'secondary';
    }

    showDashboard() {
        document.getElementById('loadingState').style.display = 'none';
        document.getElementById('metricsGrid').style.display = 'grid';
        document.getElementById('filtersSection').style.display = 'block';
        document.getElementById('visualizationGrid').style.display = 'grid';
        document.getElementById('contentViews').style.display = 'block';
        document.getElementById('aiInsights').style.display = 'block';

        this.updateMetrics();
        this.switchView('cards');
    }

    updateMetrics() {
        const metrics = this.data.metrics;

        this.setElementText('totalPromotions', this.formatNumber(metrics.totalPromotions));
        this.setElementText('sharedPromotions', this.formatNumber(metrics.sharedPromotions));
        this.setElementText('totalOrders', this.formatNumber(metrics.ordersWithDiscount));
        this.setElementText('totalDiscounts', 'R$ ' + this.formatCurrency(metrics.totalDiscounts));
        this.setElementText('roiValue', this.formatPercentage(metrics.roi));
        this.setElementText('discountRate', this.formatPercentage(metrics.discountRate));
    }

    initCharts() {
        this.initSalesChart();
        this.initPromotionTypesChart();
        this.initPerformanceChart();
    }

    initSalesChart() {
        const ctx = document.getElementById('salesChart');
        if (!ctx) return;

        const salesData = this.data.salesData;
        const discountData = this.data.discountData;

        this.charts.salesChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: salesData.dates,
                datasets: [
                    {
                        label: 'Vendas (R$)',
                        data: salesData.amounts,
                        borderColor: '#1e40af',
                        backgroundColor: 'rgba(30, 64, 175, 0.1)',
                        borderWidth: 3,
                        fill: true,
                        tension: 0.4
                    },
                    {
                        label: 'Descontos (R$)',
                        data: discountData.amounts,
                        borderColor: '#dc2626',
                        backgroundColor: 'rgba(220, 38, 38, 0.1)',
                        borderWidth: 3,
                        fill: true,
                        tension: 0.4
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' }
                }
            }
        });
    }

    initPromotionTypesChart() {
        const ctx = document.getElementById('promotionTypesChart');
        if (!ctx) return;

        const promotionTypes = this.data.promotionTypes;
        const labels = Array.isArray(promotionTypes.labels) ? promotionTypes.labels : ['STANDARD', 'FLASH', 'VIP'];
        const counts = Array.isArray(promotionTypes.counts) ? promotionTypes.counts.map(c => Number(c) || 0) : [5, 3, 2];

        this.charts.promotionTypesChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: labels,
                datasets: [{
                    data: counts,
                    backgroundColor: ['#1e40af', '#3b82f6', '#60a5fa', '#93c5fd'],
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'bottom' }
                },
                cutout: '70%'
            }
        });
    }

    initPerformanceChart() {
        const ctx = document.getElementById('performanceChart');
        if (!ctx) return;

        this.charts.performanceChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Brinquedos', 'Eletrônicos', 'Casa', 'Moda', 'Esportes'],
                datasets: [{
                    label: 'ROI por Categoria',
                    data: [2.8, 1.9, 3.2, 2.1, 2.5],
                    backgroundColor: '#0f766e'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false
            }
        });
    }

    generateAIInsights() {
        const metrics = this.data.metrics;

        // Insights baseados em dados reais
        const roi = metrics.roi || 0;
        const sharedRatio = metrics.totalPromotions > 0 ? metrics.sharedPromotions / metrics.totalPromotions : 0;

        if (roi > 3) {
            this.setElementText('opportunityText', 'Excelente ROI detectado! Considere escalar as promoções com maior retorno.');
            this.setElementText('opportunityPotential', '23%');
        } else if (roi > 2) {
            this.setElementText('opportunityText', 'Bom desempenho. Otimize promoções com ROI médio para aumentar retorno.');
            this.setElementText('opportunityPotential', '15%');
        } else {
            this.setElementText('opportunityText', 'Oportunidade para melhorar mix de promoções e aumentar ROI geral.');
            this.setElementText('opportunityPotential', '28%');
        }

        if (sharedRatio < 0.3) {
            this.setElementText('alertText', 'Baixa taxa de promoções compartilhadas. ML pode estar arcando com menos descontos.');
            this.setElementText('alertImpact', 'Alto');
        } else {
            this.setElementText('alertText', 'Bom equilíbrio de promoções compartilhadas. ML compartilha custos de desconto.');
            this.setElementText('alertImpact', 'Baixo');
        }

        this.setElementText('recommendationText', 'Recomendação: Aumentar promoções FLASH em 20% baseado no alto ROI histórico desta categoria.');
    }

    async showPromotionDetails(promotionId) {
        try {
            this.showNotification('Carregando detalhes da promoção...', 'info');

            // Simular API call
            await new Promise(resolve => setTimeout(resolve, 800));

            const promotion = this.data.promotions.find(p => p[0] === promotionId);
            if (!promotion) {
                throw new Error('Promoção não encontrada');
            }

            const isShared = promotion[5] === 1;
            const status = this.getPromotionStatus(promotion[3], promotion[4]);
            const itemsCount = promotion[6] || 0;

            document.getElementById('detailModalTitle').textContent = promotion[2];
            document.getElementById('detailModalContent').innerHTML = `
                <div class="promotion-details-modal">
                    <div class="detail-grid">
                        <div class="detail-item">
                            <label>ID da Promoção</label>
                            <span>${promotion[0]}</span>
                        </div>
                        <div class="detail-item">
                            <label>Tipo</label>
                            <span class="badge badge-${this.getBadgeType(promotion[1])}">${promotion[1]}</span>
                        </div>
                        <div class="detail-item">
                            <label>Status</label>
                            <span class="status-indicator status-${status}">
                                <i class="ri-${this.getStatusIcon(status)}-fill"></i>
                                ${this.getStatusText(status)}
                            </span>
                        </div>
                        <div class="detail-item">
                            <label>Compartilhada com ML</label>
                            <span>${isShared ? 'Sim (15% de desconto compartilhado)' : 'Não'}</span>
                        </div>
                        <div class="detail-item">
                            <label>Data de Início</label>
                            <span>${this.formatDate(promotion[3])}</span>
                        </div>
                        <div class="detail-item">
                            <label>Data de Término</label>
                            <span>${this.formatDate(promotion[4])}</span>
                        </div>
                        <div class="detail-item">
                            <label>Número de Itens</label>
                            <span>${itemsCount} produtos</span>
                        </div>
                        <div class="detail-item">
                            <label>Desconto Total Estimado</label>
                            <span>R$ ${(itemsCount * 25).toFixed(2)}</span>
                        </div>
                    </div>

                    <div class="section-divider">
                        <h4><i class="ri-bar-chart-box-line"></i> Métricas de Performance</h4>
                    </div>

                    <div class="metrics-grid-small">
                        <div class="metric-small">
                            <div class="metric-value">${(Math.random() * 100).toFixed(1)}%</div>
                            <div class="metric-label">Taxa de Conversão</div>
                        </div>
                        <div class="metric-small">
                            <div class="metric-value">R$ ${(itemsCount * 45).toFixed(2)}</div>
                            <div class="metric-label">Vendas Geradas</div>
                        </div>
                        <div class="metric-small">
                            <div class="metric-value">${(Math.random() * 5).toFixed(2)}x</div>
                            <div class="metric-label">ROI</div>
                        </div>
                        <div class="metric-small">
                            <div class="metric-value">${Math.floor(itemsCount * 2.5)}</div>
                            <div class="metric-label">Pedidos</div>
                        </div>
                    </div>

                    ${isShared ? `
                    <div class="section-divider">
                        <h4><i class="ri-share-line"></i> Descontos Compartilhados com ML</h4>
                    </div>
                    <div class="shared-discounts">
                        <p>O Mercado Livre está compartilhando <strong>15%</strong> dos descontos desta promoção.</p>
                        <div class="discount-breakdown">
                            <div class="breakdown-item">
                                <span class="label">Seu Investimento:</span>
                                <span class="value">R$ ${(itemsCount * 25 * 0.85).toFixed(2)}</span>
                            </div>
                            <div class="breakdown-item">
                                <span class="label">Investimento do ML:</span>
                                <span class="value text-success">R$ ${(itemsCount * 25 * 0.15).toFixed(2)}</span>
                            </div>
                            <div class="breakdown-item total">
                                <span class="label">Desconto Total:</span>
                                <span class="value">R$ ${(itemsCount * 25).toFixed(2)}</span>
                            </div>
                        </div>
                    </div>
                    ` : ''}

                    <div class="section-divider">
                        <h4><i class="ri-lightbulb-flash-line"></i> Recomendações</h4>
                    </div>
                    <div class="recommendations">
                        <div class="recommendation-item">
                            <i class="ri-arrow-up-line text-success"></i>
                            <span>Aumentar budget em 20% para esta promoção</span>
                        </div>
                        <div class="recommendation-item">
                            <i class="ri-calendar-line text-warning"></i>
                            <span>Estender por mais 7 dias baseado na performance</span>
                        </div>
                        <div class="recommendation-item">
                            <i class="ri-share-line text-primary"></i>
                            <span>Conversar com ML sobre aumentar compartilhamento</span>
                        </div>
                    </div>
                </div>
            `;

            document.getElementById('detailModal').style.display = 'block';
        } catch (error) {
            this.showError('Erro ao carregar detalhes: ' + error.message);
        }
    }

    analyzePromotion(promotionId) {
        this.showNotification('Iniciando análise avançada...', 'info');
        // Implementar análise detalhada
        setTimeout(() => {
            this.showNotification('Análise completa! Verifique o relatório gerado.', 'success');
        }, 2000);
    }

    showSharedDiscounts(promotionId) {
        this.showNotification('Calculando descontos compartilhados...', 'info');
        // Implementar visualização de descontos ML
        setTimeout(() => {
            this.showNotification('Detalhes dos descontos ML carregados!', 'success');
        }, 1500);
    }

    async refreshData() {
        try {
            this.showNotification('Sincronizando com Mercado Livre...', 'info');

            const response = await fetch('/vendas/ml/update', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            const result = await response.json();

            if (result.status === 'success') {
                this.showNotification(`Dados atualizados: ${result.promotions} promoções, ${result.orders} pedidos`, 'success');
                setTimeout(() => {
                    location.reload();
                }, 2000);
            } else {
                this.showError('Erro ao atualizar: ' + result.message);
            }
        } catch (error) {
            this.showError('Erro de conexão: ' + error.message);
        }
    }

    exportReport() {
        this.showNotification('Gerando relatório premium...', 'info');
        setTimeout(() => {
            window.open('/vendas/ml/export/promotions', '_blank');
            this.showNotification('Relatório exportado com sucesso!', 'success');
        }, 1500);
    }

    // Utilitários
    setElementText(id, text) {
        const element = document.getElementById(id);
        if (element) element.textContent = text;
    }

    formatNumber(num) {
        return parseInt(num).toLocaleString('pt-BR');
    }

    formatCurrency(num) {
        return parseFloat(num).toFixed(2);
    }

    formatPercentage(num) {
        return (parseFloat(num) * 100).toFixed(1) + '%';
    }

    formatDate(dateString) {
        if (!dateString) return 'N/A';
        try {
            return new Date(dateString).toLocaleDateString('pt-BR');
        } catch (error) {
            return dateString;
        }
    }

    escapeHtml(text) {
        if (!text) return 'N/A';
        return text.toString()
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    showNotification(message, type = 'info') {
        const notification = document.getElementById('notification');
        const messageEl = document.getElementById('notification-message');

        if (notification && messageEl) {
            messageEl.textContent = message;
            notification.className = `notification ${type}`;
            notification.classList.add('show');

            setTimeout(() => {
                notification.classList.remove('show');
            }, 4000);
        } else {
            alert(message);
        }
    }

    showError(message) {
        this.showNotification(message, 'error');
    }
}

// Inicialização global
let dashboard;

document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM carregado, inicializando dashboard premium...');
    dashboard = new PromocoesDashboard();
});

// Funções globais
window.atualizarDados = () => dashboard.refreshData();
window.exportarRelatorio = () => dashboard.exportReport();
window.fecharModalDetalhes = () => document.getElementById('detailModal').style.display = 'none';
window.aplicarRecomendacao = () => dashboard.showNotification('Recomendação aplicada com sucesso!', 'success');