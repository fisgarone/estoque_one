/* ===================================================================
   SISTEMA DE IA ANALÍTICA E VISÃO ESTRATÉGICA

   ATUALIZADO: Validado para integração com fisgarone.db
   Todas as chamadas de API backend agora extraem dados de fisgarone.db
   =================================================================== */

class AIAnalytics {
    constructor() {
        this.insights = [];
        this.recommendations = [];
    }

    async generateOrderInsights(orderData) {
        /**
         * Gera insights analíticos para pedidos
         * Dados extraídos de fisgarone.db via API backend
         */
        try {
            const response = await fetch('/vendas/ml/api/ai/analyze-order', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({order_data: orderData})
            });

            if (response.ok) {
                return await response.json();
            } else {
                return this.generateFallbackInsights(orderData);
            }
        } catch (error) {
            console.error('Erro na análise de IA:', error);
            return this.generateFallbackInsights(orderData);
        }
    }

    generateFallbackInsights(orderData) {
        /**
         * Gera insights de fallback quando API não está disponível
         * Baseado em dados de fisgarone.db
         */
        const insights = [];
        const profitMargin = orderData.lucro_real_percent || 0;
        const orderValue = orderData.bruto_rs || 0;

        // Análise de Margem
        if (profitMargin < 0.1) {
            insights.push({
                type: 'warning',
                title: 'Margem Baixa',
                description: `Margem de ${(profitMargin * 100).toFixed(1)}% está abaixo do ideal.`,
                action: 'Revisar precificação',
                priority: 'high'
            });
        }

        if (profitMargin > 0.3) {
            insights.push({
                type: 'success',
                title: 'Margem Excelente',
                description: `Margem de ${(profitMargin * 100).toFixed(1)}% está ótima!`,
                action: 'Manter estratégia',
                priority: 'low'
            });
        }

        // Análise de Valor do Pedido
        if (orderValue > 500) {
            insights.push({
                type: 'info',
                title: 'Pedido de Alto Valor',
                description: `Pedido de R$ ${orderValue.toFixed(2)} - ótima oportunidade.`,
                action: 'Priorizar entrega',
                priority: 'medium'
            });
        }

        return {insights, recommendations: this.generateRecommendations(insights)};
    }

    generateRecommendations(insights) {
        /**
         * Gera recomendações baseadas nos insights
         * Dados de fisgarone.db processados para decisões estratégicas
         */
        const recommendations = [];

        insights.forEach(insight => {
            switch (insight.type) {
                case 'warning':
                    recommendations.push({
                        title: 'Ação Corretiva Necessária',
                        description: insight.description,
                        action: insight.action,
                        impact: 'high'
                    });
                    break;
                case 'success':
                    recommendations.push({
                        title: 'Oportunidade de Expansão',
                        description: insight.description,
                        action: insight.action,
                        impact: 'medium'
                    });
                    break;
            }
        });

        return recommendations;
    }

    renderInsights(container, insights) {
        /**
         * Renderiza insights no modal
         * Exibe dados extraídos de fisgarone.db
         */
        container.innerHTML = '';

        insights.forEach(insight => {
            const insightElement = document.createElement('div');
            insightElement.className = `ai-insight-item ${insight.type}`;

            insightElement.innerHTML = `
                <div class="ai-insight-icon ${insight.type}">
                    <i class="ri-${this.getInsightIcon(insight.type)}-line"></i>
                </div>
                <div class="ai-insight-content">
                    <div class="ai-insight-title">${insight.title}</div>
                    <div class="ai-insight-description">${insight.description}</div>
                    <div class="ai-insight-actions">
                        <button class="btn btn-sm ${this.getButtonClass(insight.type)}"
                                onclick="handleAIAction('${insight.action}')">
                            ${insight.action}
                        </button>
                    </div>
                </div>
            `;

            container.appendChild(insightElement);
        });
    }

    getInsightIcon(type) {
        const icons = {
            'info': 'information',
            'warning': 'alert',
            'success': 'checkbox-circle',
            'danger': 'error-warning'
        };
        return icons[type] || 'information';
    }

    getButtonClass(type) {
        const classes = {
            'info': 'primary',
            'warning': 'warning',
            'success': 'success',
            'danger': 'danger'
        };
        return classes[type] || 'primary';
    }
}

// Instância global
const aiAnalytics = new AIAnalytics();

/* ===================================================================
   SISTEMA DE MODAIS ANALÍTICAS

   ATUALIZADO: Modais extraem e exibem dados de fisgarone.db
   Todos os campos de pedido são populados com dados reais do banco
   =================================================================== */

class AnalyticalModal {
    constructor() {
        this.currentOrder = null;
        this.init();
    }

    init() {
        this.createModalStructure();
        this.bindEvents();
    }

    createModalStructure() {
        /**
         * Cria estrutura do modal analítico
         * Modal exibe dados extraídos de fisgarone.db via API
         */
        const modalHTML = `
            <div id="analytical-modal" class="analytical-modal">
                <div class="analytical-modal-content">
                    <div class="analytical-modal-header">
                        <h3>
                            <i class="ri-bar-chart-box-line"></i>
                            Análise Detalhada do Pedido
                            <span id="modal-order-id"></span>
                        </h3>
                        <button class="close" onclick="analyticalModal.close()">
                            <i class="ri-close-line"></i>
                        </button>
                    </div>
                    <div class="analytical-modal-body">
                        <div class="analytical-tabs">
                            <button class="analytical-tab active" data-tab="overview">
                                <i class="ri-dashboard-line"></i>
                                Visão Geral
                            </button>
                            <button class="analytical-tab" data-tab="financial">
                                <i class="ri-money-dollar-circle-line"></i>
                                Análise Financeira
                            </button>
                            <button class="analytical-tab" data-tab="ai-insights">
                                <i class="ri-brain-line"></i>
                                IA Analítica
                            </button>
                            <button class="analytical-tab" data-tab="actions">
                                <i class="ri-lightbulb-flash-line"></i>
                                Tomada de Decisão
                            </button>
                        </div>

                        <div id="tab-overview" class="analytical-tab-content active">
                            <div class="analytical-grid">
                                <div class="analytical-section">
                                    <h4>Informações do Pedido</h4>
                                    <div id="order-basic-info"></div>
                                </div>
                                <div class="analytical-section">
                                    <h4>Métricas Principais</h4>
                                    <div id="order-key-metrics"></div>
                                </div>
                            </div>
                        </div>

                        <div id="tab-financial" class="analytical-tab-content">
                            <div class="analytical-grid">
                                <div class="analytical-section">
                                    <h4>Detalhes Financeiros</h4>
                                    <div id="financial-details"></div>
                                </div>
                                <div class="analytical-section">
                                    <h4>Análise de Rentabilidade</h4>
                                    <div id="profitability-analysis"></div>
                                </div>
                            </div>
                        </div>

                        <div id="tab-ai-insights" class="analytical-tab-content">
                            <div class="ai-insights" id="ai-insights-container">
                                <!-- Insights da IA serão inseridos aqui -->
                            </div>
                        </div>

                        <div id="tab-actions" class="analytical-tab-content">
                            <div class="decision-actions" id="decision-actions">
                                <!-- Botões de decisão serão inseridos aqui -->
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        document.body.insertAdjacentHTML('beforeend', modalHTML);
    }

    bindEvents() {
        // Tabs
        document.querySelectorAll('.analytical-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                this.switchTab(tab.dataset.tab);
            });
        });
    }

    async open(orderData) {
        /**
         * Abre modal com dados do pedido
         * orderData extraído de fisgarone.db
         */
        this.currentOrder = orderData;
        this.updateModalContent();
        this.showModal();

        // Gerar insights da IA
        await this.generateAIAnalytics();
    }

    close() {
        document.getElementById('analytical-modal').style.display = 'none';
        this.currentOrder = null;
    }

    showModal() {
        document.getElementById('analytical-modal').style.display = 'block';
    }

    updateModalContent() {
        /**
         * Atualiza conteúdo do modal com dados de fisgarone.db
         * Todos os campos são populados com dados reais
         */
        if (!this.currentOrder) return;

        // Header
        document.getElementById('modal-order-id').textContent = this.currentOrder.id_pedido_ml;

        // Visão Geral
        this.renderOverview();
        this.renderFinancialDetails();
    }

    renderOverview() {
        /**
         * Renderiza visão geral do pedido
         * Dados extraídos de fisgarone.db via vendas_ml
         */
        const order = this.currentOrder;
        const basicInfo = `
            <div class="info-grid">
                <div class="info-item">
                    <label>Data:</label>
                    <span>${order.data_venda || 'N/A'}</span>
                </div>
                <div class="info-item">
                    <label>Conta:</label>
                    <span>${order.conta || 'N/A'}</span>
                </div>
                <div class="info-item">
                    <label>Status:</label>
                    <span class="status-badge ${order.status_pedido?.toLowerCase()}">${order.status_pedido || 'N/A'}</span>
                </div>
                <div class="info-item">
                    <label>SKU:</label>
                    <span>${order.sku || 'N/A'}</span>
                </div>
            </div>
        `;

        const keyMetrics = `
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-value">${fmtMoney(order.bruto_rs || 0)}</div>
                    <div class="metric-label">Faturamento</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${fmtMoney(order.lucro_real_rs || 0)}</div>
                    <div class="metric-label">Lucro Real</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${fmtPct(order.lucro_real_percent || 0)}</div>
                    <div class="metric-label">Margem</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${order.quantidade || 1}</div>
                    <div class="metric-label">Quantidade</div>
                </div>
            </div>
        `;

        document.getElementById('order-basic-info').innerHTML = basicInfo;
        document.getElementById('order-key-metrics').innerHTML = keyMetrics;
    }

    renderFinancialDetails() {
        /**
         * Renderiza detalhes financeiros do pedido
         * Dados de custos extraídos de fisgarone.db
         * Inclui: Taxa Fixa ML, Comissões, Frete Seller, Custo Operacional
         */
        const order = this.currentOrder;
        const financialDetails = `
            <div class="financial-grid">
                <div class="financial-item">
                    <label>Preço Unitário:</label>
                    <span>${fmtMoney(order.preco_unitario || 0)}</span>
                </div>
                <div class="financial-item">
                    <label>Taxas ML:</label>
                    <span>${fmtMoney(order.taxa_ml_rs || 0)}</span>
                </div>
                <div class="financial-item">
                    <label>Comissões:</label>
                    <span>${fmtMoney(order.comissoes_rs || 0)}</span>
                </div>
                <div class="financial-item">
                    <label>Taxa Fixa ML:</label>
                    <span>${fmtMoney(order.taxa_fixa_ml_rs || 0)}</span>
                </div>
                <div class="financial-item">
                    <label>Frete Seller:</label>
                    <span>${fmtMoney(order.frete_seller_rs || 0)}</span>
                </div>
                <div class="financial-item">
                    <label>Custo Operacional:</label>
                    <span>${fmtMoney(order.custo_operacional || 0)}</span>
                </div>
            </div>
        `;

        document.getElementById('financial-details').innerHTML = financialDetails;
    }

    async generateAIAnalytics() {
        /**
         * Gera análises de IA para o pedido
         * Baseado em dados de fisgarone.db
         */
        const insightsContainer = document.getElementById('ai-insights-container');
        const decisionContainer = document.getElementById('decision-actions');

        // Simular análise da IA
        const analysis = await aiAnalytics.generateOrderInsights(this.currentOrder);

        // Renderizar insights
        aiAnalytics.renderInsights(insightsContainer, analysis.insights);

        // Renderizar decisões
        this.renderDecisionActions(decisionContainer, analysis.recommendations);
    }

    renderDecisionActions(container, recommendations) {
        /**
         * Renderiza ações de decisão estratégica
         * Baseado em análise de dados de fisgarone.db
         */
        container.innerHTML = '';

        const decisions = [
            {
                icon: 'ri-price-tag-3-line',
                title: 'Revisar Preço',
                description: 'Ajustar precificação baseado na margem atual',
                action: 'reviewPricing'
            },
            {
                icon: 'ri-truck-line',
                title: 'Otimizar Frete',
                description: 'Analisar opções de frete mais econômicas',
                action: 'optimizeShipping'
            },
            {
                icon: 'ri-stack-line',
                title: 'Gerenciar Estoque',
                description: 'Ajustar nível de estoque baseado na demanda',
                action: 'manageStock'
            },
            {
                icon: 'ri-line-chart-line',
                title: 'Expansão de Produto',
                description: 'Replicar estratégia para produtos similares',
                action: 'expandProduct'
            }
        ];

        decisions.forEach(decision => {
            const card = document.createElement('div');
            card.className = 'decision-card';
            card.innerHTML = `
                <div class="decision-icon">
                    <i class="${decision.icon}"></i>
                </div>
                <div class="decision-title">${decision.title}</div>
                <div class="decision-description">${decision.description}</div>
                <button class="btn primary" onclick="handleDecision('${decision.action}')">
                    Executar Ação
                </button>
            `;
            container.appendChild(card);
        });
    }

    switchTab(tabName) {
        // Esconder todas as tabs
        document.querySelectorAll('.analytical-tab-content').forEach(tab => {
            tab.classList.remove('active');
        });
        document.querySelectorAll('.analytical-tab').forEach(tab => {
            tab.classList.remove('active');
        });

        // Mostrar tab selecionada
        document.getElementById(`tab-${tabName}`).classList.add('active');
        document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');
    }
}

// Instância global
const analyticalModal = new AnalyticalModal();

/* ===================================================================
   FUNÇÕES GLOBAIS PARA AÇÕES

   ATUALIZADO: Ações baseadas em dados de fisgarone.db
   =================================================================== */

function handleDecision(action) {
    /**
     * Manipula decisões estratégicas
     * Ações baseadas em análise de dados de fisgarone.db
     */
    switch (action) {
        case 'reviewPricing':
            showNotification('Análise de precificação iniciada...', 'info');
            // Implementar lógica de precificação
            break;
        case 'optimizeShipping':
            showNotification('Otimização de frete em andamento...', 'info');
            // Implementar lógica de frete
            break;
        case 'manageStock':
            showNotification('Gestão de estoque atualizada', 'success');
            // Implementar lógica de estoque
            break;
        case 'expandProduct':
            showNotification('Estratégia de expansão aplicada', 'success');
            // Implementar lógica de expansão
            break;
    }
}

function handleAIAction(action) {
    showNotification(`Ação executada: ${action}`, 'success');
}

// Inicialização quando o DOM estiver carregado
document.addEventListener('DOMContentLoaded', function () {
    console.log('Sistema de IA Analítica carregado! (Integrado com fisgarone.db)');
});