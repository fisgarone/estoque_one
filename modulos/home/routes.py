# /modulos/home/routes.py (VERSÃO COM ARQUITETURA CORRETA)

from flask import Blueprint, render_template, url_for, jsonify

# A MUDANÇA CRUCIAL ESTÁ AQUI:
# 'template_folder' agora aponta para a pasta global de templates.
# O Flask automaticamente procurará por 'home/dashboard_home.html' dentro dela.
home_bp = Blueprint(
    'home',
    __name__,
    template_folder='../templates',  # Aponta para a raiz de templates
    url_prefix='/'
)

@home_bp.route('/')
def dashboard_home():
    """Renderiza o dashboard principal do sistema (ERP)."""
    # O Flask buscará por 'home/dashboard_home.html' dentro de 'templates/'
    return render_template('home/dashboard_home.html')

@home_bp.route('/api/menu')
def get_menu():
    """Fornece a estrutura do menu para a aplicação inteira."""
    # Aceita endpoint único (str) ou lista de endpoints
    def safe_url(candidates, fallback='#', **kwargs):
        eps = candidates if isinstance(candidates, (list, tuple)) else [candidates]
        for ep in eps:
            try:
                return url_for(ep, **kwargs)
            except Exception:
                continue
        return fallback

    menu_data = [
        {
            "titulo": "Principal",
            "itens": [
                {"nome": "Dashboard", "icone": "ri-home-4-line", "url": url_for('home.dashboard_home')},
            ]
        },
        {
            "titulo": "Gerenciamento",
            "itens": [
                {
                    "nome": "Estoque",
                    "icone": "ri-database-2-line",
                    "submenu": [
                        {"nome": "Dashboard Estoque", "url": url_for('estoque.dashboard')},
                        {"nome": "Análise Curva ABC", "url": url_for('estoque.curva_abc')},
                        {"nome": "Produtos", "url": url_for('estoque.listar_produtos')},
                        {"nome": "Movimentações", "url": url_for('estoque.listar_movimentacoes')},
                        {"nome": "Inventário", "url": url_for('estoque.inventarios')},
                        {"nome": "Processar Entradas", "url": url_for('estoque.processar_entradas_lista')}
                    ]
                },
                {
                    "nome": "Compras (NF-e)",
                    "icone": "ri-file-text-line",
                    "submenu": [
                        {
                            "nome": "Painel NF-e",
                            "icone": "ri-file-list-3-line",
                            "url": safe_url(['nfe.painel_nfe'], fallback='/nfe')
                        },
                        {
                            "nome": "Importador de NF (Novo)",
                            "icone": "ri-file-add-line",
                            "url": safe_url(['nfe_importador.tela'], fallback='/nfe/importador/')
                        }
                    ]
                },

            ]
        },
        # ===== RENOMEADO: Vendas -> Marketplaces =====
        {
            "titulo": "Marketplaces",
            "icone": "ri-shopping-cart-line",
            "itens": [
                {
                    "nome": "Dashboard de Vendas",
                    "icone": "ri-dashboard-line",
                    "url": safe_url(
                        ['dashboard_vendas.dashboard', 'dashboard_vendas.index', 'vendas.dashboard', 'vendas.index'],
                        fallback='/vendas'
                    )
                },
                {
                    "nome": "Mercado Livre",
                    "icone": "ri-store-2-line",
                    "submenu": [
                        {
                            "nome": "Dashboard (ML)",
                            "icone": "ri-dashboard-2-line",
                            "url": safe_url(
                                # preferir novo blueprint; manter legado como candidato
                                ["ml_vendas.dashboard", "ml.dashboard"],
                                fallback="/vendas/ml/"
                            )
                        },
                        {
                            "nome": "Vendas do Dia",
                            "icone": "ri-time-line",
                            "url": f"{safe_url(['ml_vendas.dashboard', 'ml.dashboard'], fallback='/vendas/ml/')}#daily"
                        },
                        {
                            "nome": "Curva ABC",
                            "icone": "ri-pie-chart-2-line",
                            "url": safe_url(
                                ["ml_vendas.abc_view", "ml.abc_view"],
                                fallback="/vendas/ml/abc"
                            )
                        },
                        {
                            "nome": "Dashboard de Anúncios",
                            "icone": "ri-dashboard-line",
                            "url": safe_url(
                                ["ml_anuncios.dashboard", "ml.dashboard_anuncios"],
                                fallback="/vendas/ml/anuncios/"
                            )
                        },
                        {
                            "nome": "Gerenciar Anúncios",
                            "icone": "ri-list-check-2",
                            "url": safe_url(
                                ["ml_anuncios.listar_anuncios", "ml.listar_anuncios"],
                                fallback="/vendas/ml/anuncios/listar"
                            )
                        },
                        {
                            "nome": "Novo Anúncio",
                            "icone": "ri-add-circle-line",
                            "url": safe_url(
                                ["ml_anuncios.novo_anuncio", "ml.novo_anuncio"],
                                fallback="/vendas/ml/anuncios/novo"
                            )
                        },
                        {
                            "nome": "Relatórios",
                            "icone": "ri-bar-chart-line",
                            "url": safe_url(
                                ["ml_anuncios.relatorios", "ml.relatorios"],
                                fallback="/vendas/ml/anuncios/relatorios"
                            )
                        },
                        {
                            "nome": "Configurações",
                            "icone": "ri-settings-line",
                            "url": safe_url(
                                ["ml_anuncios.configuracoes", "ml.configuracoes"],
                                fallback="/vendas/ml/anuncios/configuracoes"
                            )
                        }
                    ]
                },
                {
                    "nome": "Shopee",
                    "icone": "ri-shopping-bag-3-line",
                    "submenu": [
                        {
                            "nome": "Dashboard",
                            "icone": "ri-dashboard-2-line",
                            "url": safe_url(
                                ['shopee.analytics_page', 'shopee.dashboard', 'shopee.index'],
                                fallback='/modulos/vendas/shopee/analytics'
                            )
                        },
                        {
                            "nome": "Vendas do Dia",
                            "icone": "ri-time-line",
                            "url": safe_url(
                                ['shopee.diario_page'],
                                fallback='/modulos/vendas/shopee/diario'
                            )
                        },
                        {
                            "nome": "Curva ABC",
                            "icone": "ri-pie-chart-2-line",
                            "url": safe_url(
                                ['shopee.curva_abc_page'],
                                fallback='/modulos/vendas/shopee/curva-abc'
                            )
                        }
                    ]
                },
                {
                    "nome": "Anúncios Shopee",
                    "icone": "ri-megaphone-line",
                    "url": safe_url(
                        ['ads_shopee.ads_dashboard', 'ads.dashboard', 'ads_shopee.dashboard'],
                        fallback='/ads/shopee'
                    )
                }
            ]
        },
        {
            "titulo": "Catálogos",
            "icone": "ri-price-tag-3-line",
            "itens": [
                {
                    "nome": "Catálogos",
                    "icone": "ri-price-tag-3-line",
                    "url": safe_url(
                        ['parametros_bp.tela_parametros', 'parametros.tela_parametros'],
                        fallback='/parametros'
                    )
                }
            ]
        }
    ]
    return jsonify(menu_data)
