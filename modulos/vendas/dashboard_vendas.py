from flask import Blueprint, render_template, jsonify, request, current_app, g
import sqlite3
from datetime import datetime, timedelta

# Criação do Blueprint
dashboard_vendas_bp = Blueprint('dashboard_vendas', __name__,
                                template_folder='../../templates/vendas',
                                static_folder='../../static')

def get_db_connection():
    """Função para obter conexão com o banco de dados"""
    if 'db' not in g:
        g.db = sqlite3.connect(current_app.config['DATABASE'])
        g.db.row_factory = sqlite3.Row
    return g.db

@dashboard_vendas_bp.route('/dashboard')
def dashboard():
    """Rota principal do dashboard de vendas"""
    return render_template('dashboard_main.html')


@dashboard_vendas_bp.route('/api/kpis')
def api_kpis():
    """API para KPIs com cálculo de variação e preparação para produção"""
    try:
        db = get_db_connection()

        # 1. Definir períodos
        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=30)
        data_inicio_anterior = data_inicio - timedelta(days=30)

        # 2. Função para consultar dados
        def get_kpis(tabela, data_inicio, data_fim, plataforma):
            if plataforma == 'ml':
                return db.execute(f"""
                    SELECT 
                        COUNT(DISTINCT [ID Pedido]) as pedidos,
                        SUM([Preco Unitario] * Quantidade) as faturamento,
                        SUM(Quantidade) as unidades,
                        SUM([Lucro Real]) as lucro_real
                    FROM {tabela} 
                    WHERE date(
                        substr([Data da Venda], 7, 4) || '-' || 
                        substr([Data da Venda], 4, 2) || '-' || 
                        substr([Data da Venda], 1, 2)
                    ) BETWEEN date(?) AND date(?)
                    AND [Situacao] NOT LIKE '%Cancelado%'
                """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchone()
            else:
                return db.execute(f"""
                    SELECT 
                        COUNT(DISTINCT PEDIDO_ID) as pedidos,
                        SUM(PRECO_UNITARIO * QTD_COMPRADA) as faturamento,
                        SUM(QTD_COMPRADA) as unidades,
                        SUM(LUCRO_REAL) as lucro_real
                    FROM {tabela} 
                    WHERE date(DATA) BETWEEN ? AND ?
                    AND STATUS_PEDIDO = 'COMPLETED'
                """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchone()

        # 3. Consultar dados atuais e anteriores
        ml_atual = get_kpis('vendas_ml', data_inicio, data_fim, 'ml')
        ml_anterior = get_kpis('vendas_ml', data_inicio_anterior, data_inicio, 'ml')
        shopee_atual = get_kpis('vendas_shopee', data_inicio, data_fim, 'shopee')
        shopee_anterior = get_kpis('vendas_shopee', data_inicio_anterior, data_inicio, 'shopee')

        # 4. Consolidação com tratamento de NULL
        def safe_sum(current, previous, field):
            current_val = current[field] if current and current[field] is not None else 0
            previous_val = previous[field] if previous and previous[field] is not None else 0
            return current_val, previous_val

        # Faturamento
        fat_atual_ml, fat_anterior_ml = safe_sum(ml_atual, ml_anterior, 'faturamento')
        fat_atual_sp, fat_anterior_sp = safe_sum(shopee_atual, shopee_anterior, 'faturamento')
        faturamento_atual = fat_atual_ml + fat_atual_sp
        faturamento_anterior = fat_anterior_ml + fat_anterior_sp

        # Pedidos
        ped_atual_ml, ped_anterior_ml = safe_sum(ml_atual, ml_anterior, 'pedidos')
        ped_atual_sp, ped_anterior_sp = safe_sum(shopee_atual, shopee_anterior, 'pedidos')
        pedidos_atual = ped_atual_ml + ped_atual_sp
        pedidos_anterior = ped_anterior_ml + ped_anterior_sp

        # Unidades
        und_atual_ml, und_anterior_ml = safe_sum(ml_atual, ml_anterior, 'unidades')
        und_atual_sp, und_anterior_sp = safe_sum(shopee_atual, shopee_anterior, 'unidades')
        unidades_atual = und_atual_ml + und_atual_sp
        unidades_anterior = und_anterior_ml + und_anterior_sp

        # Lucro
        luc_atual_ml, luc_anterior_ml = safe_sum(ml_atual, ml_anterior, 'lucro_real')
        luc_atual_sp, luc_anterior_sp = safe_sum(shopee_atual, shopee_anterior, 'lucro_real')
        lucro_atual = luc_atual_ml + luc_atual_sp
        lucro_anterior = luc_anterior_ml + luc_anterior_sp

        # 5. Cálculo de variações
        def calcular_variacao(atual, anterior):
            if anterior == 0:
                return 100 if atual > 0 else 0
            return ((atual - anterior) / anterior) * 100

        # 6. Cálculo de ticket médio
        ticket_medio_atual = faturamento_atual / pedidos_atual if pedidos_atual > 0 else 0
        ticket_medio_anterior = faturamento_anterior / pedidos_anterior if pedidos_anterior > 0 else 0

        # 7. Margem de lucro
        margem_lucro = (lucro_atual / faturamento_atual * 100) if faturamento_atual > 0 else 0

        # 8. Montagem do response
        response = {
            'faturamento': {
                'valor': round(faturamento_atual, 2),
                'variacao': round(calcular_variacao(faturamento_atual, faturamento_anterior), 2)
            },
            'pedidos': {
                'valor': pedidos_atual,
                'variacao': round(calcular_variacao(pedidos_atual, pedidos_anterior), 2)
            },
            'unidades': {
                'valor': unidades_atual,
                'variacao': round(calcular_variacao(unidades_atual, unidades_anterior), 2)
            },
            'lucro_real': {
                'valor': round(lucro_atual, 2),
                'variacao': round(calcular_variacao(lucro_atual, lucro_anterior), 2)
            },
            'ticket_medio': {
                'valor': round(ticket_medio_atual, 2),
                'variacao': round(calcular_variacao(ticket_medio_atual, ticket_medio_anterior), 2)
            },
            'margem_lucro': {
                'valor': round(margem_lucro, 2),
                'variacao': 0
            }
        }

        return jsonify(response)

    except Exception as e:
        current_app.logger.error(f"Erro em /api/kpis: {str(e)}")
        return jsonify({'error': 'Erro interno ao processar KPIs'}), 500


@dashboard_vendas_bp.route('/api/vendas-diarias')
def api_vendas_diarias():
    """API para obter dados de vendas diárias corrigida"""
    try:
        db = get_db_connection()
        dias = request.args.get('dias', default=30, type=int)

        # Período fixo para compatibilidade com dados de teste (remova para produção)
        data_fim = datetime.strptime("30/04/2025", "%d/%m/%Y")
        data_inicio = data_fim - timedelta(days=dias)

        print(f"\n📅 Consultando vendas diárias de {data_inicio.date()} a {data_fim.date()}")

        # Vendas Mercado Livre
        ml_diarias = db.execute("""
            SELECT 
                [Data da Venda] as data,
                SUM([Preco Unitario] * Quantidade) as faturamento,
                COUNT(DISTINCT [ID Pedido]) as pedidos
            FROM vendas_ml 
            WHERE date(
                substr([Data da Venda], 7, 4) || '-' || 
                substr([Data da Venda], 4, 2) || '-' || 
                substr([Data da Venda], 1, 2)
            ) BETWEEN date(?) AND date(?)
            AND [Situacao] NOT LIKE '%Cancelado%'
            GROUP BY [Data da Venda]
            ORDER BY [Data da Venda]
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchall()

        # Vendas Shopee
        shopee_diarias = db.execute("""
            SELECT 
                DATA as data,
                SUM(PRECO_UNITARIO * QTD_COMPRADA) as faturamento,
                COUNT(DISTINCT PEDIDO_ID) as pedidos
            FROM vendas_shopee 
            WHERE date(DATA) BETWEEN ? AND ?
            AND STATUS_PEDIDO = 'COMPLETED'
            GROUP BY DATA
            ORDER BY DATA
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchall()

        # Consolidação
        vendas_por_data = {}

        # Processar Mercado Livre
        for venda in ml_diarias:
            data = venda['data']
            if data not in vendas_por_data:
                vendas_por_data[data] = {
                    'faturamento': 0,
                    'pedidos': 0,
                    'ml': 0,
                    'shopee': 0
                }
            vendas_por_data[data]['faturamento'] += venda['faturamento'] or 0
            vendas_por_data[data]['pedidos'] += venda['pedidos'] or 0
            vendas_por_data[data]['ml'] += venda['faturamento'] or 0

        # Processar Shopee
        for venda in shopee_diarias:
            data = venda['data']
            if data not in vendas_por_data:
                vendas_por_data[data] = {
                    'faturamento': 0,
                    'pedidos': 0,
                    'ml': 0,
                    'shopee': 0
                }
            vendas_por_data[data]['faturamento'] += venda['faturamento'] or 0
            vendas_por_data[data]['pedidos'] += venda['pedidos'] or 0
            vendas_por_data[data]['shopee'] += venda['faturamento'] or 0

        # Converter para lista ordenada
        resultado = [{
            'data': data,
            **vendas_por_data[data]
        } for data in sorted(vendas_por_data.keys())]

        print(f"✅ Retornando {len(resultado)} dias de vendas")
        return jsonify(resultado)

    except Exception as e:
        print(f"❌ Erro em vendas-diarias: {str(e)}")
        return jsonify({'error': str(e)}), 500

@dashboard_vendas_bp.route('/api/top-produtos')
def api_top_produtos():
    """API para obter top produtos por faturamento"""
    try:
        db = get_db_connection()
        limite = request.args.get('limite', 10, type=int)

        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=30)

        # Top produtos Mercado Livre
        ml_produtos = db.execute("""
            SELECT 
                Titulo as produto,
                SKU,
                SUM([Preco Unitario] * Quantidade) as faturamento,
                SUM(Quantidade) as unidades,
                SUM([Lucro Real]) as lucro
            FROM vendas_ml 
            WHERE date(
                substr([Data da Venda], 7, 4) || '-' || 
                substr([Data da Venda], 4, 2) || '-' || 
                substr([Data da Venda], 1, 2)
            ) BETWEEN date(?) AND date(?)
            AND [Situacao] NOT LIKE '%Cancelado%'
            GROUP BY Titulo, SKU
            ORDER BY faturamento DESC
            LIMIT ?
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"), limite)).fetchall()

        # Top produtos Shopee
        shopee_produtos = db.execute("""
            SELECT 
                NOME_ITEM as produto,
                SKU,
                SUM(PRECO_UNITARIO * QTD_COMPRADA) as faturamento,
                SUM(QTD_COMPRADA) as unidades,
                SUM(LUCRO_REAL) as lucro
            FROM vendas_shopee 
            WHERE date(DATA) BETWEEN ? AND ?
            AND STATUS_PEDIDO = 'COMPLETED'
            GROUP BY NOME_ITEM, SKU
            ORDER BY faturamento DESC
            LIMIT ?
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"), limite)).fetchall()

        # Consolidar e ordenar
        todos_produtos = []

        for produto in ml_produtos:
            todos_produtos.append({
                'produto': produto['produto'],
                'sku': produto['SKU'],
                'faturamento': produto['faturamento'] or 0,
                'unidades': produto['unidades'] or 0,
                'lucro': produto['lucro'] or 0,
                'plataforma': 'Mercado Livre'
            })

        for produto in shopee_produtos:
            todos_produtos.append({
                'produto': produto['produto'],
                'sku': produto['SKU'],
                'faturamento': produto['faturamento'] or 0,
                'unidades': produto['unidades'] or 0,
                'lucro': produto['lucro'] or 0,
                'plataforma': 'Shopee'
            })

        # Ordenar por faturamento e limitar
        todos_produtos.sort(key=lambda x: x['faturamento'], reverse=True)

        return jsonify(todos_produtos[:limite])

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@dashboard_vendas_bp.route('/api/logistica')
def api_logistica():
    """API para obter dados de logística"""
    try:
        db = get_db_connection()

        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=30)

        # Logística Mercado Livre
        ml_logistica = db.execute("""
            SELECT 
                [Tipo Logistica] as tipo,
                COUNT(*) as quantidade,
                SUM([Custo Total Frete]) as custo_frete,
                AVG([Custo Total Frete]) as custo_medio
            FROM vendas_ml 
            WHERE date(
                substr([Data da Venda], 7, 4) || '-' || 
                substr([Data da Venda], 4, 2) || '-' || 
                substr([Data da Venda], 1, 2)
            ) BETWEEN date(?) AND date(?)
            AND [Tipo Logistica] IS NOT NULL
            AND [Situacao] NOT LIKE '%Cancelado%'
            GROUP BY [Tipo Logistica]
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchall()

        # Logística Shopee
        shopee_logistica = db.execute("""
            SELECT 
                TRANSPORTADORA as tipo,
                COUNT(*) as quantidade,
                SUM(FRETE_UNITARIO * QTD_COMPRADA) as custo_frete,
                AVG(FRETE_UNITARIO) as custo_medio
            FROM vendas_shopee 
            WHERE date(DATA) BETWEEN ? AND ?
            AND TRANSPORTADORA IS NOT NULL
            AND STATUS_PEDIDO = 'COMPLETED'
            GROUP BY TRANSPORTADORA
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchall()

        resultado = {
            'mercado_livre': [dict(row) for row in ml_logistica],
            'shopee': [dict(row) for row in shopee_logistica]
        }

        return jsonify(resultado)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@dashboard_vendas_bp.route('/api/repasses')
def api_repasses():
    """API para obter dados de repasses"""
    try:
        db = get_db_connection()

        # Repasses dos últimos 60 dias
        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=60)

        # Consultar tabela de entradas financeiras
        repasses = db.execute("""
            SELECT 
                data_venda,
                data_liberacao,
                valor_liquido,
                origem_conta,
                status,
                pedido_id
            FROM repasses_ml 
            WHERE date(
                substr(data_venda, 7, 4) || '-' || 
                substr(data_venda, 4, 2) || '-' || 
                substr(data_venda, 1, 2)
            ) BETWEEN date(?) AND date(?)
            ORDER BY data_liberacao DESC
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchall()

        resultado = [dict(row) for row in repasses]

        return jsonify(resultado)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@dashboard_vendas_bp.route('/api/vendas-prejuizo')
def api_vendas_prejuizo():
    """API para obter vendas com prejuízo"""
    try:
        db = get_db_connection()

        data_fim = datetime.now()
        data_inicio = data_fim - timedelta(days=30)

        # Vendas com prejuízo Mercado Livre
        ml_prejuizo = db.execute("""
            SELECT 
                [ID Pedido] as pedido_id,
                [Data da Venda] as data,
                Titulo as produto,
                [Preco Unitario] * Quantidade as valor_total,
                [Lucro Real] as lucro,
                'Mercado Livre' as plataforma
            FROM vendas_ml 
            WHERE date(
                substr([Data da Venda], 7, 4) || '-' || 
                substr([Data da Venda], 4, 2) || '-' || 
                substr([Data da Venda], 1, 2)
            ) BETWEEN date(?) AND date(?)
            AND [Lucro Real] < 0
            AND [Situacao] NOT LIKE '%Cancelado%'
            ORDER BY [Lucro Real] ASC
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchall()

        # Vendas com prejuízo Shopee
        shopee_prejuizo = db.execute("""
            SELECT 
                PEDIDO_ID as pedido_id,
                DATA as data,
                NOME_ITEM as produto,
                PRECO_UNITARIO * QTD_COMPRADA as valor_total,
                LUCRO_REAL as lucro,
                'Shopee' as plataforma
            FROM vendas_shopee 
            WHERE date(DATA) BETWEEN ? AND ?
            AND LUCRO_REAL < 0
            AND STATUS_PEDIDO = 'COMPLETED'
            ORDER BY LUCRO_REAL ASC
        """, (data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d"))).fetchall()

        # Consolidar resultados
        resultado = []
        for venda in ml_prejuizo:
            resultado.append(dict(venda))
        for venda in shopee_prejuizo:
            resultado.append(dict(venda))

        # Ordenar por prejuízo (maior prejuízo primeiro)
        resultado.sort(key=lambda x: x['lucro'])

        return jsonify(resultado)

    except Exception as e:
        return jsonify({'error': str(e)}), 500