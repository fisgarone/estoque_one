# modulos/vendas/ml/promocoes_ml.py - CÓDIGO COMPLETO REVISADO
import os
import sqlite3
import requests
import json
from datetime import datetime, timedelta, timezone
from flask import Blueprint, render_template, jsonify, request, send_file
from dotenv import load_dotenv
import logging
import io
import csv

promocoes_ml_bp = Blueprint('promocoes_ml', __name__, template_folder='../../../templates/vendas/ml',
                            static_folder='../../../static/vendas/ml')

# Configurar logging avançado
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('promocoes_ml.log'), logging.StreamHandler()])

# Carregar variáveis do .env
load_dotenv()

API_URL = os.getenv('API_URL', 'https://api.mercadolibre.com')
ML_CLIENT_ID = os.getenv('ML_CLIENT_ID_TOYS')
ML_CLIENT_SECRET = os.getenv('ML_CLIENT_SECRET_TOYS')
ML_ACCESS_TOKEN = os.getenv('ML_ACCESS_TOKEN_TOYS')
ML_REFRESH_TOKEN = os.getenv('ML_REFRESH_TOKEN_TOYS')
ML_SELLER_ID = os.getenv('ML_SELLER_ID_TOYS')

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../fisgarone.db')

# Verificar credenciais obrigatórias
required_creds = [ML_CLIENT_ID, ML_CLIENT_SECRET, ML_ACCESS_TOKEN, ML_REFRESH_TOKEN, ML_SELLER_ID]
if not all(required_creds):
    raise ValueError(
        "Credenciais do Mercado Livre incompletas no .env. Verifique ML_CLIENT_ID_TOYS, ML_CLIENT_SECRET_TOYS, ML_ACCESS_TOKEN_TOYS, ML_REFRESH_TOKEN_TOYS e ML_SELLER_ID_TOYS.")

can_refresh = all([ML_CLIENT_ID, ML_CLIENT_SECRET, ML_REFRESH_TOKEN])

headers = {
    'Authorization': f'Bearer {ML_ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}


def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS promotions (
        id TEXT PRIMARY KEY,
        type TEXT,
        name TEXT,
        start_date TEXT,
        finish_date TEXT,
        benefits TEXT,
        is_shared INTEGER
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS promotion_items (
        promotion_id TEXT,
        item_id TEXT,
        original_price REAL,
        discount_price REAL,
        discount_percentage REAL,
        FOREIGN KEY (promotion_id) REFERENCES promotions(id)
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        date_created TEXT,
        total_amount REAL,
        total_discount REAL
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS order_items (
        order_id TEXT,
        item_id TEXT,
        title TEXT,
        quantity INTEGER,
        unit_price REAL,
        full_unit_price REAL,
        discount_per_unit REAL,
        total_discount REAL,
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS item_promotions (
        item_id TEXT,
        promotion_data TEXT
    )
    ''')
    conn.commit()
    conn.close()


create_tables()  # Criar tabelas se não existirem


def refresh_access_token():
    global ML_ACCESS_TOKEN, ML_REFRESH_TOKEN, headers
    if not can_refresh:
        raise ValueError("Credenciais para refresh incompletas.")
    refresh_url = f"{API_URL}/oauth/token"
    payload = {
        'grant_type': 'refresh_token',
        'client_id': ML_CLIENT_ID,
        'client_secret': ML_CLIENT_SECRET,
        'refresh_token': ML_REFRESH_TOKEN
    }
    try:
        response = requests.post(refresh_url, data=payload)
        response.raise_for_status()
        tokens = response.json()
        ML_ACCESS_TOKEN = tokens.get('access_token')
        ML_REFRESH_TOKEN = tokens.get('refresh_token', ML_REFRESH_TOKEN)
        headers['Authorization'] = f'Bearer {ML_ACCESS_TOKEN}'
        with open('.env', 'a') as f:
            f.write(
                f"\n# Atualizado em {datetime.now().isoformat()}\nML_ACCESS_TOKEN_TOYS={ML_ACCESS_TOKEN}\nML_REFRESH_TOKEN_TOYS={ML_REFRESH_TOKEN}")
        logging.info("Token atualizado com sucesso.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao refrescar token: {str(e)}")
        raise


def api_request(method, endpoint, params=None, data=None, retry_on_auth=False):
    if params is None:
        params = {}
    url = f"{API_URL}{endpoint}"
    try:
        response = requests.request(method, url, headers=headers, params=params, json=data, timeout=30)
        logging.debug(f"Requisição: {method} {url} - Status: {response.status_code}")
        if response.status_code == 401 and not retry_on_auth and can_refresh:
            logging.warning("Token expirado. Refreshing...")
            refresh_access_token()
            return api_request(method, endpoint, params, data, retry_on_auth=True)
        if response.status_code in [204, 200] and not response.text.strip():
            logging.warning(f"Resposta vazia para {endpoint}. Retornando dados vazios.")
            return {'results': []}
        response.raise_for_status()
        try:
            return response.json()
        except json.JSONDecodeError:
            logging.error(f"Erro ao decodificar JSON para {endpoint}: {response.text[:200]}")
            return {'results': []}
    except requests.exceptions.HTTPError as e:
        logging.error(
            f"Erro HTTP para {endpoint}: {str(e)} - Resposta: {response.text if 'response' in locals() else 'N/A'}")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro de requisição para {endpoint}: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado para {endpoint}: {str(e)}")
        raise


def get_active_promotions():
    if not ML_SELLER_ID:
        raise ValueError("ML_SELLER_ID_TOYS não definido no .env.")
    promotions = []
    params = {'app_version': 'v2'}
    try:
        data = api_request('GET', f"/seller-promotions/users/{ML_SELLER_ID}", params=params)
        for prom in data.get('results', []):
            if prom.get('status') == 'started':
                benefits = prom.get('benefits', {})

                # CORREÇÃO CRÍTICA: Lógica robusta para identificar promoções compartilhadas
                is_shared = 0
                # Verifica múltiplos indicadores de compartilhamento
                meli_funding = benefits.get('meli_funding', {})
                if (benefits.get('meli_percent', 0) > 0 or
                        benefits.get('meli_amount', 0) > 0 or
                        benefits.get('meli_fixed_amount', 0) > 0 or
                        meli_funding.get('amount', 0) > 0 or
                        meli_funding.get('percentage', 0) > 0 or
                        'meli' in str(benefits).lower() or
                        prom.get('funding', {}).get('meli', 0) > 0):
                    is_shared = 1
                    logging.info(f"✅ Promoção compartilhada detectada: {prom['id']} - {prom.get('name', 'N/A')}")
                    logging.info(f"📊 Benefícios: {benefits}")

                prom_details = {
                    'id': prom['id'],
                    'type': prom['type'],
                    'name': prom.get('name', 'N/A'),
                    'start_date': prom['start_date'],
                    'finish_date': prom['finish_date'],
                    'benefits': json.dumps(benefits),
                    'is_shared': is_shared  # CORREÇÃO APLICADA
                }

                items_params = {'promotion_type': prom['type'], 'app_version': 'v2'}
                items_data = api_request('GET', f"/seller-promotions/promotions/{prom['id']}/items",
                                         params=items_params)
                prom_details['items'] = items_data.get('results', [])
                promotions.append(prom_details)
    except Exception as e:
        logging.error(f"Erro ao buscar promoções: {str(e)}")
    return promotions


def get_recent_orders(days_back=90):
    if not ML_SELLER_ID:
        raise ValueError("ML_SELLER_ID_TOYS não definido no .env.")
    orders = []
    date_from = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')
    params = {
        'seller': ML_SELLER_ID,
        'order.status': 'paid',
        'order.date_created.from': date_from,
        'sort': 'date_desc',
        'limit': 50,
        'offset': 0
    }
    while True:
        try:
            data = api_request('GET', '/orders/search', params=params)
            for order in data.get('results', []):
                order_id = order['id']
                order_details = api_request('GET', f"/orders/{order_id}")
                detailed_items = []
                total_discount = 0
                for item in order_details.get('order_items', []):
                    item_id = item['item']['id']
                    full_price = item.get('full_unit_price', item['unit_price'])
                    discount = full_price - item['unit_price']
                    total_discount += discount * item['quantity']
                    try:
                        item_promos_params = {'app_version': 'v2'}
                        item_promos = api_request('GET', f"/seller-promotions/items/{item_id}",
                                                  params=item_promos_params)
                    except Exception as pe:
                        logging.warning(f"Erro ao buscar promoções para item {item_id}: {pe}")
                        item_promos = {'results': []}
                    detailed_items.append({
                        'item_id': item_id,
                        'title': item['item']['title'],
                        'quantity': item['quantity'],
                        'unit_price': item['unit_price'],
                        'full_unit_price': full_price,
                        'discount_per_unit': discount,
                        'total_discount': discount * item['quantity'],
                        'available_promotions': json.dumps(item_promos)
                    })
                orders.append({
                    'order_id': order_id,
                    'date_created': order['date_created'],
                    'total_amount': order['total_amount'],
                    'total_discount': total_discount,
                    'items': detailed_items
                })
            paging = data.get('paging', {})
            if paging['offset'] + paging['limit'] >= paging['total']:
                break
            params['offset'] += params['limit']
        except Exception as e:
            logging.error(f"Erro ao buscar ordens: {str(e)}")
            break
    return orders


def populate_db():
    try:
        promotions = get_active_promotions()
        orders = get_recent_orders()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Limpar tabelas antigas de forma segura
        cursor.execute('DELETE FROM promotion_items')
        cursor.execute('DELETE FROM promotions')
        cursor.execute('DELETE FROM item_promotions')
        cursor.execute('DELETE FROM order_items')
        cursor.execute('DELETE FROM orders')

        for prom in promotions:
            cursor.execute('''
            INSERT OR REPLACE INTO promotions (id, type, name, start_date, finish_date, benefits, is_shared)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (prom['id'], prom['type'], prom['name'], prom['start_date'], prom['finish_date'], prom['benefits'],
                  prom['is_shared']))

            for item in prom['items']:
                cursor.execute('''
                INSERT INTO promotion_items (promotion_id, item_id, original_price, discount_price, discount_percentage)
                VALUES (?, ?, ?, ?, ?)
                ''', (prom['id'], item.get('id'), item.get('original_price'), item.get('price'),
                      item.get('discount_percentage')))

        for order in orders:
            cursor.execute('''
            INSERT OR REPLACE INTO orders (order_id, date_created, total_amount, total_discount)
            VALUES (?, ?, ?, ?)
            ''', (order['order_id'], order['date_created'], order['total_amount'], order['total_discount']))

            for item in order['items']:
                cursor.execute('''
                INSERT INTO order_items (order_id, item_id, title, quantity, unit_price, full_unit_price, discount_per_unit, total_discount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (order['order_id'], item['item_id'], item['title'], item['quantity'], item['unit_price'],
                      item['full_unit_price'], item['discount_per_unit'], item['total_discount']))

                cursor.execute('''
                INSERT OR REPLACE INTO item_promotions (item_id, promotion_data)
                VALUES (?, ?)
                ''', (item['item_id'], item['available_promotions']))

        conn.commit()
        conn.close()
        logging.info(f"✅ DB populado com {len(promotions)} promoções e {len(orders)} ordens.")
        logging.info(f"📊 Promoções compartilhadas: {sum(1 for p in promotions if p['is_shared'])}")
        return len(promotions), len(orders)
    except Exception as e:
        logging.error(f"❌ Erro ao popular DB: {str(e)}")
        raise


@promocoes_ml_bp.route('/promocoes', methods=['GET'])
def promocoes():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Métricas básicas - CORRIGIDAS
        cursor.execute('SELECT COUNT(*) FROM promotions')
        total_promotions = cursor.fetchone()[0] or 0

        cursor.execute('SELECT COUNT(*) FROM promotions WHERE is_shared = 1')
        shared_promotions = cursor.fetchone()[0] or 0

        cursor.execute('SELECT COUNT(*) FROM orders')
        total_orders = cursor.fetchone()[0] or 0

        cursor.execute('SELECT COUNT(*) FROM orders WHERE total_discount > 0')
        orders_with_discount = cursor.fetchone()[0] or 0

        cursor.execute('SELECT SUM(total_amount) FROM orders')
        total_sales = cursor.fetchone()[0] or 0

        cursor.execute('SELECT SUM(total_discount) FROM orders')
        total_discounts = cursor.fetchone()[0] or 0

        cursor.execute('SELECT AVG(total_discount) FROM orders WHERE total_discount > 0')
        avg_discount = cursor.fetchone()[0] or 0

        cursor.execute('SELECT AVG(total_amount) FROM orders')
        avg_order_value = cursor.fetchone()[0] or 0

        # CORREÇÃO CRÍTICA: Cálculos realistas de ROI e Taxa de Desconto
        # ROI = (Vendas Líquidas - Investimento em Descontos) / Investimento em Descontos
        investment = total_discounts
        net_sales = total_sales - total_discounts
        roi = ((net_sales - investment) / investment * 100) if investment > 0 else 0

        # Taxa de desconto = (Total de Descontos / Total de Vendas Brutas) * 100
        discount_rate = (total_discounts / total_sales * 100) if total_sales > 0 else 0

        # Limitar valores realistas (ROI entre -100% e 500%, taxa até 50%)
        roi = max(-100, min(500, roi))
        discount_rate = min(50, discount_rate)  # Máximo 50% de taxa de desconto

        logging.info(f"📈 Métricas calculadas - ROI: {roi:.2f}%, Taxa Desconto: {discount_rate:.2f}%")

        # Dados para gráficos
        cursor.execute('''
        SELECT strftime('%Y-%m-%d', date_created) as date, SUM(total_amount) 
        FROM orders GROUP BY date ORDER BY date
        ''')
        sales_data = cursor.fetchall()
        sales_dates = [row[0] for row in sales_data]
        sales_amounts = [float(row[1]) for row in sales_data]

        cursor.execute('''
        SELECT strftime('%Y-%m-%d', date_created) as date, SUM(total_discount) 
        FROM orders GROUP BY date ORDER BY date
        ''')
        discount_data = cursor.fetchall()
        discount_dates = [row[0] for row in discount_data]
        discount_amounts = [float(row[1]) for row in discount_data]

        cursor.execute('SELECT type, COUNT(*) FROM promotions GROUP BY type')
        prom_types = cursor.fetchall()
        prom_labels = [row[0] for row in prom_types]
        prom_counts = [row[1] for row in prom_types]

        # Tabela de promoções com join
        cursor.execute('''
        SELECT p.id, p.type, p.name, p.start_date, p.finish_date, p.is_shared, COUNT(pi.item_id) as items_count
        FROM promotions p LEFT JOIN promotion_items pi ON p.id = pi.promotion_id
        GROUP BY p.id
        ''')
        promotions_table = cursor.fetchall()

        # Tabela de ordens com itens
        cursor.execute('''
        SELECT o.order_id, o.date_created, o.total_amount, o.total_discount, COUNT(oi.item_id) as items_count
        FROM orders o LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id
        ''')
        orders_table = cursor.fetchall()

        conn.close()

        return render_template('promocoes.html',
                               total_promotions=total_promotions,
                               shared_promotions=shared_promotions,
                               total_orders=total_orders,
                               orders_with_discount=orders_with_discount,
                               total_sales=total_sales,
                               total_discounts=total_discounts,
                               avg_discount=avg_discount,
                               avg_order_value=avg_order_value,
                               roi=roi,
                               discount_rate=discount_rate,
                               sales_dates=json.dumps(sales_dates),
                               sales_amounts=json.dumps(sales_amounts),
                               discount_dates=json.dumps(discount_dates),
                               discount_amounts=json.dumps(discount_amounts),
                               prom_labels=json.dumps(prom_labels),
                               prom_counts=json.dumps(prom_counts),
                               promotions_table=promotions_table,
                               orders_table=orders_table)
    except Exception as e:
        logging.error(f"❌ Erro ao renderizar dashboard: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@promocoes_ml_bp.route('/update', methods=['POST'])
def update_data():
    try:
        num_prom, num_orders = populate_db()
        return jsonify({'status': 'success', 'promotions': num_prom, 'orders': num_orders})
    except Exception as e:
        logging.error(f"❌ Erro ao atualizar dados: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@promocoes_ml_bp.route('/promotion/<prom_id>', methods=['GET'])
def get_promotion_details(prom_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM promotions WHERE id = ?', (prom_id,))
        prom = cursor.fetchone()
        if not prom:
            return jsonify({'error': 'Promoção não encontrada'}), 404
        prom_dict = {
            'id': prom[0], 'type': prom[1], 'name': prom[2], 'start_date': prom[3],
            'finish_date': prom[4], 'benefits': prom[5], 'is_shared': prom[6]
        }
        cursor.execute('SELECT * FROM promotion_items WHERE promotion_id = ?', (prom_id,))
        items = cursor.fetchall()
        prom_dict['items'] = [
            {'item_id': i[1], 'original_price': i[2], 'discount_price': i[3], 'discount_percentage': i[4]} for i in
            items]
        conn.close()
        return jsonify(prom_dict)
    except Exception as e:
        logging.error(f"❌ Erro ao buscar detalhes de promoção {prom_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@promocoes_ml_bp.route('/order/<order_id>', methods=['GET'])
def get_order_details(order_id):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM orders WHERE order_id = ?', (order_id,))
        order = cursor.fetchone()
        if not order:
            return jsonify({'error': 'Ordem não encontrada'}), 404
        order_dict = {
            'order_id': order[0], 'date_created': order[1], 'total_amount': order[2], 'total_discount': order[3]
        }
        cursor.execute('SELECT * FROM order_items WHERE order_id = ?', (order_id,))
        items = cursor.fetchall()
        order_dict['items'] = [
            {'item_id': i[1], 'title': i[2], 'quantity': i[3], 'unit_price': i[4], 'full_unit_price': i[5],
             'discount_per_unit': i[6], 'total_discount': i[7]} for i in items]
        conn.close()
        return jsonify(order_dict)
    except Exception as e:
        logging.error(f"❌ Erro ao buscar detalhes de ordem {order_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@promocoes_ml_bp.route('/export/<data_type>', methods=['GET'])
def export_data(data_type):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        output = io.StringIO()
        writer = csv.writer(output)

        if data_type == 'promotions':
            cursor.execute('SELECT * FROM promotions')
            writer.writerow(['ID', 'Type', 'Name', 'Start Date', 'Finish Date', 'Benefits', 'Is Shared'])
            writer.writerows(cursor.fetchall())
        elif data_type == 'orders':
            cursor.execute('SELECT * FROM orders')
            writer.writerow(['Order ID', 'Date Created', 'Total Amount', 'Total Discount'])
            writer.writerows(cursor.fetchall())
        else:
            return jsonify({'error': 'Tipo de dado inválido'}), 400

        conn.close()
        output.seek(0)
        return send_file(io.BytesIO(output.getvalue().encode('utf-8')), mimetype='text/csv', as_attachment=True,
                         download_name=f'{data_type}.csv')
    except Exception as e:
        logging.error(f"❌ Erro ao exportar {data_type}: {str(e)}")
        return jsonify({'error': str(e)}), 500