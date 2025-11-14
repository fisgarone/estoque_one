import sqlite3
import logging
import json
import os
import sys
import time
import requests
import hmac
import hashlib
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Configuração ABSOLUTA de caminhos - garantindo que sempre busque na raiz do projeto
ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..'))  # Volta 3 níveis para chegar na raiz
DB_PATH = os.path.join(ROOT_DIR, 'fisgarone.db')
ENV_PATH = os.path.join(ROOT_DIR, '.env')  # Agora SEMPRE na raiz do projeto

# Verificação explícita dos arquivos antes de continuar
if not os.path.exists(ENV_PATH):
    raise FileNotFoundError(f"Arquivo .env não encontrado no caminho: {ENV_PATH}\n"
                            f"Por favor, verifique se o arquivo .env está na raiz do projeto (C:\\fisgarone\\.env)")

if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"Banco de dados não encontrado no caminho: {DB_PATH}")

# Carrega variáveis do .env
load_dotenv(ENV_PATH)

# Configuração do sistema
sys.path.append(os.path.join(ROOT_DIR, 'modulos', 'vendas', 'shopee'))
from pyshopee2 import Client

# Configurações globais
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3


def get_env_variable(account_type, var_type):
    """Obtém variável de ambiente com tratamento especial para tokens JWT"""
    var_map = {
        'PARTNER_ID': f'SHOPEE_PARTNER_ID_{account_type}',
        'PARTNER_KEY': f'SHOPEE_PARTNER_KEY_{account_type}',
        'SHOP_ID': f'SHOPEE_SHOP_ID_{account_type}',
        'ACCESS_TOKEN': f'SHOPEE_ACCESS_TOKEN_{account_type}',
        'REFRESH_TOKEN': f'SHOPEE_REFRESH_TOKEN_{account_type}'
    }

    var_name = var_map.get(var_type)
    if not var_name:
        raise ValueError(f"Tipo de variável inválido: {var_type}")

    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Variável {var_name} não encontrada no .env")

    return value


def create_shopee_client(account_type):
    """Cria e configura cliente da API Shopee"""
    try:
        client = Client(
            shop_id=int(get_env_variable(account_type, 'SHOP_ID')),
            partner_id=int(get_env_variable(account_type, 'PARTNER_ID')),
            partner_key=get_env_variable(account_type, 'PARTNER_KEY'),
            redirect_url="https://google.com",
            access_token=get_env_variable(account_type, 'ACCESS_TOKEN')
        )
        client.refresh_token = get_env_variable(account_type, 'REFRESH_TOKEN')
        client.account_type = account_type
        return client
    except Exception as e:
        logging.error(f"Erro ao criar cliente para {account_type}: {str(e)}")
        raise

def check_and_update_schema(conn):
    """Garante que a tabela pedidos existe com todas colunas necessárias"""
    cursor = conn.cursor()

    # Criação completa da tabela (se não existir)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pedidos (
            buyer_user_id TEXT,
            estimated_shipping_fee REAL,
            actual_shipping_fee REAL,
            item_list TEXT,
            total_amount REAL,
            package_list TEXT,
            order_sn TEXT PRIMARY KEY,
            order_status TEXT,
            model_quantity INTEGER,
            shipping_carrier TEXT,
            item_name TEXT,
            model_discounted_price REAL,
            account_type TEXT,
            item_sku TEXT,
            model_sku TEXT,
            create_time TEXT,
            Data_Entregue TEXT
        )
    ''')

    # Verifica se todas colunas existem
    cursor.execute("PRAGMA table_info(pedidos)")
    existing_columns = {col[1] for col in cursor.fetchall()}
    required_columns = {
        'buyer_user_id', 'estimated_shipping_fee', 'actual_shipping_fee',
        'item_list', 'total_amount', 'package_list', 'order_sn',
        'order_status', 'model_quantity', 'shipping_carrier',
        'item_name', 'model_discounted_price', 'account_type',
        'item_sku', 'model_sku', 'create_time', 'Data_Entregue'
    }

    # Adiciona colunas faltantes
    for column in required_columns - existing_columns:
        try:
            cursor.execute(f'ALTER TABLE pedidos ADD COLUMN {column} {get_column_type(column)}')
            logging.info(f"Coluna {column} adicionada com sucesso")
        except sqlite3.OperationalError as e:
            logging.warning(f"Erro ao adicionar coluna {column}: {str(e)}")

    conn.commit()


def get_column_type(column_name):
    """Retorna o tipo SQL apropriado para cada coluna"""
    if column_name in ['model_quantity']:
        return 'INTEGER'
    elif column_name in ['estimated_shipping_fee', 'actual_shipping_fee',
                         'total_amount', 'model_discounted_price']:
        return 'REAL'
    return 'TEXT'

def get_last_create_time():
    """Retorna timestamp de 60 dias atrás"""
    return int((datetime.now(timezone.utc) - timedelta(days=60)).timestamp())

def serialize_field(field):
    """Serializa campos complexos para JSON"""
    if isinstance(field, (dict, list)):
        return json.dumps(field, ensure_ascii=False)
    return str(field) if field else ""

def format_timestamp(timestamp):
    """Formata timestamp para string"""
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def refresh_tokens(client):
    """Atualiza tokens de acesso com assinatura correta"""
    try:
        timestamp = int(time.time())
        path = "/api/v2/auth/access_token/get"
        base_string = f"{client.partner_id}{path}{timestamp}"

        signature = hmac.new(
            client.partner_key.encode('utf-8'),
            base_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        headers = {"Content-Type": "application/json"}
        params = {
            "partner_id": client.partner_id,
            "timestamp": timestamp,
            "sign": signature,
            "shop_id": client.shop_id
        }

        data = {
            "refresh_token": client.refresh_token,
            "partner_id": client.partner_id,
            "shop_id": client.shop_id
        }

        response = requests.post(
            "https://partner.shopeemobile.com/api/v2/auth/access_token/get",
            json=data,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            new_tokens = response.json()

            # Atualiza arquivo .env dinamicamente
            env_vars = {}
            with open(ENV_PATH, 'r') as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value

            env_vars[f"sACCESS_TOKEN_{client.account_type}"] = new_tokens['access_token']
            env_vars[f"sREFRESH_TOKEN_{client.account_type}"] = new_tokens['refresh_token']

            with open(ENV_PATH, 'w') as f:
                for key, value in env_vars.items():
                    f.write(f"{key}={value}\n")

            # Atualiza cliente
            client.access_token = new_tokens['access_token']
            client.refresh_token = new_tokens['refresh_token']

            logging.info(f"Tokens atualizados para {client.account_type}")
            return True

        logging.error(f"Falha na renovação: {response.status_code} - {response.text}")
        return False

    except Exception as e:
        logging.error(f"Erro na renovação: {str(e)}", exc_info=True)
        return False

def fetch_orders(client, last_create_time):
    """Busca pedidos com tratamento de autenticação"""
    orders = []
    end_time = int(datetime.now().timestamp())
    time_from = last_create_time

    while time_from < end_time:
        time_to = min(time_from + (15 * 86400), end_time)
        logging.info(f"Buscando pedidos de {format_timestamp(time_from)} a {format_timestamp(time_to)}")

        has_more = True
        next_cursor = 0
        retries = 0

        while has_more and retries < MAX_RETRIES:
            try:
                resp_json = client.order.get_order_list(
                    time_range_field="create_time",
                    time_from=time_from,
                    time_to=time_to,
                    page_size=50,
                    cursor=next_cursor,
                    timeout=REQUEST_TIMEOUT
                )

                # Tratamento de erros de autenticação
                if 'error' in resp_json:
                    error_code = resp_json.get('error', '')
                    if error_code in ['error_auth', 'error_token']:
                        if refresh_tokens(client):
                            continue  # Repete a requisição
                        else:
                            retries += 1
                            continue

                if 'response' not in resp_json:
                    logging.warning("Resposta da API inválida")
                    return []

                order_list = resp_json['response'].get('order_list', [])
                if not order_list:
                    return []

                # Obtém detalhes dos pedidos
                orders_sn = [o['order_sn'] for o in order_list]
                detalhes = client.order.get_order_detail(
                    order_sn_list=",".join(orders_sn),
                    response_optional_fields=",".join([
                        "buyer_user_id", "estimated_shipping_fee",
                        "actual_shipping_fee", "item_list", "total_amount",
                        "package_list", "order_sn", "order_status", "create_time"
                    ])
                )

                if 'response' in detalhes:
                    orders.extend(detalhes['response']['order_list'])

                has_more = resp_json['response']['more']
                next_cursor = int(resp_json['response']['next_cursor']) if has_more else 0

            except requests.exceptions.RequestException as e:
                retries += 1
                logging.error(f"Erro de conexão ({retries}/{MAX_RETRIES}): {str(e)}")
                time.sleep(5)

        time_from = time_to

    return orders

def insert_orders(orders, conn, account_type):
    """Insere pedidos com tratamento da nova coluna"""
    cursor = conn.cursor()
    for order in orders:
        try:
            order_data = {
                'order_sn': order.get('order_sn'),
                'create_time': order.get('create_time'),
                'order_status': order.get('order_status', '')
            }

            if not order_data['order_sn'] or not order_data['create_time']:
                continue

            # Verifica se pedido já existe
            if cursor.execute("SELECT 1 FROM pedidos WHERE order_sn = ?",
                              (order_data['order_sn'],)).fetchone():
                continue

            # Prepara dados para inserção
            create_time_str = format_timestamp(order_data['create_time'])
            data_entregue = None

            if order_data['order_status'] == 'COMPLETED':
                create_date = datetime.strptime(create_time_str, "%Y-%m-%d %H:%M:%S")
                data_entregue = (create_date + timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S")

            valores = (
                order.get('buyer_user_id', ''),
                order.get('estimated_shipping_fee', 0),
                order.get('actual_shipping_fee', 0),
                serialize_field(order.get('item_list', [])),
                order.get('total_amount', 0),
                serialize_field(order.get('package_list', [])),
                order_data['order_sn'],
                order_data['order_status'],
                order.get('model_quantity', 0),
                order.get('shipping_carrier', ''),
                order.get('item_name', ''),
                order.get('model_discounted_price', 0),
                account_type,
                order.get('item_sku', ''),
                order.get('model_sku', ''),
                create_time_str,
                data_entregue
            )

            cursor.execute('''
                INSERT INTO pedidos VALUES (
                    ?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?
                )
            ''', valores)

        except Exception as e:
            logging.error(f"Erro no pedido {order_data.get('order_sn', '')}: {str(e)}")

    conn.commit()

def update_completed_orders(conn):
    """Atualiza pedidos existentes que mudaram para COMPLETED"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT order_sn, create_time 
        FROM pedidos 
        WHERE order_status = 'COMPLETED' 
        AND Data_Entregue IS NULL
    ''')

    for order_sn, create_time in cursor.fetchall():
        try:
            create_date = datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S")
            delivery_date = (create_date + timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute('''
                UPDATE pedidos 
                SET Data_Entregue = ? 
                WHERE order_sn = ?
            ''', (delivery_date, order_sn))

            logging.info(f"Pedido {order_sn} atualizado com Data_Entregue: {delivery_date}")

        except Exception as e:
            logging.error(f"Erro ao atualizar pedido {order_sn}: {str(e)}")

    conn.commit()


def main():
    """Função principal de execução"""
    try:
        # Verificação inicial
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"Banco de dados não encontrado em: {DB_PATH}")

        if not os.path.exists(ENV_PATH):
            raise FileNotFoundError(f"Arquivo .env não encontrado em: {ENV_PATH}")

        conn = sqlite3.connect(DB_PATH)
        check_and_update_schema(conn)

        for conta in ["COMERCIAL", "TOYS"]:
            try:
                logging.info(f"Iniciando processamento para conta {conta}")
                client = create_shopee_client(conta)
                pedidos = fetch_orders(client, get_last_create_time())
                insert_orders(pedidos, conn, conta)
                logging.info(f"Conta {conta} processada com sucesso")

            except Exception as e:
                logging.error(f"Erro na conta {conta}: {str(e)}", exc_info=True)

        update_completed_orders(conn)

    except Exception as e:
        logging.error(f"Erro fatal: {str(e)}", exc_info=True)
        raise
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == '__main__':
    main()