import os
import sys
import sqlite3
import logging
import json
import time
import requests
import hmac
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pyshopee2 import Client


def data_br_safe(val):
    """
    Recebe string, timestamp ou datetime e retorna sempre DD/MM/YYYY.
    Se não der parse, retorna string vazia.
    """
    if not val:
        return ""
    if isinstance(val, datetime):
        return val.strftime("%d/%m/%Y")
    if isinstance(val, (int, float)):
        # Trata timestamps (epoch)
        try:
            return datetime.fromtimestamp(val, timezone.utc).strftime("%d/%m/%Y")
        except Exception:
            return ""
    if isinstance(val, str):
        val = val.strip()[:10]
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(val, fmt).strftime("%d/%m/%Y")
            except Exception:
                continue
    return ""


# === CONFIGURAÇÕES ===
DB_PATH = r"C:\estoque_one\fisgarone.db"
ENV_PATH = r"C:\estoque_one\.env"

print("DB Exists?", os.path.exists(DB_PATH))
print("ENV Exists?", os.path.exists(ENV_PATH))

load_dotenv(ENV_PATH)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3


def criar_tabela_custo_shopee():
    """Cria a tabela custo_shopee se não existir"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS custo_shopee (
                SKU TEXT PRIMARY KEY,
                Custo REAL DEFAULT 0,
                Data_Atualizacao TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
        print("✅ Tabela 'custo_shopee' verificada/criada com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao criar tabela custo_shopee: {str(e)}")


def traduzir_valores(coluna, valor):
    if valor is None:
        return valor
    valor = str(valor).upper()
    if coluna == "Conta":
        if valor == "TOYS":
            return "Toys"
        elif valor == "COMERCIAL":
            return "Comercial"
    return valor


def atualizar_entradas_financeiras(cursor):
    cursor.execute("""PRAGMA table_info(entradas_financeiras)""")
    colunas = [col[1] for col in cursor.fetchall()]
    if 'valor_liquido' not in colunas:
        cursor.execute("ALTER TABLE entradas_financeiras ADD COLUMN valor_liquido REAL")
    if 'frete' not in colunas:
        cursor.execute("ALTER TABLE entradas_financeiras ADD COLUMN frete REAL")
    if 'origem_conta' not in colunas:
        cursor.execute("ALTER TABLE entradas_financeiras ADD COLUMN origem_conta TEXT")

    # Garante as colunas TIPO_CONTA e REPASSE_ENVIO na repasses_shopee antes de prosseguir
    cursor.execute("PRAGMA table_info(repasses_shopee)")
    rep_cols = [col[1] for col in cursor.fetchall()]
    if 'TIPO_CONTA' not in rep_cols:
        cursor.execute("ALTER TABLE repasses_shopee ADD COLUMN TIPO_CONTA TEXT")
    if 'REPASSE_ENVIO' not in rep_cols:
        cursor.execute("ALTER TABLE repasses_shopee ADD COLUMN REPASSE_ENVIO REAL")

    cursor.execute("""
        SELECT 
            PEDIDO_ID, 
            DATA, 
            DATA_REPASSE,  
            VALOR_TOTAL, 
            COMISSAO_UNITARIA, 
            TAXA_FIXA,
            REPASSE_ENVIO,
            STATUS_PEDIDO,
            TIPO_CONTA
        FROM repasses_shopee
    """)

    # Atualize na função atualizar_entradas_financeiras(cursor)
    for row in cursor.fetchall():
        try:
            valor_total = float(row[3]) if row[3] else 0
            comissoes = float(row[4]) if row[4] else 0
            taxas = float(row[5]) if row[5] else 0
            frete = float(row[6]) if row[6] else 0
            valor_liquido = float(row[3] or 0) - float(row[4] or 0) - float(row[5] or 0)
            origem_conta = traduzir_valores("Conta", row[8]) if row[8] else 'Desconhecido'

            # *** ADICIONE AQUI para forçar o formato ***
            data_venda_br = data_br_safe(row[1])
            data_liberacao_br = data_br_safe(row[2])

            cursor.execute("""
                INSERT OR REPLACE INTO entradas_financeiras 
                (tipo, pedido_id, data_venda, data_liberacao, 
                 valor_total, valor_liquido, comissoes, taxas, frete, 
                 status, origem_conta)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pedido_id, tipo) DO UPDATE SET
                    data_venda = excluded.data_venda,
                    data_liberacao = excluded.data_liberacao,
                    valor_total = excluded.valor_total,
                    valor_liquido = excluded.valor_liquido,
                    comissoes = excluded.comissoes,
                    taxas = excluded.taxas,
                    frete = excluded.frete,
                    status = excluded.status,
                    origem_conta = excluded.origem_conta
            """, (
                'shopee',
                row[0],
                data_venda_br,  # <-- AQUI
                data_liberacao_br,  # <-- E AQUI
                valor_total,
                valor_liquido,
                comissoes,
                taxas,
                frete,
                'Recebido' if row[7] == 'COMPLETED' else 'Pendente',
                origem_conta
            ))
            print(f"✓ Shopee: Pedido {row[0]} (Líquido: R${valor_liquido:.2f})", end='\r')
        except Exception as e:
            print(f"\n✗ Erro no pedido {row[0]}: {str(e)}")


def get_env_variable(account_type, var_type):
    var_map = {
        'PARTNER_ID': f'SHOPEE_PARTNER_ID_{account_type}',
        'PARTNER_KEY': f'SHOPEE_PARTNER_KEY_{account_type}',
        'SHOP_ID': f'SHOPEE_SHOP_ID_{account_type}',
        'ACCESS_TOKEN': f'SHOPEE_ACCESS_TOKEN_{account_type}',
        'REFRESH_TOKEN': f'SHOPEE_REFRESH_TOKEN_{account_type}'
    }
    var_name = var_map[var_type]
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Variável {var_name} não encontrada no .env")
    return value


def create_shopee_client(account_type):
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


def check_and_update_schema(conn):
    cursor = conn.cursor()
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

    # Criar tabela custo_shopee se não existir
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS custo_shopee (
            SKU TEXT PRIMARY KEY,
            Custo REAL DEFAULT 0,
            Data_Atualizacao TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()


def serialize_field(field):
    if isinstance(field, (dict, list)):
        return json.dumps(field, ensure_ascii=False)
    return str(field) if field else ""


def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def generate_signature(partner_id, path, timestamp, partner_key):
    base_string = f"{partner_id}{path}{timestamp}"
    return hmac.new(
        partner_key.encode(),
        base_string.encode(),
        hashlib.sha256
    ).hexdigest()


def update_env_file(new_tokens):
    with open(ENV_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    for account, tokens in new_tokens.items():
        for idx, line in enumerate(lines):
            if line.startswith(f"SHOPEE_ACCESS_TOKEN_{account}="):
                lines[idx] = f'SHOPEE_ACCESS_TOKEN_{account}={tokens["access_token"]}\n'
            elif line.startswith(f"SHOPEE_REFRESH_TOKEN_{account}="):
                lines[idx] = f'SHOPEE_REFRESH_TOKEN_{account}={tokens["refresh_token"]}\n'
    with open(ENV_PATH, 'w', encoding='utf-8') as f:
        f.writelines(lines)


def refresh_tokens(account_type, partner_id, partner_key, shop_id, refresh_token):
    try:
        timestamp = int(time.time())
        path = "/api/v2/auth/access_token/get"
        signature = generate_signature(partner_id, path, timestamp, partner_key)
        headers = {"Content-Type": "application/json"}
        params = {
            "partner_id": int(partner_id),
            "timestamp": timestamp,
            "sign": signature
        }
        data = {
            "refresh_token": refresh_token,
            "partner_id": int(partner_id),
            "shop_id": int(shop_id)
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
            access_token = new_tokens.get("access_token")
            refresh_token = new_tokens.get("refresh_token")
            if access_token and refresh_token:
                os.environ[f"SHOPEE_ACCESS_TOKEN_{account_type}"] = access_token
                os.environ[f"SHOPEE_REFRESH_TOKEN_{account_type}"] = refresh_token
                update_env_file({
                    account_type: {
                        "access_token": access_token,
                        "refresh_token": refresh_token
                    }
                })
                logging.info(f"Tokens atualizados para {account_type}")
                return access_token, refresh_token
            else:
                logging.error(f"Resposta sem tokens para {account_type}: {new_tokens}")
                return None, None
        else:
            logging.error(f"Falha na renovação [{account_type}]: {response.status_code} - {response.text}")
            return None, None
    except Exception as e:
        logging.error(f"Erro na renovação [{account_type}]: {str(e)}", exc_info=True)
        return None, None


def atualizar_todos_tokens():
    accounts = {
        "COMERCIAL": {
            "partner_id": os.getenv("SHOPEE_PARTNER_ID_COMERCIAL"),
            "partner_key": os.getenv("SHOPEE_PARTNER_KEY_COMERCIAL"),
            "shop_id": os.getenv("SHOPEE_SHOP_ID_COMERCIAL"),
            "refresh_token": os.getenv("SHOPEE_REFRESH_TOKEN_COMERCIAL")
        },
        "TOYS": {
            "partner_id": os.getenv("SHOPEE_PARTNER_ID_TOYS"),
            "partner_key": os.getenv("SHOPEE_PARTNER_KEY_TOYS"),
            "shop_id": os.getenv("SHOPEE_SHOP_ID_TOYS"),
            "refresh_token": os.getenv("SHOPEE_REFRESH_TOKEN_TOYS")
        }
    }
    for account, cfg in accounts.items():
        access_token, refresh_token = refresh_tokens(
            account, cfg["partner_id"], cfg["partner_key"], cfg["shop_id"], cfg["refresh_token"]
        )
        if access_token:
            print(f"✅ Tokens atualizados para {account}")
        else:
            print(f"❌ Falha ao atualizar tokens para {account}")


atualizar_todos_tokens()


def fetch_orders(client, last_create_time):
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
                if 'error' in resp_json:
                    error_code = resp_json.get('error', '')
                    if error_code in ['error_auth', 'error_token']:
                        if refresh_tokens(client):
                            continue
                        else:
                            retries += 1
                            continue
                if 'response' not in resp_json:
                    logging.warning("Resposta da API inválida")
                    return []
                order_list = resp_json['response'].get('order_list', [])
                if not order_list:
                    return []
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
            if cursor.execute("SELECT 1 FROM pedidos WHERE order_sn = ?",
                              (order_data['order_sn'],)).fetchone():
                continue
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


def atualizar_preco_custo_em_vendas_shopee():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE vendas_shopee
        SET PRECO_CUSTO = (
            SELECT Custo
            FROM custo_shopee
            WHERE custo_shopee.SKU = vendas_shopee.SKU
        )
        WHERE SKU IN (SELECT SKU FROM custo_shopee)
    """)
    conn.commit()
    conn.close()
    print("✅ Coluna PRECO_CUSTO atualizada na tabela 'vendas_shopee'.")


def processar_vendas_shopee():
    conn = sqlite3.connect(DB_PATH)
    pedidos = pd.read_sql_query("SELECT * FROM pedidos", conn)

    # Verificar se há dados para processar
    if len(pedidos) == 0:
        print("⚠️ Nenhum pedido encontrado para processar")
        conn.close()
        return

    pedidos['DATA'] = pd.to_datetime(pedidos['create_time'], errors='coerce')
    pedidos['item_list'] = pedidos['item_list'].apply(json.loads)
    pedidos = pedidos.explode('item_list')
    itens_df = pd.json_normalize(pedidos['item_list'])
    itens_df.columns = [f'item_{col}' for col in itens_df.columns]
    pedidos = pedidos.drop(columns=['item_list']).reset_index(drop=True)
    pedidos = pd.concat([pedidos, itens_df], axis=1)

    def extrair_transportadora(pkg):
        try:
            pacotes = json.loads(pkg)
            if isinstance(pacotes, list) and len(pacotes) > 0:
                return pacotes[0].get('shipping_carrier', '')
        except:
            return ''
        return ''

    pedidos['TRANSPORTADORA'] = pedidos['package_list'].apply(extrair_transportadora)
    pedidos['TRANSPORTADORA'] = pedidos['TRANSPORTADORA'].replace({
        'SBS': 'Shopee Xpress',
        'SPX': 'Shopee Xpress',
        'STANDARD_EXPRESS': 'Shopee Xpress',
        'OTHER_LOGISTICS': 'Outros',
        'INHOUSE': 'Agência Shopee',
        'OWN_DELIVERY': 'Shopee Entrega Direta'
    })
    pedidos['PRECO_UNITARIO'] = pd.to_numeric(pedidos['item_model_discounted_price'], errors='coerce').fillna(0)
    pedidos['QTD_COMPRADA'] = pd.to_numeric(pedidos['item_model_quantity_purchased'], errors='coerce').fillna(0).astype(
        int)
    pedidos['FRETE_TOTAL'] = pd.to_numeric(pedidos['actual_shipping_fee'], errors='coerce').fillna(0)
    pedidos['VALOR_TOTAL'] = pedidos['PRECO_UNITARIO'] * pedidos['QTD_COMPRADA']
    pedidos['ITENS_PEDIDO'] = pedidos.groupby('order_sn')['order_sn'].transform('count')
    pedidos['FRETE_UNITARIO'] = pedidos['FRETE_TOTAL'] / pedidos['ITENS_PEDIDO'].replace(0, 1)  # Evita divisão por zero
    pedidos['DATA_ENTREGA'] = pd.NaT
    pedidos_completos = pedidos['order_status'] == 'COMPLETED'
    data_entregue_valida = pd.to_datetime(pedidos['Data_Entregue'], errors='coerce')
    pedidos.loc[pedidos_completos & data_entregue_valida.notna(), 'DATA_ENTREGA'] = data_entregue_valida

    # CORREÇÃO DO ERRO: Converter para numérico antes de arredondar
    pedidos['PRAZO_ENTREGA_DIAS'] = None
    mask_data_entrega = pedidos['DATA_ENTREGA'].notna()
    pedidos.loc[mask_data_entrega, 'PRAZO_ENTREGA_DIAS'] = (
            (pedidos.loc[mask_data_entrega, 'DATA_ENTREGA'] - pedidos.loc[mask_data_entrega, 'DATA'])
            .dt.total_seconds() / 86400
    )
    # Converter para numérico e arredondar
    pedidos['PRAZO_ENTREGA_DIAS'] = pd.to_numeric(pedidos['PRAZO_ENTREGA_DIAS'], errors='coerce').round(2)

    final = pedidos.rename(columns={
        'order_sn': 'PEDIDO_ID',
        'buyer_user_id': 'COMPRADOR_ID',
        'order_status': 'STATUS_PEDIDO',
        'account_type': 'TIPO_CONTA',
        'item_item_name': 'NOME_ITEM',
        'item_item_sku': 'SKU_ITEM',
        'item_model_sku': 'SKU_VARIACAO'
    })

    final['SKU'] = final.apply(
        lambda row: row['SKU_VARIACAO'] if pd.notna(row.get('SKU_VARIACAO')) and row['SKU_VARIACAO'] != '' else row.get(
            'SKU_ITEM', ''),
        axis=1
    )

    if 'TIPO_CONTA' not in final.columns:
        final['TIPO_CONTA'] = 'Desconhecido'

    conn_custo = sqlite3.connect(DB_PATH)

    # Verificar se a tabela custo_shopee existe
    cursor = conn_custo.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='custo_shopee'")
    tabela_existe = cursor.fetchone() is not None

    if tabela_existe:
        df_custo = pd.read_sql_query("SELECT SKU, Custo FROM custo_shopee", conn_custo)
        final = final.merge(df_custo, how='left', left_on='SKU', right_on='SKU')
        final.rename(columns={'Custo': 'PRECO_CUSTO'}, inplace=True)
        final['PRECO_CUSTO'] = pd.to_numeric(final['PRECO_CUSTO'], errors='coerce').fillna(0)
    else:
        print("⚠️ Tabela 'custo_shopee' não encontrada. Usando custo padrão 0.")
        final['PRECO_CUSTO'] = 0

    conn_custo.close()

    final['CUSTO_TOTAL_REAL'] = (final['PRECO_CUSTO'] * final['QTD_COMPRADA']).round(2)
    final['COMISSAO_UNITARIA'] = (final['VALOR_TOTAL'] * 0.22).round(2)
    final['TAXA_FIXA'] = (final['QTD_COMPRADA'] * 4.00).round(2)
    final['TOTAL_COM_FRETE'] = final['VALOR_TOTAL']

    # Mapeamento seguro de tipos de conta
    final['SM_CONTAS_PCT'] = final['TIPO_CONTA'].map({
        'TOYS': 9.27,
        'COMERCIAL': 7.06
    }).fillna(0)  # Preencher com 0 para valores não mapeados

    final['SM_CONTAS_REAIS'] = (final['TOTAL_COM_FRETE'] * final['SM_CONTAS_PCT'] / 100).round(2)
    final['REPASSE_ENVIO'] = 0
    mask_envio = final['TRANSPORTADORA'] == 'Shopee Entrega Direta'
    first_index = final[mask_envio].groupby(['PEDIDO_ID', 'COMPRADOR_ID', 'DATA']).head(1).index
    final.loc[first_index, 'REPASSE_ENVIO'] = 8
    final['CUSTO_FIXO'] = (final['VALOR_TOTAL'] * 0.13).round(2)

    final['CUSTO_OP_TOTAL'] = (
        final[['CUSTO_TOTAL_REAL', 'COMISSAO_UNITARIA', 'TAXA_FIXA', 'SM_CONTAS_REAIS']]
        .apply(pd.to_numeric, errors='coerce').sum(axis=1)
    ).round(2)

    final['MARGEM_CONTRIBUICAO'] = (final['VALOR_TOTAL'] - final['CUSTO_OP_TOTAL']).round(2)
    final['LUCRO_REAL'] = (final['MARGEM_CONTRIBUICAO'] - final['CUSTO_FIXO']).round(2)

    # Evitar divisão por zero no cálculo de porcentagem
    final['LUCRO_REAL_PCT'] = np.where(
        final['VALOR_TOTAL'] > 0,
        (final['LUCRO_REAL'] / final['VALOR_TOTAL'] * 100).round(2),
        0
    )

    colunas_finais = [
        'PEDIDO_ID', 'COMPRADOR_ID', 'STATUS_PEDIDO', 'TIPO_CONTA', 'DATA',
        'NOME_ITEM', 'SKU', 'QTD_COMPRADA', 'PRECO_UNITARIO', 'PRECO_CUSTO',
        'CUSTO_TOTAL_REAL',
        'VALOR_TOTAL', 'FRETE_UNITARIO', 'COMISSAO_UNITARIA', 'TAXA_FIXA',
        'TOTAL_COM_FRETE', 'SM_CONTAS_PCT', 'SM_CONTAS_REAIS',
        'TRANSPORTADORA', 'DATA_ENTREGA', 'PRAZO_ENTREGA_DIAS', 'CUSTO_OP_TOTAL',
        'MARGEM_CONTRIBUICAO', 'CUSTO_FIXO', 'LUCRO_REAL', 'LUCRO_REAL_PCT', 'REPASSE_ENVIO'
    ]

    # Garantir que todas as colunas existam
    for coluna in colunas_finais:
        if coluna not in final.columns:
            final[coluna] = None

    final = final[colunas_finais].drop_duplicates(subset=['PEDIDO_ID', 'SKU'], keep='first')
    final.to_sql("vendas_shopee", conn, if_exists="replace", index=False)
    conn.close()
    print("✅ Tabela 'vendas_shopee' atualizada com sucesso.")


def processar_repasses_shopee():
    conn = sqlite3.connect(DB_PATH)
    vendas = pd.read_sql_query("SELECT * FROM vendas_shopee", conn)

    if len(vendas) == 0:
        print("⚠️ Nenhuma venda encontrada para processar repasses")
        conn.close()
        return

    # Garante TIPO_CONTA e REPASSE_ENVIO na tabela repasses_shopee!
    if 'TIPO_CONTA' not in vendas.columns:
        vendas['TIPO_CONTA'] = 'Desconhecido'
    if 'REPASSE_ENVIO' not in vendas.columns:
        vendas['REPASSE_ENVIO'] = 0

    colunas_repasses = [
        'PEDIDO_ID', 'DATA', 'VALOR_TOTAL', 'COMISSAO_UNITARIA',
        'TAXA_FIXA', 'STATUS_PEDIDO', 'TRANSPORTADORA', 'TIPO_CONTA', 'REPASSE_ENVIO'
    ]

    repasses = vendas[colunas_repasses].drop_duplicates(subset=['PEDIDO_ID']).copy()

    def calcular_data_repasse(row):
        try:
            data_base = pd.to_datetime(row['DATA'])
            if row['TRANSPORTADORA'] == 'Shopee Entrega Direta':
                return (data_base + pd.Timedelta(days=7)).strftime('%Y-%m-%d')
            else:
                return (data_base + pd.Timedelta(days=15)).strftime('%Y-%m-%d')
        except:
            return None

    repasses['DATA_REPASSE'] = repasses.apply(calcular_data_repasse, axis=1)
    repasses.to_sql("repasses_shopee", conn, if_exists="replace", index=False)
    print(f"✅ Tabela 'repasses_shopee' atualizada com {len(repasses)} repasses.")

    cursor = conn.cursor()
    atualizar_entradas_financeiras(cursor)
    conn.commit()
    conn.close()


def main():
    print("\n=== AUTOMAÇÃO SHOPEE: INÍCIO ===")

    # Criar tabela custo_shopee se não existir
    criar_tabela_custo_shopee()

    conn = sqlite3.connect(DB_PATH)
    check_and_update_schema(conn)
    conn.close()
    print("[OK] Estrutura das tabelas garantida.")
    print("[PASSO 1] Buscando pedidos/vendas da Shopee...")
    for conta in ["COMERCIAL", "TOYS"]:
        try:
            logging.info(f"Iniciando processamento para conta {conta}")
            client = create_shopee_client(conta)
            pedidos = fetch_orders(client, int((datetime.now(timezone.utc) - timedelta(days=60)).timestamp()))
            conn = sqlite3.connect(DB_PATH)
            insert_orders(pedidos, conn, conta)
            conn.close()
            logging.info(f"Conta {conta} processada com sucesso")
        except Exception as e:
            logging.error(f"Erro na conta {conta}: {str(e)}", exc_info=True)
    print("[OK] Tabela 'pedidos' atualizada.")
    print("[PASSO 2] Processando vendas_shopee (normalizando, agregando, custos etc)...")
    processar_vendas_shopee()
    print("[PASSO 3] Atualizando repasses_shopee...")
    processar_repasses_shopee()
    print("=== FINALIZADO ===")


def padronizar_datas_vendas_shopee():
    from datetime import datetime
    import sqlite3

    print("[PADRÃO BR] Padronizando datas da tabela vendas_shopee...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("SELECT rowid, DATA, DATA_ENTREGA FROM vendas_shopee")
            registros = cursor.fetchall()

            for row in registros:
                rowid = row['rowid']
                data_formatada = ""
                data_entrega_formatada = ""

                try:
                    if row['DATA']:
                        data_formatada = datetime.fromisoformat(row['DATA']).strftime('%d/%m/%Y')
                except:
                    pass

                try:
                    if row['DATA_ENTREGA']:
                        data_entrega_formatada = datetime.fromisoformat(row['DATA_ENTREGA']).strftime('%d/%m/%Y')
                except:
                    pass

                cursor.execute("""
                        UPDATE vendas_shopee SET
                            DATA = ?, 
                            DATA_ENTREGA = ?
                        WHERE rowid = ?
                    """, (data_formatada, data_entrega_formatada, rowid))

            conn.commit()
            print(f"✓ Datas padronizadas com sucesso: {len(registros)} registros atualizados")

    except Exception as e:
        print(f"✗ Erro ao padronizar datas: {str(e)}")
        raise


if __name__ == "__main__":
    main()
    padronizar_datas_vendas_shopee()