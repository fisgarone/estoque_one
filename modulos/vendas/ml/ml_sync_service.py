import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
from typing import List, Dict, Any, Optional

import requests
import schedule

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ml_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('MLSync')


@dataclass
class MLAccount:
    name: str
    client_id: str
    client_secret: str
    access_token: str
    refresh_token: str
    seller_id: str
    last_sync: Optional[datetime] = None


class MLSyncService:
    def __init__(self, db_path: str = 'fisgarone.db'):
        """
        Serviço de sincronização do Mercado Livre
        Usa fisgarone.db como banco de dados global
        """
        self.db_path = db_path
        self.accounts: List[MLAccount] = []
        self.lock = Lock()
        self.sync_interval = int(os.getenv('ML_SYNC_INTERVAL', '300'))
        self._load_accounts_from_master_env()
        self._init_database()

    def _load_accounts_from_master_env(self):
        """Carrega as contas do padrão existente no .env"""
        accounts_config = [
            {
                'name': 'TOYS',
                'client_id': os.getenv('CLIENT_ID_TOYS'),
                'client_secret': os.getenv('CLIENT_SECRET_TOYS'),
                'access_token': os.getenv('ACCESS_TOKEN_TOYS'),
                'refresh_token': os.getenv('REFRESH_TOKEN_TOYS'),
                'seller_id': os.getenv('SELLER_ID_TOYS')
            },
            {
                'name': 'COMERCIAL',
                'client_id': os.getenv('CLIENT_ID_COMERCIAL'),
                'client_secret': os.getenv('CLIENT_SECRET_COMERCIAL'),
                'access_token': os.getenv('ACCESS_TOKEN_COMERCIAL'),
                'refresh_token': os.getenv('REFRESH_TOKEN_COMERCIAL'),
                'seller_id': os.getenv('SELLER_ID_COMERCIAL')
            },
            {
                'name': 'PESCA',
                'client_id': os.getenv('CLIENT_ID_PESCA'),
                'client_secret': os.getenv('CLIENT_SECRET_PESCA'),
                'access_token': os.getenv('ACCESS_TOKEN_PESCA'),
                'refresh_token': os.getenv('REFRESH_TOKEN_PESCA'),
                'seller_id': os.getenv('SELLER_ID_PESCA')
            },
            {
                'name': 'CAMPING',
                'client_id': os.getenv('CLIENT_ID_CAMPING'),
                'client_secret': os.getenv('CLIENT_SECRET_CAMPING'),
                'access_token': os.getenv('ACCESS_TOKEN_CAMPING'),
                'refresh_token': os.getenv('REFRESH_TOKEN_CAMPING'),
                'seller_id': os.getenv('SELLER_ID_CAMPING')
            }
        ]

        for config in accounts_config:
            # Remove aspas simples se existirem
            access_token = config['access_token']
            refresh_token = config['refresh_token']

            if access_token and access_token.startswith("'") and access_token.endswith("'"):
                access_token = access_token[1:-1]
            if refresh_token and refresh_token.startswith("'") and refresh_token.endswith("'"):
                refresh_token = refresh_token[1:-1]

            if all([config['name'], config['client_id'], config['client_secret'], access_token]):
                self.accounts.append(MLAccount(
                    name=config['name'],
                    client_id=config['client_id'],
                    client_secret=config['client_secret'],
                    access_token=access_token,
                    refresh_token=refresh_token,
                    seller_id=config['seller_id']
                ))
                logger.info(f"Conta carregada: {config['name']}")
            else:
                missing = []
                if not config['name']:
                    missing.append('NAME')
                if not config['client_id']:
                    missing.append('CLIENT_ID')
                if not config['client_secret']:
                    missing.append('CLIENT_SECRET')
                if not access_token:
                    missing.append('ACCESS_TOKEN')
                logger.warning(f"Conta {config['name']} incompleta. Campos faltando: {missing}")

        if not self.accounts:
            logger.error("Nenhuma conta do Mercado Livre configurada corretamente")
        else:
            logger.info(f"Total de {len(self.accounts)} contas carregadas do .env master")

    def _init_database(self):
        """
        Inicializa/valida a estrutura do banco de dados
        Usa fisgarone.db como banco global
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        # Verifica se a tabela existe e cria se necessário em fisgarone.db
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS vendas_ml
                       (
                           "ID Pedido"
                           TEXT
                           PRIMARY
                           KEY,
                           "Preco Unitario"
                           REAL,
                           "Quantidade"
                           INTEGER,
                           "Data da Venda"
                           TEXT,
                           "Taxa Mercado Livre"
                           REAL,
                           "Frete"
                           REAL,
                           "Conta"
                           TEXT,
                           "Cancelamentos"
                           TEXT,
                           "Titulo"
                           TEXT,
                           "MLB"
                           TEXT,
                           "SKU"
                           TEXT,
                           "Codigo Envio"
                           TEXT,
                           "Comprador"
                           TEXT,
                           "Modo Envio"
                           TEXT,
                           "Custo Frete Base"
                           REAL,
                           "Custo Frete Opcional"
                           REAL,
                           "Custo Pedido Frete"
                           REAL,
                           "Custo Lista Frete"
                           REAL,
                           "Custo Total Frete"
                           REAL,
                           "Tipo Logistica"
                           TEXT,
                           "Pago Por"
                           TEXT,
                           "Situacao"
                           TEXT,
                           "Situacao Entrega"
                           TEXT,
                           "Data Liberacao"
                           TEXT,
                           "Taxa Fixa ML"
                           REAL,
                           "Comissoes"
                           REAL,
                           "Comissao (%)"
                           REAL,
                           "Preço Custo ML"
                           REAL,
                           "Custo Total Calculado"
                           REAL,
                           "Aliquota (%)"
                           REAL,
                           "Imposto R$"
                           REAL,
                           "Frete Comprador"
                           REAL,
                           "Frete Seller"
                           REAL,
                           "Custo Operacional"
                           REAL,
                           "Total Custo Operacional"
                           REAL,
                           "MC Total"
                           REAL,
                           "Custo Fixo"
                           REAL,
                           "Lucro Real"
                           REAL,
                           "Lucro Real %"
                           REAL,
                           "Data Sincronizacao"
                           TEXT,
                           "Ultima Atualizacao"
                           TEXT
                       )
                       ''')

        # Índices para performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vendas_ml_conta ON vendas_ml("Conta")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vendas_ml_data ON vendas_ml("Data da Venda")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vendas_ml_sku ON vendas_ml("SKU")')

        conn.commit()
        conn.close()
        logger.info("Banco de dados fisgarone.db inicializado/validado")

    def get_connection(self):
        """Retorna conexão com o banco definido em self.db_path"""
        return sqlite3.connect(self.db_path)

    def _convert_to_br_date(self, date_string):
        """Converte data do formato internacional para formato Brasil (DD/MM/AAAA)"""
        if not date_string:
            return ''

        try:
            # Remove o Z do final se existir
            if date_string.endswith('Z'):
                date_string = date_string[:-1] + '+00:00'

            # Converte para datetime
            dt = datetime.fromisoformat(date_string)

            # Formata para Brasil
            return dt.strftime('%d/%m/%Y')
        except Exception as e:
            logger.warning(f"Erro ao converter data {date_string}: {e}")
            return date_string

    def _refresh_token(self, account: MLAccount) -> bool:
        """Atualiza o token de acesso usando refresh token"""
        try:
            url = f"{os.getenv('API_URL', 'https://api.mercadolibre.com')}/oauth/token"
            data = {
                'grant_type': 'refresh_token',
                'client_id': account.client_id,
                'client_secret': account.client_secret,
                'refresh_token': account.refresh_token
            }

            response = requests.post(url, data=data, timeout=30)
            if response.status_code == 200:
                token_data = response.json()
                account.access_token = token_data['access_token']
                account.refresh_token = token_data.get('refresh_token', account.refresh_token)
                logger.info(f"Token atualizado para conta: {account.name}")
                return True
            else:
                logger.error(f"Falha ao atualizar token: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Erro ao atualizar token: {e}")
            return False

    def _make_api_request(self, account: MLAccount, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Faz requisição para API do Mercado Livre com tratamento de erro"""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                headers = {
                    'Authorization': f'Bearer {account.access_token}',
                    'Content-Type': 'application/json'
                }

                url = f"{os.getenv('API_URL', 'https://api.mercadolibre.com')}{endpoint}"
                response = requests.get(url, headers=headers, params=params, timeout=30)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401 and attempt < max_retries - 1:
                    logger.warning(f"Token expirado, tentando renovar... (tentativa {attempt + 1})")
                    if self._refresh_token(account):
                        time.sleep(retry_delay)
                        continue
                else:
                    logger.error(f"Erro API ML [{response.status_code}]: {response.text}")
                    break

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout na requisição (tentativa {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
            except Exception as e:
                logger.error(f"Erro na requisição API: {e}")
                break

        return None

    def _calcular_custos_ml_corretos(self, order: Dict, account: MLAccount) -> Dict[str, Any]:
        """
        Calcula custos do Mercado Livre baseado nas políticas definidas.
        Busca políticas em fisgarone.db (self.db_path).
        """
        try:
            # Dados do pedido
            order_items = order.get('order_items', [])
            first_item = order_items[0] if order_items else {}
            unit_price = first_item.get('unit_price', 0)
            quantity = first_item.get('quantity', 1)
            faturamento_bruto = unit_price * quantity

            # Conexão com políticas
            pol_conn = sqlite3.connect(self.db_path)
            pol_cur = pol_conn.cursor()

            # Buscar política para ML
            pol_cur.execute('''
                            SELECT *
                            FROM politicas_canais
                            WHERE canal = 'ml'
                              AND plano = 'padrao'
                              AND preco_unit_min <= ?
                              AND (preco_unit_max > ? OR preco_unit_max IS NULL)
                              AND ativo = 1
                            ''', (unit_price, unit_price))

            politica_row = pol_cur.fetchone()
            if not politica_row:
                # Política padrão se não encontrar
                col_names = [desc[0] for desc in pol_cur.description]
                politica = dict(zip(col_names, [None] * len(col_names)))
                politica.update({
                    'comissao_percent_base': None,
                    'taxa_fixa_tipo': 'NENHUMA',
                    'taxa_fixa_valor': 0,
                    'frete_seller_tipo': 'NENHUM',
                    'frete_seller_valor': 0,
                    'insumos_percent': 0.015,
                    'ads_percent': 0.035
                })
            else:
                col_names = [desc[0] for desc in pol_cur.description]
                politica = dict(zip(col_names, politica_row))

            # Buscar política CNPJ
            pol_cur.execute('''
                            SELECT *
                            FROM politicas_cnpj
                            WHERE conta = ?
                              AND ativo = 1
                            ''', (account.name,))

            politica_cnpj_row = pol_cur.fetchone()
            if politica_cnpj_row:
                col_names = [desc[0] for desc in pol_cur.description]
                politica_cnpj = dict(zip(col_names, politica_cnpj_row))
            else:
                politica_cnpj = {
                    'custo_estrutura_percent': 0.13,
                    'aliquota_fiscal_percent': 0.0706
                }

            # 1. Taxa fixa por faixa (ML < 79)
            taxa_fixa_unit = 0
            if politica.get('taxa_fixa_tipo') == 'POR_UNIDADE_FAIXA' and unit_price < 79:
                pol_cur.execute('''
                                SELECT valor
                                FROM politicas_canais_faixas
                                WHERE canal = 'ml'
                                  AND plano = 'padrao'
                                  AND preco_unit_min <= ?
                                  AND preco_unit_max > ?
                                  AND ativo = 1
                                ''', (unit_price, unit_price))
                faixa = pol_cur.fetchone()
                if faixa:
                    taxa_fixa_unit = faixa[0]
            elif politica.get('taxa_fixa_tipo') == 'POR_UNIDADE':
                taxa_fixa_unit = politica.get('taxa_fixa_valor', 0)

            pol_conn.close()

            taxa_fixa_total = taxa_fixa_unit * quantity

            # 2. Comissões - estimativa inicial
            comissao_percent = 0.12  # Estimativa padrão
            comissao_unit = unit_price * comissao_percent
            comissao_total = comissao_unit * quantity

            # 3. Frete Seller (ML >= 79)
            frete_seller_unit = 0
            if (
                    politica.get('frete_seller_tipo') == 'POR_UNIDADE'
                    and politica.get('frete_seller_valor', 0) > 0
                    and unit_price >= 79
            ):
                frete_seller_unit = politica.get('frete_seller_valor', 0)

            frete_seller_total = frete_seller_unit * quantity

            # 4. Custos variáveis
            insumos_percent = politica.get('insumos_percent', 0.015)
            ads_percent = politica.get('ads_percent', 0.035)

            custo_insumos = faturamento_bruto * insumos_percent
            custo_ads = faturamento_bruto * ads_percent

            # 5. Custos fixos e fiscais
            custo_estrutura_percent = politica_cnpj.get('custo_estrutura_percent', 0.13)
            aliquota_fiscal_percent = politica_cnpj.get('aliquota_fiscal_percent', 0.0706)

            custo_estrutura = faturamento_bruto * custo_estrutura_percent
            imposto_rs = faturamento_bruto * aliquota_fiscal_percent

            # 6. Custo operacional (soma de custos variáveis)
            custo_operacional = custo_insumos + custo_ads

            # 7. Preço de custo (estimativa)
            preco_custo_ml = faturamento_bruto * 0.6  # 40% margem padrão

            custo_total_calculado = preco_custo_ml + custo_operacional + imposto_rs + custo_estrutura

            # 8. Taxa Mercado Livre total (sale_fee)
            taxa_ml_total = comissao_total + taxa_fixa_total

            # 9. Lucro real
            lucro_real = faturamento_bruto - custo_total_calculado - taxa_ml_total - frete_seller_total
            lucro_real_percent = (lucro_real / faturamento_bruto) if faturamento_bruto > 0 else 0

            return {
                "Taxa Mercado Livre": taxa_ml_total,
                "Taxa Fixa ML": taxa_fixa_total,
                "Comissoes": comissao_total,
                "Comissao (%)": comissao_percent,
                "Preço Custo ML": preco_custo_ml,
                "Custo Total Calculado": custo_total_calculado,
                "Aliquota (%)": aliquota_fiscal_percent,
                "Imposto R$": imposto_rs,
                "Frete Seller": frete_seller_total,
                "Custo Operacional": custo_operacional,
                "Total Custo Operacional": custo_operacional,
                "Custo Fixo": custo_estrutura,
                "Lucro Real": lucro_real,
                "Lucro Real %": lucro_real_percent
            }

        except Exception as e:
            logger.error(f"Erro cálculo custos ML: {e}")
            # Retorno padrão em caso de erro
            return {
                "Taxa Mercado Livre": 0,
                "Taxa Fixa ML": 0,
                "Comissoes": 0,
                "Comissao (%)": 0,
                "Preço Custo ML": 0,
                "Custo Total Calculado": 0,
                "Aliquota (%)": 0,
                "Imposto R$": 0,
                "Frete Seller": 0,
                "Custo Operacional": 0,
                "Total Custo Operacional": 0,
                "Custo Fixo": 0,
                "Lucro Real": 0,
                "Lucro Real %": 0
            }

    def _parse_order_data(self, order: Dict, account: MLAccount) -> Dict[str, Any]:
        """
        Parseia os dados do pedido da API para o formato do banco
        Persiste em fisgarone.db
        """
        try:
            # Informações básicas do pedido
            order_id = order.get('id', '')

            # CONVERSÃO DE DATA - Formato Brasil
            date_created_raw = order.get('date_created', '')
            date_created = self._convert_to_br_date(date_created_raw)

            status = order.get('status', '')

            # Informações de shipping
            shipping = order.get('shipping', {})

            # Itens do pedido
            order_items = order.get('order_items', [])
            first_item = order_items[0] if order_items else {}

            # Dados do item
            item_title = first_item.get('item', {}).get('title', '')
            item_id = first_item.get('item', {}).get('id', '')
            quantity = first_item.get('quantity', 1)
            unit_price = first_item.get('unit_price', 0)

            # Dados financeiros
            shipping_cost = shipping.get('cost', 0)

            # Calcular custos corretamente
            custos = self._calcular_custos_ml_corretos(order, account)

            return {
                "ID Pedido": order_id,
                "Preco Unitario": unit_price,
                "Quantidade": quantity,
                "Data da Venda": date_created,
                "Taxa Mercado Livre": custos["Taxa Mercado Livre"],
                "Frete": shipping_cost,
                "Conta": account.name,
                "Cancelamentos": "",
                "Titulo": item_title,
                "MLB": item_id,
                "SKU": "",
                "Codigo Envio": shipping.get('id', ''),
                "Comprador": order.get('buyer', {}).get('id', ''),
                "Modo Envio": shipping.get('mode', ''),
                "Custo Frete Base": shipping_cost * 0.5,
                "Custo Frete Opcional": shipping_cost * 0.2,
                "Custo Pedido Frete": shipping_cost * 0.1,
                "Custo Lista Frete": shipping_cost * 0.1,
                "Custo Total Frete": shipping_cost * 0.9,
                "Tipo Logistica": shipping.get('logistic_type', ''),
                "Pago Por": "buyer" if shipping_cost > 0 else "seller",
                "Situacao": status,
                "Situacao Entrega": shipping.get('status', ''),
                "Data Liberacao": self._convert_to_br_date(order.get('date_closed', '')),
                "Taxa Fixa ML": custos["Taxa Fixa ML"],
                "Comissoes": custos["Comissoes"],
                "Comissao (%)": custos["Comissao (%)"],
                "Preço Custo ML": custos["Preço Custo ML"],
                "Custo Total Calculado": custos["Custo Total Calculado"],
                "Aliquota (%)": custos["Aliquota (%)"],
                "Imposto R$": custos["Imposto R$"],
                "Frete Comprador": shipping_cost * 0.3,
                "Frete Seller": custos["Frete Seller"],
                "Custo Operacional": custos["Custo Operacional"],
                "Total Custo Operacional": custos["Total Custo Operacional"],
                "MC Total": (unit_price * quantity)
                            - custos["Preço Custo ML"]
                            - custos["Custo Operacional"]
                            - custos["Imposto R$"],
                "Custo Fixo": custos["Custo Fixo"],
                "Lucro Real": custos["Lucro Real"],
                "Lucro Real %": custos["Lucro Real %"],
                "Data Sincronizacao": datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
                "Ultima Atualizacao": datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            }

        except Exception as e:
            logger.error(f"Erro ao parsear dados do pedido {order.get('id', 'unknown')}: {e}")
            return {}

    def _save_order_data(self, order_data: Dict) -> str:
        """
        Salva ou atualiza os dados do pedido no banco fisgarone.db
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Verifica se o pedido já existe
            cursor.execute(
                'SELECT "ID Pedido" FROM vendas_ml WHERE "ID Pedido" = ?',
                (order_data['ID Pedido'],)
            )
            existing = cursor.fetchone()

            if existing:
                # Atualiza pedido existente
                set_clause = ', '.join([f'"{key}" = ?' for key in order_data.keys()])
                values = list(order_data.values()) + [order_data['ID Pedido']]

                cursor.execute(
                    f'UPDATE vendas_ml SET {set_clause} WHERE "ID Pedido" = ?',
                    values
                )
                action = 'updated'
            else:
                # Insere novo pedido
                columns = ', '.join([f'"{key}"' for key in order_data.keys()])
                placeholders = ', '.join(['?' for _ in order_data])

                cursor.execute(
                    f'INSERT INTO vendas_ml ({columns}) VALUES ({placeholders})',
                    list(order_data.values())
                )
                action = 'inserted'

            conn.commit()
            return action

        except Exception as e:
            logger.error(f"Erro ao salvar pedido {order_data.get('ID Pedido', 'desconhecido')}: {e}")
            conn.rollback()
            return 'error'
        finally:
            conn.close()

    def sync_account_orders(self, account: MLAccount, days_back: int = 30) -> Dict[str, Any]:
        """
        Sincroniza pedidos de uma conta específica
        Persiste em fisgarone.db
        """
        stats = {
            'account': account.name,
            'total_orders': 0,
            'new_orders': 0,
            'updated_orders': 0,
            'errors': 0,
            'start_time': datetime.now()
        }

        try:
            # Calcula data de início para busca
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00.000-00:00')

            # Usa seller_id do .env master
            seller_id = account.seller_id
            if not seller_id:
                logger.error(f"Seller ID não configurado para conta {account.name}")
                stats['errors'] += 1
                return stats

            # Parâmetros da busca
            params = {
                'seller': seller_id,
                'order.date_created.from': start_date,
                'limit': 50,
                'offset': 0
            }

            all_orders = []
            has_more = True

            # Paginação para buscar todos os pedidos
            while has_more:
                orders_data = self._make_api_request(account, '/orders/search', params)

                if not orders_data:
                    break

                orders = orders_data.get('results', [])
                all_orders.extend(orders)

                # Verifica se há mais páginas
                has_more = len(orders) == params['limit']
                params['offset'] += params['limit']

                # Delay para não sobrecarregar a API
                time.sleep(0.5)

            stats['total_orders'] = len(all_orders)

            # Processa cada pedido
            for order in all_orders:
                try:
                    order_data = self._parse_order_data(order, account)
                    if order_data:
                        success = self._save_order_data(order_data)
                        if success == 'inserted':
                            stats['new_orders'] += 1
                        elif success == 'updated':
                            stats['updated_orders'] += 1
                    else:
                        stats['errors'] += 1

                except Exception as e:
                    logger.error(f"Erro ao processar pedido {order.get('id', 'unknown')}: {e}")
                    stats['errors'] += 1

            account.last_sync = datetime.now()
            logger.info(f"Sincronização concluída para {account.name}: {stats}")

        except Exception as e:
            logger.error(f"Erro na sincronização da conta {account.name}: {e}")
            stats['errors'] += 1

        stats['end_time'] = datetime.now()
        stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()

        return stats

    def sync_all_accounts(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Sincroniza todas as contas configuradas
        Persiste em fisgarone.db
        """
        with self.lock:
            total_stats = {
                'total_accounts': len(self.accounts),
                'successful_accounts': 0,
                'failed_accounts': 0,
                'total_new_orders': 0,
                'total_updated_orders': 0,
                'total_errors': 0,
                'start_time': datetime.now(),
                'account_stats': []
            }

            for account in self.accounts:
                logger.info(f"Iniciando sincronização para: {account.name}")
                stats = self.sync_account_orders(account, days_back)
                total_stats['account_stats'].append(stats)

                if stats['errors'] == 0:
                    total_stats['successful_accounts'] += 1
                else:
                    total_stats['failed_accounts'] += 1

                total_stats['total_new_orders'] += stats['new_orders']
                total_stats['total_updated_orders'] += stats['updated_orders']
                total_stats['total_errors'] += stats['errors']

                # Delay entre contas para não sobrecarregar
                time.sleep(1)

            total_stats['end_time'] = datetime.now()
            total_stats['duration'] = (total_stats['end_time'] - total_stats['start_time']).total_seconds()

            logger.info(f"Sincronização completa: {total_stats}")
            return total_stats

    def start_auto_sync(self):
        """
        Inicia a sincronização automática em background.

        Respeita a variável de ambiente ML_AUTO_SYNC:
        - Se não estiver habilitada (0, vazio, false), não agenda nada.
        - Se a lib schedule estiver incorreta (sem .every), não quebra a app.
        """

        auto_flag = os.getenv('ML_AUTO_SYNC', '0').strip().lower()
        if auto_flag not in ('1', 'true', 'sim', 'yes', 'on'):
            logger.info("Sincronização automática ML desativada (ML_AUTO_SYNC não habilitada).")
            return

        if not hasattr(schedule, "every"):
            logger.error("Biblioteca 'schedule' não possui método 'every'. Verifique a instalação do pacote.")
            return

        def sync_job():
            logger.info("Executando sincronização automática do Mercado Livre...")
            self.sync_all_accounts(days_back=7)

        # Agenda sincronização a cada X segundos
        schedule.every(self.sync_interval).seconds.do(sync_job)

        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(1)

        # Inicia em thread separada
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info(f"Sincronização automática iniciada (intervalo: {self.sync_interval}s)")

    def get_sync_status(self) -> Dict[str, Any]:
        """Retorna status atual da sincronização"""
        status = {
            'total_accounts': len(self.accounts),
            'last_sync': None,
            'accounts_status': []
        }

        for account in self.accounts:
            status['accounts_status'].append({
                'name': account.name,
                'last_sync': account.last_sync.isoformat() if account.last_sync else None,
                'configured': bool(account.access_token),
                'seller_id': account.seller_id
            })

            if account.last_sync and (not status['last_sync'] or account.last_sync > status['last_sync']):
                status['last_sync'] = account.last_sync

        return status


# Instância global do serviço
sync_service = MLSyncService()

# Estado da sincronização
sync_status = {
    'is_syncing': False,
    'last_sync': None,
    'last_stats': None,
    'current_operation': None
}
