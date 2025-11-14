import sqlite3
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import json
import hashlib
from calendar import monthrange



class ShopeeCalculos:
    def __init__(self, db_path: str):
        """
        Cálculos para análise de vendas Shopee.
        -> LÓGICA CORRIGIDA NA LEITURA (sem alterar o .db):
           - VALOR_TOTAL = PRECO_UNITARIO * QTD_COMPRADA
           - COMISSAO_UNITARIA = 22% * VALOR_TOTAL
           - TAXA_FIXA = 4,50 * QTD_COMPRADA
           - TOTAL_COM_FRETE = VALOR_TOTAL + FRETE_UNITARIO
           - SM_CONTAS_REAIS = (SM_CONTAS_PCT/100) * TOTAL_COM_FRETE
           - CUSTO_TOTAL_REAL (CMV) = PRECO_CUSTO * QTD_COMPRADA
           - OP_4_5 = 4,5% * VALOR_TOTAL
           - CUSTO_OP_TOTAL = CMV + COMISSÃO + TAXA_FIXA + SM + OP_4_5   [inclui 4,5%]
           - MARGEM_CONTRIBUICAO = VALOR_TOTAL - CUSTO_OP_TOTAL           [MC já inclui o 4,5%]
           - CUSTO_FIXO = 13% * VALOR_TOTAL
           - LUCRO_REAL = MARGEM_CONTRIBUICAO - CUSTO_FIXO                [SEM repasse_envio]
        """
        if not db_path or not Path(db_path).exists():
            raise FileNotFoundError(
                f"ShopeeCalculos não recebeu um caminho válido para o banco de dados ou o arquivo não existe: {db_path}"
            )

        self.db_path = db_path
        print(f"✅ [ShopeeCalculos] Usando DB em: {self.db_path}")
        self._verify_database_integrity()
        self._cache: Dict[str, Dict[str, Any]] = {}

    # ===================== Infra =====================

    def get_connection(self) -> sqlite3.Connection:
        """Conexão SQLite."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-10000")
        return conn

    def _verify_database_integrity(self) -> None:
        """Verifica existência da tabela e colunas mínimas."""
        required_columns = {
            "PEDIDO_ID", "DATA", "VALOR_TOTAL", "LUCRO_REAL", "MARGEM_CONTRIBUICAO",
            "TRANSPORTADORA", "TIPO_CONTA", "QTD_COMPRADA", "PRECO_UNITARIO",
            "PRECO_CUSTO", "SKU", "NOME_ITEM", "FRETE_UNITARIO",
            "SM_CONTAS_PCT", "SM_CONTAS_REAIS", "TAXA_FIXA", "COMISSAO_UNITARIA",
            "CUSTO_TOTAL_REAL", "CUSTO_OP_TOTAL", "CUSTO_FIXO", "LUCRO_REAL_PCT",
            "REPASSE_ENVIO"
        }
        conn = self.get_connection()
        cur = conn.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vendas_shopee'")
        if not cur.fetchone():
            raise ValueError("Tabela 'vendas_shopee' não encontrada")

        cur.execute("PRAGMA table_info(vendas_shopee)")
        existing = {col[1] for col in cur.fetchall()}
        missing = required_columns - existing
        if missing:
            print(f"⚠️ Colunas ausentes (seguiremos mesmo assim): {missing}")

        cur.execute("SELECT COUNT(*) FROM vendas_shopee")
        count = cur.fetchone()[0]
        print(f"✅ Tabela válida com {count} registros")

        conn.close()

    def _generate_cache_key(self, filtros: Optional[Dict[str, Any]] = None) -> str:
        if not filtros:
            return "no_filters"
        filtros_str = json.dumps(filtros, sort_keys=True, default=str)
        return hashlib.md5(filtros_str.encode()).hexdigest()

    def _build_where_clause(self, filtros: Optional[Dict[str, Any]] = None) -> Tuple[str, List[Any]]:
        """
        WHERE tolerante: DATA no DB em DD/MM/YYYY (ou ISO),
        filtros do front em YYYY-MM-DD.
        AGORA COM SUPORTE PARA FILTRO DE STATUS.
        """
        if not filtros:
            return "", []
        conditions, params = [], []

        db_date_as_iso = (
            "CASE WHEN substr(DATA,3,1)='/' "
            "THEN substr(DATA,7,4) || '-' || substr(DATA,4,2) || '-' || substr(DATA,1,2) "
            "ELSE substr(DATA,1,10) END"
        )

        di = filtros.get("data_inicio")
        df = filtros.get("data_fim")
        if di:
            conditions.append(f"{db_date_as_iso} >= ?")
            params.append(di)
        if df:
            conditions.append(f"{db_date_as_iso} <= ?")
            params.append(df)

        if filtros.get("transportadora"):
            t = str(filtros["transportadora"])
            conditions.append("(TRANSPORTADORA = ? OR TRANSPORTADORA = UPPER(?) OR TRANSPORTADORA = LOWER(?))")
            params.extend([t, t, t])

        if filtros.get("tipo_conta"):
            c = str(filtros["tipo_conta"])
            conditions.append("(TIPO_CONTA = ? OR TIPO_CONTA = UPPER(?) OR TIPO_CONTA = LOWER(?))")
            params.extend([c, c, c])

        # ===== NOVO: FILTRO DE STATUS DO PEDIDO =====
        if filtros.get("status_pedido"):
            status = str(filtros["status_pedido"]).lower()

            if status == "confirmadas":
                # Apenas vendas com lucro positivo (confirmadas e pagas)
                conditions.append("LUCRO_REAL_CORR > 0")
            elif status == "cancelados_nao_pagos":
                # Vendas com lucro negativo ou zero (canceladas/não pagas)
                conditions.append("LUCRO_REAL_CORR <= 0")
            # status "todas" não adiciona filtro

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        return where_clause, params
    # ===================== Seleção corrigida (core da cirurgia) =====================

    def _select_corr_base(self) -> str:
        """
        Retorna a CTE que calcula todas as colunas CORRIGIDAS,
        sem alterar os dados gravados.
        - Nomes *_CORR são internos; no SELECT final mapeamos para os nomes esperados pelo front.
        """
        # Importante: em CTEs do SQLite não dá para referenciar alias na mesma SELECT;
        # por isso repetimos expressões onde necessário.
        return f"""
        WITH base AS (
            SELECT
                PEDIDO_ID,
                DATA,
                TRANSPORTADORA,
                TIPO_CONTA,
                NOME_ITEM,
                SKU,
                QTD_COMPRADA,
                COALESCE(PRECO_UNITARIO,0.0)    AS PRECO_UNITARIO,
                COALESCE(PRECO_CUSTO,0.0)       AS PRECO_CUSTO,
                COALESCE(FRETE_UNITARIO,0.0)    AS FRETE_UNITARIO,
                COALESCE(SM_CONTAS_PCT,0.0)     AS SM_CONTAS_PCT,
                COALESCE(REPASSE_ENVIO,0.0)     AS REPASSE_ENVIO
            FROM vendas_shopee
        ),
        corr AS (
            SELECT
                PEDIDO_ID, DATA, TRANSPORTADORA, TIPO_CONTA, NOME_ITEM, SKU, QTD_COMPRADA,
                PRECO_UNITARIO, PRECO_CUSTO, FRETE_UNITARIO, SM_CONTAS_PCT, REPASSE_ENVIO,

                ROUND(PRECO_UNITARIO * QTD_COMPRADA, 2)                                    AS VALOR_TOTAL_CORR,
                ROUND(0.22 * (PRECO_UNITARIO * QTD_COMPRADA), 2)                           AS COMISSAO_UNITARIA_CORR,
                ROUND(4.50 * QTD_COMPRADA, 2)                                              AS TAXA_FIXA_CORR,
                ROUND((PRECO_UNITARIO * QTD_COMPRADA) + FRETE_UNITARIO, 2)                 AS TOTAL_COM_FRETE_CORR,
                ROUND((SM_CONTAS_PCT/100.0) * ((PRECO_UNITARIO * QTD_COMPRADA) + FRETE_UNITARIO), 2) AS SM_CONTAS_REAIS_CORR,
                ROUND(PRECO_CUSTO * QTD_COMPRADA, 2)                                       AS CUSTO_TOTAL_REAL_CORR,
                ROUND(0.045 * (PRECO_UNITARIO * QTD_COMPRADA), 2)                          AS OP_4_5_CORR,

                /* Custo que incide sobre a venda (inclui 4,5%) */
                ROUND(
                    (PRECO_CUSTO * QTD_COMPRADA) +
                    (0.22 * (PRECO_UNITARIO * QTD_COMPRADA)) +
                    (4.50 * QTD_COMPRADA) +
                    ((SM_CONTAS_PCT/100.0) * ((PRECO_UNITARIO * QTD_COMPRADA) + FRETE_UNITARIO)) +
                    (0.045 * (PRECO_UNITARIO * QTD_COMPRADA))
                , 2) AS CUSTO_OP_TOTAL_CORR,

                /* MC já descontando todo custo incidente (inclusive 4,5%) */
                ROUND(
                    (PRECO_UNITARIO * QTD_COMPRADA) -
                    (
                        (PRECO_CUSTO * QTD_COMPRADA) +
                        (0.22 * (PRECO_UNITARIO * QTD_COMPRADA)) +
                        (4.50 * QTD_COMPRADA) +
                        ((SM_CONTAS_PCT/100.0) * ((PRECO_UNITARIO * QTD_COMPRADA) + FRETE_UNITARIO)) +
                        (0.045 * (PRECO_UNITARIO * QTD_COMPRADA))
                    )
                , 2) AS MARGEM_CONTRIBUICAO_CORR,

                ROUND(0.13 * (PRECO_UNITARIO * QTD_COMPRADA), 2)                            AS CUSTO_FIXO_CORR,

                /* LUCRO_REAL sem repasse_envio (pedido do cliente) */
                ROUND(
                    (
                        (PRECO_UNITARIO * QTD_COMPRADA) -
                        (
                            (PRECO_CUSTO * QTD_COMPRADA) +
                            (0.22 * (PRECO_UNITARIO * QTD_COMPRADA)) +
                            (4.50 * QTD_COMPRADA) +
                            ((SM_CONTAS_PCT/100.0) * ((PRECO_UNITARIO * QTD_COMPRADA) + FRETE_UNITARIO)) +
                            (0.045 * (PRECO_UNITARIO * QTD_COMPRADA))
                        )
                    ) - (0.13 * (PRECO_UNITARIO * QTD_COMPRADA))
                , 2) AS LUCRO_REAL_CORR
            FROM base
        )
        """

    def _select_columns_mapeadas(self) -> str:
        """
        Mapeia as colunas *_CORR para os nomes que o front já consome.
        """
        return """
            PEDIDO_ID AS id_venda,
            NOME_ITEM AS produto,
            SKU,
            QTD_COMPRADA,
            PRECO_UNITARIO,
            PRECO_CUSTO,
            FRETE_UNITARIO,
            SM_CONTAS_PCT,
            /* expõe valores já corrigidos com os nomes esperados */
            VALOR_TOTAL_CORR          AS VALOR_TOTAL,
            COMISSAO_UNITARIA_CORR    AS COMISSAO_UNITARIA,
            TAXA_FIXA_CORR            AS TAXA_FIXA,
            TOTAL_COM_FRETE_CORR      AS TOTAL_COM_FRETE,
            SM_CONTAS_REAIS_CORR      AS SM_CONTAS_REAIS,
            CUSTO_TOTAL_REAL_CORR     AS CUSTO_TOTAL_REAL,
            CUSTO_OP_TOTAL_CORR       AS CUSTO_OP_TOTAL,
            MARGEM_CONTRIBUICAO_CORR  AS MARGEM_CONTRIBUICAO,
            CUSTO_FIXO_CORR           AS CUSTO_FIXO,
            LUCRO_REAL_CORR           AS LUCRO_REAL,
            CASE WHEN VALOR_TOTAL_CORR > 0 THEN ROUND((MARGEM_CONTRIBUICAO_CORR / VALOR_TOTAL_CORR) * 100.0, 2) ELSE 0 END AS mc_percentual,
            TRANSPORTADORA,
            TIPO_CONTA,
            DATA AS data_venda
        """

    # --------------------- Períodos / Comparação ---------------------

    @staticmethod
    def _shift_one_month(iso_date: str, back: bool = True) -> str:
        """
        Devolve a mesma data no mês anterior (ou seguinte), respeitando fim de mês.
        iso_date: 'YYYY-MM-DD'
        """
        y, m, d = map(int, iso_date.split("-"))
        if back:
            m2 = m - 1
            y2 = y - 1 if m2 == 0 else y
            m2 = 12 if m2 == 0 else m2
        else:
            m2 = m + 1
            y2 = y + 1 if m2 == 13 else y
            m2 = 1 if m2 == 13 else m2
        last_day = monthrange(y2, m2)[1]
        d2 = min(d, last_day)
        return f"{y2:04d}-{m2:02d}-{d2:02d}"

    def _resolve_periodo(self, filtros: Optional[Dict[str, Any]]) -> Tuple[str, str, Dict[str, Any]]:
        """
        Garante um período [data_inicio, data_fim] em ISO. Se não vier do front,
        usa últimos 30 dias (fim = hoje).
        Retorna (di, df, filtros_sem_periodo).
        """
        base = dict(filtros or {})
        di = base.get("data_inicio")
        df = base.get("data_fim")
        if not di or not df:
            hoje = datetime.now().date()
            df = df or hoje.strftime("%Y-%m-%d")
            di = di or (hoje - timedelta(days=29)).strftime("%Y-%m-%d")
        # remove qualquer lixo
        base["data_inicio"] = di
        base["data_fim"] = df
        return di, df, base

    def _calc_metricas_periodo(self, filtros_periodo: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executa as métricas do período INFORMADO (usa calcular_metricas_gerais).
        """
        # calcular_metricas_gerais já aceita filtros; só garantimos que tem datas
        _, _, filtros_ok = self._resolve_periodo(filtros_periodo)
        return self.calcular_metricas_gerais(filtros_ok)

    def _comparar_mes_anterior(self, filtros: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compara período atual com o MESMO recorte no mês anterior.
        Ex.: 2025-08-01..2025-08-17  ->  2025-07-01..2025-07-17
        Retorna deltas em % e os valores bruto atual/anterior.
        """
        di, df, base = self._resolve_periodo(filtros)

        di_prev = self._shift_one_month(di, back=True)
        df_prev = self._shift_one_month(df, back=True)

        filtros_prev = dict(base)
        filtros_prev["data_inicio"] = di_prev
        filtros_prev["data_fim"] = df_prev

        atual = self._calc_metricas_periodo(base)
        anterior = self._calc_metricas_periodo(filtros_prev)

        def delta_pct(curr, prev):
            prev = float(prev or 0)
            curr = float(curr or 0)
            if prev == 0:
                return None if curr == 0 else 999.99  # “infinito” prático
            return ((curr - prev) / abs(prev)) * 100.0

        out = {
            "periodo_atual": {"inicio": di, "fim": df},
            "periodo_anterior": {"inicio": di_prev, "fim": df_prev},
            "valores": {
                "atual": {
                    "receita_total": float(atual.get("receita_total", 0) or 0),
                    "lucro_total": float(atual.get("lucro_total", 0) or 0),
                    "total_vendas": int(atual.get("total_vendas", 0) or 0),
                    "margem_liquida": float(atual.get("margem_liquida", 0) or 0),
                },
                "anterior": {
                    "receita_total": float(anterior.get("receita_total", 0) or 0),
                    "lucro_total": float(anterior.get("lucro_total", 0) or 0),
                    "total_vendas": int(anterior.get("total_vendas", 0) or 0),
                    "margem_liquida": float(anterior.get("margem_liquida", 0) or 0),
                },
            },
        }
        vA = out["valores"]["atual"]; vP = out["valores"]["anterior"]
        out["deltas_percent"] = {
            "receita_total": delta_pct(vA["receita_total"], vP["receita_total"]),
            "lucro_total":   delta_pct(vA["lucro_total"],   vP["lucro_total"]),
            "total_vendas":  delta_pct(vA["total_vendas"],  vP["total_vendas"]),
            "margem_liquida":delta_pct(vA["margem_liquida"],vP["margem_liquida"]),
        }
        return out


    # ===================== Métricas =====================

    def calcular_metricas_gerais(self, filtros: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Métricas do dashboard com cache de 5 minutos, usando colunas CORRIGIDAS."""
        cache_key = f"metricas::{self._generate_cache_key(filtros)}"
        cache_hit = self._cache.get(cache_key)
        if cache_hit and (datetime.now() - cache_hit["timestamp"]).seconds < 300:
            return cache_hit["data"]

        conn = self.get_connection()
        where_clause, params = self._build_where_clause(filtros)

        query = f"""
        {self._select_corr_base()}
        SELECT
            COUNT(*) AS total_vendas,
            COALESCE(SUM(VALOR_TOTAL_CORR), 0) AS receita_total,
            COALESCE(SUM(LUCRO_REAL_CORR), 0) AS lucro_total,
            COALESCE(AVG(LUCRO_REAL_CORR), 0) AS lucro_medio,
            COALESCE(AVG(CASE WHEN VALOR_TOTAL_CORR > 0 THEN (MARGEM_CONTRIBUICAO_CORR / VALOR_TOTAL_CORR) * 100 ELSE 0 END), 0) AS mc_media,
            COALESCE(MIN(CASE WHEN VALOR_TOTAL_CORR > 0 THEN (MARGEM_CONTRIBUICAO_CORR / VALOR_TOTAL_CORR) * 100 ELSE 0 END), 0) AS mc_minima,
            COALESCE(MAX(CASE WHEN VALOR_TOTAL_CORR > 0 THEN (MARGEM_CONTRIBUICAO_CORR / VALOR_TOTAL_CORR) * 100 ELSE 0 END), 0) AS mc_maxima,
            SUM(CASE WHEN LUCRO_REAL_CORR > 0 THEN 1 ELSE 0 END) AS vendas_lucrativas,
            SUM(CASE WHEN LUCRO_REAL_CORR <= 0 THEN 1 ELSE 0 END) AS vendas_prejuizo,
            SUM(CASE WHEN MARGEM_CONTRIBUICAO_CORR < 0 THEN 1 ELSE 0 END) AS vendas_mc_negativa,
            COUNT(DISTINCT TRANSPORTADORA) AS qtd_transportadoras,
            MIN(DATA) AS data_mais_antiga,
            MAX(DATA) AS data_mais_recente,
            COALESCE(AVG(QTD_COMPRADA), 0) AS media_itens_por_venda,
            COALESCE(SUM(QTD_COMPRADA), 0) AS total_itens_vendidos,
            COUNT(DISTINCT SKU) AS qtd_produtos_diferentes,
            COALESCE(AVG(VALOR_TOTAL_CORR), 0) AS ticket_medio,
            COALESCE(MIN(VALOR_TOTAL_CORR), 0) AS ticket_minimo,
            COALESCE(MAX(VALOR_TOTAL_CORR), 0) AS ticket_maximo
        FROM corr
        {where_clause}
        """
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        metrics = df.iloc[0].to_dict()

        # Derivadas
        tv = metrics.get("total_vendas", 0) or 0
        rt = float(metrics.get("receita_total", 0) or 0)
        lt = float(metrics.get("lucro_total", 0) or 0)
        ti = float(metrics.get("total_itens_vendidos", 0) or 0)
        vmc_neg = metrics.get("vendas_mc_negativa", 0) or 0
        v_luc = metrics.get("vendas_lucrativas", 0) or 0
        v_prej = metrics.get("vendas_prejuizo", 0) or 0

        if tv > 0:
            metrics.update({
                "perc_lucrativas": (v_luc / tv) * 100.0,
                "perc_prejuizo": (v_prej / tv) * 100.0,
                "perc_mc_negativa": (vmc_neg / tv) * 100.0,
                "equilibrio": 100.0 - (vmc_neg / tv) * 100.0,
                "lucro_por_venda": lt / tv if tv else 0,
                "receita_por_item": rt / ti if ti else 0,
                "lucro_por_item": lt / ti if ti else 0,
            })
        if rt > 0:
            metrics["margem_liquida"] = (lt / rt) * 100.0

        metrics["variacao_receita"] = 0
        metrics["variacao_lucro"] = 0
        metrics["variacao_vendas"] = 0

        self._cache[cache_key] = {"data": metrics, "timestamp": datetime.now()}
        return metrics

    # ===================== Listagens / Análises =====================

    def get_top_produtos(
        self,
        limit: int = 10,
        filtros: Optional[Dict[str, Any]] = None,
        order_by: str = "lucro",
    ) -> List[Dict[str, Any]]:
        """
        Top itens (por venda). Ordenações: lucro, receita, margem, quantidade, prejuizo.
        Usa colunas CORRIGIDAS e devolve com os nomes esperados pelo front.
        """
        order_mapping = {
            "lucro": "LUCRO_REAL DESC",
            "receita": "VALOR_TOTAL DESC",
            "margem": "mc_percentual DESC",
            "quantidade": "QTD_COMPRADA DESC",
            "prejuizo": "LUCRO_REAL ASC",
        }
        if order_by not in order_mapping:
            order_by = "lucro"

        where_clause, params = self._build_where_clause(filtros)
        sql = f"""
        {self._select_corr_base()}
        SELECT
            {self._select_columns_mapeadas()}
        FROM corr
        {where_clause}
        ORDER BY {order_mapping[order_by]}
        LIMIT ?
        """
        conn = self.get_connection()
        df = pd.read_sql_query(sql, conn, params=params + [int(limit)])
        conn.close()
        return df.to_dict("records")

    def get_analise_transportadoras(self, filtros: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Análise agregada por transportadora com colunas CORRIGIDAS."""
        where_clause, params = self._build_where_clause(filtros)
        sql = f"""
        {self._select_corr_base()}
        SELECT
            TRANSPORTADORA,
            COUNT(*) AS quantidade,
            COALESCE(SUM(VALOR_TOTAL_CORR), 0) AS receita_total,
            COALESCE(SUM(LUCRO_REAL_CORR), 0) AS lucro_total,
            COALESCE(AVG(VALOR_TOTAL_CORR), 0) AS ticket_medio,
            COALESCE(AVG(CASE WHEN VALOR_TOTAL_CORR > 0 THEN (MARGEM_CONTRIBUICAO_CORR / VALOR_TOTAL_CORR) * 100 ELSE 0 END), 0) AS mc_media,
            SUM(CASE WHEN LUCRO_REAL_CORR > 0 THEN 1 ELSE 0 END) AS vendas_lucrativas,
            SUM(CASE WHEN LUCRO_REAL_CORR <= 0 THEN 1 ELSE 0 END) AS vendas_prejuizo
        FROM corr
        {where_clause}
        GROUP BY TRANSPORTADORA
        ORDER BY receita_total DESC
        """
        conn = self.get_connection()
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()
        return df.to_dict("records")

    # ===================== ABC =====================

    def calcular_curva_abc(self, filtros: Optional[Dict[str, Any]] = None, criterio: str = "receita") -> List[Dict[str, Any]]:
        """
        Curva ABC por critério: receita (VALOR_TOTAL), lucro (LUCRO_REAL),
        margem (MARGEM_CONTRIBUICAO) ou quantidade (QTD_COMPRADA).
        Usa colunas CORRIGIDAS.
        """
        valid_criteria = {
            "receita": "VALOR_TOTAL",
            "lucro": "LUCRO_REAL",
            "margem": "MARGEM_CONTRIBUICAO",
            "quantidade": "QTD_COMPRADA",
        }
        if criterio not in valid_criteria:
            raise ValueError(f"Critério inválido. Use: {', '.join(valid_criteria.keys())}")

        order_column = valid_criteria[criterio]
        where_clause, params = self._build_where_clause(filtros)

        sql = f"""
        {self._select_corr_base()}
        SELECT
            {self._select_columns_mapeadas()}
        FROM corr
        {where_clause}
        ORDER BY {order_column} DESC
        """
        conn = self.get_connection()
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()

        if df.empty:
            return []

        # percentuais para ABC
        key_map = {
            "receita": "VALOR_TOTAL",
            "lucro": "LUCRO_REAL",
            "margem": "MARGEM_CONTRIBUICAO",
            "quantidade": "QTD_COMPRADA"
        }
        key = key_map[criterio]
        df["valor_criterio"] = df[key].astype(float)
        total = float(df["valor_criterio"].sum() or 0.0)
        if total <= 0.0:
            return []

        df["percentual"] = (df["valor_criterio"] / total) * 100.0
        df["percentual_acumulado"] = df["percentual"].cumsum()

        def classificar_abc(p: float) -> str:
            if p <= 80:
                return "A"
            elif p <= 95:
                return "B"
            return "C"

        df["classe_abc"] = df["percentual_acumulado"].apply(classificar_abc)
        df["ranking"] = range(1, len(df) + 1)
        return df.to_dict("records")

    def get_distribuicao_abc(self, filtros: Optional[Dict[str, Any]] = None, criterio: str = "receita") -> Dict[str, Any]:
        """Distribuição agregada das classes ABC (com colunas corrigidas)."""
        itens = self.calcular_curva_abc(filtros, criterio)
        if not itens:
            return {"A": 0, "B": 0, "C": 0, "perc": {"A": 0, "B": 0, "C": 0}}

        counts = {"A": 0, "B": 0, "C": 0}
        key = {"receita": "VALOR_TOTAL", "lucro": "LUCRO_REAL", "margem": "MARGEM_CONTRIBUICAO", "quantidade": "QTD_COMPRADA"}[criterio]
        totals = {"A": 0.0, "B": 0.0, "C": 0.0}
        total_all = 0.0

        for r in itens:
            classe = r.get("classe_abc", "C")
            counts[classe] = counts.get(classe, 0) + 1
            v = float(r.get(key, 0) or 0)
            totals[classe] += v
            total_all += v

        perc = {k: (totals[k] / total_all * 100.0 if total_all else 0.0) for k in ["A", "B", "C"]}
        return {"A": counts["A"], "B": counts["B"], "C": counts["C"], "perc": perc}

    # ===================== Filtros / Evolução / Export =====================

    def get_filtros_disponiveis(self) -> Dict[str, Any]:
        """Transportadoras, tipos de conta e range de datas (BR e ISO)."""
        conn = self.get_connection()

        transportadoras = pd.read_sql_query(
            """
            SELECT DISTINCT TRANSPORTADORA
            FROM vendas_shopee
            WHERE TRANSPORTADORA IS NOT NULL AND TRIM(TRANSPORTADORA) <> ''
            ORDER BY TRANSPORTADORA
            """,
            conn
        )
        tipos_conta = pd.read_sql_query(
            """
            SELECT DISTINCT TIPO_CONTA
            FROM vendas_shopee
            WHERE TIPO_CONTA IS NOT NULL AND TRIM(TIPO_CONTA) <> ''
            ORDER BY TIPO_CONTA
            """,
            conn
        )
        datas = pd.read_sql_query(
            """
            SELECT
                MIN(
                    CASE WHEN substr(DATA,3,1)='/' 
                         THEN substr(DATA,7,4)||'-'||substr(DATA,4,2)||'-'||substr(DATA,1,2)
                         ELSE substr(DATA,1,10) END
                ) AS min_iso,
                MAX(
                    CASE WHEN substr(DATA,3,1)='/' 
                         THEN substr(DATA,7,4)||'-'||substr(DATA,4,2)||'-'||substr(DATA,1,2)
                         ELSE substr(DATA,1,10) END
                ) AS max_iso
            FROM vendas_shopee
            WHERE DATA IS NOT NULL AND TRIM(DATA) <> ''
            """,
            conn
        )
        conn.close()

        min_iso = datas.iloc[0]["min_iso"] if not datas.empty else None
        max_iso = datas.iloc[0]["max_iso"] if not datas.empty else None

        def iso_to_br(s: Optional[str]) -> Optional[str]:
            if not s or len(s) < 10:
                return None
            return f"{s[8:10]}/{s[5:7]}/{s[0:4]}"

        return {
            "success": True,
            "transportadoras": transportadoras["TRANSPORTADORA"].tolist(),
            "tipos_conta": tipos_conta["TIPO_CONTA"].tolist(),
            "data_minima": iso_to_br(min_iso),
            "data_maxima": iso_to_br(max_iso),
            "data_minima_iso": min_iso,
            "data_maxima_iso": max_iso,
        }

    def get_evolucao_vendas(self, periodo: str = "30d", filtros: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Série diária (ISO yyyy-mm-dd) de receita e lucro no período informado, usando colunas CORRIGIDAS.
        periodo: '7d', '30d', '90d', '1y'
        """
        days = {"7d": 7, "30d": 30, "90d": 90, "1y": 365}.get(periodo, 30)
        start_iso = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        filtros = dict(filtros or {})
        if not filtros.get("data_inicio"):
            filtros["data_inicio"] = start_iso

        where_clause, params = self._build_where_clause(filtros)

        data_iso = (
            "CASE WHEN substr(DATA,3,1)='/' "
            "THEN substr(DATA,7,4) || '-' || substr(DATA,4,2) || '-' || substr(DATA,1,2) "
            "ELSE substr(DATA,1,10) END"
        )

        sql = f"""
        {self._select_corr_base()}
        SELECT
            {data_iso} AS dia,
            COALESCE(SUM(VALOR_TOTAL_CORR), 0) AS receita,
            COALESCE(SUM(LUCRO_REAL_CORR), 0) AS lucro
        FROM corr
        {where_clause}
        GROUP BY dia
        ORDER BY dia ASC
        """
        conn = self.get_connection()
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()
        return df.to_dict("records")

    def exportar_dados(self, formato: str = "csv", filtros: Optional[Dict[str, Any]] = None) -> str:
        """
        Exporta dados com as colunas CORRIGIDAS (sem alterar o DB).
        """
        where_clause, params = self._build_where_clause(filtros)
        sql = f"""
        {self._select_corr_base()}
        SELECT
            PEDIDO_ID, DATA, TIPO_CONTA, TRANSPORTADORA, NOME_ITEM, SKU, QTD_COMPRADA,
            PRECO_UNITARIO, PRECO_CUSTO, FRETE_UNITARIO, SM_CONTAS_PCT,
            /* valores corrigidos */
            VALOR_TOTAL_CORR          AS VALOR_TOTAL,
            COMISSAO_UNITARIA_CORR    AS COMISSAO_UNITARIA,
            TAXA_FIXA_CORR            AS TAXA_FIXA,
            TOTAL_COM_FRETE_CORR      AS TOTAL_COM_FRETE,
            SM_CONTAS_REAIS_CORR      AS SM_CONTAS_REAIS,
            CUSTO_TOTAL_REAL_CORR     AS CUSTO_TOTAL_REAL,
            OP_4_5_CORR               AS OP_4_5,
            CUSTO_OP_TOTAL_CORR       AS CUSTO_OP_TOTAL,
            MARGEM_CONTRIBUICAO_CORR  AS MARGEM_CONTRIBUICAO,
            CUSTO_FIXO_CORR           AS CUSTO_FIXO,
            LUCRO_REAL_CORR           AS LUCRO_REAL,
            CASE WHEN VALOR_TOTAL_CORR > 0 THEN ROUND((LUCRO_REAL_CORR / VALOR_TOTAL_CORR) * 100.0, 2) ELSE 0 END AS LUCRO_REAL_PCT
        FROM corr
        {where_clause}
        """
        conn = self.get_connection()
        df = pd.read_sql_query(sql, conn, params=params)
        conn.close()

        if formato.lower() == "json":
            return df.to_json(orient="records", force_ascii=False)
        return df.to_csv(index=False)

    # ===================== Payload do Dashboard =====================

    def get_dados_dashboard(self, filtros: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Consolida tudo que o frontend precisa usando colunas CORRIGIDAS,
        agora com COMPARATIVO vs MÊS ANTERIOR (mesmo período).
        """
        metricas = self.calcular_metricas_gerais(filtros)
        comparativos = self._comparar_mes_anterior(filtros)

        top_produtos = self.get_top_produtos(10, filtros, "lucro")
        transportadoras = self.get_analise_transportadoras(filtros)
        curva_abc = self.calcular_curva_abc(filtros, "receita")

        abc_counts = {"A": 0, "B": 0, "C": 0}
        for item in curva_abc:
            classe = item.get("classe_abc")
            if classe in abc_counts:
                abc_counts[classe] += 1

        data = {
            "success": True,
            "metrics": metricas,
            "comparativos": comparativos,  # <<< AQUI
            "charts": {
                "sales": {
                    "labels": ["Período selecionado"],
                    "values": [metricas.get("receita_total", 0)],
                },
                "margins": {
                    "labels": [
                        (p.get("produto") or "")[:20] + ("..." if len(p.get("produto") or "") > 20 else "")
                        for p in top_produtos[:5]
                    ],
                    "values": [float(p.get("mc_percentual", 0) or 0) for p in top_produtos[:5]],
                },
                "abc": {
                    "labels": ["Classe A", "Classe B", "Classe C"],
                    "values": [abc_counts["A"], abc_counts["B"], abc_counts["C"]],
                },
                "topProducts": {
                    "labels": [
                        (p.get("produto") or "")[:15] + ("..." if len(p.get("produto") or "") > 15 else "")
                        for p in top_produtos[:5]
                    ],
                    "values": [float(p.get("LUCRO_REAL", 0) or 0) for p in top_produtos[:5]],
                },
                "transport": {
                    "labels": [t.get("TRANSPORTADORA", "-") for t in transportadoras[:5]],
                    "values": [int(t.get("quantidade", 0) or 0) for t in transportadoras[:5]],
                },
            },
            "tables": {
                "products": top_produtos,
                "transport": transportadoras,
            },
        }
        return data