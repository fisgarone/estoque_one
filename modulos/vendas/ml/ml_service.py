# -*- coding: utf-8 -*-
import io
import sqlite3
from datetime import date, datetime, timedelta


# =================
#  Conexão / utils
# =================
def _conn():
    """Conexão principal com fisgarone.db - banco de dados global do ERP"""
    return sqlite3.connect("fisgarone.db")


def _politicas_conn():
    """Conexão com politicas_canais - MIGRADO PARA fisgarone.db
    As tabelas de políticas agora estão em fisgarone.db"""
    return sqlite3.connect("fisgarone.db")


def _br_date_to_iso(col_expr='"Data da Venda"'):
    return f"date(substr({col_expr}, 7,4)||'-'||substr({col_expr},4,2)||'-'||substr({col_expr},1,2))"


def _default_period(start, end):
    if start and end:
        return start, end
    today = date.today()
    start = f"{today.year}-{str(today.month).zfill(2)}-01"
    end = today.isoformat()
    return start, end


def _prev_period(start_iso, end_iso):
    s = datetime.fromisoformat(start_iso).date()
    e = datetime.fromisoformat(end_iso).date()
    span = (e - s).days + 1
    e_prev = s - timedelta(days=1)
    s_prev = e_prev - timedelta(days=span - 1)
    return s_prev.isoformat(), e_prev.isoformat()


def _table_exists(con, name):
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None


# =================
#  CÁLCULOS ML OPERACIONAIS
# =================
def _calcular_custos_operacionais_ml(pedido):
    """
    Calcula apenas custos operacionais do Mercado Livre
    (comissões, taxa fixa, frete seller) - LUCRO OPERACIONAL

    ATUALIZADO: Busca políticas de fisgarone.db
    """
    try:
        # Conexão com políticas - AGORA EM fisgarone.db
        pol_conn = _politicas_conn()
        pol_cur = pol_conn.cursor()

        # Dados do pedido
        preco_unit = float(pedido.get("Preco Unitario", 0))
        quantidade = int(pedido.get("Quantidade", 1))
        sale_fee_unit = float(pedido.get("Taxa Mercado Livre", 0))

        # Buscar política para ML em fisgarone.db
        pol_cur.execute('''
                        SELECT *
                        FROM politicas_canais
                        WHERE canal = 'ml'
                          AND plano = 'padrao'
                          AND preco_unit_min <= ?
                          AND (preco_unit_max > ? OR preco_unit_max IS NULL)
                          AND ativo = 1
                        ''', (preco_unit, preco_unit))

        politica = pol_cur.fetchone()
        if not politica:
            # Política padrão se não encontrar
            politica = {
                'comissao_percent_base': None,
                'taxa_fixa_tipo': 'NENHUMA',
                'taxa_fixa_valor': 0,
                'frete_seller_tipo': 'NENHUM',
                'frete_seller_valor': 0,
                'insumos_percent': 0.015,
                'ads_percent': 0.035
            }
        else:
            # Converter para dict
            col_names = [desc[0] for desc in pol_cur.description]
            politica = dict(zip(col_names, politica))

        pol_conn.close()

        # CÁLCULOS OPERACIONAIS APENAS
        faturamento_bruto = preco_unit * quantidade

        # 1. Taxa fixa por faixa (ML < 79)
        taxa_fixa_unit = 0
        if politica.get('taxa_fixa_tipo') == 'POR_UNIDADE_FAIXA' and preco_unit < 79:
            pol_conn = _politicas_conn()
            pol_cur = pol_conn.cursor()
            pol_cur.execute('''
                            SELECT valor
                            FROM politicas_canais_faixas
                            WHERE canal = 'ml'
                              AND plano = 'padrao'
                              AND preco_unit_min <= ?
                              AND preco_unit_max > ?
                              AND ativo = 1
                            ''', (preco_unit, preco_unit))
            faixa = pol_cur.fetchone()
            if faixa:
                taxa_fixa_unit = faixa[0]
            pol_conn.close()

        elif politica.get('taxa_fixa_tipo') == 'POR_UNIDADE':
            taxa_fixa_unit = politica.get('taxa_fixa_valor', 0)

        taxa_fixa_total = taxa_fixa_unit * quantidade

        # 2. Comissões
        comissao_unit = 0
        comissao_percent = 0

        if politica.get('comissao_percent_base') is not None:
            # Usar percentual base da política
            comissao_percent = politica.get('comissao_percent_base', 0)
            comissao_unit = preco_unit * comissao_percent
        else:
            # Calcular da sale_fee (ML) - DADO REAL
            comissao_unit = sale_fee_unit - taxa_fixa_unit
            comissao_percent = comissao_unit / preco_unit if preco_unit > 0 else 0

        comissao_total = comissao_unit * quantidade

        # 3. Frete Seller (ML >= 79)
        frete_seller_unit = 0
        if (politica.get('frete_seller_tipo') == 'POR_UNIDADE' and
                politica.get('frete_seller_valor', 0) > 0 and preco_unit >= 79):
            frete_seller_unit = politica.get('frete_seller_valor', 0)

        frete_seller_total = frete_seller_unit * quantidade

        # 4. Custos operacionais do ML (SOMENTE ESTES)
        custo_operacional_ml = comissao_total + taxa_fixa_total + frete_seller_total

        # 5. Lucro operacional (Faturamento - Custos ML)
        lucro_operacional = faturamento_bruto - custo_operacional_ml
        lucro_operacional_percent = (lucro_operacional / faturamento_bruto) if faturamento_bruto > 0 else 0

        return {
            # Custos ML
            "Taxa Mercado Livre": sale_fee_unit * quantidade,
            "Taxa Fixa ML": taxa_fixa_total,
            "Comissoes": comissao_total,
            "Comissao (%)": comissao_percent,
            "Frete Seller": frete_seller_total,

            # Resultado operacional
            "Custo Operacional ML": custo_operacional_ml,
            "Lucro Real": lucro_operacional,
            "Lucro Real %": lucro_operacional_percent,

            # Dados base
            "Faturamento Bruto": faturamento_bruto
        }

    except Exception as e:
        print(f"Erro cálculo custos operacionais ML: {e}")
        # Retorno padrão em caso de erro
        return {
            "Taxa Mercado Livre": 0,
            "Taxa Fixa ML": 0,
            "Comissoes": 0,
            "Comissao (%)": 0,
            "Frete Seller": 0,
            "Custo Operacional ML": 0,
            "Lucro Real": 0,
            "Lucro Real %": 0,
            "Faturamento Bruto": 0
        }


# ==========
#  HEALTH
# ==========
def health():
    """Verifica saúde do banco de dados fisgarone.db"""
    con = _conn()
    ok = _table_exists(con, "vendas_ml")
    con.close()
    return {"status": "ok" if ok else "no-vendas", "table": "vendas_ml"}


# ==========
#  FILTROS
# ==========
def get_filters():
    """Extrai filtros disponíveis de fisgarone.db"""
    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        'SELECT DISTINCT "Conta" AS conta FROM vendas_ml WHERE "Conta" IS NOT NULL AND TRIM("Conta")<>"" ORDER BY 1')
    contas = [r["conta"] for r in cur.fetchall()]
    cur.execute(
        'SELECT DISTINCT "Situacao" AS status FROM vendas_ml WHERE "Situacao" IS NOT NULL AND TRIM("Situacao")<>"" ORDER BY 1')
    status = [r["status"] for r in cur.fetchall()]
    con.close()
    return {
        "contas": contas,
        "status": status,
        "modes": ["lucro", "faturamento", "unidades", "margem"]
    }


def normalize_filters(filters):
    """Normaliza filtros para case-insensitive"""
    normalized = {}
    for key, value in filters.items():
        if value:
            if key == 'conta':
                normalized[key] = value.upper()
            else:
                normalized[key] = value
    return normalized


# =========================
#  OVERVIEW (KPIs do topo)
# =========================
def get_overview(start=None, end=None, conta="", status="", q=""):
    """Extrai KPIs principais de fisgarone.db"""
    start, end = _default_period(start, end)
    iso_date = _br_date_to_iso()
    params = {"start": start, "end": end}
    where = [f"{iso_date} BETWEEN :start AND :end"]

    if conta:
        where.append('UPPER("Conta") = UPPER(:conta)')
        params["conta"] = conta.upper()
    if status:
        where.append('"Situacao" = :status')
        params["status"] = status
    if q:
        where.append('("ID Pedido" LIKE :q OR "SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
        params["q"] = f"%{q}%"

    where_sql = " AND ".join(where)
    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    sql = f"""
    SELECT
      COUNT(*)                                         AS pedidos,
      SUM("Preco Unitario" * "Quantidade")             AS bruto,
      SUM(COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0)) AS custo_operacional,
      SUM("Preco Unitario" * "Quantidade") - SUM(COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0)) AS lucro_real
    FROM vendas_ml
    WHERE {where_sql}
    """
    cur.execute(sql, params)
    r = cur.fetchone() or {}
    bruto = float(r["bruto"] or 0.0)
    custo_operacional = float(r["custo_operacional"] or 0.0)
    lucro_real = float(r["lucro_real"] or 0.0)
    pedidos = int(r["pedidos"] or 0)

    lucro_percent_medio = (lucro_real / bruto) if bruto > 0 else 0.0

    # repasse_real (opcional): se existir tabela de repasse em fisgarone.db
    repasse_real = None
    try:
        if _table_exists(con, "repasses_ml"):
            sqlr = f"""
              SELECT SUM(COALESCE("Valor do Repasse",0)) AS rep
              FROM repasses_ml
              WHERE "ID Pedido" IN (
                SELECT "ID Pedido" FROM vendas_ml WHERE {where_sql}
              )
            """
            cur.execute(sqlr, params)
            rr = cur.fetchone()
            repasse_real = float((rr and rr["rep"]) or 0.0)
        elif _table_exists(con, "repasse_ml"):
            sqlr = f"""
              SELECT SUM(COALESCE("Valor do Repasse",0)) AS rep
              FROM repasse_ml
              WHERE "ID Pedido" IN (
                SELECT "ID Pedido" FROM vendas_ml WHERE {where_sql}
              )
            """
            cur.execute(sqlr, params)
            rr = cur.fetchone()
            repasse_real = float((rr and rr["rep"]) or 0.0)
    except Exception:
        repasse_real = None

    con.close()
    return {
        "pedidos": pedidos,
        "bruto": bruto,
        "taxa_total": custo_operacional,
        "frete_net": 0,
        "lucro_real": lucro_real,
        "lucro_percent_medio": lucro_percent_medio,
        "repasse_real": repasse_real,
        "divergencias": count_divergences(start, end, conta, status, q)
    }


# ======================
#  TRENDS (linha diária)
# ======================
def get_trends(start=None, end=None, conta="", status="", q=""):
    """Extrai tendências diárias de fisgarone.db"""
    start, end = _default_period(start, end)
    iso_date = _br_date_to_iso()
    params = {"start": start, "end": end}
    where = [f"{iso_date} BETWEEN :start AND :end"]

    if conta:
        where.append('UPPER("Conta") = UPPER(:conta)')
        params["conta"] = conta.upper()
    if status:
        where.append('"Situacao" = :status')
        params["status"] = status
    if q:
        where.append('("ID Pedido" LIKE :q OR "SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
        params["q"] = f"%{q}%"

    where_sql = " AND ".join(where)
    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    sql = f"""
    SELECT
      {iso_date} AS dia,
      SUM("Preco Unitario" * "Quantidade") AS bruto,
      SUM(COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0)) AS custo_operacional,
      SUM("Preco Unitario" * "Quantidade") - SUM(COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0)) AS lucro
    FROM vendas_ml
    WHERE {where_sql}
    GROUP BY dia
    ORDER BY dia
    """
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    for r in rows:
        r["bruto"] = float(r.get("bruto") or 0)
        r["custo_operacional"] = float(r.get("custo_operacional") or 0)
        r["lucro"] = float(r.get("lucro") or 0)

    return rows


# ======================
#  DADOS DIÁRIOS (mini-cards)
# ======================
def get_daily(start=None, end=None, conta="", status="", q=""):
    """Extrai dados diários agregados de fisgarone.db para mini-cards"""
    start, end = _default_period(start, end)
    iso_date = _br_date_to_iso()
    params = {"start": start, "end": end}
    where = [f"{iso_date} BETWEEN :start AND :end"]

    if conta:
        where.append('UPPER("Conta") = UPPER(:conta)')
        params["conta"] = conta.upper()
    if status:
        where.append('"Situacao" = :status')
        params["status"] = status
    if q:
        where.append('("ID Pedido" LIKE :q OR "SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
        params["q"] = f"%{q}%"

    where_sql = " AND ".join(where)
    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    sql = f"""
    SELECT
      {iso_date} AS dia,
      SUM("Preco Unitario" * "Quantidade") AS bruto,
      SUM("Quantidade") AS unidades
    FROM vendas_ml
    WHERE {where_sql}
    GROUP BY dia
    ORDER BY dia
    """
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    for r in rows:
        r["bruto"] = float(r.get("bruto") or 0)
        r["unidades"] = int(r.get("unidades") or 0)

    return rows


# ======================
#  DIVERGÊNCIAS
# ======================
def get_divergences(start=None, end=None, conta="", status="", q=""):
    """Retorna lista de divergências encontradas em fisgarone.db"""
    count = count_divergences(start, end, conta, status, q)
    return {"count": count, "divergencias": count}


def count_divergences(start=None, end=None, conta="", status="", q=""):
    """Conta divergências em fisgarone.db"""
    start, end = _default_period(start, end)
    iso_date = _br_date_to_iso()
    params = {"start": start, "end": end}
    where = [f"{iso_date} BETWEEN :start AND :end"]

    if conta:
        where.append('UPPER("Conta") = UPPER(:conta)')
        params["conta"] = conta.upper()
    if status:
        where.append('"Situacao" = :status')
        params["status"] = status
    if q:
        where.append('("ID Pedido" LIKE :q OR "SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
        params["q"] = f"%{q}%"

    where_sql = " AND ".join(where)
    con = _conn()
    cur = con.cursor()

    # Exemplo: pedidos sem SKU ou MLB
    sql = f"""
    SELECT COUNT(*) AS n
    FROM vendas_ml
    WHERE {where_sql}
      AND ("SKU" IS NULL OR TRIM("SKU")='' OR "MLB" IS NULL OR TRIM("MLB")='')
    """
    cur.execute(sql, params)
    count = int(cur.fetchone()[0] or 0)
    con.close()
    return count


# ======================
#  DISTRIBUIÇÃO POR CONTA
# ======================
def get_distribution_by_account(start=None, end=None, metric="vendas"):
    """Extrai distribuição por conta de fisgarone.db para gráfico de pizza"""
    start, end = _default_period(start, end)
    iso_date = _br_date_to_iso()
    params = {"start": start, "end": end}
    where_sql = f"{iso_date} BETWEEN :start AND :end"

    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Determinar métrica
    if metric == "faturamento":
        value_expr = 'SUM("Preco Unitario" * "Quantidade")'
    elif metric == "lucro":
        value_expr = 'SUM("Preco Unitario" * "Quantidade") - SUM(COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0))'
    else:
        value_expr = 'COUNT(*)'

    sql = f"""
    SELECT
      "Conta" AS conta,
      {value_expr} AS valor
    FROM vendas_ml
    WHERE {where_sql}
    GROUP BY conta
    ORDER BY valor DESC
    """
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    for r in rows:
        r["valor"] = float(r.get("valor") or 0)

    return rows


# ======================
#  LISTAGEM DE PEDIDOS
# ======================
def list_orders(start=None, end=None, conta="", status="", q="", sku="", mlb="", page=1, page_size=50):
    """Lista pedidos paginados de fisgarone.db"""
    start, end = _default_period(start, end)
    iso_date = _br_date_to_iso()
    params = {"start": start, "end": end}
    where = [f"{iso_date} BETWEEN :start AND :end"]

    if conta:
        where.append('UPPER("Conta") = UPPER(:conta)')
        params["conta"] = conta.upper()
    if status:
        where.append('"Situacao" = :status')
        params["status"] = status
    if q:
        where.append('("ID Pedido" LIKE :q OR "SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
        params["q"] = f"%{q}%"
    if sku:
        where.append('"SKU" = :sku')
        params["sku"] = sku
    if mlb:
        where.append('"MLB" = :mlb')
        params["mlb"] = mlb

    where_sql = " AND ".join(where)
    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute(f"SELECT COUNT(*) AS n FROM vendas_ml WHERE {where_sql}", params)
    total = int(cur.fetchone()["n"] or 0)
    total_pages = (total + page_size - 1) // page_size
    offset = max(0, (max(1, page) - 1) * page_size)

    select_sql = f"""
    SELECT
      "ID Pedido"               AS id_pedido_ml,
      "Conta"                   AS conta,
      "Situacao"                AS status_pedido,
      "SKU"                     AS sku,
      "MLB"                     AS mlb,
      "Titulo"                  AS titulo,
      "Quantidade"              AS quantidade,
      "Preco Unitario"          AS preco_unitario,
      ("Preco Unitario" * "Quantidade")                         AS bruto_rs,
      COALESCE("Frete Seller", 0)                                AS frete_seller_rs,
      COALESCE("Comissoes", 0)                                   AS comissoes_rs,
      COALESCE("Taxa Fixa ML", 0)                                AS taxa_fixa_ml_rs,
      COALESCE("Taxa Mercado Livre", 0)                          AS taxa_ml_rs,
      ("Preco Unitario" * "Quantidade") - 
        (COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0)) AS lucro_real_rs,
      {iso_date}                                                 AS data_venda_iso
    FROM vendas_ml
    WHERE {where_sql}
    ORDER BY {iso_date} DESC, "ID Pedido" DESC
    LIMIT :limit OFFSET :offset
    """
    params_data = dict(params)
    params_data["limit"] = page_size
    params_data["offset"] = offset
    cur.execute(select_sql, params_data)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    def _f(x):
        try:
            return float(x)
        except:
            return 0.0

    def _i(x):
        try:
            return int(x)
        except:
            return 0

    for r in rows:
        r["quantidade"] = _i(r.get("quantidade"))
        for k in ("preco_unitario", "bruto_rs", "frete_seller_rs", "comissoes_rs", "taxa_fixa_ml_rs", "taxa_ml_rs",
                  "lucro_real_rs"):
            r[k] = _f(r.get(k))

    return {
        "items": rows,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages
    }


def get_order_by_id(id_pedido):
    """Busca pedido específico por ID em fisgarone.db"""
    iso_date = _br_date_to_iso()
    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(f"""
      SELECT
        "ID Pedido"               AS "ID Pedido",
        "Conta"                   AS "Conta",
        "Situacao"                AS "Situacao",
        "SKU"                     AS "SKU",
        "MLB"                     AS "MLB",
        "Titulo"                  AS "Titulo",
        "Quantidade"              AS "Quantidade",
        "Preco Unitario"          AS "Preco Unitario",
        ("Preco Unitario" * "Quantidade")                         AS "Bruto (R$)",
        COALESCE("Comissoes",0)                                    AS "Comissoes",
        COALESCE("Taxa Fixa ML",0)                                 AS "Taxa Fixa ML",
        COALESCE("Frete Seller",0)                                 AS "Frete Seller",
        COALESCE("Taxa Mercado Livre",0)                           AS "Taxa Mercado Livre",
        ("Preco Unitario" * "Quantidade") - 
          (COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0)) AS "Lucro Real",
        {iso_date}                                                 AS data_venda_iso
      FROM vendas_ml
      WHERE "ID Pedido" = ?
      LIMIT 1
    """, (id_pedido,))
    row = cur.fetchone()
    con.close()
    return dict(row) if row else None


# ==========
#  EXPORTAR
# ==========
def export_orders_csv(start=None, end=None, conta="", status="", q="", sku="", mlb=""):
    """Exporta pedidos para CSV a partir de fisgarone.db"""
    data = list_orders(start, end, conta, status, q, sku, mlb, page=1, page_size=200000)
    rows = data["items"]
    mem = io.StringIO()
    w = __import__("csv").writer(mem, delimiter=';')
    w.writerow([
        "ID Pedido", "Conta", "Status", "Data ISO", "SKU", "MLB", "Título", "Qtd",
        "Preço Unitário", "Bruto (R$)", "Comissões (R$)", "Taxa Fixa (R$)", "Frete Seller (R$)",
        "Taxa ML Total (R$)", "Lucro Real (R$)"
    ])
    for r in rows:
        w.writerow([
            r.get("id_pedido_ml") or "", r.get("conta") or "", r.get("status_pedido") or "",
            r.get("data_venda_iso") or "", r.get("sku") or "", r.get("mlb") or "", r.get("titulo") or "",
            int(r.get("quantidade") or 0),
            str(r.get("preco_unitario") or 0).replace(".", ","),
            str(r.get("bruto_rs") or 0).replace(".", ","),
            str(r.get("comissoes_rs") or 0).replace(".", ","),
            str(r.get("taxa_fixa_ml_rs") or 0).replace(".", ","),
            str(r.get("frete_seller_rs") or 0).replace(".", ","),
            str(r.get("taxa_ml_rs") or 0).replace(".", ","),
            str(r.get("lucro_real_rs") or 0).replace(".", ","),
        ])
    return io.BytesIO(mem.getvalue().encode("utf-8-sig"))


def export_orders_xlsx(start=None, end=None, conta="", status="", q="", sku="", mlb=""):
    """Exporta pedidos para XLSX a partir de fisgarone.db"""
    try:
        import xlsxwriter
    except Exception:
        raise RuntimeError("Instale xlsxwriter: pip install xlsxwriter")
    data = list_orders(start, end, conta, status, q, sku, mlb, page=1, page_size=200000)
    rows = data["items"]
    mem = io.BytesIO()
    wb = xlsxwriter.Workbook(mem, {'in_memory': True})
    ws = wb.add_worksheet("Vendas ML")
    fmt_h = wb.add_format({'bold': True, 'bg_color': '#E4F7FF', 'border': 1})
    fmt_n = wb.add_format({'num_format': '#,##0.00'})

    header = [
        "ID Pedido", "Conta", "Status", "Data ISO", "SKU", "MLB", "Título", "Qtd",
        "Preço Unitário", "Bruto (R$)", "Comissões (R$)", "Taxa Fixa (R$)", "Frete Seller (R$)",
        "Taxa ML Total (R$)", "Lucro Real (R$)"
    ]
    ws.write_row(0, 0, header, fmt_h)
    r = 1
    for it in rows:
        ws.write(r, 0, it.get("id_pedido_ml") or "")
        ws.write(r, 1, it.get("conta") or "")
        ws.write(r, 2, it.get("status_pedido") or "")
        ws.write(r, 3, it.get("data_venda_iso") or "")
        ws.write(r, 4, it.get("sku") or "")
        ws.write(r, 5, it.get("mlb") or "")
        ws.write(r, 6, it.get("titulo") or "")
        ws.write_number(r, 7, int(it.get("quantidade") or 0))
        ws.write_number(r, 8, float(it.get("preco_unitario") or 0), fmt_n)
        ws.write_number(r, 9, float(it.get("bruto_rs") or 0), fmt_n)
        ws.write_number(r, 10, float(it.get("comissoes_rs") or 0), fmt_n)
        ws.write_number(r, 11, float(it.get("taxa_fixa_ml_rs") or 0), fmt_n)
        ws.write_number(r, 12, float(it.get("frete_seller_rs") or 0), fmt_n)
        ws.write_number(r, 13, float(it.get("taxa_ml_rs") or 0), fmt_n)
        ws.write_number(r, 14, float(it.get("lucro_real_rs") or 0), fmt_n)
        r += 1

    ws.autofilter(0, 0, r - 1, len(header) - 1)
    ws.freeze_panes(1, 0)
    ws.set_column(0, 6, 18)
    ws.set_column(7, 14, 16)
    wb.close()
    mem.seek(0)
    return mem


# =============
#  TOP ITENS
# =============
def get_top_items(n=10, modo="lucro", start=None, end=None, conta="", q=""):
    """Extrai top N itens de fisgarone.db (Top 10 SKUs by profit)"""
    start, end = _default_period(start, end)
    iso_date = _br_date_to_iso()

    params = {"start": start, "end": end}
    where = [f"{iso_date} BETWEEN :start AND :end"]
    if conta:
        where.append('UPPER("Conta") = UPPER(:conta)')
        params["conta"] = conta.upper()
    if q:
        where.append('("SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
        params["q"] = f"%{q}%"
    where_sql = " AND ".join(where)

    bruto_sql = 'SUM("Preco Unitario" * "Quantidade")'
    custo_operacional_sql = 'SUM(COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0))'
    lucro_real_sql = f'{bruto_sql} - {custo_operacional_sql}'
    unidades_sql = 'SUM("Quantidade")'

    if modo == "faturamento":
        valor_rank = bruto_sql
    elif modo == "unidades":
        valor_rank = unidades_sql
    else:
        modo = "lucro"
        valor_rank = lucro_real_sql

    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    sql_now = f"""
    SELECT
      "SKU" AS sku,
      "MLB" AS mlb,
      "Titulo" AS titulo,
      {unidades_sql} AS unidades_now,
      {bruto_sql}    AS faturamento_now,
      {custo_operacional_sql} AS custo_operacional_now,
      {lucro_real_sql} AS lucro_real_now,
      {valor_rank}   AS valor_now
    FROM vendas_ml
    WHERE {where_sql}
    GROUP BY sku, mlb, titulo
    ORDER BY valor_now DESC, sku
    LIMIT :limit
    """
    params_now = dict(params)
    params_now["limit"] = n
    cur.execute(sql_now, params_now)
    now_rows = [dict(r) for r in cur.fetchall()]

    start_prev, end_prev = _prev_period(start, end)
    params_prev = {"start": start_prev, "end": end_prev}
    where_prev = [f"{iso_date} BETWEEN :start AND :end"]
    if conta:
        where_prev.append('UPPER("Conta") = UPPER(:conta)')
        params_prev["conta"] = conta.upper()
    if q:
        where_prev.append('("SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
        params_prev["q"] = f"%{q}%"
    where_prev_sql = " AND ".join(where_prev)

    sql_prev = f"""
    SELECT
      "SKU" AS sku,
      "MLB" AS mlb,
      {unidades_sql} AS unidades_prev,
      {bruto_sql}    AS faturamento_prev,
      {custo_operacional_sql} AS custo_operacional_prev,
      {lucro_real_sql} AS lucro_real_prev,
      {valor_rank}   AS valor_prev
    FROM vendas_ml
    WHERE {where_prev_sql}
    GROUP BY sku, mlb
    """
    cur.execute(sql_prev, params_prev)
    prev_map = {(r["sku"], r["mlb"]): dict(r) for r in cur.fetchall()}
    con.close()

    out = []
    for r in now_rows:
        key = (r["sku"], r["mlb"])
        p = prev_map.get(key, {})
        now_val = float(r.get("valor_now") or 0)
        prev_val = float(p.get("valor_prev") or 0)
        delta_pct = ((now_val - prev_val) / prev_val) if prev_val > 0 else 0.0

        out.append({
            "sku": r["sku"],
            "mlb": r["mlb"],
            "titulo": r.get("titulo") or "",
            "unidades_now": int(r.get("unidades_now") or 0),
            "faturamento_now": float(r.get("faturamento_now") or 0),
            "custo_operacional_now": float(r.get("custo_operacional_now") or 0),
            "lucro_now": float(r.get("lucro_real_now") or 0),
            "unidades_prev": int(p.get("unidades_prev") or 0),
            "faturamento_prev": float(p.get("faturamento_prev") or 0),
            "lucro_prev": float(p.get("lucro_real_prev") or 0),
            "delta_pct": float(delta_pct)
        })
    return out


# ==========
#  CURVA ABC
# ==========
def get_abc(modo="lucro", start=None, end=None, conta="", q="", include_delta=True):
    """Extrai curva ABC de fisgarone.db"""
    iso_date = _br_date_to_iso()
    start, end = _default_period(start, end)

    params = {"start": start, "end": end}
    where = [f"{iso_date} BETWEEN :start AND :end"]

    if conta:
        where.append('UPPER("Conta") = UPPER(:conta)')
        params["conta"] = conta.upper()
    if q:
        where.append('("SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
        params["q"] = f"%{q}%"

    bruto_sql = 'SUM("Preco Unitario" * "Quantidade")'
    custo_operacional_sql = 'SUM(COALESCE("Comissoes",0) + COALESCE("Taxa Fixa ML",0) + COALESCE("Frete Seller",0))'
    lucro_real_sql = f'{bruto_sql} - {custo_operacional_sql}'
    unidades_sql = 'SUM("Quantidade")'
    frete_seller_sql = 'SUM("Frete Seller")'
    margem_real_sql = f'CASE WHEN {bruto_sql} > 0 THEN {lucro_real_sql}/{bruto_sql} ELSE 0 END'
    frete_pct_sql = f'CASE WHEN {bruto_sql} > 0 THEN {frete_seller_sql}/{bruto_sql} ELSE 0 END'

    if modo == "faturamento":
        valor_rank = bruto_sql
    elif modo == "unidades":
        valor_rank = unidades_sql
    elif modo == "margem":
        valor_rank = f'({margem_real_sql} * {bruto_sql})'
    else:
        modo = "lucro"
        valor_rank = lucro_real_sql

    where_sql = " AND ".join(where)

    sql = f"""
    WITH base AS (
        SELECT
            "Conta"  AS conta,
            "SKU"    AS sku,
            "MLB"    AS mlb,
            "Titulo" AS titulo,
            {unidades_sql}     AS unidades,
            {bruto_sql}        AS bruto_rs,
            {custo_operacional_sql} AS custo_operacional_rs,
            {lucro_real_sql} AS lucro_real_rs,
            {margem_real_sql} AS margem_real_pct,
            {frete_seller_sql} AS frete_seller_rs,
            {frete_pct_sql}    AS frete_pct,
            {valor_rank}       AS valor_rank
        FROM vendas_ml
        WHERE {where_sql}
        GROUP BY conta, sku, mlb, titulo
    ),
    ranked AS (
        SELECT
            *,
            SUM(valor_rank) OVER () AS total_valor,
            SUM(valor_rank) OVER (ORDER BY valor_rank DESC, sku) AS acum_valor
        FROM base
        WHERE valor_rank IS NOT NULL
    )
    SELECT
        conta, sku, mlb, titulo,
        unidades, bruto_rs, custo_operacional_rs, lucro_real_rs, margem_real_pct, 
        frete_seller_rs, frete_pct,
        CASE WHEN total_valor>0 THEN acum_valor/total_valor ELSE 0 END AS pct_acum
    FROM ranked
    ORDER BY valor_rank DESC, sku;
    """

    con = _conn()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]

    delta_map = {}
    if include_delta and rows:
        start_prev, end_prev = _prev_period(start, end)
        params_prev = {"start": start_prev, "end": end_prev}
        where_prev = [f"{iso_date} BETWEEN :start AND :end"]
        if conta:
            where_prev.append('UPPER("Conta") = UPPER(:conta)')
            params_prev["conta"] = conta.upper()
        if q:
            where_prev.append('("SKU" LIKE :q OR "MLB" LIKE :q OR UPPER("Titulo") LIKE UPPER(:q))')
            params_prev["q"] = f"%{q}%"
        where_prev_sql = " AND ".join(where_prev)

        sql_prev = f"""
        SELECT "SKU" AS sku, "MLB" AS mlb,
               {unidades_sql} AS unidades,
               {bruto_sql}    AS bruto_rs,
               {custo_operacional_sql} AS custo_operacional_rs,
               {lucro_real_sql} AS lucro_real_rs,
               {margem_real_sql} AS margem_real_pct
        FROM vendas_ml
        WHERE {where_prev_sql}
        GROUP BY sku, mlb;
        """
        cur.execute(sql_prev, params_prev)
        for pr in cur.fetchall():
            if modo == "faturamento":
                val = pr["bruto_rs"] or 0.0
            elif modo == "unidades":
                val = pr["unidades"] or 0.0
            elif modo == "margem":
                val = (pr["margem_real_pct"] or 0.0) * (pr["bruto_rs"] or 0.0)
            else:
                val = pr["lucro_real_rs"] or 0.0
            delta_map[(pr["sku"], pr["mlb"])] = float(val or 0.0)

    con.close()

    out = []
    for r in rows:
        sku = r["sku"]
        mlb = r["mlb"]
        if modo == "faturamento":
            now_val = float(r["bruto_rs"] or 0)
        elif modo == "unidades":
            now_val = float(r["unidades"] or 0)
        elif modo == "margem":
            now_val = float(r["margem_real_pct"] or 0) * float(r["bruto_rs"] or 0)
        else:
            now_val = float(r["lucro_real_rs"] or 0)
        prev_val = delta_map.get((sku, mlb), 0.0)
        delta_pct = ((now_val - prev_val) / prev_val) if (include_delta and prev_val > 0) else 0.0

        out.append({
            "conta": r["conta"],
            "sku": sku,
            "mlb": mlb,
            "titulo": r["titulo"],
            "unidades": int(r["unidades"] or 0),
            "faturamento_rs": float(r["bruto_rs"] or 0),
            "custo_operacional_rs": float(r["custo_operacional_rs"] or 0),
            "lucro_rs": float(r["lucro_real_rs"] or 0),
            "margem_pct": float(r["margem_real_pct"] or 0),
            "frete_seller_rs": float(r["frete_seller_rs"] or 0),
            "frete_pct": float(r["frete_pct"] or 0),
            "pct_acum": float(r["pct_acum"] or 0),
            "classe": ("A" if r["pct_acum"] <= 0.80 else "B" if r["pct_acum"] <= 0.95 else "C"),
            "delta_pct": float(delta_pct)
        })
    return out


def abc_to_xlsx(rows, titulo="Curva ABC"):
    """Exporta curva ABC para XLSX"""
    try:
        import xlsxwriter
    except Exception:
        raise RuntimeError("Instale xlsxwriter: pip install xlsxwriter")
    mem = io.BytesIO()
    wb = xlsxwriter.Workbook(mem, {'in_memory': True})
    ws = wb.add_worksheet("ABC")

    header = [
        "Conta", "SKU", "MLB", "Título", "Unidades", "Faturamento (R$)", "Custo Operacional (R$)", "Lucro Real (R$)",
        "Margem Real (%)", "Frete Seller (R$)", "Frete/Bruto (%)", "Acumulado (%)", "Classe", "Evolução (%)"
    ]
    fmt_h = wb.add_format({'bold': True, 'bg_color': '#E4F7FF', 'border': 1})
    fmt_n = wb.add_format({'num_format': '#,##0.00'})
    fmt_p = wb.add_format({'num_format': '0.00%'})
    ws.write_row(0, 0, header, fmt_h)

    r = 1
    for it in rows:
        ws.write(r, 0, it.get("conta") or "")
        ws.write(r, 1, it.get("sku") or "")
        ws.write(r, 2, it.get("mlb") or "")
        ws.write(r, 3, it.get("titulo") or "")
        ws.write_number(r, 4, it.get("unidades") or 0)
        ws.write_number(r, 5, it.get("faturamento_rs") or 0, fmt_n)
        ws.write_number(r, 6, it.get("custo_operacional_rs") or 0, fmt_n)
        ws.write_number(r, 7, it.get("lucro_rs") or 0, fmt_n)
        ws.write_number(r, 8, it.get("margem_pct") or 0, fmt_p)
        ws.write_number(r, 9, it.get("frete_seller_rs") or 0, fmt_n)
        ws.write_number(r, 10, it.get("frete_pct") or 0, fmt_p)
        ws.write_number(r, 11, it.get("pct_acum") or 0, fmt_p)
        ws.write(r, 12, it.get("classe") or "")
        ws.write_number(r, 13, it.get("delta_pct") or 0, fmt_p)
        r += 1

    ws.autofilter(0, 0, r - 1, len(header) - 1)
    ws.freeze_panes(1, 0)
    ws.set_column(0, 0, 12)
    ws.set_column(1, 2, 16)
    ws.set_column(3, 3, 42)
    ws.set_column(4, 13, 18)
    wb.close()
    mem.seek(0)
    return mem
