# importar_custos_componentes_v7.py
# -*- coding: utf-8 -*-
import argparse, sys, sqlite3, re, unicodedata
from pathlib import Path
import pandas as pd

SHEET = "Custos Componentes"
HEADERS = ["SKU do Kit","SKU do Componente","Nome do Componente","Custo no Anúncios_Kits"]

def norm_text(s):
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s.strip()

def to_float_ptbr(x):
    s = str(x).strip()
    if s == "" or s.lower() in {"nan","none","null"}: return None
    if re.search(r",\d{1,3}$", s): s = s.replace(".","").replace(",",".")
    s = re.sub(r"[^0-9.\-]","", s)
    try: return float(s)
    except: return None

def read_table(xlsx: Path, header_row: int|None):
    df0 = pd.read_excel(xlsx, sheet_name=SHEET, header=None, dtype=str).fillna("")
    hdr = header_row-1 if header_row else None
    if hdr is None:
        for i in range(min(25, len(df0))):
            row = [norm_text(v) for v in df0.iloc[i].tolist()]
            if all(norm_text(h) in row for h in HEADERS):
                hdr = i; break
    if hdr is None:
        print("ERRO: não encontrei o cabeçalho esperado. Passe --header-row N (linha 1-based).")
        print(df0.head(6).to_string(index=True)); sys.exit(2)
    headers = df0.iloc[hdr].tolist()
    df = df0.iloc[hdr+1:].copy(); df.columns = headers
    missing = [h for h in HEADERS if h not in df.columns]
    if missing:
        print("ERRO: Cabeçalhos ausentes:", missing, "\nEncontrados:", list(df.columns)); sys.exit(2)
    df = df[HEADERS].copy().fillna("")
    df["SKU do Componente"] = df["SKU do Componente"].astype(str).str.strip()
    df["Nome do Componente"] = df["Nome do Componente"].astype(str).str.strip()
    df["Custo no Anúncios_Kits"] = df["Custo no Anúncios_Kits"].apply(to_float_ptbr)
    valid = df[(df["SKU do Componente"]!="") & (df["Custo no Anúncios_Kits"].notna()) & (df["Custo no Anúncios_Kits"]>0)].copy()
    invalid = df[(df["SKU do Componente"]=="") | (df["Custo no Anúncios_Kits"].isna()) | (df["Custo no Anúncios_Kits"]<=0)].copy()
    return valid, invalid

def get_schema(conn, table):
    return conn.execute(f"PRAGMA table_info('{table}')").fetchall()

def main():
    ap = argparse.ArgumentParser(description="Insere faltantes em estoque (por SKU), atualiza anuncios_kits (por SKU) e recalcula anuncios_ml (por id_anuncio).")
    ap.add_argument("--db", required=True)
    ap.add_argument("--xlsx", required=True)
    ap.add_argument("--apply", action="store_true", help="Sem isso é DRY-RUN")
    ap.add_argument("--header-row", type=int, default=None, help="Cabeçalho 1-based, se não estiver na 1ª linha útil")
    # nomes de colunas (EXATOS) nas tabelas — sem adivinhação
    ap.add_argument("--estoque-sku-col", default="sku")
    ap.add_argument("--estoque-nome-col", default="nome")
    ap.add_argument("--estoque-custo-col", default="custo_com_ipi")
    ap.add_argument("--estoque-ativo-col", default="ativo")
    ap.add_argument("--estoque-dtcriacao-col", default="data_criacao")
    ap.add_argument("--kits-sku-col", default="sku", help="coluna do SKU do componente em anuncios_kits (você disse que é 'sku')")
    ap.add_argument("--kits-custo-col", default="custo_componente_ipi")
    ap.add_argument("--kits-ts-col", default="custo_atualizado_em")  # opcional, se existir
    args = ap.parse_args()

    dbp, xlp = Path(args.db), Path(args.xlsx)
    if not dbp.exists(): print(f"ERRO: DB não encontrado: {dbp}"); sys.exit(2)
    if not xlp.exists(): print(f"ERRO: XLSX não encontrado: {xlp}"); sys.exit(2)

    valid, invalid = read_table(xlp, args.header_row)
    conn = sqlite3.connect(str(dbp)); conn.row_factory = sqlite3.Row

    # Checar existência de colunas (sem chute)
    est_cols = [r[1] for r in get_schema(conn, "estoque")]
    ak_cols  = [r[1] for r in get_schema(conn, "anuncios_kits")]
    for need, cols, tab in [
        (args.estoque-sku-col if False else args.estoque_sku_col, est_cols, "estoque"),  # evitar bug do hífen
        (args.estoque_nome_col, est_cols, "estoque"),
        (args.estoque_custo_col, est_cols, "estoque"),
        (args.kits_sku_col, ak_cols, "anuncios_kits"),
        (args.kits_custo_col, ak_cols, "anuncios_kits"),
    ]:
        if need not in cols:
            print(f"ERRO: coluna '{need}' não existe em '{tab}'. Colunas: {cols}")
            conn.close(); sys.exit(2)
    has_ativo = args.estoque_ativo_col in est_cols
    has_dtcri = args.estoque_dtcriacao_col in est_cols
    has_kits_ts = args.kits_ts_col in ak_cols

    # Mapas atuais para DRY-RUN
    cur = conn.cursor()
    m_est = {r["sku"]: r["c"] for r in conn.execute(f"SELECT {args.estoque_sku_col} AS sku, {args.estoque_custo_col} AS c FROM estoque")}
    m_ak  = {r["sku"]: r["c"] for r in conn.execute(f"SELECT {args.kits_sku_col} AS sku, {args.kits_custo_col} AS c FROM anuncios_kits")}

    inserts_estoque, updates_estoque, updates_kits = [], [], []
    pend_kits = []  # SKUs que não existem em anuncios_kits

    try:
        if args.apply:
            with conn:
                # 1) ESTOQUE: INSERT faltantes + UPDATE custo
                for _, r in valid.iterrows():
                    sku = r["SKU do Componente"]; nome = r["Nome do Componente"]; custo = float(r["Custo no Anúncios_Kits"])
                    cur.execute(f"SELECT 1 FROM estoque WHERE {args.estoque_sku_col}=? LIMIT 1", (sku,))
                    exists = cur.fetchone() is not None
                    if not exists:
                        # INSERT mínimo, seguro
                        cols = [args.estoque_sku_col, args.estoque_nome_col, args.estoque_custo_col]
                        vals = [sku, nome, custo]
                        if has_ativo:  cols += [args.estoque_ativo_col];    vals += [1]
                        if has_dtcri:  cols += [args.estoque_dtcriacao_col]; vals += ["datetime('now')"]
                        placeholders = ",".join(["?"]*len(vals))
                        sql = f"INSERT INTO estoque ({','.join(cols)}) VALUES ({placeholders})"
                        cur.execute(sql, vals)
                        inserts_estoque.append(sku)
                    else:
                        cur.execute(f"UPDATE estoque SET {args.estoque_custo_col}=? WHERE {args.estoque_sku_col}=?", (custo, sku))
                        updates_estoque.append({"sku": sku, "de": m_est.get(sku), "para": custo})

                # 2) ANUNCIOS_KITS: UPDATE custo por SKU
                for _, r in valid.iterrows():
                    sku = r["SKU do Componente"]; custo = float(r["Custo no Anúncios_Kits"])
                    cur.execute(f"SELECT 1 FROM anuncios_kits WHERE {args.kits_sku_col}=? LIMIT 1", (sku,))
                    if cur.fetchone():
                        if has_kits_ts:
                            cur.execute(
                                f"UPDATE anuncios_kits SET {args.kits_custo_col}=?, {args.kits_ts_col}=datetime('now') WHERE {args.kits_sku_col}=?",
                                (custo, sku)
                            )
                        else:
                            cur.execute(
                                f"UPDATE anuncios_kits SET {args.kits_custo_col}=? WHERE {args.kits_sku_col}=?",
                                (custo, sku)
                            )
                        updates_kits.append({"sku": sku, "de": m_ak.get(sku), "para": custo})
                    else:
                        pend_kits.append(sku)

                # 3) Recalcular anuncios_ml por id_anuncio (agregado)
                conn.executescript("""
                DROP VIEW IF EXISTS v_custos_por_id;
                CREATE VIEW v_custos_por_id AS
                SELECT
                    id_anuncio,
                    SUM(COALESCE(quantidade,0) * COALESCE(custo_componente_ipi,0)) AS custo_anuncio,
                    COUNT(*) AS componentes_total,
                    SUM(CASE
                          WHEN custo_componente_ipi IS NULL
                           OR TRIM(CAST(custo_componente_ipi AS TEXT)) = ''
                           OR CAST(custo_componente_ipi AS REAL) <= 0
                        THEN 1 ELSE 0 END) AS componentes_sem_custo
                FROM anuncios_kits
                WHERE id_anuncio IS NOT NULL
                GROUP BY id_anuncio;
                UPDATE anuncios_ml
                SET
                    custo_anuncio = COALESCE((SELECT v.custo_anuncio FROM v_custos_por_id v WHERE v.id_anuncio = anuncios_ml.id_anuncio), 0),
                    componentes_total = COALESCE((SELECT v.componentes_total FROM v_custos_por_id v WHERE v.id_anuncio = anuncios_ml.id_anuncio), 0),
                    componentes_sem_custo = COALESCE((SELECT v.componentes_sem_custo FROM v_custos_por_id v WHERE v.id_anuncio = anuncios_ml.id_anuncio), 0),
                    custo_incompleto = CASE WHEN COALESCE((SELECT v.componentes_sem_custo FROM v_custos_por_id v WHERE v.id_anuncio = anuncios_ml.id_anuncio), 0) > 0 THEN 1 ELSE 0 END;
                """)
        else:
            # DRY-RUN
            for _, r in valid.iterrows():
                sku = r["SKU do Componente"]; custo = float(r["Custo no Anúncios_Kits"])
                if sku not in m_est:
                    inserts_estoque.append(sku)
                else:
                    updates_estoque.append({"sku": sku, "de": m_est.get(sku), "para": custo})
                if sku in m_ak:
                    updates_kits.append({"sku": sku, "de": m_ak.get(sku), "para": custo})
                else:
                    pend_kits.append(sku)
    finally:
        conn.close()

    # Sumário direto
    print("==== SUMÁRIO ====")
    print(f"Modo: {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"Inserções em ESTOQUE (faltantes): {len(inserts_estoque)}")
    print(f"Updates de custo em ESTOQUE: {len(updates_estoque)}")
    print(f"Updates de custo em ANUNCIOS_KITS: {len(updates_kits)}")
    print(f"SKUs NÃO encontrados em ANUNCIOS_KITS (pendentes): {len(set(pend_kits))}")
    print("Chaves: ESTOQUE.sku = ANUNCIOS_KITS.sku ; ANUNCIOS_KITS.id_anuncio = ANUNCIOS_ML.id_anuncio")

if __name__ == "__main__":
    main()
