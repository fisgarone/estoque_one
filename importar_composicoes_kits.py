# -*- coding: utf-8 -*-
"""
importar_composicoes_kits.py

Execução única: cria snapshot da composição de kits a partir de
'RAIZ/Relatório_Composição Kits.xlsx' e grava em 'RAIZ/fisgarone.db'.

Política: estoque tem só produtos simples (sem kits).
Comparação: APENAS componentes (planilha."Cód. Referência") × estoque.sku.

Tabela criada/recriada: composicao_de_kits_ml
Relatórios CSV em: RAIZ/_saida/

PT-BR. Sem placeholders. Sem dados fictícios.
"""

import os
import sys
import sqlite3
from datetime import datetime
import argparse
import pandas as pd


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def to_float_safe(x):
    try:
        return float(str(x).replace(",", ".").strip())
    except Exception:
        return None


def clean_str(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None


def pick_column(original_columns, *candidates_lower):
    """Escolhe coluna por nome (case-insensitive), preservando acentos do cabeçalho original."""
    lower_map = {c.lower().strip(): c for c in original_columns}
    for cand in candidates_lower:
        if cand in lower_map:
            return lower_map[cand]
    return None


def criar_tabela(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys=OFF;")
    cur.execute("BEGIN;")
    cur.execute("DROP TABLE IF EXISTS composicao_de_kits_ml;")
    cur.execute("""
    CREATE TABLE composicao_de_kits_ml (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku_kit TEXT NOT NULL,
        nome_kit TEXT,
        sku_componente TEXT NOT NULL,
        nome_componente TEXT,
        quantidade REAL,
        id_sku_kit TEXT,
        id_sku_componente TEXT,
        id_relacao TEXT,
        snapshot_at TEXT NOT NULL
    );
    """)
    conn.commit()


def criar_indices(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ckm_sku_kit ON composicao_de_kits_ml(sku_kit);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ckm_sku_comp ON composicao_de_kits_ml(sku_componente);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ckm_relacao ON composicao_de_kits_ml(id_relacao);")
    conn.commit()


def carregar_planilha(caminho_excel: str, aba: str = "Report") -> pd.DataFrame:
    if not os.path.exists(caminho_excel):
        raise FileNotFoundError(f"Planilha não encontrada: {caminho_excel}")

    df = pd.read_excel(caminho_excel, sheet_name=aba)

    # Captura dos nomes (robusto a variações usuais)
    col_sku_kit = pick_column(df.columns,
                              "cód. referência kit", "cod. referência kit", "cod. referencia kit")
    col_nome_kit = pick_column(df.columns, "nome kit")
    # EXIGIDO: componentes = "Cód. Referência"
    col_sku_comp = pick_column(df.columns,
                               "cód. referência", "cod. referência", "cod. referencia")
    col_nome_comp = pick_column(df.columns, "nome sku")
    col_qtd = pick_column(df.columns, "quantidade")
    col_id_kit = pick_column(df.columns, "|idskukit|")
    col_id_comp = pick_column(df.columns, "|idskucomponent|")
    col_id_rel = pick_column(df.columns, "|idstockkeepingunitkititems|")

    if col_sku_comp is None:
        raise ValueError("Coluna 'Cód. Referência' (itens do kit) não encontrada na planilha.")

    df = df.rename(columns={
        col_sku_kit: "sku_kit",
        col_nome_kit: "nome_kit",
        col_sku_comp: "sku_componente",
        col_nome_comp: "nome_componente",
        col_qtd: "quantidade",
        col_id_kit: "id_sku_kit",
        col_id_comp: "id_sku_componente",
        col_id_rel: "id_relacao"
    })

    # Limpeza e tipagem
    df["sku_kit"] = df.get("sku_kit", None)
    df["nome_kit"] = df.get("nome_kit", None)
    df["sku_componente"] = df.get("sku_componente", None)
    df["nome_componente"] = df.get("nome_componente", None)

    df["sku_kit"] = df["sku_kit"].apply(clean_str)
    df["sku_componente"] = df["sku_componente"].apply(clean_str)
    df["nome_kit"] = df["nome_kit"].apply(lambda x: None if pd.isna(x) else str(x).strip())
    df["nome_componente"] = df["nome_componente"].apply(lambda x: None if pd.isna(x) else str(x).strip())
    if "quantidade" in df.columns:
        df["quantidade"] = df["quantidade"].apply(to_float_safe)
    else:
        df["quantidade"] = None

    # Mantém somente linhas válidas
    df = df[(df["sku_kit"].notna()) & (df["sku_componente"].notna())].copy()

    # Snapshot
    df["snapshot_at"] = datetime.now().isoformat(timespec="seconds")

    cols_final = [
        "sku_kit", "nome_kit", "sku_componente", "nome_componente", "quantidade",
        "id_sku_kit", "id_sku_componente", "id_relacao", "snapshot_at"
    ]
    return df[cols_final]


def salvar_csv(df: pd.DataFrame, caminho: str) -> None:
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    df.to_csv(caminho, index=False, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Importar composições de kits (execução única).")
    parser.add_argument("--root", default="estoque_one",
                        help="Pasta raiz onde estão a planilha e o banco (ex.: C:\\estoque_one).")
    parser.add_argument("--excel", default=None,
                        help="(Opcional) Caminho completo para a planilha. Se omitido, usa ROOT/Relatório_Composição Kits.xlsx")
    parser.add_argument("--db", default=None,
                        help="(Opcional) Caminho completo para o banco. Se omitido, usa ROOT/fisgarone.db")
    args = parser.parse_args()

    raiz = args.root
    excel_path = args.excel or os.path.join(raiz, "Relatório_Composição Kits.xlsx")
    db_path = args.db or os.path.join(raiz, "fisgarone.db")
    out_dir = os.path.join(raiz, "_saida")

    log(f"Raiz: {raiz}")
    log(f"Planilha: {excel_path}")
    log(f"Banco: {db_path}")
    os.makedirs(out_dir, exist_ok=True)

    # Carregar e normalizar planilha
    df_comp = carregar_planilha(excel_path)
    log(f"Linhas válidas após limpeza: {len(df_comp)}")

    # Conectar e recriar tabela
    conn = sqlite3.connect(db_path)
    criar_tabela(conn)
    df_comp.to_sql("composicao_de_kits_ml", conn, if_exists="append", index=False)
    criar_indices(conn)

    # Métricas gerais
    total = pd.read_sql_query("SELECT COUNT(*) AS total FROM composicao_de_kits_ml;", conn).iloc[0, 0]
    kits = pd.read_sql_query("SELECT COUNT(DISTINCT sku_kit) AS kits FROM composicao_de_kits_ml;", conn).iloc[0, 0]
    componentes = pd.read_sql_query("SELECT COUNT(DISTINCT sku_componente) AS componentes FROM composicao_de_kits_ml;", conn).iloc[0, 0]
    log(f"Linhas inseridas: {total} | Kits distintos: {kits} | Componentes distintos: {componentes}")

    # Verificações com estoque.sku (apenas COMPONENTES)
    t_exists = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name='estoque';", conn)
    if t_exists.empty:
        log("ATENÇÃO: Tabela 'estoque' não encontrada. Comparações puladas.")
        conn.close()
        sys.exit(0)

    cols = pd.read_sql_query("PRAGMA table_info(estoque);", conn)
    if "sku" not in cols["name"].tolist():
        log("ATENÇÃO: Coluna 'sku' não existe em 'estoque'. Comparações puladas.")
        conn.close()
        sys.exit(0)

    # Componentes que existem no estoque
    componentes_no_estoque = pd.read_sql_query("""
        SELECT DISTINCT c.sku_componente
        FROM composicao_de_kits_ml c
        INNER JOIN estoque e ON e.sku = c.sku_componente
        ORDER BY c.sku_componente;
    """, conn)

    # Componentes da composição que não existem no estoque
    componentes_fora_estoque = pd.read_sql_query("""
        SELECT DISTINCT c.sku_componente
        FROM composicao_de_kits_ml c
        LEFT JOIN estoque e ON e.sku = c.sku_componente
        WHERE e.sku IS NULL
        ORDER BY c.sku_componente;
    """, conn)

    # Pares duplicados e quantidades inválidas
    duplicidades = pd.read_sql_query("""
        SELECT sku_kit, sku_componente, COUNT(*) AS repeticoes
        FROM composicao_de_kits_ml
        GROUP BY sku_kit, sku_componente
        HAVING COUNT(*) > 1
        ORDER BY repeticoes DESC, sku_kit, sku_componente;
    """, conn)

    qtd_invalida = pd.read_sql_query("""
        SELECT sku_kit, sku_componente, quantidade
        FROM composicao_de_kits_ml
        WHERE quantidade IS NULL OR quantidade <= 0
        ORDER BY sku_kit, sku_componente;
    """, conn)

    # Top componentes e kits (apenas informativo)
    top_componentes = pd.read_sql_query("""
        SELECT sku_componente, COUNT(*) AS vezes_em_kits, SUM(quantidade) AS qtd_total
        FROM composicao_de_kits_ml
        GROUP BY sku_componente
        ORDER BY vezes_em_kits DESC, sku_componente
        LIMIT 50;
    """, conn)

    top_kits_por_componentes = pd.read_sql_query("""
        SELECT sku_kit, COUNT(*) AS componentes
        FROM composicao_de_kits_ml
        GROUP BY sku_kit
        ORDER BY componentes DESC, sku_kit
        LIMIT 50;
    """, conn)

    # Persistir relatórios
    salvar_csv(componentes_no_estoque, os.path.join(out_dir, "componentes_no_estoque.csv"))
    salvar_csv(componentes_fora_estoque, os.path.join(out_dir, "componentes_fora_estoque.csv"))
    salvar_csv(duplicidades, os.path.join(out_dir, "duplicidades_kit_componente.csv"))
    salvar_csv(qtd_invalida, os.path.join(out_dir, "quantidades_invalidas.csv"))
    salvar_csv(top_componentes, os.path.join(out_dir, "top_componentes.csv"))
    salvar_csv(top_kits_por_componentes, os.path.join(out_dir, "top_kits_por_componentes.csv"))

    # Resumo final (política: NÃO avaliar kits no estoque)
    log("=== RESUMO (POLÍTICA: estoque não tem kits) ===")
    log(f"Componentes NO estoque: {componentes_no_estoque.shape[0]}")
    log(f"Componentes FORA do estoque: {componentes_fora_estoque.shape[0]}")
    log(f"Pares (kit, componente) DUPLICADOS: {duplicidades.shape[0]}")
    log(f"Registros com QUANTIDADE nula/zero: {qtd_invalida.shape[0]}")
    log(f"Relatórios gerados em: {out_dir}")

    conn.close()


if __name__ == "__main__":
    main()
