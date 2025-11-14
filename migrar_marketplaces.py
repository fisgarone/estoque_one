# -*- coding: utf-8 -*-
"""
Migração de tabelas de Marketplaces (SEM estoque).
Origem:  grupofisgar.db
Destino: fisgarone.db
Tabelas: vendas_ml, pedidos, vendas_shopee

Uso:
  python scripts/migrar_marketplaces.py \
      --old "C:\\estoque_one\\grupofisgar.db" \
      --new "C:\\estoque_one\\fisgarone.db"

Opções:
  --no-recreate  -> não dropa tabelas no destino; cria só se não existir.
"""

import argparse
import os
import sqlite3
import sys
import re
from contextlib import closing

TABLES = ["entradas_financeiras", "custos_ml", "repasses_ml", "repasses_shopee"]

DEFAULT_OLD = r"C:\estoque_one\grupofisgar.db"
DEFAULT_NEW = r"C:\estoque_one\fisgarone.db"

def die(msg, code=1):
    print(f"ERRO: {msg}")
    sys.exit(code)

def fetch_create_sql(conn, table_name):
    cur = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND LOWER(name)=LOWER(?)",
        (table_name,)
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def table_exists(conn, table_name):
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND LOWER(name)=LOWER(?)",
        (table_name,)
    )
    return cur.fetchone() is not None

def norm_sql(sql):
    # normaliza para comparar esquemas (remove múltiplos espaços/linhas)
    s = re.sub(r"\s+", " ", sql or "").strip().lower()
    return s

def drop_table(conn, table_name):
    q = f'DROP TABLE IF EXISTS "{table_name}"'
    conn.execute(q)

def create_table_from_sql(conn, create_sql, table_name, if_not_exists=True):
    if create_sql is None:
        return False
    # Garante IF NOT EXISTS se solicitado
    sql = create_sql.strip()
    # substitui apenas a primeira ocorrência de CREATE TABLE "nome"
    patt = re.compile(r'(?i)^create\s+table\s+("?%s"?)' % re.escape(table_name))
    if if_not_exists:
        sql = patt.sub(r'CREATE TABLE IF NOT EXISTS \1', sql, count=1)
    else:
        sql = patt.sub(r'CREATE TABLE \1', sql, count=1)
    conn.execute(sql)
    return True

def count_rows(conn, table_name):
    cur = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"')
    return int(cur.fetchone()[0])

def migrate_table(old_db, new_db, table_name, recreate=True):
    report = {"table": table_name, "source_rows": 0, "dest_rows": 0, "status": "skipped", "note": ""}

    with closing(sqlite3.connect(old_db)) as src, closing(sqlite3.connect(new_db)) as dst:
        src.row_factory = sqlite3.Row
        dst.row_factory = sqlite3.Row

        # Checa existência da tabela na origem
        src_sql = fetch_create_sql(src, table_name)
        if not src_sql:
            report["status"] = "missing_source"
            report["note"] = "Tabela não existe no banco de origem."
            return report

        # (Re)cria tabela no destino
        dst.execute("PRAGMA foreign_keys = OFF;")
        dst.execute("BEGIN;")
        try:
            if recreate:
                drop_table(dst, table_name)
                create_table_from_sql(dst, src_sql, table_name, if_not_exists=False)
            else:
                if not table_exists(dst, table_name):
                    create_table_from_sql(dst, src_sql, table_name, if_not_exists=True)
                else:
                    # se já existe, confere esquema; se diferente, falha claro
                    dst_sql = fetch_create_sql(dst, table_name)
                    if norm_sql(dst_sql) != norm_sql(src_sql):
                        raise RuntimeError(
                            f'Esquema diferente no destino para "{table_name}". '
                            f'Use sem --no-recreate para alinhar, ou ajuste manualmente.'
                        )
            dst.commit()
        except Exception as e:
            dst.rollback()
            raise

        # Copia dados via ATTACH (evita listar colunas, funciona com nomes com espaços)
        # Limpa dados se recriamos? Já dropamos. Se não recriamos, garantimos insert limpo:
        if not recreate:
            # para evitar PK duplicada, faça limpeza opcional aqui (comente se quiser acumular)
            dst.execute("BEGIN;")
            try:
                dst.execute(f'DELETE FROM "{table_name}"')
                dst.commit()
            except Exception as e:
                dst.rollback()
                raise

        # attach + insert
        dst.execute("BEGIN;")
        try:
            dst.execute(f"ATTACH DATABASE ? AS olddb", (old_db,))
            # count source rows
            cur = dst.execute(f'SELECT COUNT(*) FROM olddb."{table_name}"')
            report["source_rows"] = int(cur.fetchone()[0])

            dst.execute(f'INSERT INTO "{table_name}" SELECT * FROM olddb."{table_name}"')
            dst.execute("DETACH DATABASE olddb")
            dst.commit()
            report["dest_rows"] = count_rows(dst, table_name)
            report["status"] = "ok"
        except Exception as e:
            dst.rollback()
            report["status"] = "error"
            report["note"] = f"Falha ao inserir dados: {e}"
            # tenta garantir detach
            try:
                dst.execute("DETACH DATABASE olddb")
            except Exception:
                pass

    return report

def main():
    ap = argparse.ArgumentParser(description="Migrar tabelas de Marketplaces (ML/Shopee) entre SQLite.")
    ap.add_argument("--old", default=DEFAULT_OLD, help="Caminho do banco ORIGEM (grupofisgar.db)")
    ap.add_argument("--new", default=DEFAULT_NEW, help="Caminho do banco DESTINO (fisgarone.db)")
    ap.add_argument("--no-recreate", action="store_true", help="Não dropar/recriar tabelas destino.")
    args = ap.parse_args()

    old_db = os.path.abspath(args.old)
    new_db = os.path.abspath(args.new)
    recreate = not args.no_recreate

    if not os.path.exists(old_db):
        die(f'Arquivo de origem não encontrado: {old_db}')

    # Garante pasta do destino
    os.makedirs(os.path.dirname(new_db), exist_ok=True)
    # Cria arquivo destino se não existir
    if not os.path.exists(new_db):
        open(new_db, "a").close()

    print("=== MIGRAÇÃO MARKETPLACES ===")
    print(f"Origem : {old_db}")
    print(f"Destino: {new_db}")
    print(f"Recriar tabelas destino: {'SIM' if recreate else 'NÃO'}")
    print("-------------------------------------")

    results = []
    for tb in TABLES:
        print(f">>> Migrando tabela: {tb} ...")
        try:
            rep = migrate_table(old_db, new_db, tb, recreate=recreate)
            status = rep['status']
            if status == "ok":
                print(f"    OK  | linhas origem: {rep['source_rows']}  -> destino: {rep['dest_rows']}")
            elif status == "missing_source":
                print(f"    AVISO: tabela não existe na origem, pulada.")
            else:
                print(f"    ERRO: {rep.get('note','')}")
            results.append(rep)
        except Exception as e:
            print(f"    ERRO FATAL: {e}")
            results.append({"table": tb, "status": "error", "note": str(e)})

    print("\n=== RESUMO ===")
    for r in results:
        t = r["table"]
        st = r["status"]
        if st == "ok":
            print(f"- {t}: OK ({r['source_rows']} -> {r['dest_rows']})")
        elif st == "missing_source":
            print(f"- {t}: PULADA (não existe na origem)")
        else:
            print(f"- {t}: ERRO ({r.get('note','')})")

    # Sinaliza exit code != 0 se houve erro
    if any(r["status"] not in ("ok", "missing_source") for r in results):
        sys.exit(2)
    print("Pronto.")

if __name__ == "__main__":
    main()
