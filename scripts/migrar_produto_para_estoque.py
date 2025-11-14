# scripts/migrar_produto_para_estoque.py
import sqlite3, sys, os
from pathlib import Path

# Garante o .db na RAIZ do projeto (mesmo nível do app.py)
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = str(BASE_DIR / "fisgarone.db")

def table_exists(c, name):
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return c.fetchone() is not None

def index_names(c, table):
    c.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?;", (table,))
    return [r[0] for r in c.fetchall()]

def sqlite_version(c):
    c.execute("select sqlite_version();")
    return c.fetchone()[0]

def main():
    print(f"[OK] Banco: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("[ERRO] fisgarone.db não encontrado na raiz do projeto."); sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON;")
    c = conn.cursor()

    ver = sqlite_version(c)
    print(f"[INFO] SQLite versão: {ver}")

    has_produto  = table_exists(c, "produto")
    has_estoque  = table_exists(c, "estoque")

    if not has_produto and not has_estoque:
        print("[INFO] Nem 'produto' nem 'estoque' existem. Nada a fazer."); return
    if has_estoque and not has_produto:
        print("[INFO] 'estoque' já existe e 'produto' não existe. Nada a fazer."); return
    if has_estoque and has_produto:
        print("[ALERTA] Existem as duas: 'produto' e 'estoque'. Interrompendo para evitar perda/duplicidade.")
        print("→ Resolva manualmente (comparar colunas/dados) e remova uma delas. Depois rode de novo.")
        sys.exit(2)

    print("[RUN] Renomeando 'produto' → 'estoque'...")
    try:
        conn.execute("BEGIN;")
        # SQLite moderno atualiza FKs ao renomear a tabela. Em versões antigas, cairíamos num plano B.
        c.execute("ALTER TABLE produto RENAME TO estoque;")

        # Opcional: renomear índices comuns (melhor estética)
        for ix in index_names(c, "estoque"):
            if ix.startswith("ix_produto_"):
                novo = ix.replace("ix_produto_", "ix_estoque_")
                try:
                    c.execute(f"ALTER TABLE sqlite_master RENAME TO _block_")  # impossível; força fallback
                except:
                    # SQLite não renomeia índice por ALTER; recriar:
                    # 1) pegar DDL
                    c.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (ix,))
                    ddl = c.fetchone()
                    if ddl and ddl[0]:
                        ddl_new = ddl[0].replace(ix, novo).replace(" ON produto(", " ON estoque(")
                        c.execute(f"DROP INDEX IF EXISTS {ix};")
                        c.execute(ddl_new)
        conn.commit()
        print("[OK] Tabela renomeada para 'estoque'.")
    except Exception as e:
        conn.rollback()
        print("[ERRO] Falha ao renomear:", e)
        sys.exit(3)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
