import os
import sqlite3
from pathlib import Path

# Diretório raiz do projeto
BASE_DIR = Path(__file__).resolve().parent

# Garantir que o processo está rodando dentro da pasta correta
os.chdir(BASE_DIR)

DB_MAIN = BASE_DIR / "fisgarone.db"
DB_POLITICAS = BASE_DIR / "politicas_canais.db"

if not DB_MAIN.exists():
    raise FileNotFoundError(f"Banco principal não encontrado: {DB_MAIN}")

if not DB_POLITICAS.exists():
    raise FileNotFoundError(f"Banco de políticas não encontrado: {DB_POLITICAS}")

SQL_SCRIPT = """
-- 1) Anexar o banco de políticas como "politicas"
ATTACH DATABASE 'politicas_canais.db' AS politicas;

-- 2) Criar tabelas em fisgarone.db (se ainda não existirem)

CREATE TABLE IF NOT EXISTS politicas_canais (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canal TEXT NOT NULL,
    plano TEXT NOT NULL,
    preco_unit_min REAL NOT NULL,
    preco_unit_max REAL,
    comissao_percent_base REAL,
    taxa_fixa_tipo TEXT NOT NULL,
    taxa_fixa_valor REAL,
    frete_seller_tipo TEXT NOT NULL,
    frete_seller_valor REAL NOT NULL DEFAULT 0,
    insumos_percent REAL NOT NULL,
    ads_percent REAL NOT NULL,
    ativo INTEGER NOT NULL DEFAULT 1,
    observacoes_regra TEXT,
    UNIQUE (canal, plano, preco_unit_min, preco_unit_max)
);

CREATE TABLE IF NOT EXISTS politicas_canais_faixas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canal TEXT NOT NULL,
    plano TEXT NOT NULL,
    preco_unit_min REAL NOT NULL,
    preco_unit_max REAL NOT NULL,
    tipo_valor TEXT NOT NULL,
    valor REAL NOT NULL,
    ativo INTEGER NOT NULL DEFAULT 1,
    observacoes TEXT,
    UNIQUE (canal, plano, preco_unit_min, preco_unit_max)
);

CREATE TABLE IF NOT EXISTS politicas_cnpj (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conta TEXT NOT NULL,
    custo_estrutura_percent REAL NOT NULL,
    aliquota_fiscal_percent REAL NOT NULL,
    ativo INTEGER NOT NULL DEFAULT 1,
    observacoes TEXT,
    UNIQUE (conta)
);

-- 3) Copiar os dados do politicas_canais.db para o fisgarone.db

INSERT OR IGNORE INTO politicas_canais (
    id,
    canal,
    plano,
    preco_unit_min,
    preco_unit_max,
    comissao_percent_base,
    taxa_fixa_tipo,
    taxa_fixa_valor,
    frete_seller_tipo,
    frete_seller_valor,
    insumos_percent,
    ads_percent,
    ativo,
    observacoes_regra
)
SELECT
    id,
    canal,
    plano,
    preco_unit_min,
    preco_unit_max,
    comissao_percent_base,
    taxa_fixa_tipo,
    taxa_fixa_valor,
    frete_seller_tipo,
    frete_seller_valor,
    insumos_percent,
    ads_percent,
    ativo,
    observacoes_regra
FROM politicas.politicas_canais;

INSERT OR IGNORE INTO politicas_canais_faixas (
    id,
    canal,
    plano,
    preco_unit_min,
    preco_unit_max,
    tipo_valor,
    valor,
    ativo,
    observacoes
)
SELECT
    id,
    canal,
    plano,
    preco_unit_min,
    preco_unit_max,
    tipo_valor,
    valor,
    ativo,
    observacoes
FROM politicas.politicas_canais_faixas;

INSERT OR IGNORE INTO politicas_cnpj (
    id,
    conta,
    custo_estrutura_percent,
    aliquota_fiscal_percent,
    ativo,
    observacoes
)
SELECT
    id,
    conta,
    custo_estrutura_percent,
    aliquota_fiscal_percent,
    ativo,
    observacoes
FROM politicas.politicas_cnpj;

-- 4) Desanexar o banco de políticas
DETACH DATABASE politicas;
"""


def main():
    print(f"Usando banco principal: {DB_MAIN}")
    print(f"Usando banco de políticas: {DB_POLITICAS}")

    conn = sqlite3.connect(DB_MAIN)
    try:
        conn.executescript(SQL_SCRIPT)
        conn.commit()
    finally:
        conn.close()

    print("Migração concluída com sucesso.")

    # Checagem rápida das contagens
    conn = sqlite3.connect(DB_MAIN)
    try:
        cur = conn.cursor()
        for tabela in [
            "politicas_canais",
            "politicas_canais_faixas",
            "politicas_cnpj",
        ]:
            cur.execute(f"SELECT COUNT(*) FROM {tabela}")
            qtd = cur.fetchone()[0]
            print(f"Tabela {tabela}: {qtd} registros.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
