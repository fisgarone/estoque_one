# scripts/reset_schema.py
# Reset LIMPO: derruba legados e _new, recria só as tabelas relevantes.
# Preserva 'estoque' (mude KEEP_ESTOQUE=False se quiser zerar também).

import sqlite3
from pathlib import Path

KEEP_ESTOQUE = True  # True = mantém a tabela 'estoque' existente

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "fisgarone.db"

LEGACY_EXACT = {
    "movimentacao_estoque",
    "movimentacaoestoque",
    "inventario_item",
    "inventarioitem",
    "movimentacoes_estoque_new",
    "inventario_itens_new",
}

CANONICAL = [
    "estoque",
    "fornecedor",
    "produto_fornecedor",
    "produto_nf",
    "inventario",
    "inventario_itens",
    "movimentacoes_estoque",
    "produto_canal",
]

DDL_CREATE = {
    "estoque": """
CREATE TABLE IF NOT EXISTS estoque (
  id                   INTEGER PRIMARY KEY,
  sku                  TEXT NOT NULL UNIQUE,
  nome                 TEXT NOT NULL,
  apelido              TEXT,
  categoria            TEXT,
  marca                TEXT,
  tipo_produto         TEXT,
  ncm                  TEXT,
  cest                 TEXT,
  cod_barras           TEXT,
  cod_fabricante       TEXT,
  cod_interno          TEXT,
  unidade_venda        TEXT DEFAULT 'UN',
  quantidade_atual     REAL DEFAULT 0.0,
  estoque_minimo       REAL DEFAULT 0.0,
  estoque_maximo       REAL DEFAULT 0.0,
  local_rua            TEXT,
  local_prateleira     TEXT,
  local_posicao        TEXT,
  peso_gramas          REAL,
  largura_cm           REAL,
  altura_cm            REAL,
  profundidade_cm      REAL,
  preco_venda          REAL DEFAULT 0.0,
  preco_promocional    REAL,
  custo_unitario       REAL DEFAULT 0.0,          -- CMP (por UN)
  custo_ultima_compra  REAL,
  icms_cst             TEXT,
  pis_cst              TEXT,
  cofins_cst           TEXT,
  ipi_cst              TEXT,
  ipi_percentual       REAL DEFAULT 0.0,
  cod_anp              TEXT,
  cod_servico          TEXT,
  fornecedor_padrao_id INTEGER,
  url_imagem_principal TEXT,
  url_imagens          TEXT,
  ativo                INTEGER DEFAULT 1,
  criado_em_iso        TEXT DEFAULT (datetime('now')),
  criado_em_br         TEXT
);
CREATE INDEX IF NOT EXISTS idx_estoque_nome      ON estoque(nome);
CREATE INDEX IF NOT EXISTS idx_estoque_categoria ON estoque(categoria);
CREATE INDEX IF NOT EXISTS idx_estoque_marca     ON estoque(marca);
CREATE INDEX IF NOT EXISTS idx_estoque_ativo     ON estoque(ativo);
""",
    "fornecedor": """
CREATE TABLE fornecedor (
  id                INTEGER PRIMARY KEY,
  nome              TEXT NOT NULL UNIQUE,
  cnpj_cpf          TEXT UNIQUE,
  inscricao_estadual TEXT,
  telefone          TEXT,
  email             TEXT,
  endereco          TEXT,
  observacoes       TEXT,
  ativo             INTEGER DEFAULT 1,
  criado_em_iso     TEXT DEFAULT (datetime('now')),
  criado_em_br      TEXT
);
""",
    "produto_fornecedor": """
CREATE TABLE produto_fornecedor (
  id                  INTEGER PRIMARY KEY,
  estoque_id          INTEGER NOT NULL,
  fornecedor_id       INTEGER,
  codigo_fornecedor   TEXT,
  descricao_compra    TEXT NOT NULL,              -- ex.: "Caixa 20 pacotes de 50"
  unidade_compra      TEXT NOT NULL DEFAULT 'CX', -- CX, DP, PC...
  fator_conversao     REAL NOT NULL DEFAULT 1.0,  -- UN de venda por unidade_compra
  custo_ultima_compra REAL,
  prazo_medio_dias    INTEGER,
  lead_time_dias      INTEGER,
  moeda               TEXT DEFAULT 'BRL',
  ativo               INTEGER DEFAULT 1,
  FOREIGN KEY (estoque_id)    REFERENCES estoque(id),
  FOREIGN KEY (fornecedor_id) REFERENCES fornecedor(id),
  CONSTRAINT u_regra_unica UNIQUE (estoque_id, fornecedor_id, unidade_compra, descricao_compra)
);
CREATE INDEX IF NOT EXISTS idx_pf_estoque    ON produto_fornecedor(estoque_id);
CREATE INDEX IF NOT EXISTS idx_pf_fornecedor ON produto_fornecedor(fornecedor_id);
""",
    "produto_nf": """
CREATE TABLE produto_nf (
  id                     INTEGER PRIMARY KEY,
  chave_nfe              TEXT NOT NULL,
  numero_nfe             TEXT,
  serie_nfe              TEXT,
  fornecedor_nome        TEXT,
  fornecedor_cnpj        TEXT,
  produto_nome           TEXT NOT NULL,
  produto_sku            TEXT NOT NULL,
  ncm                    TEXT,
  cest                   TEXT,
  cfop                   TEXT,
  unidade_compra         TEXT NOT NULL,
  quantidade_compra      REAL NOT NULL,
  valor_unitario_compra  REAL NOT NULL,
  ipi_percentual         REAL DEFAULT 0.0,
  ipi_valor              REAL,
  valor_total_item       REAL,
  status                 TEXT DEFAULT 'Pendente',
  data_emissao_iso       TEXT,
  data_emissao_br        TEXT,
  data_criacao_iso       TEXT DEFAULT (datetime('now')),
  data_criacao_br        TEXT
);
CREATE INDEX IF NOT EXISTS idx_pnf_chave   ON produto_nf(chave_nfe);
CREATE INDEX IF NOT EXISTS idx_pnf_sku     ON produto_nf(produto_sku);
CREATE INDEX IF NOT EXISTS idx_pnf_status  ON produto_nf(status);
CREATE INDEX IF NOT EXISTS idx_pnf_emissao ON produto_nf(data_emissao_iso);
""",
    "inventario": """
CREATE TABLE inventario (
  id              INTEGER PRIMARY KEY,
  nome            TEXT NOT NULL,
  tipo            TEXT DEFAULT 'Geral',          -- Geral / Cíclico
  status          TEXT DEFAULT 'Pendente',       -- Pendente / Em Contagem / Fechado
  aberto_por      TEXT,
  fechado_por     TEXT,
  data_inicio_iso TEXT DEFAULT (datetime('now')),
  data_inicio_br  TEXT,
  data_fim_iso    TEXT,
  data_fim_br     TEXT
);
CREATE INDEX IF NOT EXISTS idx_inv_status ON inventario(status);
CREATE INDEX IF NOT EXISTS idx_inv_tipo   ON inventario(tipo);
""",
    "inventario_itens": """
CREATE TABLE inventario_itens (
  id                   INTEGER PRIMARY KEY,
  inventario_id        INTEGER NOT NULL,
  estoque_id           INTEGER NOT NULL,
  quantidade_sistema   REAL NOT NULL,
  quantidade_contada   REAL,
  divergencia          REAL,
  ajuste_aplicado      INTEGER DEFAULT 0,
  status               TEXT DEFAULT 'Nao Contado',
  observacao           TEXT,
  FOREIGN KEY (inventario_id) REFERENCES inventario(id),
  FOREIGN KEY (estoque_id)    REFERENCES estoque(id),
  CONSTRAINT u_inventario_item UNIQUE (inventario_id, estoque_id)
);
CREATE INDEX IF NOT EXISTS idx_inv_itens_inv ON inventario_itens(inventario_id);
CREATE INDEX IF NOT EXISTS idx_inv_itens_est ON inventario_itens(estoque_id);
""",
    "movimentacoes_estoque": """
CREATE TABLE movimentacoes_estoque (
  id                 INTEGER PRIMARY KEY,
  estoque_id         INTEGER NOT NULL,
  tipo               TEXT NOT NULL CHECK (tipo IN ('ENTRADA','SAIDA','AJUSTE')),
  origem             TEXT,                   -- NFE, VENDA, INVENTARIO, MANUAL...
  documento_ref      TEXT,                   -- chave NFe, id pedido etc.
  canal              TEXT,                   -- ML, SHOPEE, SHEIN, LOJA
  conta              TEXT,                   -- apelido/ID da conta
  quantidade         REAL NOT NULL,          -- em UN de venda
  custo_unitario     REAL,
  custo_total        REAL,
  saldo_quantidade   REAL,
  saldo_custo_medio  REAL,
  data_mov_iso       TEXT DEFAULT (datetime('now')),
  data_mov_br        TEXT,
  usuario            TEXT,
  observacao         TEXT,
  inventario_id      INTEGER,
  FOREIGN KEY (estoque_id)   REFERENCES estoque(id),
  FOREIGN KEY (inventario_id) REFERENCES inventario(id)
);
CREATE INDEX IF NOT EXISTS idx_mov_est_estoque ON movimentacoes_estoque(estoque_id);
CREATE INDEX IF NOT EXISTS idx_mov_est_data    ON movimentacoes_estoque(data_mov_iso);
CREATE INDEX IF NOT EXISTS idx_mov_est_tipo    ON movimentacoes_estoque(tipo);
CREATE INDEX IF NOT EXISTS idx_mov_est_canal   ON movimentacoes_estoque(canal);
""",
    "produto_canal": """
CREATE TABLE produto_canal (
  id                      INTEGER PRIMARY KEY,
  estoque_id              INTEGER NOT NULL,
  canal                   TEXT NOT NULL,       -- ML, SHOPEE, SHEIN, LOJA
  conta                   TEXT NOT NULL,       -- apelido/ID da conta
  sku_canal               TEXT NOT NULL,
  anuncio_id              TEXT,
  titulo_canal            TEXT,
  url_anuncio             TEXT,
  status                  TEXT DEFAULT 'Ativo',
  preco_venda_canal       REAL,
  ultima_atualizacao_iso  TEXT,
  ultima_atualizacao_br   TEXT,
  FOREIGN KEY (estoque_id) REFERENCES estoque(id),
  CONSTRAINT u_mapa_canal UNIQUE (estoque_id, canal, conta, sku_canal)
);
CREATE INDEX IF NOT EXISTS idx_prod_canal_est   ON produto_canal(estoque_id);
CREATE INDEX IF NOT EXISTS idx_prod_canal_canal ON produto_canal(canal);
CREATE INDEX IF NOT EXISTS idx_prod_canal_conta ON produto_canal(conta);
""",
}

def current_tables(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {r[0] for r in cur.fetchall()}

def drop_tables(cur, names):
    for t in names:
        cur.execute(f"DROP TABLE IF EXISTS {t}")

def main():
    if not DB_PATH.exists():
        raise SystemExit(f"[ERRO] Banco não encontrado: {DB_PATH}")

    con = sqlite3.connect(str(DB_PATH))
    cur = con.cursor()

    try:
        # Nada de isolation_level=None; controlamos com commit/rollback Python
        cur.execute("PRAGMA foreign_keys=OFF;")
        con.commit()

        # Descobre existentes
        existentes = current_tables(cur)

        # 1) Derruba qualquer *_new e legados exatos
        drop_suffix_new = {t for t in existentes if t.endswith("_new")}
        drop_legados = existentes.intersection(LEGACY_EXACT)
        con.execute("BEGIN;")
        drop_tables(cur, drop_suffix_new | drop_legados)
        con.commit()

        # 2) Derruba canônicas (exceto 'estoque' se KEEP_ESTOQUE=True)
        existentes = current_tables(cur)
        to_drop = (set(CANONICAL) - {"estoque"}) if KEEP_ESTOQUE else set(CANONICAL)
        to_drop = to_drop.intersection(existentes)
        con.execute("BEGIN;")
        drop_tables(cur, to_drop)
        con.commit()

        # 3) Recria canônicas (garantindo ordem para FKs)
        con.execute("BEGIN;")
        if not KEEP_ESTOQUE:
            cur.executescript(DDL_CREATE["estoque"])
        else:
            # garante índices se já existe
            cur.executescript("""
            CREATE INDEX IF NOT EXISTS idx_estoque_nome      ON estoque(nome);
            CREATE INDEX IF NOT EXISTS idx_estoque_categoria ON estoque(categoria);
            CREATE INDEX IF NOT EXISTS idx_estoque_marca     ON estoque(marca);
            CREATE INDEX IF NOT EXISTS idx_estoque_ativo     ON estoque(ativo);
            """)
        # ordem de criação para FKs:
        cur.executescript(DDL_CREATE["fornecedor"])
        cur.executescript(DDL_CREATE["produto_fornecedor"])
        cur.executescript(DDL_CREATE["produto_nf"])
        cur.executescript(DDL_CREATE["inventario"])
        cur.executescript(DDL_CREATE["inventario_itens"])
        cur.executescript(DDL_CREATE["movimentacoes_estoque"])
        cur.executescript(DDL_CREATE["produto_canal"])
        con.commit()

        cur.execute("PRAGMA foreign_keys=ON;")
        con.commit()

        # VACUUM tem que ser fora de transação
        cur.execute("VACUUM;")

        finais = current_tables(cur)
        print("[OK] Tabelas atuais:", ", ".join(sorted(finais)))
    except Exception as e:
        con.rollback()
        raise
    finally:
        con.close()

if __name__ == "__main__":
    main()
