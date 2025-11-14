
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).with_name("politicas_canais.db")

# ===============================
# CONFIGURAÇÃO DAS REGRAS (EDITE AQUI)
# ===============================

POLITICAS_CANAIS = [
    # Shopee
    {
        "canal": "shopee",
        "plano": "padrao",
        "preco_unit_min": 0.0,
        "preco_unit_max": None,
        "comissao_percent_base": 0.22,
        "taxa_fixa_tipo": "POR_UNIDADE",
        "taxa_fixa_valor": 4.50,
        "frete_seller_tipo": "NENHUM",
        "frete_seller_valor": 0.0,
        "insumos_percent": 0.015,
        "ads_percent": 0.035,
        "ativo": 1,
        "observacoes_regra": "Shopee: comissão 22%, taxa fixa 4,50 por unidade, insumos 1,5%, ads 3,5%."
    },
    # Shein
    {
        "canal": "shein",
        "plano": "padrao",
        "preco_unit_min": 0.0,
        "preco_unit_max": None,
        "comissao_percent_base": 0.16,
        "taxa_fixa_tipo": "POR_VENDA",
        "taxa_fixa_valor": 5.00,
        "frete_seller_tipo": "NENHUM",
        "frete_seller_valor": 0.0,
        "insumos_percent": 0.015,
        "ads_percent": 0.0,
        "ativo": 1,
        "observacoes_regra": "Shein: comissão 16%, taxa fixa 5,00 por venda, insumos 1,5%."
    },
    # TEMU
    {
        "canal": "temu",
        "plano": "padrao",
        "preco_unit_min": 0.0,
        "preco_unit_max": None,
        "comissao_percent_base": 0.0,
        "taxa_fixa_tipo": "POR_VENDA",
        "taxa_fixa_valor": 15.00,
        "frete_seller_tipo": "NENHUM",
        "frete_seller_valor": 0.0,
        "insumos_percent": 0.015,
        "ads_percent": 0.0,
        "ativo": 1,
        "observacoes_regra": "TEMU: sem comissão, taxa fixa 15,00 por venda, insumos 1,5%."
    },
    # Mercado Livre < 79
    {
        "canal": "ml",
        "plano": "padrao",
        "preco_unit_min": 0.0,
        "preco_unit_max": 79.0,
        "comissao_percent_base": None,
        "taxa_fixa_tipo": "POR_UNIDADE_FAIXA",
        "taxa_fixa_valor": None,
        "frete_seller_tipo": "NENHUM",
        "frete_seller_valor": 0.0,
        "insumos_percent": 0.015,
        "ads_percent": 0.035,
        "ativo": 1,
        "observacoes_regra": "ML: preco_unit < 79 usa taxa fixa por unidade via faixas; sale_fee_unit = taxa_fixa_unit + comissao_unit."
    },
    # Mercado Livre >= 79
    {
        "canal": "ml",
        "plano": "padrao",
        "preco_unit_min": 79.0,
        "preco_unit_max": None,
        "comissao_percent_base": None,
        "taxa_fixa_tipo": "NENHUMA",
        "taxa_fixa_valor": 0.0,
        "frete_seller_tipo": "POR_UNIDADE",
        "frete_seller_valor": 29.0,
        "insumos_percent": 0.015,
        "ads_percent": 0.035,
        "ativo": 1,
        "observacoes_regra": "ML: preco_unit >= 79 sem taxa fixa de faixa; frete_seller 29,00 por unidade embutido no preço."
    },
]

POLITICAS_CANAIS_FAIXAS = [
    {
        "canal": "ml",
        "plano": "padrao",
        "preco_unit_min": 12.50,
        "preco_unit_max": 29.00,
        "tipo_valor": "TAXA_FIXA_POR_UNIDADE",
        "valor": 6.25,
        "ativo": 1,
        "observacoes": "Taxa fixa ML por unidade (faixa 12,50–29)."
    },
    {
        "canal": "ml",
        "plano": "padrao",
        "preco_unit_min": 29.00,
        "preco_unit_max": 50.00,
        "tipo_valor": "TAXA_FIXA_POR_UNIDADE",
        "valor": 6.50,
        "ativo": 1,
        "observacoes": "Taxa fixa ML por unidade (faixa 29–50)."
    },
    {
        "canal": "ml",
        "plano": "padrao",
        "preco_unit_min": 50.00,
        "preco_unit_max": 79.00,
        "tipo_valor": "TAXA_FIXA_POR_UNIDADE",
        "valor": 6.75,
        "ativo": 1,
        "observacoes": "Taxa fixa ML por unidade (faixa 50–79)."
    },
]

POLITICAS_CNPJ = [
    {
        "conta": "Comercial",
        "custo_estrutura_percent": 0.13,
        "aliquota_fiscal_percent": 0.0706,
        "ativo": 1,
        "observacoes": "Conta Comercial: custo fixo 13% + alíquota 7,06% sobre faturamento."
    },
    {
        "conta": "Pesca",
        "custo_estrutura_percent": 0.13,
        "aliquota_fiscal_percent": 0.0654,
        "ativo": 1,
        "observacoes": "Conta Pesca: custo fixo 13% + alíquota 6,54% sobre faturamento."
    },
    {
        "conta": "Shop",
        "custo_estrutura_percent": 0.13,
        "aliquota_fiscal_percent": 0.1014,
        "ativo": 1,
        "observacoes": "Conta Shop: custo fixo 13% + alíquota 10,14% sobre faturamento."
    },
    {
        "conta": "Camping",
        "custo_estrutura_percent": 0.13,
        "aliquota_fiscal_percent": 0.0424,
        "ativo": 1,
        "observacoes": "Conta Camping: custo fixo 13% + alíquota 4,24% sobre faturamento."
    },
]

# ===============================
# FUNÇÕES DE UPSERT
# ===============================

def connect_db(path: Path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def upsert_politicas_canais(conn, rows):
    sql = '''
    INSERT INTO politicas_canais (
        canal, plano, preco_unit_min, preco_unit_max,
        comissao_percent_base, taxa_fixa_tipo, taxa_fixa_valor,
        frete_seller_tipo, frete_seller_valor,
        insumos_percent, ads_percent, ativo, observacoes_regra
    ) VALUES (
        :canal, :plano, :preco_unit_min, :preco_unit_max,
        :comissao_percent_base, :taxa_fixa_tipo, :taxa_fixa_valor,
        :frete_seller_tipo, :frete_seller_valor,
        :insumos_percent, :ads_percent, :ativo, :observacoes_regra
    )
    ON CONFLICT(canal, plano, preco_unit_min, preco_unit_max) DO UPDATE SET
        comissao_percent_base = excluded.comissao_percent_base,
        taxa_fixa_tipo        = excluded.taxa_fixa_tipo,
        taxa_fixa_valor       = excluded.taxa_fixa_valor,
        frete_seller_tipo     = excluded.frete_seller_tipo,
        frete_seller_valor    = excluded.frete_seller_valor,
        insumos_percent       = excluded.insumos_percent,
        ads_percent           = excluded.ads_percent,
        ativo                 = excluded.ativo,
        observacoes_regra     = excluded.observacoes_regra;
    '''
    conn.executemany(sql, rows)

def upsert_politicas_canais_faixas(conn, rows):
    sql = '''
    INSERT INTO politicas_canais_faixas (
        canal, plano, preco_unit_min, preco_unit_max,
        tipo_valor, valor, ativo, observacoes
    ) VALUES (
        :canal, :plano, :preco_unit_min, :preco_unit_max,
        :tipo_valor, :valor, :ativo, :observacoes
    )
    ON CONFLICT(canal, plano, preco_unit_min, preco_unit_max) DO UPDATE SET
        tipo_valor  = excluded.tipo_valor,
        valor       = excluded.valor,
        ativo       = excluded.ativo,
        observacoes = excluded.observacoes;
    '''
    conn.executemany(sql, rows)

def upsert_politicas_cnpj(conn, rows):
    sql = '''
    INSERT INTO politicas_cnpj (
        conta, custo_estrutura_percent, aliquota_fiscal_percent, ativo, observacoes
    ) VALUES (
        :conta, :custo_estrutura_percent, :aliquota_fiscal_percent, :ativo, :observacoes
    )
    ON CONFLICT(conta) DO UPDATE SET
        custo_estrutura_percent = excluded.custo_estrutura_percent,
        aliquota_fiscal_percent = excluded.aliquota_fiscal_percent,
        ativo                   = excluded.ativo,
        observacoes             = excluded.observacoes;
    '''
    conn.executemany(sql, rows)

def main():
    print(f"Atualizando banco de políticas em: {DB_PATH}")
    conn = connect_db(DB_PATH)

    try:
        upsert_politicas_canais(conn, POLITICAS_CANAIS)
        upsert_politicas_canais_faixas(conn, POLITICAS_CANAIS_FAIXAS)
        upsert_politicas_cnpj(conn, POLITICAS_CNPJ)
        conn.commit()
        print("Atualização concluída com sucesso.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
