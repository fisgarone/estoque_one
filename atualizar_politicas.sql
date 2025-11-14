-- SCRIPT DE ATUALIZAÇÃO DO politicas_canais.db
-- SEMPRE use este padrão para mudar regras, sem criar linhas duplicadas.

PRAGMA foreign_keys = ON;

-- 1) Atualizar/Inserir regras de canais (UPSERT usando UNIQUE(canal,plano,preco_min,preco_max))
INSERT INTO politicas_canais (
    canal, plano, preco_unit_min, preco_unit_max,
    comissao_percent_base, taxa_fixa_tipo, taxa_fixa_valor,
    frete_seller_tipo, frete_seller_valor,
    insumos_percent, ads_percent, ativo, observacoes_regra
) VALUES
    -- EXEMPLO Shopee (ajuste aqui se algum dia mudar comissão / taxa)
    ('shopee','padrao',0, NULL,
     0.22,                 -- comissao_percent_base
     'POR_UNIDADE',        -- taxa_fixa_tipo
     4.50,                 -- taxa_fixa_valor
     'NENHUM',             -- frete_seller_tipo
     0.0,                  -- frete_seller_valor
     0.015,                -- insumos_percent
     0.035,                -- ads_percent
     1,
     'Shopee: comissão 22%, taxa fixa 4,50 por unidade, insumos 1,5%, ads 3,5%.'
    ),

    -- EXEMPLO Mercado Livre faixa >= 79
    ('ml','padrao',79, NULL,
     NULL,                 -- comissao_percent_base (vem da venda)
     'NENHUMA',            -- taxa_fixa_tipo
     0.0,                  -- taxa_fixa_valor
     'POR_UNIDADE',        -- frete_seller_tipo
     29.0,                 -- frete_seller_valor
     0.015,                -- insumos_percent
     0.035,                -- ads_percent
     1,
     'ML: preco_unit >= 79 sem taxa fixa; frete_seller 29,00 por unidade embutido no preço.'
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
    observacoes_regra     = excluded.observacoes_regra
;

-- 2) Atualizar/Inserir faixas internas (ex.: ML < 79)
INSERT INTO politicas_canais_faixas (
    canal, plano, preco_unit_min, preco_unit_max,
    tipo_valor, valor, ativo, observacoes
) VALUES
    ('ml','padrao',12.50,29.00,'TAXA_FIXA_POR_UNIDADE',6.25,1,
     'Taxa fixa ML por unidade (faixa 12,50–29).'),
    ('ml','padrao',29.00,50.00,'TAXA_FIXA_POR_UNIDADE',6.50,1,
     'Taxa fixa ML por unidade (faixa 29–50).'),
    ('ml','padrao',50.00,79.00,'TAXA_FIXA_POR_UNIDADE',6.75,1,
     'Taxa fixa ML por unidade (faixa 50–79).')

ON CONFLICT(canal, plano, preco_unit_min, preco_unit_max) DO UPDATE SET
    tipo_valor  = excluded.tipo_valor,
    valor       = excluded.valor,
    ativo       = excluded.ativo,
    observacoes = excluded.observacoes
;

-- 3) Atualizar/Inserir alíquotas por conta/CNPJ
INSERT INTO politicas_cnpj (
    conta, custo_estrutura_percent, aliquota_fiscal_percent, ativo, observacoes
) VALUES
    ('Comercial',0.13,0.0706,1,'Conta Comercial: custo fixo 13% + alíquota 7,06% sobre faturamento.'),
    ('Pesca',    0.13,0.0654,1,'Conta Pesca: custo fixo 13% + alíquota 6,54% sobre faturamento.'),
    ('Shop',     0.13,0.1014,1,'Conta Shop: custo fixo 13% + alíquota 10,14% sobre faturamento.'),
    ('Camping',  0.13,0.0424,1,'Conta Camping: custo fixo 13% + alíquota 4,24% sobre faturamento.')

ON CONFLICT(conta) DO UPDATE SET
    custo_estrutura_percent  = excluded.custo_estrutura_percent,
    aliquota_fiscal_percent  = excluded.aliquota_fiscal_percent,
    ativo                    = excluded.ativo,
    observacoes              = excluded.observacoes
;
