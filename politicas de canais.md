README – Base Oficial de Políticas de Canais (politicas_canais.db)
1. Objetivo

Este arquivo .db guarda apenas parâmetros de negócio, não dados de venda.

Ele define, de forma padronizada e legível, todas as regras de custo por canal e por CNPJ, para ser usado por:

Back-end (cálculo de custos, precificação, simulações)

Dashboards

Ferramentas de IA / desenvolvedores (sem precisar ler o banco operacional inteiro)

Nenhum cálculo de custo deve ser “chutado” em tela/código.
Tudo deve derivar das 3 tabelas abaixo.

2. Estrutura do politicas_canais.db

O banco contém exatamente 3 tabelas:

politicas_canais – regras por canal/faixa de preço

politicas_canais_faixas – faixas internas (hoje usadas para ML < 79)

politicas_cnpj – custos fixos + alíquotas por conta/CNPJ

2.1. Tabela politicas_canais

Função: definir, para cada canal + faixa de preço_unit + plano, quais percentuais e valores usar.

1 linha = 1 combinação de (canal, plano, faixa de preço_unit).

Campos:

id – INTEGER PK

canal – TEXT

Ex.: 'ml', 'shopee', 'shein', 'temu'.

plano – TEXT

Ex.: 'padrao' (pode virar 'classico', 'premium' no futuro).

preco_unit_min – REAL

Preço unitário mínimo (inclusivo) para essa regra.

preco_unit_max – REAL ou NULL

Preço unitário máximo (exclusivo).

NULL = sem limite superior.

comissao_percent_base – REAL ou NULL

Percentual “de catálogo” de comissão do canal (decimal).

Ex.: Shopee 0.22, Shein 0.16, TEMU 0.

Para Mercado Livre esse campo pode ficar NULL (comissão real vem da decomposição da venda).

taxa_fixa_tipo – TEXT
Tipos previstos:

'POR_UNIDADE' – taxa fixa multiplicada pela quantidade

'POR_VENDA' – taxa fixa por pedido/venda

'POR_UNIDADE_FAIXA' – taxa fixa por unidade definida em politicas_canais_faixas

'NENHUMA' – não há taxa fixa nesse cenário.

taxa_fixa_valor – REAL ou NULL

Valor da taxa fixa quando não é por faixa.

Shopee: 4.50 (por unidade)

Shein: 5.00 (por venda)

TEMU: 15.00 (por venda)

ML <79: NULL (usa faixas)

ML >=79: 0.

frete_seller_tipo – TEXT

'POR_UNIDADE' – frete que é custo do seller por unidade (ex.: ML >=79)

'POR_VENDA' – frete custo seller por venda (se algum canal vier assim)

'NENHUM' – canais em que não existe frete seller separado (Shopee, Shein, TEMU, ML <79 hoje).

frete_seller_valor – REAL

Valor do frete_seller conforme o tipo.

ML >=79: 29.00

Demais canais: 0, até existir regra específica.

Definição: frete_seller é custo de frete do seller embutido no preço e descontado pelo canal no repasse (não é comissão).

insumos_percent – REAL

Percentual de custo com insumos (decimal, ex.: 0.015 = 1,5%).

ads_percent – REAL

Percentual de custo com ads (decimal, ex.: 0.035 = 3,5%).

ativo – INTEGER (0 ou 1)

1 = regra ativa, 0 = desativada.

observacoes_regra – TEXT

Resumo curto da lógica daquela linha (pra humano/IA bater o olho).

2.2. Tabela politicas_canais_faixas

Função: detalhar faixas internas da taxa fixa por unidade quando taxa_fixa_tipo = 'POR_UNIDADE_FAIXA'.

Hoje é usada para Mercado Livre com preco_unit < 79
(pode ser expandida pra outros canais, se necessário).

Campos:

id – INTEGER PK

canal – TEXT ('ml')

plano – TEXT ('padrao', ou outro plano futuro)

preco_unit_min – REAL (inclusivo)

preco_unit_max – REAL (exclusivo)

tipo_valor – TEXT

Ex.: 'TAXA_FIXA_POR_UNIDADE'

valor – REAL

Valor em R$ da taxa fixa por unidade para essa subfaixa.

ativo – INTEGER (0/1)

observacoes – TEXT

2.3. Tabela politicas_cnpj

Função: representar custo fixo de estrutura + alíquota fiscal por conta/CNPJ.
Aplica-se sobre o faturamento da venda (valor da venda), independentemente do canal.

Campos:

id – INTEGER PK

conta – TEXT

Ex.: 'Comercial', 'Pesca', 'Shop', 'Camping'.

custo_estrutura_percent – REAL

Percentual fixo de estrutura (hoje: 0.13 = 13%).

aliquota_fiscal_percent – REAL

Percentual fiscal da conta (decimal, ex.: 0.0706 = 7,06%).

ativo – INTEGER (0/1)

observacoes – TEXT

Regra de uso:

Para uma venda de valor V na conta X:
custo_fixos_estrutura_fiscal = (custo_estrutura_percent + aliquota_fiscal_percent) * V.

3. Regras por canal (como preencher as tabelas)
3.1. Shopee

Comissão: 22% sobre o valor da venda.

Taxa fixa: R$ 4,50 por unidade.

Variáveis próprias:

Insumos: 1,5%

Ads: 3,5%

Linha em politicas_canais:

canal = 'shopee'

plano = 'padrao'

preco_unit_min = 0

preco_unit_max = NULL

comissao_percent_base = 0.22

taxa_fixa_tipo = 'POR_UNIDADE'

taxa_fixa_valor = 4.5

frete_seller_tipo = 'NENHUM'

frete_seller_valor = 0

insumos_percent = 0.015

ads_percent = 0.035

3.2. Shein

Comissão: 16% sobre o valor da venda.

Taxa fixa frete: R$ 5,00 por venda (por pedido).

Variáveis próprias:

Insumos: 1,5%

Ads: 0% (por enquanto)

Linha em politicas_canais:

canal = 'shein'

plano = 'padrao'

preco_unit_min = 0

preco_unit_max = NULL

comissao_percent_base = 0.16

taxa_fixa_tipo = 'POR_VENDA'

taxa_fixa_valor = 5

frete_seller_tipo = 'NENHUM'

frete_seller_valor = 0

insumos_percent = 0.015

ads_percent = 0

3.3. TEMU

Comissão: 0%.

Taxa de frete: R$ 15,00 por venda.

Variáveis próprias:

Insumos: 1,5%

Ads: 0% (por enquanto)

Linha em politicas_canais:

canal = 'temu'

plano = 'padrao'

preco_unit_min = 0

preco_unit_max = NULL

comissao_percent_base = 0

taxa_fixa_tipo = 'POR_VENDA'

taxa_fixa_valor = 15

frete_seller_tipo = 'NENHUM'

frete_seller_valor = 0

insumos_percent = 0.015

ads_percent = 0

3.4. Mercado Livre
3.4.1. Coluna Taxa Mercado Livre (sale_fee_unit)

Na tabela operacional vendas_ml (em outro .db):

Taxa Mercado Livre representa a sale_fee unitária (por unidade).

Ela já soma:

comissão_unit

taxa_fixa_unit (quando houver, para preco_unit < 79)

O frete de 29 (quando preco_unit ≥ 79) não entra nessa coluna, aparece em outra coluna de frete/logística.

3.4.2. Produtos com preco_unit < 79 (taxa fixa por faixa)

Para preco_unit >= 12,50 e < 79, aplica-se uma taxa fixa por unidade de acordo com a faixa de preço.

Faixas válidas (por unidade):

12,50 < preço_unit < 29,00 → taxa_fixa_unit = 6,25

29,00 < preço_unit < 50,00 → taxa_fixa_unit = 6,50

50,00 < preço_unit < 79,00 → taxa_fixa_unit = 6,75

Para preco_unit < 12,50 → sem taxa fixa.
Na prática, hoje não existem produtos nessa faixa.

Linha em politicas_canais (ML <79):

canal = 'ml'

plano = 'padrao'

preco_unit_min = 0

preco_unit_max = 79

comissao_percent_base = NULL (usa comissão real da venda)

taxa_fixa_tipo = 'POR_UNIDADE_FAIXA'

taxa_fixa_valor = NULL

frete_seller_tipo = 'NENHUM'

frete_seller_valor = 0

insumos_percent = 0.015

ads_percent = 0.035

Linhas em politicas_canais_faixas:

12,50–29 → 6,25

29–50 → 6,50

50–79 → 6,75

Observação: faixa < 12,50 não é cadastrada porque hoje não há produtos abaixo desse valor; se surgirem, terão que ser tratadas com uma nova linha (com ou sem taxa fixa).

Decomposição da Taxa Mercado Livre (sale_fee_unit):

Para uma venda com:

P = preco_unit

Q = quantidade

sale_fee_unit = valor da Taxa Mercado Livre (unitária)

Passos:

Buscar taxa_fixa_unit na faixa correta.

Calcular:

taxa_fixa_ml_linha = taxa_fixa_unit * Q

comissao_unit = sale_fee_unit - taxa_fixa_unit

comissao_ml_linha = comissao_unit * Q

comissao_ml_percent = comissao_unit / P

3.4.3. Produtos com preco_unit >= 79 (frete_seller = 29)

Quando o preço unitário é >= 79,00:

Não se aplica taxa fixa por faixa.

Taxa Mercado Livre (sale_fee_unit) passa a ser apenas comissão_unit (sem taxa_fixa_unit).

Além disso:

O Mercado Livre compra o frete e desconta R$ 29,00 por unidade do repasse.

Este valor é embutido no preço de venda e lançado em outra coluna de frete/logística (não na sale_fee).

Deve ser tratado como frete_seller.

Linha em politicas_canais (ML >=79):

canal = 'ml'

plano = 'padrao'

preco_unit_min = 79

preco_unit_max = NULL

comissao_percent_base = NULL

taxa_fixa_tipo = 'NENHUMA'

taxa_fixa_valor = 0

frete_seller_tipo = 'POR_UNIDADE'

frete_seller_valor = 29

insumos_percent = 0.015

ads_percent = 0.035

Decomposição (resumida):

taxa_fixa_unit = 0

comissao_unit = sale_fee_unit

comissao_ml_percent = comissao_unit / P

Frete:

frete_seller_unit = 29

frete_seller_linha = 29 * Q

Vem de outra coluna (frete), mas a regra de valor/condição mora neste .db.

4. Regras por CNPJ – politicas_cnpj (valores atuais)

Preencher:

Contas: Comercial, Pesca, Shop, Camping.

custo_estrutura_percent = 0.13 (13%) para todas.

aliquota_fiscal_percent:

Comercial: 0.0706 (7,06%)

Pesca: 0.0654 (6,54%)

Shop: 0.1014 (10,14%)

Camping: 0.0424 (4,24%)

5. Como qualquer código/IA deve usar o politicas_canais.db

Dada uma venda:

canal (ml, shopee, shein, temu)

conta (Comercial, Pesca, Shop, Camping)

preco_unit, quantidade

valor_venda = preco_unit * quantidade

Passos padrão:

Buscar a linha em politicas_canais:

canal + plano compatível

preco_unit_min <= preco_unit < preco_unit_max (ou max NULL)

ativo = 1.

Se taxa_fixa_tipo = 'POR_UNIDADE_FAIXA':

Buscar faixa em politicas_canais_faixas para o canal/plano e preço_unit e pegar valor como taxa_fixa_unit.

Buscar linha em politicas_cnpj para a conta.

Aplicar:

comissões

taxas fixas

frete_seller

insumos

ads

custo_estrutura + aliquota_fiscal

Tudo sem interpretar texto; apenas usando colunas numéricas e tipos.