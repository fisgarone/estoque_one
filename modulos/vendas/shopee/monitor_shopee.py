import os
import pandas as pd
import sqlite3
import json
from datetime import datetime

def criar_tabela_vendas_shopee():
    db_path = r"C:\fisgarone\fisgarone.db"

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Banco de dados não encontrado: {db_path}")

    conn = sqlite3.connect(db_path)
    tabelas = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    if 'pedidos' not in tabelas['name'].tolist():
        raise ValueError("A tabela 'pedidos' não existe no banco de dados.")

    pedidos = pd.read_sql("SELECT * FROM pedidos", conn)
    pedidos['data'] = pd.to_datetime(pedidos['create_time'], errors='coerce')
    pedidos['item_list'] = pedidos['item_list'].apply(json.loads)
    pedidos = pedidos.explode('item_list')
    itens_df = pd.json_normalize(pedidos['item_list'])
    itens_df.columns = [f'item_{col}' for col in itens_df.columns]
    pedidos = pedidos.drop(columns=['item_list']).reset_index(drop=True)
    pedidos = pd.concat([pedidos, itens_df], axis=1)

    def extrair_transportadora(pkg):
        try:
            pacotes = json.loads(pkg)
            if isinstance(pacotes, list) and len(pacotes) > 0:
                return pacotes[0].get('shipping_carrier', '')
        except:
            return ''
        return ''

    pedidos['transportadora'] = pedidos['package_list'].apply(extrair_transportadora)
    pedidos['transportadora'] = pedidos['transportadora'].replace({
        'SBS': 'Shopee Xpress',
        'SPX': 'Shopee Xpress',
        'STANDARD_EXPRESS': 'Shopee Xpress',
        'OTHER_LOGISTICS': 'Outros',
        'INHOUSE': 'Agência Shopee',
        'OWN_DELIVERY': 'Shopee Entrega Direta'
    })

    pedidos['preco_unitario'] = pedidos['item_model_discounted_price'].astype(float)
    pedidos['qtd_comprada'] = pedidos['item_model_quantity_purchased'].astype(int)
    pedidos['frete_total'] = pedidos['actual_shipping_fee'].astype(float)
    pedidos['valor_total'] = pedidos['preco_unitario'] * pedidos['qtd_comprada']
    pedidos['itens_pedido'] = pedidos.groupby('order_sn')['order_sn'].transform('count')
    pedidos['frete_unitario'] = pedidos['frete_total'] / pedidos['itens_pedido']

    pedidos['data_entrega'] = pd.NaT
    pedidos_completos = pedidos['order_status'] == 'COMPLETED'
    data_entregue_valida = pd.to_datetime(pedidos['Data_Entregue'], errors='coerce')
    pedidos.loc[pedidos_completos & data_entregue_valida.notna(), 'data_entrega'] = data_entregue_valida

    pedidos['prazo_entrega_dias'] = None
    pedidos.loc[pedidos['data_entrega'].notna(), 'prazo_entrega_dias'] = \
        (pedidos['data_entrega'] - pedidos['data']).dt.total_seconds() / 86400
    pedidos['prazo_entrega_dias'] = pedidos['prazo_entrega_dias'].round(2)

    final = pedidos.rename(columns={
        'order_sn': 'PEDIDO_ID',
        'buyer_user_id': 'COMPRADOR_ID',
        'order_status': 'STATUS_PEDIDO',
        'account_type': 'TIPO_CONTA',
        'item_item_name': 'NOME_ITEM',
        'item_item_sku': 'SKU_ITEM',
        'item_model_sku': 'SKU_VARIACAO'
    })

    # Gera coluna SKU unificada (preferência para variacao)
    final['SKU'] = final.apply(
        lambda row: row['SKU_VARIACAO'] if pd.notna(row.get('SKU_VARIACAO')) and row['SKU_VARIACAO'] != '' else row.get('SKU_ITEM', ''),
        axis=1
    )

    # Alimenta preço de custo a partir da planilha externa
    caminho_custos = r"C:\fisgarone\Custos Anúncios Shopee.xlsx"
    custos_df = pd.read_excel(caminho_custos)
    custos_df = custos_df.rename(columns={
        custos_df.columns[3]: 'SKU',
        custos_df.columns[4]: 'PRECO_CUSTO'
    })[['SKU', 'PRECO_CUSTO']]

    final = final.merge(custos_df, on='SKU', how='left')
    final['PRECO_CUSTO'] = (final['PRECO_CUSTO'] * final['qtd_comprada']).round(2).astype(str)  # TEXT

    final['COMISSAO_UNITARIA'] = (final['valor_total'] * 0.22).round(2)
    final['TAXA_FIXA'] = (final['qtd_comprada'] * 4.00).round(2)
    final['CUSTO_OPERACIONAL'] = (final['valor_total'] * 0.05).round(2)

    final['TOTAL_COM_FRETE'] = final['valor_total']

    final['SM_CONTAS_PCT'] = final['TIPO_CONTA'].map({
        'TOYS': 9.27,
        'COMERCIAL': 7.06
    })
    final['SM_CONTAS_REAIS'] = (final['TOTAL_COM_FRETE'] * final['SM_CONTAS_PCT'] / 100).round(2)

    final['REPASSE_ENVIO'] = 0
    mask_envio = final['transportadora'] == 'Shopee Entrega Direta'
    first_index = final[mask_envio].groupby(['PEDIDO_ID', 'COMPRADOR_ID', 'data']).head(1).index
    final.loc[first_index, 'REPASSE_ENVIO'] = 8

    final['CUSTO_FIXO'] = (final['valor_total'] * 0.13).round(2)

    final['CUSTO_OP_TOTAL'] = (
        final[['PRECO_CUSTO', 'COMISSAO_UNITARIA', 'TAXA_FIXA', 'CUSTO_OPERACIONAL', 'SM_CONTAS_REAIS']]
        .apply(lambda x: pd.to_numeric(x, errors='coerce')).sum(axis=1)
    ).round(2)

    final['MARGEM_CONTRIBUICAO'] = (final['valor_total'] - final['CUSTO_OP_TOTAL']).round(2)
    final['LUCRO_REAL'] = (final['MARGEM_CONTRIBUICAO'] - final['CUSTO_FIXO']).round(2)
    final['LUCRO_REAL_PCT'] = (final['LUCRO_REAL'] / final['valor_total'] * 100).round(2)

    # Padroniza as colunas finais para o banco (maiúsculo e sem acento)
    colunas_finais = [
        'PEDIDO_ID', 'COMPRADOR_ID', 'STATUS_PEDIDO', 'TIPO_CONTA', 'data',
        'NOME_ITEM', 'SKU', 'qtd_comprada', 'preco_unitario', 'PRECO_CUSTO',
        'valor_total', 'frete_unitario', 'COMISSAO_UNITARIA', 'TAXA_FIXA',
        'TOTAL_COM_FRETE', 'SM_CONTAS_PCT', 'SM_CONTAS_REAIS', 'CUSTO_OPERACIONAL',
        'transportadora', 'data_entrega', 'prazo_entrega_dias', 'CUSTO_OP_TOTAL',
        'MARGEM_CONTRIBUICAO', 'CUSTO_FIXO', 'LUCRO_REAL', 'LUCRO_REAL_PCT', 'REPASSE_ENVIO'
    ]

    # Renomeia colunas para bater 100% com o banco (tudo maiúsculo)
    rename_map = {
        'data': 'DATA',
        'qtd_comprada': 'QTD_COMPRADA',
        'preco_unitario': 'PRECO_UNITARIO',
        'valor_total': 'VALOR_TOTAL',
        'frete_unitario': 'FRETE_UNITARIO',
        'transportadora': 'TRANSPORTADORA',
        'data_entrega': 'DATA_ENTREGA',
        'prazo_entrega_dias': 'PRAZO_ENTREGA_DIAS',
    }
    final = final.rename(columns=rename_map)

    # Ordena colunas, drop duplicates
    colunas_finais_padrao = [
        'PEDIDO_ID', 'COMPRADOR_ID', 'STATUS_PEDIDO', 'TIPO_CONTA', 'DATA',
        'NOME_ITEM', 'SKU', 'QTD_COMPRADA', 'PRECO_UNITARIO', 'PRECO_CUSTO',
        'VALOR_TOTAL', 'FRETE_UNITARIO', 'COMISSAO_UNITARIA', 'TAXA_FIXA',
        'TOTAL_COM_FRETE', 'SM_CONTAS_PCT', 'SM_CONTAS_REAIS', 'CUSTO_OPERACIONAL',
        'TRANSPORTADORA', 'DATA_ENTREGA', 'PRAZO_ENTREGA_DIAS', 'CUSTO_OP_TOTAL',
        'MARGEM_CONTRIBUICAO', 'CUSTO_FIXO', 'LUCRO_REAL', 'LUCRO_REAL_PCT', 'REPASSE_ENVIO'
    ]
    final = final.drop_duplicates(subset=['PEDIDO_ID', 'COMPRADOR_ID', 'DATA', 'NOME_ITEM'], keep='first')
    final = final[colunas_finais_padrao].sort_values(by='VALOR_TOTAL', ascending=False)

    # Alimenta o banco
    final.to_sql("vendas_shopee", conn, if_exists="replace", index=False)
    conn.close()
    print("✅ Tabela 'vendas_shopee' atualizada com sucesso no banco raiz.")

if __name__ == "__main__":
    criar_tabela_vendas_shopee()
