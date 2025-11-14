# -*- coding: utf-8 -*-
"""
fracionado_service.py
----------------------------------------
Serviço de processamento de ENTRADAS com 3 níveis de embalagem
(CX/FD -> DP/PC -> UN), usando SEMPRE custo oficial = custo COM IPI.

O serviço:
- Converte compra para UN (unidade de venda)
- Calcula custo_un_sem_ipi, custo_ipi_un, custo_un_com_ipi
- Grava/atualiza regra em produto_fornecedor.fator_conversao
- Cria/atualiza Estoque (SKU), atualiza custos e quantidade
- Lança MovimentacaoEstoque do tipo ENTRADA (em UN)
- Atualiza ProdutoNF.status = 'Concluido' (id ou chave)

NÃO altera telas. Você apenas importa e chama as funções aqui.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import load_only

from extensions import db
from modulos.estoque.models import (
    Estoque, MovimentacaoEstoque,
    ProdutoFornecedor, ProdutoNF
)

# =========================
# Helpers numéricos
# =========================

def _norm_pct(x) -> float:
    """5 ou '5' ou '5%' => 0.05 ; '0,5' => 0.5 se já fator."""
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x) / 100.0 if x > 1 else float(x)
    s = str(x).strip().replace('%', '')
    # normaliza BR -> US se vier "1.234,56"
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '.')
    try:
        v = float(s)
        return v / 100.0 if v > 1 else v
    except Exception:
        return 0.0

def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _now_br() -> str:
    return datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")


# =========================
# Conversão 3 níveis
# =========================

def _fator_total(unidade_compra: str, decomposicao1: float, decomposicao2: float) -> float:
    """
    unidade_compra:
      - CX / FD: fator = decomposicao1 (pacotes por CX/FD) * decomposicao2 (un por pacote)
      - DP / PC : fator = decomposicao2 (un por pacote)
      - UN      : fator = 1
    """
    uc = (unidade_compra or '').upper()
    d1 = float(decomposicao1 or 0)
    d2 = float(decomposicao2 or 0)
    if uc in ('CX', 'FD'):
        return d1 * d2
    if uc in ('DP', 'PC'):
        return d2
    return 1.0


@dataclass
class ConversaoResultado:
    fator: float
    qtd_un: float
    custo_un_sem_ipi: float
    custo_ipi_un: float
    custo_un_com_ipi: float


def converter_para_un(
    unidade_compra: str,
    qtd_documento: float,
    valor_unitario_uc: float,
    ipi_percentual,
    decomposicao1: float,
    decomposicao2: float
) -> ConversaoResultado:
    fator = _fator_total(unidade_compra, decomposicao1, decomposicao2)
    if fator <= 0:
        raise ValueError("Fator inválido. Informe a decomposição corretamente.")
    qtd_un = float(qtd_documento) * fator
    custo_un_sem_ipi = float(valor_unitario_uc) / fator
    ipi_f = _norm_pct(ipi_percentual)
    custo_ipi_un = (float(valor_unitario_uc) * ipi_f) / fator
    custo_un_com_ipi = custo_un_sem_ipi + custo_ipi_un
    # arredondamentos operacionais
    return ConversaoResultado(
        fator=round(fator, 6),
        qtd_un=round(qtd_un, 6),
        custo_un_sem_ipi=round(custo_un_sem_ipi, 4),
        custo_ipi_un=round(custo_ipi_un, 4),
        custo_un_com_ipi=round(custo_un_com_ipi, 4),
    )


# =========================
# Regra em ProdutoFornecedor
# =========================

def upsert_regra_produto_fornecedor(
    estoque_id: int,
    unidade_compra: str,
    fator_conversao: float,
    descricao_compra: Optional[str] = None,
    fornecedor_id: Optional[int] = None
) -> int:
    """
    Salva/atualiza a regra por SKU+unidade_compra (opcionalmente por fornecedor).
    - Para CX/FD: fator_conversao = decomposicao1 * decomposicao2 (UN por CX/FD)
    - Para DP/PC: fator_conversao = decomposicao2 (UN por DP/PC)
    - Para UN   : 1
    """
    uc = (unidade_compra or '').upper()
    fator = float(fator_conversao)
    q = (db.session.query(ProdutoFornecedor)
         .filter(ProdutoFornecedor.estoque_id == estoque_id,
                 ProdutoFornecedor.unidade_compra == uc))
    if fornecedor_id:
        q = q.filter(ProdutoFornecedor.fornecedor_id == fornecedor_id)

    rec = q.first()
    if rec:
        rec.fator_conversao = fator
        if descricao_compra:
            rec.descricao_compra = descricao_compra
        db.session.commit()
        return rec.id

    novo = ProdutoFornecedor(
        estoque_id=estoque_id,
        fornecedor_id=fornecedor_id,
        unidade_compra=uc,
        fator_conversao=fator,
        descricao_compra=descricao_compra or uc
    )
    db.session.add(novo)
    db.session.commit()
    return novo.id


# =========================
# Estoque + Movimentação
# =========================

def _get_or_create_estoque(sku: str, nome: str) -> Estoque:
    e = (db.session.query(Estoque)
         .filter(Estoque.sku == sku)
         .first())
    if e:
        return e
    e = Estoque(
        sku=sku,
        nome=nome or sku,
        unidade_medida_venda='UN',
        quantidade_atual=0.0,
        custo_unitario=0.0,
        custo_ipi=0.0,
        custo_com_ipi=0.0,
        percentual_ipi=0.0
    )
    db.session.add(e)
    db.session.flush()  # pega id
    return e


def _atualizar_custos_estoque_oficial_mais_medio(
    est: Estoque,
    qtd_entrada_un: float,
    custo_oficial_un: float,
    custo_sem_ipi_un: float,
    custo_ipi_un: float
):
    """
    - custo oficial = custo COM IPI  (conforme solicitado)
    - custo médio ponderado com base no custo_oficial_un
    """
    qtd_ant = float(est.quantidade_atual or 0.0)
    cm_ant = float(est.custo_com_ipi or 0.0)  # usando COM IPI como base do "médio"

    total_ant = qtd_ant * cm_ant
    total_novo = float(qtd_entrada_un or 0.0) * float(custo_oficial_un or 0.0)
    qtd_final = qtd_ant + float(qtd_entrada_un or 0.0)

    if qtd_final > 0:
        cm_final = (total_ant + total_novo) / qtd_final
    else:
        cm_final = float(custo_oficial_un or 0.0)

    # Atualiza campos principais
    est.quantidade_atual = qtd_final
    est.custo_unitario = float(custo_sem_ipi_un or 0.0)
    est.custo_ipi = float(custo_ipi_un or 0.0)
    est.custo_com_ipi = float(custo_oficial_un or 0.0)
    est.custo_medio = round(cm_final, 4)
    est.custo_ultima_compra = float(custo_oficial_un or 0.0)


def _lançar_movimentacao_entrada(
    est: Estoque,
    qtd_un: float,
    custo_oficial_un: float,
    origem: str,
    documento_ref: str,
    usuario: Optional[str] = None,
    observacao: Optional[str] = None
) -> MovimentacaoEstoque:
    """Cria registro de ENTRADA em 'movimentacoes' já em UN."""
    saldo_qtd_apos = float(est.quantidade_atual or 0.0)
    mov = MovimentacaoEstoque(
        estoque_id=est.id,
        tipo='ENTRADA',
        origem=origem,
        documento_ref=documento_ref,
        canal='COMPRAS',
        conta='ESTOQUE',
        quantidade=float(qtd_un or 0.0),
        custo_unitario=float(custo_oficial_un or 0.0),
        custo_total=round(float(qtd_un or 0.0) * float(custo_oficial_un or 0.0), 2),
        saldo_quantidade=saldo_qtd_apos,
        saldo_custo_medio=float(est.custo_medio or est.custo_com_ipi or 0.0),
        data_mov_iso=_now_iso(),
        data_mov_br=_now_br(),
        usuario=usuario or 'sistema',
        observacao=observacao
    )
    db.session.add(mov)
    return mov


# =========================
# API principal (processar um item de ProdutoNF)
# =========================

def processar_item_produto_nf(
    produto_nf_id: int,
    decomposicao1: float,
    decomposicao2: float,
    salvar_regra: bool = True,
    fornecedor_id: Optional[int] = None,
    usar_custo_com_ipi: bool = True,  # solicitado: SEMPRE True
    usuario: Optional[str] = None
) -> Dict:
    """
    Processa UMA linha da fila (ProdutoNF):
    - Converte para UN (3 níveis)
    - Atualiza/insere regra ProdutoFornecedor (se salvar_regra=True)
    - Cria/atualiza Estoque (SKU) e custos (oficial = com IPI)
    - Lança MovimentacaoEstoque (ENTRADA)
    - Marca ProdutoNF.status = 'Concluido'

    Retorna dict com resumo dos cálculos.
    """
    pn = (db.session.query(ProdutoNF).filter(ProdutoNF.id == produto_nf_id).first())
    if not pn:
        raise ValueError(f"ProdutoNF id={produto_nf_id} não encontrado.")

    # 1) Conversão 3 níveis
    conv = converter_para_un(
        unidade_compra=pn.unidade_compra,
        qtd_documento=pn.quantidade_compra,
        valor_unitario_uc=pn.valor_unitario_compra,
        ipi_percentual=(pn.ipi_percentual or 0.0),
        decomposicao1=decomposicao1,
        decomposicao2=decomposicao2
    )

    # 2) Regra (opcional)
    fator_conv = conv.fator
    if salvar_regra:
        desc = None
        uc = (pn.unidade_compra or '').upper()
        if uc in ('CX','FD'):
            desc = f"{int(decomposicao1 or 0)}x{int(decomposicao2 or 0)} {uc}"
        elif uc in ('DP','PC'):
            desc = f"{int(decomposicao2 or 0)} UN/{uc}"
        upsert_regra_produto_fornecedor(
            estoque_id=None,  # vamos resolver depois do get_or_create
            unidade_compra=pn.unidade_compra,
            fator_conversao=fator_conv,
            descricao_compra=desc,
            fornecedor_id=fornecedor_id
        )
        # OBS: vamos corrigir o estoque_id no upsert ao final, quando tivermos o id.
        # Na maioria dos casos você já terá o Estoque; abaixo resolvemos isso.

    # 3) Estoque (garante SKU)
    est = _get_or_create_estoque(pn.produto_sku, pn.produto_nome)

    # Se marcou salvar_regra, garanta que ficou com estoque_id correto
    if salvar_regra:
        upsert_regra_produto_fornecedor(
            estoque_id=est.id,
            unidade_compra=pn.unidade_compra,
            fator_conversao=fator_conv,
            descricao_compra=None,
            fornecedor_id=fornecedor_id
        )

    # 4) Custos e quantidade — SEMPRE custo oficial = COM IPI
    custo_oficial_un = conv.custo_un_com_ipi if usar_custo_com_ipi else conv.custo_un_sem_ipi
    _atualizar_custos_estoque_oficial_mais_medio(
        est=est,
        qtd_entrada_un=conv.qtd_un,
        custo_oficial_un=custo_oficial_un,
        custo_sem_ipi_un=conv.custo_un_sem_ipi,
        custo_ipi_un=conv.custo_ipi_un
    )

    # 5) Movimentação (ENTRADA)
    mov = _lançar_movimentacao_entrada(
        est=est,
        qtd_un=conv.qtd_un,
        custo_oficial_un=custo_oficial_un,
        origem='NF-e' if (pn.chave_nfe and pn.chave_nfe.startswith('NFe')) else 'MANUAL',
        documento_ref=pn.chave_nfe or (pn.numero_nfe or 'MANUAL'),
        usuario=usuario,
        observacao=f"Entrada por processamento (UC={pn.unidade_compra}, fator={conv.fator})"
    )

    # 6) Atualiza ProdutoNF
    pn.status = 'Concluido'
    pn.valor_total_item = pn.valor_total_item or conv.qtd_un * conv.custo_un_sem_ipi + conv.qtd_un * conv.custo_ipi_un
    pn.valor_total_compra = pn.valor_total_compra or pn.valor_total_item
    db.session.commit()

    return {
        "produto_nf_id": pn.id,
        "sku": est.sku,
        "estoque_id": est.id,
        "unidade_compra": pn.unidade_compra,
        "fator_conversao": conv.fator,
        "qtd_entrada_un": conv.qtd_un,
        "custo_un_sem_ipi": conv.custo_un_sem_ipi,
        "custo_ipi_un": conv.custo_ipi_un,
        "custo_un_com_ipi": conv.custo_un_com_ipi,
        "custo_oficial_usado": custo_oficial_un,
        "mov_id": mov.id if getattr(mov, "id", None) else None,
        "status_produto_nf": pn.status
    }


# =========================
# Batch por CHAVE
# =========================

def processar_documento_por_chave(
    chave_nfe: str,
    decomposicoes: Dict[int, Tuple[float, float]],
    salvar_regras: bool = True,
    fornecedor_id: Optional[int] = None,
    usar_custo_com_ipi: bool = True,
    usuario: Optional[str] = None
) -> List[Dict]:
    """
    Processa todas as linhas de um documento (ProdutoNF) identificado por chave_nfe.
    - 'decomposicoes' é um dict {produto_nf_id: (decomp1, decomp2)}
      (ex.: { 123: (10, 50), 124: (1, 25) })
    Retorna lista de resumos.
    """
    itens = (db.session.query(ProdutoNF)
             .filter(ProdutoNF.chave_nfe == chave_nfe)
             .all())
    resultados = []
    for pn in itens:
        d1, d2 = decomposicoes.get(pn.id, (None, None))
        if d1 in (None, 0) and d2 in (None, 0):
            # se vier UN, aceita (fator=1). Para CX/FD/DP/PC, exige informar.
            if (pn.unidade_compra or '').upper() not in ('UN',):
                raise ValueError(f"Faltam decomposições para ProdutoNF id={pn.id}")
            d1, d2 = 1, 1
        r = processar_item_produto_nf(
            produto_nf_id=pn.id,
            decomposicao1=d1,
            decomposicao2=d2,
            salvar_regra=salvar_regras,
            fornecedor_id=fornecedor_id,
            usar_custo_com_ipi=usar_custo_com_ipi,
            usuario=usuario
        )
        resultados.append(r)
    return resultados
