# -*- coding: utf-8 -*-
# /modulos/estoque/models.py

from datetime import datetime
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import CheckConstraint, UniqueConstraint, text
from sqlalchemy.orm import relationship
from extensions import db


# =========================
#   TABELAS DE ESTOQUE
# =========================
class Estoque(db.Model):
    __tablename__ = 'estoque'
    id = db.Column(db.Integer, primary_key=True)

    nome = db.Column(db.String(120), nullable=False, unique=True)
    sku = db.Column(db.String(80), nullable=False, unique=True, index=True)

    # Estoque SEMPRE em unidade de venda (fracionado quando necessário)
    quantidade_atual = db.Column(db.Float, default=0.0)

    # CMP BASE (SEM IPI)
    custo_unitario = db.Column(db.Float, default=0.0)

    # NOVAS COLUNAS (precisam existir no .db também)
    custo_ipi = db.Column(db.Float, default=0.0)        # IPI por unidade
    custo_com_ipi = db.Column(db.Float, default=0.0)    # custo_unitario + custo_ipi

    unidade_medida_venda = db.Column(db.String(10), default='UN')
    ponto_reposicao = db.Column(db.Integer, default=20)
    estoque_seguranca = db.Column(db.Integer, default=5)

    data_criacao = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    fornecedor = db.Column(db.String(120))
    url_imagens = db.Column(db.Text)

    # ATENÇÃO: guardar como FATOR (ex.: 7,5% => 0.075)
    percentual_ipi = db.Column(db.Float, default=0.0)

    # Relacionamentos (como seu projeto já usava)
    movimentacoes = db.relationship(
        'MovimentacaoEstoque',
        backref='estoque',
        lazy='dynamic',
        cascade="all, delete-orphan"
    )
    formas_de_compra = db.relationship(
        'ProdutoFornecedor',
        backref='estoque',
        lazy='dynamic',
        cascade="all, delete-orphan"
    )
    itens_inventario = db.relationship(
        'InventarioItem',
        backref='estoque',
        lazy='dynamic',
        cascade="all, delete-orphan"
    )

    @hybrid_property
    def valor_total_estoque(self):
        """Total (sem IPI) -> se quiser com IPI mude para custo_com_ipi."""
        try:
            return float(self.quantidade_atual or 0) * float(self.custo_unitario or 0)
        except Exception:
            return 0.0


# =========================
#   MOVIMENTAÇÕES (ALINHADO AO SEU DDL)
# =========================
# dentro de /modulos/estoque/models.py

from datetime import datetime
from sqlalchemy import CheckConstraint, text
from extensions import db

class MovimentacaoEstoque(db.Model):
    __tablename__ = 'movimentacoes'   # <-- AQUI: tabela nova

    id = db.Column(db.Integer, primary_key=True)
    estoque_id = db.Column(db.Integer, db.ForeignKey('estoque.id'), nullable=False, index=True)

    # ENTRADA | SAIDA | AJUSTE (maiúsculo)
    tipo = db.Column(db.String(10), nullable=False)
    origem = db.Column(db.String(50))
    documento_ref = db.Column(db.String(100))
    canal = db.Column(db.String(30))
    conta = db.Column(db.String(50))

    quantidade = db.Column(db.Float, nullable=False)
    custo_unitario = db.Column(db.Float)
    custo_total = db.Column(db.Float)

    saldo_quantidade = db.Column(db.Float)
    saldo_custo_medio = db.Column(db.Float)

    # datas (strings no seu DDL)
    data_mov_iso = db.Column(db.String(19), server_default=text("(datetime('now'))"))
    data_mov_br  = db.Column(db.String(19))

    usuario = db.Column(db.String(60))
    observacao = db.Column(db.Text)

    inventario_id = db.Column(db.Integer, db.ForeignKey('inventario.id'))

    __table_args__ = (
        CheckConstraint("tipo in ('ENTRADA','SAIDA','AJUSTE')", name='ck_mov_tipo'),
    )

    # ---- Compat com o template: fornece um datetime para strftime
    @property
    def data_movimentacao(self):
        """
        Converte data_mov_iso (YYYY-MM-DD HH:MM[:SS]) ou data_mov_br (dd/MM/YYYY HH:MM[:SS])
        em datetime. Fallback: now().
        """
        def _try(s, pats):
            if not s: return None
            s = s.replace('T', ' ').strip()
            for p in pats:
                try:   return datetime.strptime(s, p)
                except: pass
            return None

        dt = _try(self.data_mov_iso, ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'])
        if dt: return dt
        dt = _try(self.data_mov_br,  ['%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M'])
        return dt or datetime.utcnow()

    # ---- Compat com telas antigas que ainda chamam mov.produto
    @property
    def produto(self):
        return self.estoque

    # Compat com telas antigas que ainda chamam mov.produto
    @property
    def produto(self):
        return self.estoque

    def __repr__(self):
        return f"<MovimentacaoEstoque id={self.id} estoque_id={self.estoque_id} tipo={self.tipo}>"


# =========================
#   FORNECEDOR / REGRAS DE COMPRA
# =========================
class Fornecedor(db.Model):
    __tablename__ = 'fornecedor'
    id = db.Column(db.Integer, primary_key=True)

    nome = db.Column(db.String(150), nullable=False, unique=True, index=True)
    cnpj = db.Column(db.String(20), unique=True)

    formas_de_compra = db.relationship(
        'ProdutoFornecedor',
        backref='fornecedor',
        lazy='dynamic'
    )

    def __repr__(self):
        return f"<Fornecedor id={self.id} nome={self.nome!r}>"


class ProdutoFornecedor(db.Model):
    __tablename__ = 'produto_fornecedor'
    id = db.Column(db.Integer, primary_key=True)

    estoque_id = db.Column(db.Integer, db.ForeignKey('estoque.id'), nullable=False, index=True)
    fornecedor_id = db.Column(db.Integer, db.ForeignKey('fornecedor.id'))

    descricao_compra = db.Column(db.String(150), nullable=False)              # ex.: "Caixa 20 pacotes de 50"
    unidade_compra   = db.Column(db.String(10),  nullable=False, default='CX')# CX, DP, PC...
    fator_conversao  = db.Column(db.Float,      nullable=False, default=1.0)  # ex.: 20*50=1000
    custo_ultima_compra = db.Column(db.Float)

    __table_args__ = (
        UniqueConstraint('estoque_id', 'unidade_compra', 'descricao_compra', name='u_estoque_regra_compra'),
    )

    def __repr__(self):
        return f"<ProdutoFornecedor id={self.id} estoque_id={self.estoque_id} unidade={self.unidade_compra}>"


# =========================
#   FILA DE ITENS (NF-e)
# =========================
class ProdutoNF(db.Model):
    __tablename__ = 'produto_nf'

    id               = db.Column(db.Integer, primary_key=True)
    chave_nfe        = db.Column(db.String(44), nullable=False, index=True)
    data_emissao = db.Column(db.String(19))  # <--- COLUNA FALTANTE
    # Cabeçalho / Fornecedor
    numero_nfe       = db.Column(db.String(20))
    serie_nfe        = db.Column(db.String(10))
    fornecedor_nome  = db.Column(db.String(120))
    fornecedor_cnpj  = db.Column(db.String(20))
    fornecedor       = db.Column(db.String(120))  # legado, se existir

    # Item
    produto_nome           = db.Column(db.String(120), nullable=False)
    produto_sku            = db.Column(db.String(60),  nullable=False, index=True)
    ncm                    = db.Column(db.String(10))
    cest                   = db.Column(db.String(10))
    cfop                   = db.Column(db.String(10))

    unidade_compra         = db.Column(db.String(10), nullable=False)
    quantidade_compra      = db.Column(db.Float,      nullable=False)
    valor_unitario_compra  = db.Column(db.Float,      nullable=False)

    # IPI (como armazenado na NF)
    ipi_percentual   = db.Column(db.Float)     # pode ser nulo
    valor_ipi        = db.Column(db.Float)     # total do IPI da linha (se veio na NF)

    # Totais
    valor_total_item   = db.Column(db.Float)   # total cheio da linha, se a NF gravou
    valor_total_compra = db.Column(db.Float)   # total cheio usado no seu fluxo (coluna existente)

    # Status / Datas (padrão Brasil em texto)
    status           = db.Column(db.String(20), index=True, default='Pendente')
    data_emissao_iso = db.Column(db.String(19))  # YYYY-MM-DD HH:MM:SS
    data_emissao_br  = db.Column(db.String(19))  # dd/MM/YYYY HH:MM:SS
    data_criacao_iso = db.Column(db.String(19))
    data_criacao_br  = db.Column(db.String(19))
    data_criacao     = db.Column(db.String(19))

    # --------- PROPRIEDADES CÁLCULO (sem sobrescrever colunas) ---------
    @hybrid_property
    def base_total(self):
        """valor_unitario_compra * quantidade_compra"""
        try:
            return float(self.valor_unitario_compra or 0.0) * float(self.quantidade_compra or 0.0)
        except Exception:
            return 0.0

    @hybrid_property
    def valor_ipi_calc(self):
        """
        IPI calculado: usa 'valor_ipi' se existir; senão deriva por 'ipi_percentual' sobre a base.
        """
        try:
            ipi_val = float(self.valor_ipi or 0.0)
        except Exception:
            ipi_val = 0.0
        if ipi_val > 0:
            return ipi_val

        base = self.base_total
        try:
            perc = float(self.ipi_percentual or 0.0)
        except Exception:
            try:
                perc = float(str(self.ipi_percentual).replace(',', '.'))
            except Exception:
                perc = 0.0
        if perc > 1.0:
            perc = perc / 100.0
        if base <= 0 or perc <= 0:
            return 0.0
        return base * perc

    @hybrid_property
    def valor_total_calc(self):
        """
        Total efetivo calculado (se a coluna valor_total_compra estiver nula):
        - Prioriza 'valor_total_item' se existir;
        - Senão, base_total + valor_ipi_calc.
        """
        if self.valor_total_item is not None:
            try:
                return float(self.valor_total_item or 0.0)
            except Exception:
                pass
        return float(self.base_total) + float(self.valor_ipi_calc or 0.0)

    def __repr__(self):
        return f"<ProdutoNF id={self.id} chave={self.chave_nfe}>"



# =========================
#   INVENTÁRIO
# =========================
class Inventario(db.Model):
    __tablename__ = 'inventario'
    id = db.Column(db.Integer, primary_key=True)

    nome = db.Column(db.String(150), nullable=False)
    status = db.Column(db.String(20), nullable=False, default='Pendente')
    criado_em = db.Column(db.DateTime, default=datetime.utcnow, index=True)

    itens = db.relationship(
        'InventarioItem',
        backref='inventario',
        lazy='dynamic',
        cascade="all, delete-orphan"
    )


class InventarioItem(db.Model):
    """
    Registra a contagem de um item do estoque dentro de um evento de inventário.
    """
    __tablename__ = 'inventario_itens'
    id = db.Column(db.Integer, primary_key=True)

    inventario_id = db.Column(db.Integer, db.ForeignKey('inventario.id'), nullable=False)

    # referência a ESTOQUE
    estoque_id = db.Column(db.Integer, db.ForeignKey('estoque.id'), nullable=False)

    # Fracionado
    quantidade_sistema = db.Column(db.Float, nullable=False)
    quantidade_contada = db.Column(db.Float)

    status = db.Column(db.String(20), nullable=False, default='Nao Contado')

    __table_args__ = (
        UniqueConstraint('inventario_id', 'estoque_id', name='_inventario_estoque_uc'),
    )

    @hybrid_property
    def divergencia(self):
        if self.quantidade_contada is None:
            return 0.0
        return float(self.quantidade_contada) - float(self.quantidade_sistema)
