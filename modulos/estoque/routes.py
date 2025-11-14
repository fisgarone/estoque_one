# -*- coding: utf-8 -*-
# /modulos/estoque/routes.py
"""
Arquivo revisado e sincronizado com o schema atual (padrão Brasil):
- Tabela de movimentos: movimentacoes_estoque (data_mov_iso, data_mov_br, tipo em MAIÚSCULO)
- Campos de produto/estoque: somente os que existem no banco
- Rotas sem duplicação de endpoints (evita BuildError/overwriting)
- Filtros por período usando strings ISO (SQLite)
- Datas para exibir em BR (dd/mm/aaaa HH:MM:SS)
- Nenhum dado fictício; campos opcionais ficam NULL quando não enviados

Este arquivo preserva: páginas, CRUD de produtos, movimentações, inventário,
processamento de NF-e, e APIs do dashboard (cards, gráficos, alertas, buscas).
"""

from __future__ import annotations
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from datetime import datetime, timezone, timedelta
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Union

from flask import (
    Blueprint,
    render_template,
    request,
    flash,
    redirect,
    url_for,
    jsonify,
)
from sqlalchemy import func, desc, or_, and_, text

try:
    from zoneinfo import ZoneInfo

    TZ_BR = ZoneInfo("America/Sao_Paulo")
except Exception:
    TZ_BR = None  # fallback: usa UTC formatado como BR quando tz não disponível

from extensions import db
from .models import (
    Estoque,
    MovimentacaoEstoque,
    Inventario,
    InventarioItem,
    ProdutoNF,
    ProdutoFornecedor,
)

# -----------------------------------------------------------------------------
# BLUEPRINT
# -----------------------------------------------------------------------------
estoque_bp = Blueprint(
    "estoque",
    __name__,
    template_folder="../templates",
    static_folder="../static",
    url_prefix="/estoque",
)


# -----------------------------------------------------------------------------
# UTILITÁRIOS
# -----------------------------------------------------------------------------

def agora_iso() -> str:
    """Retorna data/hora UTC em ISO (YYYY-MM-DD HH:MM:SS)."""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def agora_br() -> str:
    """Retorna data/hora no fuso de São Paulo em formato BR."""
    if TZ_BR:
        return datetime.now(TZ_BR).strftime("%d/%m/%Y %H:%M:%S")
    return datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")


def brl(valor: float) -> str:
    return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fbr(valor: Any, default: float = 0.0) -> float:
    """Converte valor para float de forma segura (fallback para default)."""
    if valor is None:
        return default
    try:
        if isinstance(valor, str):
            valor = valor.strip().replace(',', '.')
        return float(valor)
    except (ValueError, TypeError):
        return default


def get_attr(obj: Any, attr_name: str, default: Any = None) -> Any:
    """Obtém atributo de objeto de forma segura com fallback."""
    return getattr(obj, attr_name, default)


def as_percent(valor: Any) -> float:
    """Converte valor para percentual (divide por 100 se > 1)."""
    try:
        num = fbr(valor, 0.0)
        if num > 1.0:
            return num / 100.0
        return num
    except (ValueError, TypeError):
        return 0.0


def now_iso() -> str:
    """Alias para agora_iso() para compatibilidade."""
    return agora_iso()


def now_br() -> str:
    """Alias para agora_br() para compatibilidade."""
    return agora_br()


def formatar_decimal(valor: Any, casas: int = 4) -> float:
    """Formata valor decimal com número específico de casas (truncamento, não arredondamento)."""
    try:
        # Converte para float primeiro
        if isinstance(valor, str):
            valor = valor.replace(',', '.').strip()
        num = float(valor)

        # Truncamento para N casas decimais
        factor = 10.0 ** casas
        return int(num * factor) / factor
    except (ValueError, TypeError):
        return 0.0  # Retorna 0.0 em caso de erro


def formatar_data_br(data_iso: str) -> str:
    """Converte data ISO (YYYY-MM-DD HH:MM:SS) para formato BR (DD/MM/YYYY HH:MM:SS)."""
    try:
        if not data_iso:
            return ""
        # Remove microssegundos se existirem
        if '.' in data_iso:
            data_iso = data_iso.split('.')[0]
        dt = datetime.strptime(data_iso, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except (ValueError, TypeError):
        return data_iso  # Retorna original se não conseguir converter

# ========= FORMATADORES PADRÃO =========
from decimal import Decimal, localcontext, InvalidOperation, ROUND_DOWN

def _fmt4_core(value, usar_virgula: bool) -> str:
    """
    Formata número com no MÁXIMO 4 casas decimais (TRUNCADO, não arredonda).
    Remove zeros à direita. Aceita '7,5' ou '7.5'.
    Ex.: 0.1195999492 -> 0.1195 ; 0.123433666666667 -> 0.1234
    """
    if value is None:
        return ""
    s = str(value).strip()
    if s == "":
        return ""
    s = s.replace(",", ".")
    try:
        d = Decimal(s)
    except Exception:
        # Se não for número, devolve como veio
        return s

    with localcontext() as ctx:
        ctx.rounding = ROUND_DOWN  # TRUNCA
        d = d.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

    out = format(d, "f")  # sem notação científica
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    if usar_virgula:
        out = out.replace(".", ",")
    return out

@estoque_bp.app_template_filter("fmt4")     # 0.1234 (ponto)
def jfmt4(value):
    return _fmt4_core(value, usar_virgula=False)

@estoque_bp.app_template_filter("fmt4_br")  # 0,1234 (vírgula)
def jfmt4_br(value):
    return _fmt4_core(value, usar_virgula=True)


@estoque_bp.app_template_filter("data_br")
def jdata_br(v):
    """
    Converte várias entradas para 'dd/mm/aaaa' (sem hora).
    Aceita: 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS', 'dd/mm/aaaa HH:MM:SS', datetime.
    """
    if v is None:
        return ""
    try:
        # datetime
        if hasattr(v, "strftime"):
            return v.strftime("%d/%m/%Y")
        s = str(v).strip()
        if not s:
            return ""
        # formatos mais comuns
        fmts = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y")
        for f in fmts:
            try:
                dt = datetime.strptime(s, f)
                return dt.strftime("%d/%m/%Y")
            except Exception:
                pass
        # heurística: se vier 'dd/mm/aaaa HH:MM:SS'
        if " " in s and "/" in s:
            return s.split(" ")[0]
        # tentativa ISO
        try:
            dt = datetime.fromisoformat(s)
            return dt.strftime("%d/%m/%Y")
        except Exception:
            return s
    except Exception:
        return str(v)

@estoque_bp.app_template_filter("datahora_br")
def jdatahora_br(v):
    """
    Converte para 'dd/MM/yyyy HH:MM:SS' quando você quiser mostrar hora.
    """
    if v is None:
        return ""
    try:
        if hasattr(v, "strftime"):
            return v.strftime("%d/%m/%Y %H:%M:%S")
        s = str(v).strip()
        fmts = ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d")
        for f in fmts:
            try:
                dt = datetime.strptime(s, f)
                # Se veio só a data, mantém 00:00:00
                if f == "%Y-%m-%d":
                    return dt.strftime("%d/%m/%Y")
                return dt.strftime("%d/%m/%Y %H:%M:%S")
            except Exception:
                pass
        # fallback: se já vier em BR com hora, retorna
        if "/" in s:
            return s
        return s
    except Exception:
        return str(v)

# -----------------------------------------------------------------------------
# PÁGINAS / DASHBOARD / PRODUTOS
# -----------------------------------------------------------------------------

@estoque_bp.route("/")
def dashboard():
    return render_template("estoque/dashboard_estoque.html")


@estoque_bp.route("/produtos")
def listar_produtos():
    page = request.args.get("page", 1, type=int)
    produtos_paginados = Estoque.query.order_by(Estoque.nome.asc()).paginate(page=page, per_page=12)

    # Formata valores decimais
    for produto in produtos_paginados.items:
        if hasattr(produto, 'quantidade_atual'):
            produto.quantidade_atual = formatar_decimal(produto.quantidade_atual or 0.0)
        if hasattr(produto, 'custo_unitario'):
            produto.custo_unitario = formatar_decimal(produto.custo_unitario or 0.0)
        if hasattr(produto, 'ipi_percentual'):
            produto.ipi_percentual = formatar_decimal(produto.ipi_percentual or 0.0, 2)

    return render_template("estoque/listar_produtos.html", produtos=produtos_paginados)


@estoque_bp.route("/produtos/novo", methods=["GET"])
def adicionar_produto():
    return render_template("estoque/form_produto.html", produto=None, titulo="Adicionar Novo Produto")


@estoque_bp.route("/produtos/editar/<int:id>", methods=["GET"])
def editar_produto(id: int):
    produto = Estoque.query.get_or_404(id)

    # Formata valores decimais para exibição no formulário
    if hasattr(produto, 'custo_unitario'):
        produto.custo_unitario = formatar_decimal(produto.custo_unitario or 0.0)
    if hasattr(produto, 'ipi_percentual'):
        produto.ipi_percentual = formatar_decimal(produto.ipi_percentual or 0.0, 2)

    return render_template("estoque/form_produto.html", produto=produto, titulo="Editar Produto")


@estoque_bp.route("/produtos/salvar", methods=["POST"])
def salvar_produto():
    """Salva/atualiza SOMENTE colunas existentes no banco (sem gambiarras)."""
    try:
        produto_id = request.form.get("id")

        nome = (request.form.get("nome") or "").strip()
        sku = (request.form.get("sku") or "").strip()
        custo_unitario = formatar_decimal(float(request.form.get("custo_unitario") or 0.0))
        ponto_reposicao = int(request.form.get("ponto_reposicao") or 0)
        estoque_seguranca = int(request.form.get("estoque_seguranca") or 0)
        unidade_medida_venda = (request.form.get("unidade_medida_venda") or "UN").upper()
        fornecedor = (request.form.get("fornecedor") or None)
        url_imagens = (request.form.get("url_imagens") or None)
        ipi_percentual = formatar_decimal(float(request.form.get("ipi_percentual") or 0.0), 2)

        if not nome or not sku:
            flash("Nome e SKU são obrigatórios.", "error")
            return redirect(url_for("estoque.listar_produtos"))

        if produto_id:
            p = Estoque.query.get_or_404(produto_id)
            p.nome = nome
            p.sku = sku
            p.custo_unitario = custo_unitario
            p.ponto_reposicao = ponto_reposicao
            p.estoque_seguranca = estoque_seguranca
            p.unidade_medida_venda = unidade_medida_venda
            p.fornecedor = fornecedor
            p.url_imagens = url_imagens
            p.ipi_percentual = ipi_percentual
            flash("Produto atualizado com sucesso!", "success")
        else:
            novo = Estoque(
                nome=nome,
                sku=sku,
                custo_unitario=custo_unitario,
                ponto_reposicao=ponto_reposicao,
                estoque_seguranca=estoque_seguranca,
                unidade_medida_venda=unidade_medida_venda,
                fornecedor=fornecedor,
                url_imagens=url_imagens,
                ipi_percentual=ipi_percentual,
                quantidade_atual=0.0,
            )
            db.session.add(novo)
            flash("Produto cadastrado com sucesso!", "success")

        db.session.commit()
    except Exception as e:
        db.session.rollback()
        flash(f"Erro ao salvar o produto: {e}", "error")

    return redirect(url_for("estoque.listar_produtos"))


@estoque_bp.route("/produto/<int:id>")
def detalhe_produto(id: int):
    p = Estoque.query.get_or_404(id)

    # Formata valores decimais
    if hasattr(p, 'quantidade_atual'):
        p.quantidade_atual = formatar_decimal(p.quantidade_atual or 0.0)
    if hasattr(p, 'custo_unitario'):
        p.custo_unitario = formatar_decimal(p.custo_unitario or 0.0)
    if hasattr(p, 'ipi_percentual'):
        p.ipi_percentual = formatar_decimal(p.ipi_percentual or 0.0, 2)

    periodo = request.args.get("periodo", 90, type=int)
    limite_iso = (datetime.utcnow() - timedelta(days=periodo)).strftime("%Y-%m-%d %H:%M:%S")

    # saídas no período (para consumo médio)
    saidas = (
            db.session.query(func.coalesce(func.sum(MovimentacaoEstoque.quantidade), 0.0))
            .filter(
                MovimentacaoEstoque.estoque_id == id,
                MovimentacaoEstoque.tipo == "SAIDA",
                MovimentacaoEstoque.data_mov_iso >= limite_iso,
            )
            .scalar()
            or 0.0
    )

    cmd = (saidas / max(periodo, 1)) if saidas > 0 else 0.0
    data_ruptura_estimada: Optional[datetime] = None
    dias_para_ruptura = 0.0
    if cmd > 0:
        dias_para_ruptura = (p.quantidade_atual or 0.0) / cmd
        data_ruptura_estimada = datetime.utcnow() + timedelta(days=dias_para_ruptura)

    # série temporal de movimentações (ordem cronológica)
    movimentacoes = (
        MovimentacaoEstoque.query.filter_by(estoque_id=id)
        .order_by(MovimentacaoEstoque.data_mov_iso.asc())
        .all()
    )

    chart_labels: List[str] = []
    chart_data: List[float] = []
    quantidade_acumulada = 0.0

    if movimentacoes:
        # ponto inicial (antes da primeira mov)
        primeira_iso = movimentacoes[0].data_mov_iso or agora_iso()
        try:
            primeira_dt = datetime.strptime(primeira_iso, "%Y-%m-%d %H:%M:%S") - timedelta(seconds=1)
        except Exception:
            primeira_dt = datetime.utcnow() - timedelta(seconds=1)
        chart_labels.append(primeira_dt.strftime("%d/%m %H:%M"))
        chart_data.append(0.0)

    for m in movimentacoes:
        if m.tipo == "ENTRADA":
            quantidade_acumulada += float(m.quantidade or 0.0)
        elif m.tipo == "SAIDA":
            quantidade_acumulada -= float(m.quantidade or 0.0)
        # Formata data para BR
        data_formatada = formatar_data_br(m.data_mov_iso or "")
        chart_labels.append(data_formatada[0:16] if data_formatada else "")
        chart_data.append(formatar_decimal(quantidade_acumulada))

    return render_template(
        "estoque/detalhe_produto.html",
        produto=p,
        chart_labels=chart_labels,
        chart_data=chart_data,
        consumo_medio_diario=formatar_decimal(cmd),
        dias_para_ruptura=formatar_decimal(dias_para_ruptura),
        data_ruptura_estimada=data_ruptura_estimada,
        periodo_selecionado=periodo,
    )


# -----------------------------------------------------------------------------
# MOVIMENTAÇÕES
# -----------------------------------------------------------------------------

@estoque_bp.route("/movimentacoes")
def listar_movimentacoes():
    page = request.args.get("page", 1, type=int)
    movs = (
        MovimentacaoEstoque.query.order_by(MovimentacaoEstoque.data_mov_iso.desc())
        .paginate(page=page, per_page=15)
    )

    # Formata valores decimais e datas
    for mov in movs.items:
        if hasattr(mov, 'quantidade'):
            mov.quantidade = formatar_decimal(mov.quantidade or 0.0)
        if hasattr(mov, 'custo_unitario'):
            mov.custo_unitario = formatar_decimal(mov.custo_unitario or 0.0)
        if hasattr(mov, 'custo_total'):
            mov.custo_total = formatar_decimal(mov.custo_total or 0.0)
        if hasattr(mov, 'saldo_quantidade'):
            mov.saldo_quantidade = formatar_decimal(mov.saldo_quantidade or 0.0)
        if hasattr(mov, 'saldo_custo_medio'):
            mov.saldo_custo_medio = formatar_decimal(mov.saldo_custo_medio or 0.0)
        # Formata data para exibição
        if hasattr(mov, 'data_mov_br') and mov.data_mov_br:
            mov.data_mov_br = formatar_data_br(mov.data_mov_iso or mov.data_mov_br)

    return render_template("estoque/listar_movimentacoes.html", movimentacoes=movs)


@estoque_bp.route("/movimentacoes/nova", methods=["GET"])
def nova_movimentacao():
    produtos = Estoque.query.order_by(Estoque.nome.asc()).all()

    # Formata valores decimais dos produtos
    for produto in produtos:
        if hasattr(produto, 'quantidade_atual'):
            produto.quantidade_atual = formatar_decimal(produto.quantidade_atual or 0.0)
        if hasattr(produto, 'custo_unitario'):
            produto.custo_unitario = formatar_decimal(produto.custo_unitario or 0.0)

    return render_template("estoque/form_movimentacao.html", produtos=produtos)


@estoque_bp.route("/movimentacoes/registrar", methods=["POST"])
def registrar_movimentacao():
    try:
        estoque_id = int(request.form.get("estoque_id"))
        tipo = (request.form.get("tipo") or "").strip().upper()  # ENTRADA | SAIDA | AJUSTE
        if tipo not in ("ENTRADA", "SAIDA", "AJUSTE"):
            flash("Tipo inválido. Use ENTRADA, SAIDA ou AJUSTE.", "error")
            return redirect(url_for("estoque.nova_movimentacao"))

        quantidade = formatar_decimal(float(request.form.get("quantidade") or 0))
        if quantidade == 0:
            flash("Quantidade obrigatória.", "error")
            return redirect(url_for("estoque.nova_movimentacao"))

        origem = (request.form.get("origem") or None)  # NFE, VENDA, INVENTARIO, MANUAL...
        documento_ref = (request.form.get("documento_ref") or None)
        canal = (request.form.get("canal") or None)  # ML, SHOPEE, SHEIN, LOJA
        conta = (request.form.get("conta") or None)  # apelido/ID da conta
        usuario = (request.form.get("usuario") or None)
        observacao = (request.form.get("observacao") or request.form.get("motivo") or None)

        p = Estoque.query.get_or_404(estoque_id)

        # Atualiza saldo do produto
        if tipo == "ENTRADA":
            p.quantidade_atual = formatar_decimal(float(p.quantidade_atual or 0.0) + quantidade)
        elif tipo == "SAIDA":
            if (p.quantidade_atual or 0.0) < quantidade:
                flash(f"Estoque insuficiente para {p.nome}.", "error")
                return redirect(url_for("estoque.nova_movimentacao"))
            p.quantidade_atual = formatar_decimal(float(p.quantidade_atual or 0.0) - quantidade)
        elif tipo == "AJUSTE":
            p.quantidade_atual = formatar_decimal(
                float(p.quantidade_atual or 0.0) + quantidade)  # ajuste pode ser negativo

        iso = agora_iso()
        br = agora_br()

        custo_unit = formatar_decimal(float(p.custo_unitario or 0.0))
        custo_total = formatar_decimal(custo_unit * float(quantidade))

        mov = MovimentacaoEstoque(
            estoque_id=p.id,
            tipo=tipo,
            origem=origem,
            documento_ref=documento_ref,
            canal=canal,
            conta=conta,
            quantidade=quantidade,
            custo_unitario=custo_unit,
            custo_total=custo_total,
            saldo_quantidade=formatar_decimal(float(p.quantidade_atual or 0.0)),
            saldo_custo_medio=formatar_decimal(float(p.custo_unitario or 0.0)),
            data_mov_iso=iso,
            data_mov_br=br,
            usuario=usuario,
            observacao=observacao,
            inventario_id=None,
        )

        db.session.add(mov)
        db.session.commit()
        flash(f"Movimentação {tipo} registrada com sucesso.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Erro ao registrar movimentação: {e}", "error")

    return redirect(url_for("estoque.listar_movimentacoes"))


@estoque_bp.route("/produtos-sem-giro")
def listar_produtos_sem_giro():
    try:
        periodo_dias = request.args.get("periodo", 90, type=int)
        limite_iso = (datetime.utcnow() - timedelta(days=periodo_dias)).strftime("%Y-%m-%d %H:%M:%S")

        ids_saida = {
            r.estoque_id
            for r in db.session.query(MovimentacaoEstoque.estoque_id)
            .filter(
                MovimentacaoEstoque.tipo == "SAIDA",
                MovimentacaoEstoque.data_mov_iso >= limite_iso,
            )
            .distinct()
            .all()
        }

        produtos_parados = [
            p for p in Estoque.query.filter(Estoque.quantidade_atual > 0).all() if p.id not in ids_saida
        ]

        # Formata valores decimais
        for produto in produtos_parados:
            if hasattr(produto, 'quantidade_atual'):
                produto.quantidade_atual = formatar_decimal(produto.quantidade_atual or 0.0)
            if hasattr(produto, 'custo_unitario'):
                produto.custo_unitario = formatar_decimal(produto.custo_unitario or 0.0)

        produtos_parados.sort(
            key=lambda p: (float(p.quantidade_atual or 0.0) * float(p.custo_unitario or 0.0)),
            reverse=True,
        )

        valor_total_parado = sum(
            (float(p.quantidade_atual or 0.0) * float(p.custo_unitario or 0.0)) for p in produtos_parados
        )

        return render_template(
            "estoque/produtos_sem_giro.html",
            produtos=produtos_parados,
            periodo_dias=periodo_dias,
            valor_total_parado=formatar_decimal(valor_total_parado),
        )
    except Exception as e:
        flash(f"Erro ao buscar produtos sem giro: {e}", "error")
        return render_template(
            "estoque/produtos_sem_giro.html",
            produtos=[],
            periodo_dias=90,
            valor_total_parado=0.0,
        )


# -----------------------------------------------------------------------------
# INVENTÁRIO
# -----------------------------------------------------------------------------

@estoque_bp.route("/inventarios", methods=["GET", "POST"])
def inventarios():
    if request.method == "POST":
        try:
            nome = (request.form.get("nome") or "").strip()
            if not nome:
                flash("O nome do inventário é obrigatório.", "error")
                return redirect(url_for("estoque.inventarios"))

            inv = Inventario(nome=nome, status="Em Andamento")
            db.session.add(inv)
            db.session.flush()

            produtos_para_contar = Estoque.query.filter(Estoque.quantidade_atual > 0).all()
            if not produtos_para_contar:
                inv.status = "Concluido"
                flash("Nenhum produto com estoque para contar. Inventário concluído.", "info")
            else:
                for p in produtos_para_contar:
                    item = InventarioItem(
                        inventario_id=inv.id,
                        estoque_id=p.id,
                        quantidade_sistema=formatar_decimal(p.quantidade_atual or 0.0),
                        status="Nao Contado",
                    )
                    db.session.add(item)
                flash(f'Inventário "{nome}" iniciado com {len(produtos_para_contar)} itens.', "success")

            db.session.commit()
            return redirect(url_for("estoque.contagem_inventario", id=inv.id))
        except Exception as e:
            db.session.rollback()
            flash(f"Erro ao iniciar inventário: {e}", "error")

    inventarios_registrados = Inventario.query.order_by(desc(Inventario.id)).all()
    return render_template("estoque/listar_inventarios.html", inventarios=inventarios_registrados)


@estoque_bp.route("/inventario/<int:id>/contagem")
def contagem_inventario(id: int):
    inv = Inventario.query.get_or_404(id)
    if inv.status not in ["Em Andamento", "Pendente"]:
        flash(f'Este inventário está com status "{inv.status}" e não pode mais ser editado.', "warning")
        return redirect(url_for("estoque.inventarios"))
    itens = inv.itens.join(Estoque, InventarioItem.estoque_id == Estoque.id).order_by(Estoque.nome.asc()).all()

    # Formata valores decimais
    for item in itens:
        if hasattr(item, 'quantidade_sistema'):
            item.quantidade_sistema = formatar_decimal(item.quantidade_sistema or 0.0)
        if hasattr(item, 'quantidade_contada'):
            item.quantidade_contada = formatar_decimal(item.quantidade_contada or 0.0)
        if hasattr(item, 'divergencia'):
            item.divergencia = formatar_decimal(item.divergencia or 0.0)

    return render_template("estoque/contagem_inventario.html", inventario=inv, itens=itens)


@estoque_bp.route("/api/inventario/item/<int:item_id>/contar", methods=["POST"])
def contar_item_inventario(item_id: int):
    try:
        item = InventarioItem.query.get_or_404(item_id)
        data = request.get_json() or {}
        if "quantidade_contada" not in data:
            return jsonify({"success": False, "error": "Campo quantidade_contada ausente."}), 400

        q = data["quantidade_contada"]
        if q is None or str(q).strip() == "":
            item.quantidade_contada = None
            item.status = "Nao Contado"
        else:
            quantidade = formatar_decimal(float(q))
            if quantidade < 0:
                raise ValueError("Quantidade não pode ser negativa")
            item.quantidade_contada = quantidade
            item.status = "Contado"

        db.session.commit()
        return jsonify({
            "success": True,
            "item_id": item.id,
            "status": item.status,
            "divergencia": formatar_decimal(item.divergencia or 0.0),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@estoque_bp.route("/inventario/<int:id>/finalizar", methods=["POST"])
def finalizar_inventario(id: int):
    inv = Inventario.query.get_or_404(id)
    if inv.status != "Em Andamento":
        return jsonify({"success": False, "error": "Este inventário não está em andamento."}), 400

    try:
        itens = inv.itens.all()
        nao_contados = [it for it in itens if it.status == "Nao Contado"]
        if nao_contados:
            return jsonify({"success": False, "error": f"{len(nao_contados)} itens ainda não foram contados."}), 400

        for it in itens:
            if it.divergencia != 0:
                # Ajuste positivo => ENTRADA; ajuste negativo => SAIDA
                tipo = "ENTRADA" if it.divergencia > 0 else "SAIDA"
                quantidade = abs(float(it.divergencia))

                # Atualiza estoque para a contagem final
                est = Estoque.query.get(it.estoque_id)
                if est is None:
                    raise ValueError("Item de estoque inexistente para ajuste de inventário.")
                est.quantidade_atual = formatar_decimal(float(it.quantidade_contada or 0.0))

                # Cria movimentação de ajuste (origem INVENTARIO)
                iso = agora_iso()
                br = agora_br()
                mov = MovimentacaoEstoque(
                    estoque_id=it.estoque_id,
                    tipo=tipo,
                    origem="INVENTARIO",
                    documento_ref=f"INV-{inv.id}",
                    canal=None,
                    conta=None,
                    quantidade=formatar_decimal(quantidade),
                    custo_unitario=formatar_decimal(float(est.custo_unitario or 0.0)),
                    custo_total=formatar_decimal(float(est.custo_unitario or 0.0) * quantidade),
                    saldo_quantidade=formatar_decimal(float(est.quantidade_atual or 0.0)),
                    saldo_custo_medio=formatar_decimal(float(est.custo_unitario or 0.0)),
                    data_mov_iso=iso,
                    data_mov_br=br,
                    usuario=None,
                    observacao=f"Ajuste do Inventário: {inv.nome}",
                    inventario_id=inv.id,
                )
                db.session.add(mov)

        inv.status = "Concluido"
        db.session.commit()
        flash(f'Inventário "{inv.nome}" finalizado e estoque ajustado com sucesso!', "success")
        return jsonify({"success": True, "redirect_url": url_for("estoque.inventarios")})
    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "error": f"Ocorreu um erro: {e}"}), 500


# -----------------------------------------------------------------------------
# PROCESSADOR DE ENTRADAS (NF-e → ESTOQUE)
# -----------------------------------------------------------------------------
@estoque_bp.route('/processar-entradas')
def processar_entradas_lista():
    """
    Mostra SOMENTE itens 'Pendente' que ainda NÃO têm regra em produto_fornecedor
    (chave prática: estoque_id + unidade_compra, normalizados).
    """
    from sqlalchemy import func

    itens_pendentes = (ProdutoNF.query
                       .filter(ProdutoNF.status == 'Pendente')
                       .order_by(ProdutoNF.chave_nfe, ProdutoNF.id)
                       .all())

    # Mapa SKU -> estoque.id (case-insensitive)
    skus = list({(it.produto_sku or '').strip().lower() for it in itens_pendentes if it.produto_sku})
    mapa_sku2estoque = {}
    if skus:
        for p in (Estoque.query
                  .filter(func.lower(Estoque.sku).in_(skus))
                  .all()):
            mapa_sku2estoque[(p.sku or '').strip().lower()] = p.id

    # Nome do campo FK no model ProdutoFornecedor (compat: estoque_id OU produto_id)
    fk_field = 'estoque_id' if hasattr(ProdutoFornecedor, 'estoque_id') else (
               'produto_id' if hasattr(ProdutoFornecedor, 'produto_id') else None)
    if fk_field is None:
        flash("Model ProdutoFornecedor sem campo 'estoque_id' ou 'produto_id'. Ajuste o model.", "error")
        return render_template('estoque/processar_entradas.html', itens=[])

    itens_sem_regra = []
    for it in itens_pendentes:
        sku_key = (it.produto_sku or '').strip().lower()
        est_id = mapa_sku2estoque.get(sku_key)
        if not est_id:
            # Sem produto ainda → precisa aparecer para o usuário criar/confirmar e gerar regra
            itens_sem_regra.append({'obj': it, 'tem_regra': False, 'fator_sugerido': None})
            continue

        # Normalização da unidade_compra
        uncompra = (it.unidade_compra or '').strip().upper()

        # Monta filtro dinâmico
        pf = ProdutoFornecedor
        cond = [
            getattr(pf, fk_field) == est_id,
            func.upper(pf.unidade_compra) == uncompra
        ]
        # se existir coluna 'ativo', filtra por ativos
        if hasattr(pf, 'ativo'):
            cond.append(pf.ativo == 1)

        regra = (pf.query.filter(*cond)
                       .order_by(pf.id.desc())
                       .first())

        # Se NÃO tem regra, entra na lista
        if not regra or not getattr(regra, 'fator_conversao', None) or float(regra.fator_conversao) <= 0:
            itens_sem_regra.append({'obj': it, 'tem_regra': False, 'fator_sugerido': None})

    # O template espera 'itens'; cada item tem a chave 'obj'
    return render_template('estoque/processar_entradas.html', itens=itens_sem_regra)


# ======================================================
@estoque_bp.route('/processar-entradas/<int:item_id>', methods=['GET'])
def processar_entradas_item(item_id: int):
    item = ProdutoNF.query.get_or_404(item_id)

    # Formata valores decimais
    if hasattr(item, 'quantidade_compra'):
        item.quantidade_compra = formatar_decimal(item.quantidade_compra or 0.0)
    if hasattr(item, 'valor_unitario_compra'):
        item.valor_unitario_compra = formatar_decimal(item.valor_unitario_compra or 0.0)
    if hasattr(item, 'valor_ipi'):
        item.valor_ipi = formatar_decimal(item.valor_ipi or 0.0)
    if hasattr(item, 'percentual_ipi'):
        item.percentual_ipi = formatar_decimal(item.percentual_ipi or 0.0)

    # Produto por SKU (case-insensitive)
    produto = (Estoque.query
               .filter(func.lower(Estoque.sku) == func.lower(item.produto_sku or ''))
               .first())

    # Garante o atributo que o template usa: item_nf.valor_total_compra
    base_total = float(item.quantidade_compra or 0.0) * float(item.valor_unitario_compra or 0.0)

    valor_ipi = 0.0
    if hasattr(item, 'valor_ipi') and item.valor_ipi is not None:
        try:
            valor_ipi = float(item.valor_ipi)
        except:
            valor_ipi = 0.0

    vtc = None
    if hasattr(item, 'valor_total_compra') and item.valor_total_compra is not None:
        try:
            vtc = float(item.valor_total_compra)
        except:
            vtc = None
    if vtc is None and hasattr(item, 'valor_total_item') and item.valor_total_item is not None:
        try:
            vtc = float(item.valor_total_item)
        except:
            vtc = None
    if vtc is None:
        vtc = base_total + valor_ipi

    setattr(item, 'valor_total_compra', formatar_decimal(vtc))  # evita UndefinedError no Jinja

    return render_template(
        "estoque/processar_entrada_item.html",
        item=item,
        item_nf=item,
        produto_estoque=produto
    )

def _aplicar_entrada_com_fator(item_nf, fator_total):
        """
        Aplica a ENTRADA no ESTOQUE usando fator_total já conhecido (regra),
        replicando a confirmação: CMP base, IPI, movimentação ENTRADA.
        """
        from sqlalchemy import func
        from zoneinfo import ZoneInfo

        def fbr(v, default=0.0):
            if v is None: return default
            if isinstance(v, (int, float)): return float(v)
            s = str(v).strip().replace(' ', '').replace(',', '.')
            try:
                return float(s) if s != '' else default
            except:
                return default

        def get_attr(obj, name, default=None):
            return getattr(obj, name, default) if hasattr(obj, name) else default

        if not fator_total or fator_total <= 0:
            raise ValueError("Fator total inválido para processamento em lote.")

        qtd_compra = fbr(item_nf.quantidade_compra, 0.0)
        vunit_compra = fbr(item_nf.valor_unitario_compra, 0.0)
        if qtd_compra <= 0:
            raise ValueError("Quantidade de compra inválida na NF.")

        base_total = vunit_compra * qtd_compra
        total_cheio = fbr(get_attr(item_nf, 'valor_total_compra', None), None)
        if total_cheio is None or total_cheio <= 0:
            total_item = fbr(get_attr(item_nf, 'valor_total_item', None), None)
            valor_ipi = fbr(get_attr(item_nf, 'valor_ipi', 0.0), 0.0)
            total_cheio = total_item if (total_item is not None and total_item > 0) else (base_total + valor_ipi)

        valor_ipi = fbr(get_attr(item_nf, 'valor_ipi', 0.0), 0.0)
        perc_nf = get_attr(item_nf, 'percentual_ipi', None)
        if perc_nf is None:
            perc_nf = (valor_ipi / total_cheio) if total_cheio > 0 else 0.0
        perc_nf = fbr(perc_nf, 0.0)
        if perc_nf < 0: perc_nf = 0.0

        qtd_un = qtd_compra * float(fator_total)
        if qtd_un <= 0:
            raise ValueError("Quantidade em unidades de venda resultou 0.")
        custo_base_unit = base_total / qtd_un  # SEM IPI

        # Upsert por SKU → Estoque
        sku = (item_nf.produto_sku or "").strip()
        nome_nf = (item_nf.produto_nome or "").strip()
        if not sku:
            raise ValueError("SKU vazio na NF.")

        produto = Estoque.query.filter(func.lower(Estoque.sku) == func.lower(sku)).first()
        if not produto:
            if nome_nf:
                conflito = Estoque.query.filter(func.lower(Estoque.nome) == func.lower(nome_nf)).first()
                if conflito:
                    raise ValueError(f'Nome já existe: "{nome_nf}". Ajuste o cadastro ou use o SKU existente.')
            produto = Estoque(
                nome=nome_nf or sku,
                sku=sku,
                unidade_medida_venda='UN',
                quantidade_atual=0.0,
                custo_unitario=custo_base_unit,  # CMP base inicial (sem IPI)
                fornecedor=get_attr(item_nf, 'fornecedor_nome') or get_attr(item_nf, 'fornecedor')
            )
            db.session.add(produto)
            db.session.flush()

        # CMP base (média ponderada)
        qtd_ant = float(get_attr(produto, 'quantidade_atual', 0.0) or 0.0)
        cmp_ant = float(get_attr(produto, 'custo_unitario', 0.0) or 0.0)
        novo_qtd = qtd_ant + qtd_un
        cmp_novo = ((qtd_ant * cmp_ant) + (qtd_un * custo_base_unit)) / novo_qtd if novo_qtd > 0 else custo_base_unit

        produto.quantidade_atual = novo_qtd
        produto.custo_unitario = cmp_novo  # SEM IPI

        # IPI no produto (se colunas existirem)
        custo_ipi_calc = produto.custo_unitario * perc_nf
        custo_com_ipi_calc = produto.custo_unitario + custo_ipi_calc

        if hasattr(Estoque, 'percentual_ipi'):
            produto.percentual_ipi = perc_nf
        elif hasattr(Estoque, 'ipi_percentual'):
            produto.ipi_percentual = perc_nf

        if hasattr(Estoque, 'custo_ipi'):
            produto.custo_ipi = custo_ipi_calc
        if hasattr(Estoque, 'custo_com_ipi'):
            produto.custo_com_ipi = custo_com_ipi_calc

        # Movimentação ENTRADA com custo COM IPI
        iso = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        from zoneinfo import ZoneInfo
        br = datetime.now(ZoneInfo('America/Sao_Paulo')).strftime('%d/%m/%Y %H:%M:%S')

        custo_mov = (
            getattr(produto, 'custo_com_ipi', None)
            if hasattr(Estoque, 'custo_com_ipi')
            else None
        )
        if custo_mov is None:
            custo_mov = custo_com_ipi_calc

        mov = MovimentacaoEstoque(
            estoque_id=produto.id,
            tipo='ENTRADA',
            origem='NFE',
            documento_ref=item_nf.chave_nfe,
            canal=None,
            conta=None,
            quantidade=qtd_un,
            custo_unitario=float(custo_mov or 0.0),
            custo_total=float(custo_mov or 0.0) * float(qtd_un or 0.0),
            saldo_quantidade=float(produto.quantidade_atual or 0.0),
            saldo_custo_medio=float(custo_mov or 0.0),
            data_mov_iso=iso,
            data_mov_br=br,
            usuario=None,
            observacao=f"Entrada NF-e {item_nf.chave_nfe}",
            inventario_id=None
        )
        db.session.add(mov)

        # Finaliza item
        item_nf.status = 'Processado'

    # --- PROCESSAR EM MASSA: aplicar regras salvas (ProdutoFornecedor) ---
@estoque_bp.route('/processar-entradas/aplicar-regras', methods=['POST'])
def aplicar_regras_entrada_em_massa():
    """
    Processa os itens selecionados na fila usando o fator de conversão salvo
    em produto_fornecedor (chave: estoque_id + unidade_compra).
    Commit por item; falhas não travam o lote.
    Só processa os IDs recebidos que de fato tenham regra.
    """
    from sqlalchemy import func

    ids = request.form.getlist('item_nf_id')
    if not ids:
        flash('Nenhum item selecionado.', 'warning')
        return redirect(url_for('estoque.processar_entradas_lista'))

    itens = (ProdutoNF.query
             .filter(ProdutoNF.id.in_(ids), ProdutoNF.status == 'Pendente')
             .all())
    if not itens:
        flash('Nenhum item pendente para processar.', 'info')
        return redirect(url_for('estoque.processar_entradas_lista'))

    # Mapa SKU -> estoque.id
    skus = list({(it.produto_sku or '').strip().lower() for it in itens if it.produto_sku})
    mapa_sku2estoque = {}
    if skus:
        for p in (Estoque.query
                  .filter(func.lower(Estoque.sku).in_(skus))
                  .all()):
            mapa_sku2estoque[(p.sku or '').strip().lower()] = p.id

    # Compat do campo FK no model ProdutoFornecedor
    fk_field = 'estoque_id' if hasattr(ProdutoFornecedor, 'estoque_id') else (
               'produto_id' if hasattr(ProdutoFornecedor, 'produto_id') else None)
    if fk_field is None:
        flash("Model ProdutoFornecedor sem campo 'estoque_id' ou 'produto_id'. Ajuste o model.", "error")
        return redirect(url_for('estoque.processar_entradas_lista'))

    ok, falhas = 0, []

    for it in itens:
        try:
            sku_key = (it.produto_sku or '').strip().lower()
            est_id = mapa_sku2estoque.get(sku_key)
            if not est_id:
                raise ValueError("SKU sem produto cadastrado; não há regra associada.")

            uncompra = (it.unidade_compra or '').strip().upper()

            pf = ProdutoFornecedor
            cond = [
                getattr(pf, fk_field) == est_id,
                func.upper(pf.unidade_compra) == uncompra
            ]
            if hasattr(pf, 'ativo'):
                cond.append(pf.ativo == 1)

            regra = (pf.query.filter(*cond)
                         .order_by(pf.id.desc())
                         .first())
            if not regra or not getattr(regra, 'fator_conversao', None) or float(regra.fator_conversao) <= 0:
                raise ValueError("Regra/fator de conversão não encontrada para esta unidade de compra.")

            # Usa o mesmo caminho da confirmação manual, mas passando o fator conhecido
            _aplicar_entrada_com_fator(it, float(regra.fator_conversao))
            db.session.commit()
            ok += 1
        except Exception as e:
            db.session.rollback()
            falhas.append(f'ID {it.id} ({it.produto_sku}): {e}')

    if ok:
        flash(f'{ok} item(ns) processado(s) automaticamente.', 'success')
    if falhas:
        flash('Alguns itens falharam:\n' + '\n'.join(falhas), 'error')

    return redirect(url_for('estoque.processar_entradas_lista'))

# ======================================================
# CONFIRMAR: grava no estoque e lança movimentação (COM IPI)
# ======================================================
@estoque_bp.route('/processar-entradas/<int:item_id>/confirmar', methods=['POST', 'GET'])
def confirmar_entrada(item_id: int):
    """
    Confirma a entrada do item da NF:
      - CMP do produto é SEM IPI (custo_unitario)
      - Movimentação grava custo_unitario COM IPI
      - Ao final, faz UPSERT da regra em produto_fornecedor para (estoque_id, unidade_compra)
    """
    item = ProdutoNF.query.get_or_404(item_id)

    try:
        # --- FATOR ---
        f1 = fbr(request.form.get('fator1'), 1.0) or 1.0
        f2 = fbr(request.form.get('fator2'), 1.0) or 1.0
        ft_calc = request.form.get('fator_conversao_calculado')
        if ft_calc:
            ft = fbr(ft_calc, 0.0)
            if ft > 0:
                f1, f2 = ft, 1.0
        fator_total = f1 * f2
        if fator_total <= 0:
            fator_total = 1.0

        # --- BASES NF ---
        qtd_compra   = fbr(item.quantidade_compra, 0.0)
        vunit_compra = fbr(item.valor_unitario_compra, 0.0)
        if qtd_compra <= 0:
            raise ValueError("Quantidade de compra inválida na NF.")
        base_total   = vunit_compra * qtd_compra
        valor_ipi    = fbr(get_attr(item, 'valor_ipi', 0.0), 0.0)

        # percentual_ipi da NF (se não vier, deriva pela base)
        perc_nf = get_attr(item, 'percentual_ipi', None)
        if perc_nf is None:
            perc_nf = (valor_ipi / base_total) if base_total > 0 else 0.0
        perc_nf = as_percent(perc_nf)

        # --- CONVERSÃO ---
        qtd_un = qtd_compra * fator_total
        if qtd_un <= 0:
            raise ValueError("Quantidade em unidades de venda resultou 0.")
        custo_base_unit = base_total / qtd_un  # SEM IPI

        # --- UPSERT por SKU no ESTOQUE ---
        sku = (item.produto_sku or '').strip()
        nome_nf = (item.produto_nome or '').strip()
        if not sku:
            raise ValueError("SKU vazio na NF.")

        produto = Estoque.query.filter(func.lower(Estoque.sku) == func.lower(sku)).first()
        if not produto:
            if nome_nf:
                conflito = Estoque.query.filter(func.lower(Estoque.nome) == func.lower(nome_nf)).first()
                if conflito:
                    raise ValueError(f'Nome já existe: "{nome_nf}". Ajuste o cadastro ou use o SKU existente.')
            produto = Estoque(
                nome=nome_nf or sku,
                sku=sku,
                unidade_medida_venda='UN',
                quantidade_atual=0.0,
                custo_unitario=formatar_decimal(custo_base_unit),  # CMP base inicial (sem IPI)
                fornecedor=get_attr(item, 'fornecedor_nome') or get_attr(item, 'fornecedor')
            )
            db.session.add(produto)
            db.session.flush()  # garante produto.id

        # --- CMP (média) SEM IPI ---
        qtd_ant  = float(get_attr(produto, 'quantidade_atual', 0.0) or 0.0)
        cmp_ant  = float(get_attr(produto, 'custo_unitario',   0.0) or 0.0)
        novo_qtd = qtd_ant + qtd_un
        cmp_novo = ((qtd_ant * cmp_ant) + (qtd_un * custo_base_unit)) / novo_qtd if novo_qtd > 0 else custo_base_unit

        produto.quantidade_atual = formatar_decimal(novo_qtd)
        produto.custo_unitario   = formatar_decimal(cmp_novo)  # SEM IPI

        # --- IPI no produto, se colunas existirem ---
        if hasattr(Estoque, 'percentual_ipi'):
            produto.percentual_ipi = formatar_decimal(perc_nf)
        elif hasattr(Estoque, 'ipi_percentual'):
            produto.ipi_percentual = formatar_decimal(perc_nf)

        custo_ipi_calc     = produto.custo_unitario * perc_nf
        custo_com_ipi_calc = produto.custo_unitario + custo_ipi_calc
        if hasattr(Estoque, 'custo_ipi'):
            produto.custo_ipi = formatar_decimal(custo_ipi_calc)
        if hasattr(Estoque, 'custo_com_ipi'):
            produto.custo_com_ipi = formatar_decimal(custo_com_ipi_calc)

        # --- MOV ENTRADA (COM IPI) ---
        custo_mov_unit = getattr(produto, 'custo_com_ipi', None) if hasattr(Estoque, 'custo_com_ipi') else None
        if custo_mov_unit is None:
            custo_mov_unit = custo_com_ipi_calc

        mov = MovimentacaoEstoque(
            estoque_id=produto.id,
            tipo='ENTRADA',
            origem='NFE',
            documento_ref=item.chave_nfe,
            canal=None,
            conta=None,
            quantidade=formatar_decimal(qtd_un),
            custo_unitario=formatar_decimal(float(custo_mov_unit or 0.0)),             # COM IPI
            custo_total=formatar_decimal(float(custo_mov_unit or 0.0) * float(qtd_un or 0.0)),
            saldo_quantidade=formatar_decimal(float(produto.quantidade_atual or 0.0)),
            saldo_custo_medio=formatar_decimal(float(produto.custo_unitario or 0.0)),   # SEM IPI (CMP)
            data_mov_iso=now_iso(),
            data_mov_br=now_br(),
            usuario=None,
            observacao=f"Entrada NF-e {item.chave_nfe}",
            inventario_id=None
        )
        db.session.add(mov)

        # --- UPSERT DA REGRA EM produto_fornecedor ---
        pf = ProdutoFornecedor
        fk_field = 'estoque_id' if hasattr(pf, 'estoque_id') else ('produto_id' if hasattr(pf, 'produto_id') else None)
        if fk_field is None:
            raise RuntimeError("Model ProdutoFornecedor sem 'estoque_id' ou 'produto_id'.")

        uncompra_norm = (item.unidade_compra or '').strip().upper()
        # procura a regra existente (por estoque_id + unidade_compra + ativo = 1 se existir)
        cond = [
            getattr(pf, fk_field) == produto.id,
            func.upper(pf.unidade_compra) == uncompra_norm
        ]
        if hasattr(pf, 'ativo'):
            cond.append(pf.ativo == 1)

        regra = (pf.query.filter(*cond)
                     .order_by(pf.id.desc())
                     .first())

        if regra:
            # atualiza fator e custo da última compra
            if hasattr(regra, 'fator_conversao'):
                regra.fator_conversao = float(fator_total)
            if hasattr(regra, 'custo_ultima_compra'):
                regra.custo_ultima_compra = float(vunit_compra)
        else:
            # cria uma regra nova (evita violar sua UNIQUE incluindo descricao_compra)
            kwargs = {
                fk_field: produto.id,
                'unidade_compra': uncompra_norm,
                'fator_conversao': float(fator_total),
                'custo_ultima_compra': float(vunit_compra),
            }
            # Campos opcionais do seu schema:
            if hasattr(pf, 'descricao_compra'):
                kwargs['descricao_compra'] = request.form.get('descricao_compra') or f'AUTO {uncompra_norm}'
            if hasattr(pf, 'ativo'):
                kwargs['ativo'] = 1
            if hasattr(pf, 'moeda'):
                kwargs['moeda'] = 'BRL'
            # fornecedor_id só se você quiser atrelar por fornecedor específico:
            if hasattr(pf, 'fornecedor_id'):
                # Deixe NULL por padrão para regra geral do SKU; se quiser, vincule aqui
                kwargs['fornecedor_id'] = None

            nova_regra = pf(**kwargs)
            db.session.add(nova_regra)

        # --- Finaliza item da fila e COMMIT ---
        item.status = 'Processado'
        db.session.commit()

        flash("Entrada confirmada e regra gravada/atualizada.", "success")
        return redirect(url_for('estoque.processar_entradas_lista'))

    except IntegrityError as ie:
        db.session.rollback()
        msg = str(getattr(ie, 'orig', ie))
        if 'estoque.nome' in msg.lower():
            flash('Nome de produto já existe. Ajuste o nome no cadastro.', 'error')
        elif 'estoque.sku' in msg.lower():
            flash('SKU já existe. Vincule ao SKU existente.', 'error')
        elif 'u_regra_unica' in msg.lower():
            # Colisão da UNIQUE (estoque_id, fornecedor_id, unidade_compra, descricao_compra)
            flash('Regra já existe para este SKU/unidade/descrição. Ajuste a descrição ou edite a regra.', 'error')
        else:
            flash(f'Erro de integridade: {msg}', 'error')
        return redirect(url_for('estoque.processar_entradas_item', item_id=item_id))
    except Exception as e:
        db.session.rollback()
        import traceback; traceback.print_exc()
        flash(f'Erro ao confirmar entrada: {e}', 'error')
        return redirect(url_for('estoque.processar_entradas_item', item_id=item_id))

# -----------------------------------------------------------------------------
# APIs – BUSCA, CARDS, GRÁFICOS, ALERTAS
# -----------------------------------------------------------------------------

@estoque_bp.route("/api/buscar_produtos", methods=["GET", "POST"])
def api_buscar_produtos():
    q = (request.values.get("q") or "").strip()
    limit = min(max(int(request.values.get("limit") or 20), 1), 100)

    base = Estoque.query
    if q:
        like = f"%{q}%"
        base = base.filter(or_(Estoque.nome.ilike(like), Estoque.sku.ilike(like)))

    produtos = base.order_by(Estoque.nome.asc()).limit(limit).all()
    retorno = [
        {
            "id": p.id,
            "nome": p.nome,
            "sku": p.sku,
            "quantidade_atual": formatar_decimal(float(p.quantidade_atual or 0.0)),
            "custo_unitario": formatar_decimal(float(p.custo_unitario or 0.0)),
            "unidade_medida_venda": p.unidade_medida_venda or "UN",
            "ponto_reposicao": int(p.ponto_reposicao or 0),
            "estoque_seguranca": int(p.estoque_seguranca or 0),
        }
        for p in produtos
    ]

    return jsonify(retorno)


@estoque_bp.route("/api/total_produtos")
def api_total_produtos():
    total = db.session.query(func.count(Estoque.id)).scalar() or 0
    return jsonify({"total_produtos": int(total)})


@estoque_bp.route("/api/evolucao_estoque")
def api_evolucao_estoque():
    try:
        hoje = datetime.utcnow()
        labels: List[str] = []
        valores: List[float] = []

        # últimos 12 meses (inclui mês atual)
        for i in range(11, -1, -1):
            ref = hoje.replace(day=1) - timedelta(days=30 * i)
            prox = (ref.replace(day=28) + timedelta(days=4)).replace(day=1)
            ultimo_dia = prox - timedelta(days=1)
            ultimo_dia_iso = ultimo_dia.strftime("%Y-%m-%d %H:%M:%S")

            valor_total = 0.0
            for p in Estoque.query.all():
                qtd = float(p.quantidade_atual or 0.0)
                # desfaz movimentos ocorridos APÓS o último dia
                movs_post = (
                    MovimentacaoEstoque.query.filter(
                        MovimentacaoEstoque.estoque_id == p.id,
                        MovimentacaoEstoque.data_mov_iso > ultimo_dia_iso,
                    ).all()
                )
                for m in movs_post:
                    if m.tipo == "ENTRADA":
                        qtd -= float(m.quantidade or 0.0)
                    elif m.tipo == "SAIDA":
                        qtd += float(m.quantidade or 0.0)
                valor_total += qtd * float(p.custo_unitario or 0.0)

            labels.append(ultimo_dia.strftime("%b/%y"))
            valores.append(formatar_decimal(valor_total))

        return jsonify({"labels": labels, "data": valores})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@estoque_bp.route("/api/giro_estoque")
def api_giro_estoque():
    try:
        periodo_dias = request.args.get("periodo", 90, type=int)
        limite_iso = (datetime.utcnow() - timedelta(days=periodo_dias)).strftime("%Y-%m-%d %H:%M:%S")

        # saídas no período
        saidas = (
            db.session.query(
                MovimentacaoEstoque.estoque_id,
                func.sum(MovimentacaoEstoque.quantidade).label("qtd_saida"),
            )
            .filter(
                MovimentacaoEstoque.tipo == "SAIDA",
                MovimentacaoEstoque.data_mov_iso >= limite_iso,
            )
            .group_by(MovimentacaoEstoque.estoque_id)
            .all()
        )
        saidas_map = {r.estoque_id: formatar_decimal(float(r.qtd_saida or 0.0)) for r in saidas}

        # entradas no período (para estoque médio)
        entradas = (
            db.session.query(
                MovimentacaoEstoque.estoque_id,
                func.sum(MovimentacaoEstoque.quantidade).label("qtd_entrada"),
            )
            .filter(
                MovimentacaoEstoque.tipo == "ENTRADA",
                MovimentacaoEstoque.data_mov_iso >= limite_iso,
            )
            .group_by(MovimentacaoEstoque.estoque_id)
            .all()
        )
        entradas_map = {r.estoque_id: formatar_decimal(float(r.qtd_entrada or 0.0)) for r in entradas}

        itens: List[Tuple[str, float]] = []
        for p in Estoque.query.all():
            s = saidas_map.get(p.id, 0.0)
            e = entradas_map.get(p.id, 0.0)
            qtd_atual = formatar_decimal(float(p.quantidade_atual or 0.0))
            qtd_inicial_aprox = qtd_atual - (e - s)
            estoque_medio = max(0.0, (qtd_atual + qtd_inicial_aprox) / 2.0)
            giro = (s / estoque_medio) if estoque_medio > 0 else 0.0
            if giro > 0:
                itens.append((p.nome, formatar_decimal(giro)))

        top10 = sorted(itens, key=lambda x: x[1], reverse=True)[:10]
        labels, data = (list(zip(*top10)) if top10 else ([], []))
        return jsonify({"labels": list(labels), "data": list(map(float, data))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@estoque_bp.route("/api/produtos_sem_giro")
def api_produtos_sem_giro():
    try:
        periodo = request.args.get("periodo", 90, type=int)
        limite_iso = (datetime.utcnow() - timedelta(days=periodo)).strftime("%Y-%m-%d %H:%M:%S")

        ids_com_saida = {
            r.estoque_id
            for r in db.session.query(MovimentacaoEstoque.estoque_id)
            .filter(
                MovimentacaoEstoque.tipo == "SAIDA",
                MovimentacaoEstoque.data_mov_iso >= limite_iso,
            )
            .distinct()
            .all()
        }

        produtos = Estoque.query.filter(Estoque.quantidade_atual > 0).all()
        parados = [p for p in produtos if p.id not in ids_com_saida]
        parados.sort(key=lambda p: (p.quantidade_atual or 0) * (p.custo_unitario or 0), reverse=True)

        dados = [
            {
                "id": p.id,
                "nome": p.nome,
                "quantidade": formatar_decimal(float(p.quantidade_atual or 0.0)),
                "valor_parado": formatar_decimal(float((p.quantidade_atual or 0.0) * (p.custo_unitario or 0.0))),
            }
            for p in parados
        ]

        return jsonify(dados)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@estoque_bp.route("/api/valor_parado")
def api_valor_parado():
    try:
        periodo = request.args.get("periodo", 90, type=int)
        limite_iso = (datetime.utcnow() - timedelta(days=periodo)).strftime("%Y-%m-%d %H:%M:%S")

        ids_com_saida = {
            r.estoque_id
            for r in db.session.query(MovimentacaoEstoque.estoque_id)
            .filter(
                MovimentacaoEstoque.tipo == "SAIDA",
                MovimentacaoEstoque.data_mov_iso >= limite_iso,
            )
            .distinct()
            .all()
        }

        valor_total = 0.0
        for p in Estoque.query.filter(Estoque.quantidade_atual > 0).all():
            if p.id in ids_com_saida:
                continue
            valor_total += float(p.quantidade_atual or 0.0) * float(p.custo_unitario or 0.0)

        return jsonify({
            "valor_parado": formatar_decimal(float(valor_total)),
            "valor_parado_formatado": brl(valor_total),
        })
    except Exception as e:
        return jsonify({"error": "Erro no cálculo do valor parado: " + str(e)}), 500


@estoque_bp.route("/api/produtos_alerta")
def produtos_alerta():
    produtos = (
        Estoque.query.filter(
            or_(
                Estoque.quantidade_atual <= Estoque.ponto_reposicao,
                Estoque.quantidade_atual < (Estoque.estoque_seguranca),
            )
        )
        .order_by(Estoque.quantidade_atual.asc())
        .all()
    )

    retorno = [
        {
            "id": p.id,
            "nome": p.nome,
            "sku": p.sku,
            "quantidade": formatar_decimal(float(p.quantidade_atual or 0.0)),
            "ponto_reposicao": int(p.ponto_reposicao or 0),
            "estoque_seguranca": int(p.estoque_seguranca or 0),
        }
        for p in produtos
    ]

    return jsonify(retorno)


@estoque_bp.route("/api/custo_por_produto")
def api_custo_por_produto():
    itens: List[Tuple[str, float]] = []
    for p in Estoque.query.all():
        valor = float(p.quantidade_atual or 0.0) * float(p.custo_unitario or 0.0)
        if valor > 0:
            itens.append((p.nome, formatar_decimal(valor)))
    itens.sort(key=lambda x: x[1], reverse=True)
    top10 = itens[:10]
    labels, data = (list(zip(*top10)) if top10 else ([], []))
    return jsonify({"labels": list(labels), "data": list(map(float, data))})


@estoque_bp.route("/api/cards_info")
def api_cards_info():
    total_itens = db.session.query(func.coalesce(func.sum(Estoque.quantidade_atual), 0.0)).scalar() or 0.0
    custo_total = (
            db.session.query(func.coalesce(func.sum(Estoque.quantidade_atual * Estoque.custo_unitario), 0.0)).scalar()
            or 0.0
    )
    criticos = (
            db.session.query(func.count(Estoque.id))
            .filter(
                or_(
                    Estoque.quantidade_atual <= Estoque.ponto_reposicao,
                    Estoque.quantidade_atual < Estoque.estoque_seguranca,
                )
            )
            .scalar()
            or 0
    )

    return jsonify(
        {
            "total_itens": formatar_decimal(float(total_itens)),
            "custo_total": formatar_decimal(float(custo_total)),
            "custo_total_formatado": brl(custo_total),
            "produtos_criticos": int(criticos),
        }
    )


# -----------------------------------------------------------------------------
# PÁGINA – CURVA ABC
# -----------------------------------------------------------------------------

@estoque_bp.route("/curva-abc")
def curva_abc():
    try:
        itens: List[Dict[str, object]] = []
        total = 0.0
        for p in Estoque.query.all():
            valor = (p.quantidade_atual or 0.0) * (p.custo_unitario or 0.0)
            if valor > 0:
                itens.append({"id": p.id, "nome": p.nome, "sku": p.sku, "valor_total": formatar_decimal(valor)})
                total += valor
        itens.sort(key=lambda x: x["valor_total"], reverse=True)

        perc_acum = 0.0
        classificados: List[Dict[str, object]] = []
        resumo = {
            "A": {"count": 0, "value": 0.0},
            "B": {"count": 0, "value": 0.0},
            "C": {"count": 0, "value": 0.0},
            "total_items": len(itens),
            "total_value": formatar_decimal(total),
        }
        for it in itens:
            perc = (it["valor_total"] / total) * 100 if total > 0 else 0.0
            perc_acum += perc
            if perc_acum <= 80:
                classe = "A"
            elif perc_acum <= 95:
                classe = "B"
            else:
                classe = "C"
            it["percentual_individual"] = formatar_decimal(perc, 2)
            it["percentual_acumulado"] = formatar_decimal(perc_acum, 2)
            it["classe"] = classe
            resumo[classe]["count"] += 1
            resumo[classe]["value"] += it["valor_total"]
            classificados.append(it)

        chart_data = {
            "labels": [p["nome"] for p in classificados],
            "barData": [p["valor_total"] for p in classificados],
            "lineData": [p["percentual_acumulado"] for p in classificados],
        }
        return render_template("estoque/curva_abc.html", resumo=resumo, produtos_classificados=classificados,
                               chart_data=chart_data)
    except Exception as e:
        flash(f"Ocorreu um erro ao gerar a análise ABC: {e}", "error")
        return render_template("estoque/curva_abc.html", resumo={}, produtos_classificados=[], chart_data={})