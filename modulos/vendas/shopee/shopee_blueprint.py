# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, render_template, current_app, send_file, redirect, url_for
import os
import io
from datetime import datetime

# tenta pegar db do SQLAlchemy se existir (novo app)
try:
    from extensions import db as _sa_db  # opcional, só se teu app novo tiver SQLAlchemy
except Exception:
    _sa_db = None

# --- IMPORTA A CLASSE CORRETA (nome do arquivo: calculos.py) ---
try:
    from .calculos import ShopeeCalculos
except Exception as e:
    ShopeeCalculos = None
    _import_error = f"Falha ao importar .calculos.ShopeeCalculos: {e}"
else:
    _import_error = None

# ATENÇÃO: sem url_prefix aqui; o app registra com url_prefix
shopee_bp = Blueprint("shopee", __name__)

_calc = None

def _normalize_path(p: str) -> str:
    if not p:
        return p
    p = os.path.expanduser(p)
    # se vier relativo (ex.: "fisgarone.db"), torna relativo ao root do app
    if not os.path.isabs(p):
        try:
            base = current_app.root_path
            p = os.path.abspath(os.path.join(base, p))
        except Exception:
            p = os.path.abspath(p)
    return p

def _sqlite_path_from_uri(uri: str) -> str | None:
    if not uri:
        return None
    u = uri.strip()
    if not u.lower().startswith("sqlite"):
        return None
    # padrões aceitos:
    # sqlite:///C:/path/db.sqlite  | sqlite:////C:/path/db.sqlite | sqlite:///relative.db
    # sqlite:///:memory:  -> inválido para este módulo
    if ":memory:" in u:
        return None
    # remove prefixos comuns
    for pre in ("sqlite:////", "sqlite:///"):
        if u.startswith(pre):
            path = u[len(pre):]
            return _normalize_path(path)
    # fallback bruto
    return _normalize_path(u.replace("sqlite://", "").lstrip("/"))

def _resolve_db_path() -> str | None:
    """
    Retorna um caminho de arquivo SQLite para a Shopee ler,
    procurando em várias fontes (compatível com app antigo e novo).
    """
    # 1) Config legado
    p = current_app.config.get("DATABASE")
    if p:
        return _normalize_path(p) if os.path.exists(_normalize_path(p)) else _normalize_path(p)

    # 2) SQLAlchemy do app novo
    try:
        if _sa_db is not None and _sa_db.engine:
            url = str(_sa_db.engine.url)
            path = _sqlite_path_from_uri(url)
            if path:
                return path
    except Exception:
        pass

    # 3) Config SQLALCHEMY_DATABASE_URI direto
    uri = current_app.config.get("SQLALCHEMY_DATABASE_URI")
    path = _sqlite_path_from_uri(uri) if uri else None
    if path:
        return path

    # 4) Variáveis de ambiente usuais
    for envk in ("DATABASE_URL", "SQLALCHEMY_DATABASE_URI", "APP_DATABASE", "FISGARONE_DB"):
        envv = os.environ.get(envk)
        path = _sqlite_path_from_uri(envv) if envv else None
        if path:
            return path
        if envv and envv.endswith(".db"):
            return _normalize_path(envv)

    # 5) Tentativas padrão (compat antigo)
    candidates = [
        os.path.join(current_app.root_path, "fisgarone.db"),
        os.path.join(os.path.dirname(current_app.root_path), "fisgarone.db"),
        "fisgarone.db",
    ]
    for c in candidates:
        c = _normalize_path(c)
        if os.path.exists(c):
            return c

    return None  # não achou

def _get_calc():
    """Inicializa e retorna a instância de ShopeeCalculos 1x."""
    global _calc
    if isinstance(_calc, ShopeeCalculos):
        return _calc

    if _import_error:
        raise RuntimeError(_import_error)
    if ShopeeCalculos is None:
        raise RuntimeError("Classe ShopeeCalculos indisponível.")

    db_path = _resolve_db_path()
    if not db_path:
        raise FileNotFoundError(
            "DATABASE inválido ou inexistente: None. "
            "Defina app.config['DATABASE'] (ex: C:\\fisgarone\\fisgarone.db) "
            "ou use SQLALCHEMY_DATABASE_URI=sqlite:///C:/caminho/fisgarone.db"
        )
    # opcional: se não existir, retorna erro mais claro (melhor que estourar depois)
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"Arquivo do banco não encontrado: {db_path}. "
            "Aponte para um .db SQLite válido."
        )

    _calc = ShopeeCalculos(db_path=db_path)
    return _calc

def _ok(data):
    return jsonify(data), 200

def _err(msg, code=500, error_type="api_error"):
    return jsonify({"success": False, "message": str(msg), "error_type": error_type}), code

# ----------------- PÁGINAS -----------------

@shopee_bp.route("/", strict_slashes=False)
def index():
    try:
        return render_template('vendas/shopee/index.html')
    except Exception:
        return redirect(url_for('shopee.analytics_page'))

@shopee_bp.route("/dashboard", strict_slashes=False)
def dashboard():
    return redirect(url_for('shopee.analytics_page'))

@shopee_bp.route('/analytics')
def analytics_page():
    return render_template('vendas/shopee/analytics.html')

@shopee_bp.get("/curva-abc")
def curva_abc_page():
    return render_template("vendas/shopee/curva_abc.html")

@shopee_bp.get("/diario")
def diario_page():
    return render_template("vendas/shopee/diario.html")

# ----------------- APIS -----------------

@shopee_bp.route('/api/shopee/test-connection')
def api_test_connection():
    try:
        c = _get_calc()
        return jsonify({"success": True, "message": "OK", "db_path": c.db_path})
    except Exception as e:
        return jsonify({"success": False, "message": str(e), "error_type": "initialization_error"}), 500

@shopee_bp.route('/api/shopee/filtros')
def api_filtros():
    try:
        c = _get_calc()
        filtros = c.get_filtros_disponiveis()
        return jsonify({"success": True, **filtros})
    except Exception as e:
        return _err(e, 500, "initialization_error")

@shopee_bp.route('/api/shopee/dashboard-data')
def api_dashboard_data():
    try:
        c = _get_calc()
        filtros = {k: v for k, v in request.args.items() if v}
        data = c.get_dados_dashboard(filtros or None)
        return jsonify(data)
    except Exception as e:
        return _err(e, 500, "initialization_error")

@shopee_bp.route('/api/shopee/curva-abc')
def api_curva_abc():
    try:
        c = _get_calc()
        args = request.args.to_dict()
        criterio = args.pop("criterio", "receita")
        curva = c.calcular_curva_abc(args or None, criterio)
        try:
            distrib = c.get_distribuicao_abc(args or None, criterio)
        except Exception:
            distrib = None
        return jsonify({"success": True, "curva_abc": curva, "ranking": curva, "distribuicao": distrib})
    except Exception as e:
        return _err(e, 500, "initialization_error")

@shopee_bp.route('/api/shopee/top-produtos')
def api_top_produtos():
    try:
        c = _get_calc()
        limit = int(request.args.get('limit', 10))
        order_by = request.args.get('order_by', 'lucro')
        filtros = {k: v for k, v in request.args.items() if k not in ['limit', 'order_by'] and v}
        produtos = c.get_top_produtos(limit, filtros or None, order_by)
        return jsonify({"success": True, "produtos": produtos})
    except Exception as e:
        return _err(e, 500, "initialization_error")

@shopee_bp.route('/api/shopee/transportadoras')
def api_transportadoras():
    try:
        c = _get_calc()
        filtros = {k: v for k, v in request.args.items() if v}
        dados = c.get_analise_transportadoras(filtros or None)
        return jsonify({"success": True, "transportadoras": dados})
    except Exception as e:
        return _err(e, 500, "initialization_error")

@shopee_bp.route('/api/shopee/evolucao-vendas')
def api_evolucao_vendas():
    try:
        c = _get_calc()
        periodo = request.args.get('periodo', '30d')
        filtros = {k: v for k, v in request.args.items() if k != 'periodo' and v}
        dados = c.get_evolucao_vendas(periodo, filtros or None)
        return jsonify({"success": True, "evolucao": dados})
    except Exception as e:
        return _err(e, 500, "initialization_error")

@shopee_bp.route('/api/shopee/export')
def api_export():
    try:
        c = _get_calc()
        formato = request.args.get("format", "csv").lower()
        filtros = {k: v for k, v in request.args.items() if k != "format" and v}
        if formato == "csv":
            csv_text = c.exportar_dados("csv", filtros or None)
            mem = io.BytesIO(csv_text.encode("utf-8-sig")); mem.seek(0)
            name = f"shopee_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            return send_file(mem, as_attachment=True, download_name=name, mimetype="text/csv")
        if formato == "json":
            json_text = c.exportar_dados("json", filtros or None)
            mem = io.BytesIO(json_text.encode("utf-8")); mem.seek(0)
            name = f"shopee_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            return send_file(mem, as_attachment=True, download_name=name, mimetype="application/json")
        return jsonify({"success": False, "message": "Formato não suportado"}), 400
    except Exception as e:
        return _err(e, 500, "initialization_error")
