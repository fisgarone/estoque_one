# modulos/vendas/ml/anuncios_routes.py
# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
from .models import AnuncioML
from .ai_service import MLAIService
from flask import Response, abort
import requests
from urllib.parse import urlparse

ml_anuncios_bp = Blueprint("ml_anuncios", __name__)

anuncio_model = AnuncioML()
ai_service = MLAIService()  # tolerante a ausência de OPENAI_API_KEY

# ===========================
#  DASHBOARD ANÚNCIOS (HOME)
# ===========================
@ml_anuncios_bp.route("/")
def dashboard():
    stats = anuncio_model.get_dashboard_stats()
    return render_template("vendas/ml/dashboard_anuncios.html", stats=stats)

@ml_anuncios_bp.route("/api/dashboard-data")
def api_dashboard_data():
    stats = anuncio_model.get_dashboard_stats()
    return jsonify(stats)

# ===========================
#  LISTAGEM / DETALHES
# ===========================
@ml_anuncios_bp.route("/listar")
def listar_anuncios():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    offset = (page - 1) * per_page

    filters = {
        'status': request.args.get('status'),
        'categoria': request.args.get('categoria'),
        'preco_min': request.args.get('preco_min'),
        'preco_max': request.args.get('preco_max')
    }
    filters = {k: v for k, v in filters.items() if v}

    anuncios = anuncio_model.get_all(limit=per_page, offset=offset, filters=filters)
    return render_template('vendas/ml/anuncios.html', anuncios=anuncios, page=page)

@ml_anuncios_bp.route('/ver/<id_anuncio>')
def ver_anuncio(id_anuncio):
    anuncio = anuncio_model.get_by_id(id_anuncio)
    if not anuncio:
        flash('Anúncio não encontrado', 'error')
        return redirect(url_for('ml_anuncios.listar_anuncios'))
    return render_template('vendas/ml/anuncio_detalhes.html', anuncio=anuncio)

# modulos/vendas/ml/anuncios_routes.py

@ml_anuncios_bp.route('/api/recentes')
def api_recentes():
    """
    Retorna anúncios recentes com imagem da capa definida EXCLUSIVAMENTE pelas colunas:
      - url_imagem_principal (prioridade)
      - miniatura (fallback)
    """
    import os, sqlite3
    from flask import current_app, jsonify, request

    try:
        limit = int(request.args.get('limit', 10))
        limit = max(1, min(limit, 50))

        # Localiza o DB na raiz do projeto
        db1 = os.path.join(current_app.root_path, 'fisgarone.db')
        db2 = os.path.abspath(os.path.join(current_app.root_path, '..', 'fisgarone.db'))
        db_path = db1 if os.path.exists(db1) else db2
        if not os.path.exists(db_path):
            return jsonify({"error": f"Banco não encontrado em {db1} ou {db2}"}), 500

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            # Não supõe outras colunas; traz tudo e usa só as duas pedidas para imagem
            cur.execute("SELECT * FROM anuncios_ml ORDER BY rowid DESC LIMIT ?", (limit,))
            rows = cur.fetchall()
            col_names = [d[0] for d in cur.description]

            out = []
            for row in rows:
                item = {col: row[col] for col in col_names}

                url_capa = (row['url_imagem_principal'] if 'url_imagem_principal' in col_names else None) or ""
                mini     = (row['miniatura'] if 'miniatura' in col_names else None) or ""

                url_capa = url_capa.strip() if isinstance(url_capa, str) else ""
                mini     = mini.strip() if isinstance(mini, str) else ""

                imagem_capa_url = url_capa or mini or None
                item['imagem_capa_url'] = imagem_capa_url

                out.append(item)

            return jsonify(out)
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ===========================
#  CRUD
# ===========================
@ml_anuncios_bp.route('/novo', methods=['GET', 'POST'])
def novo_anuncio():
    if request.method == 'POST':
        data = request.get_json() if request.is_json else request.form.to_dict()
        if data.get('usar_ia'):
            data = ai_service.otimizar_anuncio(data)

        if anuncio_model.create(data):
            return jsonify({'success': True, 'message': 'Anúncio criado com sucesso'})
        return jsonify({'success': False, 'message': 'Erro ao criar anúncio'})

    return render_template('vendas/ml/anuncio_form.html', action='criar')

@ml_anuncios_bp.route('/editar/<id_anuncio>', methods=['GET', 'POST'])
def editar_anuncio(id_anuncio):
    anuncio = anuncio_model.get_by_id(id_anuncio)
    if not anuncio:
        return jsonify({'success': False, 'message': 'Anúncio não encontrado'})

    if request.method == 'POST':
        data = request.get_json() if request.is_json else request.form.to_dict()
        if data.get('usar_ia'):
            data = ai_service.otimizar_anuncio(data)

        if anuncio_model.update(id_anuncio, data):
            return jsonify({'success': True, 'message': 'Anúncio atualizado com sucesso'})
        return jsonify({'success': False, 'message': 'Erro ao atualizar anúncio'})

    return render_template('vendas/ml/anuncio_form.html', anuncio=anuncio, action='editar')

@ml_anuncios_bp.route('/clonar/<id_anuncio>', methods=['POST'])
def clonar_anuncio(id_anuncio):
    data = request.get_json() if request.is_json else {}
    new_id = anuncio_model.clone(id_anuncio, data)
    if new_id:
        return jsonify({'success': True, 'new_id': new_id, 'message': 'Anúncio clonado com sucesso'})
    return jsonify({'success': False, 'message': 'Erro ao clonar anúncio'})

@ml_anuncios_bp.route('/deletar/<id_anuncio>', methods=['DELETE'])
def deletar_anuncio(id_anuncio):
    if anuncio_model.delete(id_anuncio):
        return jsonify({'success': True, 'message': 'Anúncio removido com sucesso'})
    return jsonify({'success': False, 'message': 'Erro ao remover anúncio'})

# ===========================
#  IA
# ===========================
@ml_anuncios_bp.route('/api/sugestoes-ia', methods=['POST'])
def sugestoes_ia():
    data = request.get_json()
    sugestoes = ai_service.gerar_sugestoes(data)
    return jsonify(sugestoes)

@ml_anuncios_bp.route('/api/otimizar-titulo', methods=['POST'])
def otimizar_titulo():
    data = request.get_json()
    titulo_otimizado = ai_service.otimizar_titulo(data.get('titulo', ''), data.get('categoria', ''))
    return jsonify({'titulo_otimizado': titulo_otimizado})

@ml_anuncios_bp.route('/api/gerar-descricao', methods=['POST'])
def gerar_descricao():
    data = request.get_json()
    descricao = ai_service.gerar_descricao(data)
    return jsonify({'descricao': descricao})

@ml_anuncios_bp.route('/api/analisar-concorrencia', methods=['POST'])
def analisar_concorrencia():
    data = request.get_json()
    analise = ai_service.analisar_concorrencia(data.get('categoria'), data.get('palavra_chave'))
    return jsonify(analise)

# ===========================
#  RELATÓRIOS / CONFIGS
# ===========================
@ml_anuncios_bp.route('/relatorios')
def relatorios():
    return render_template('vendas/ml/relatorios.html')

@ml_anuncios_bp.route('/configuracoes')
def configuracoes():
    return render_template('vendas/ml/configuracoes.html')

@ml_anuncios_bp.route('/img')
def proxy_imagem():
    """
    Proxy seguro para exibir imagens externas no dashboard, evitando CORS/mixed-content/hotlink.
    Exemplo de uso: /vendas/ml/anuncios/img?u=https%3A%2F%2Fcdn...%2Ffoto.jpg
    """
    url = request.args.get('u', '').strip()
    if not url:
        return abort(400)

    # Permite apenas http/https
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        return abort(400)

    try:
        # Timeout curto para não travar; sem repassar cookies
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; MLDashboard/1.0)",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Referer": "",  # evita bloqueios por referer
        }
        r = requests.get(url, headers=headers, stream=True, timeout=5)
        if r.status_code != 200:
            return abort(404)

        # Descobrir Content-Type; fallback genérico
        ct = r.headers.get("Content-Type", "image/jpeg")
        # Opcional: cache curto no browser
        resp = Response(r.raw.read(), content_type=ct)
        resp.headers["Cache-Control"] = "public, max-age=3600"
        return resp
    except Exception:
        # 1x1 PNG transparente (sem bater em /static)
        transparent_png = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00"
            b"\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDAT"
            b"\x08\xd7c\x60\x00\x00\x00\x02\x00\x01\xe2!\xbc3\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        return Response(transparent_png, content_type="image/png")

    # ===========================
    #  PERFORMANCE DE VENDAS (API)
    # ===========================
@ml_anuncios_bp.route('/api/performance')
def api_performance():
    """
    Agrega vendas diárias a partir da tabela 'vendas_ml', usando:
      - Data:  "Data da Venda" (TEXT; aceita dd/mm/aaaa e ISO)
      - Vendas (unidades): SUM(Quantidade)
      - Receita (R$):      SUM(Quantidade * Preco Unitario)

    Parâmetros:
      - days: 7|30|90 (default 30)

    Retorno:
      { "labels": ["dd/mm", ...], "vendas": [int...], "receita": [float...] }
    """
    import os, sqlite3
    from datetime import date, datetime, timedelta
    from flask import current_app, jsonify, request

    try:
        days = int(request.args.get('days', 30))
        if days not in (7, 30, 90):
            days = 30

        end_dt   = date.today()
        start_dt = end_dt - timedelta(days=days-1)

        # Localiza o DB
        db1 = os.path.join(current_app.root_path, 'fisgarone.db')
        db2 = os.path.abspath(os.path.join(current_app.root_path, '..', 'fisgarone.db'))
        db_path = db1 if os.path.exists(db1) else db2
        if not os.path.exists(db_path):
            return jsonify({"labels": [], "vendas": [], "receita": [], "error": f"Banco não encontrado em {db1} ou {db2}"}), 200

        # Formatos aceitos para "Data da Venda"
        FORMATS = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M',
            '%d/%m/%Y %H:%M',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y %H:%M:%S',
        ]

        def parse_data(txt):
            if not isinstance(txt, str):
                return None
            s = txt.strip()
            for fmt in FORMATS:
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.date()
                except Exception:
                    continue
            return None

        # Lê somente as colunas necessárias (sem WHERE por formato)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            cur.execute('SELECT "Data da Venda", "Quantidade", "Preco Unitario" FROM vendas_ml')
            rows = cur.fetchall()
        finally:
            conn.close()

        # Agrega em memória
        by_day_units  = {}
        by_day_revenue= {}

        for r in rows:
            d  = parse_data(r['Data da Venda'])
            if d is None:
                continue
            if not (start_dt <= d <= end_dt):
                continue

            q  = r['Quantidade'] if r['Quantidade'] is not None else 0
            pu = r['Preco Unitario'] if r['Preco Unitario'] is not None else 0.0
            try:
                q  = int(q)
            except Exception:
                try:
                    q = int(float(q))
                except Exception:
                    q = 0
            try:
                pu = float(pu)
            except Exception:
                pu = 0.0

            by_day_units[d]   = by_day_units.get(d, 0) + q
            by_day_revenue[d] = round(by_day_revenue.get(d, 0.0) + q * pu, 2)

        # Serie completa dia a dia
        labels, vendas, receita = [], [], []
        for i in range(days):
            d = start_dt + timedelta(days=i)
            labels.append(d.strftime('%d/%m'))                       # BR
            vendas.append(int(by_day_units.get(d, 0)))
            receita.append(round(by_day_revenue.get(d, 0.0), 2))

        return jsonify({"labels": labels, "vendas": vendas, "receita": receita})
    except Exception as e:
        return jsonify({"labels": [], "vendas": [], "receita": [], "error": str(e)}), 200


