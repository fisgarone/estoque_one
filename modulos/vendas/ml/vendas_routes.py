# modulos/vendas/ml/vendas_routes.py
# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify, send_file
from datetime import date
import io, csv

from .ml_service import (
    get_filters, health, get_overview, get_trends, get_daily, get_divergences,
    list_orders, get_order_by_id, export_orders_csv, export_orders_xlsx,
    get_top_items, get_abc, abc_to_xlsx,
)

ml_vendas_bp = Blueprint("ml_vendas", __name__)

# ======================
#  TELA PRINCIPAL (DASH)
# ======================
@ml_vendas_bp.route("/")
def dashboard():
    # templates/vendas/ml/dashboard.html
    return render_template("vendas/ml/dashboard.html")

# ==========
#  SAÚDE API
# ==========
@ml_vendas_bp.route("/api/health")
def api_health():
    try:
        return jsonify(health())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==========
#  FILTROS
# ==========
@ml_vendas_bp.route("/api/filters")
def api_filters():
    try:
        # Usar a mesma função de conexão do ml_service
        from .ml_service import _conn

        con = _conn()
        cursor = con.cursor()

        # CORREÇÃO: Buscar contas únicas em maiúsculo e sem duplicatas
        cursor.execute(
            'SELECT DISTINCT UPPER("Conta") as conta FROM vendas_ml WHERE "Conta" IS NOT NULL ORDER BY conta')
        contas = [row[0] for row in cursor.fetchall() if row[0]]

        # Buscar status únicos
        cursor.execute('SELECT DISTINCT "Situacao" FROM vendas_ml WHERE "Situacao" IS NOT NULL ORDER BY "Situacao"')
        status = [row[0] for row in cursor.fetchall() if row[0]]

        con.close()

        return jsonify({
            'contas': contas,
            'status': status
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
# ==========================
#  OVERVIEW (KPIs do topo)
# ==========================
@ml_vendas_bp.route("/api/overview")
def api_overview():
    try:
        start  = request.args.get("start")
        end    = request.args.get("end")
        conta  = request.args.get("conta", "")
        status = request.args.get("status", "")
        q      = request.args.get("q", "")
        data = get_overview(start=start, end=end, conta=conta, status=status, q=q)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =====================
#  TRENDS (linha diária)
# =====================
@ml_vendas_bp.route("/api/trends")
def api_trends():
    try:
        start  = request.args.get("start")
        end    = request.args.get("end")
        conta  = request.args.get("conta", "")
        status = request.args.get("status", "")
        q      = request.args.get("q", "")
        data = get_trends(start=start, end=end, conta=conta, status=status, q=q)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ===========================
#  MINI-CARDS DIÁRIOS (grid)
# ===========================
@ml_vendas_bp.route("/api/daily")
def api_daily():
    try:
        start  = request.args.get("start")
        end    = request.args.get("end")
        conta  = request.args.get("conta", "")
        status = request.args.get("status", "")
        q      = request.args.get("q", "")
        data = get_daily(start=start, end=end, conta=conta, status=status, q=q)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =================
#  DIVERGÊNCIAS
# =================
@ml_vendas_bp.route("/api/divergences")
def api_divergences():
    try:
        start  = request.args.get("start")
        end    = request.args.get("end")
        conta  = request.args.get("conta", "")
        status = request.args.get("status", "")
        q      = request.args.get("q", "")
        data = get_divergences(start=start, end=end, conta=conta, status=status, q=q)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =============
#  TOP ITENS
# =============
@ml_vendas_bp.route("/api/top-items")
def api_top_items():
    try:
        n     = int(request.args.get("n", 10))
        modo  = (request.args.get("mode") or "lucro").lower()
        start = request.args.get("start")
        end   = request.args.get("end")
        conta = request.args.get("conta", "")
        q     = request.args.get("q", "")
        items = get_top_items(n=n, modo=modo, start=start, end=end, conta=conta, q=q)
        return jsonify(items)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@ml_vendas_bp.route("/api/top-items-compare")
def api_top_items_compare():
    return api_top_items()

# ==========
#  PEDIDOS
# ==========
@ml_vendas_bp.route("/api/orders")
def api_orders():
    try:
        page      = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 50))
        conta  = request.args.get("conta", "")
        status = request.args.get("status", "")
        start  = request.args.get("start")
        end    = request.args.get("end")
        q      = request.args.get("q", "")
        sku    = request.args.get("sku", "")
        mlb    = request.args.get("mlb", "")
        data = list_orders(start=start, end=end, conta=conta, status=status, q=q, sku=sku, mlb=mlb,
                           page=page, page_size=page_size)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@ml_vendas_bp.route("/api/order/<id_pedido>")
def api_order_by_id(id_pedido):
    try:
        data = get_order_by_id(id_pedido)
        if not data:
            return jsonify({"error": "Pedido não encontrado"}), 404
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==========
#  EXPORTAR
# ==========
@ml_vendas_bp.route("/export")
def export_orders():
    try:
        fmt = (request.args.get("format") or "csv").lower()
        start  = request.args.get("start")
        end    = request.args.get("end")
        conta  = request.args.get("conta", "")
        status = request.args.get("status", "")
        q      = request.args.get("q", "")
        sku    = request.args.get("sku", "")
        mlb    = request.args.get("mlb", "")

        if fmt == "xlsx":
            data = export_orders_xlsx(start=start, end=end, conta=conta, status=status, q=q, sku=sku, mlb=mlb)
            return send_file(data,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True, download_name="vendas_ml.xlsx")
        else:
            data = export_orders_csv(start=start, end=end, conta=conta, status=status, q=q, sku=sku, mlb=mlb)
            return send_file(data,
                mimetype="text/csv",
                as_attachment=True, download_name="vendas_ml.csv")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ==========
#  CURVA ABC
# ==========
@ml_vendas_bp.route("/abc")
def abc_view():
    # templates/vendas/ml/abc.html
    return render_template("vendas/ml/abc.html")

@ml_vendas_bp.route("/api/abc")
def api_abc():
    try:
        modo  = (request.args.get("mode") or "lucro").lower()
        start = request.args.get("start")
        end   = request.args.get("end")
        conta = request.args.get("conta") or ""
        q     = request.args.get("q") or ""
        fmt   = (request.args.get("format") or "").lower()

        rows = get_abc(modo=modo, start=start, end=end, conta=conta, q=q, include_delta=True)

        if fmt in ("csv", "xlsx"):
            if not start or not end:
                today = date.today()
                start = f"{today.year}-{str(today.month).zfill(2)}-01"
                end   = today.isoformat()
            fname = f"abc_{modo}_{start}_a_{end}.{fmt}"

            if fmt == "csv":
                mem = io.StringIO()
                w = csv.writer(mem, delimiter=';')
                w.writerow([
                    "Conta","SKU","MLB","Título","Unidades","Faturamento (R$)","Lucro (R$)",
                    "Margem (%)","Frete do Seller (R$)","Frete/Bruto (%)","Acumulado (%)","Classe","Evolução (%)"
                ])
                for r in rows:
                    w.writerow([
                        r.get("conta") or "", r.get("sku") or "", r.get("mlb") or "", r.get("titulo") or "",
                        int(r.get("unidades") or 0),
                        str(r.get("faturamento_rs") or 0).replace(".", ","),
                        str(r.get("lucro_rs") or 0).replace(".", ","),
                        f'{(r.get("margem_pct") or 0)*100:.2f}'.replace(".", ","),
                        str(r.get("frete_seller_rs") or 0).replace(".", ","),
                        f'{(r.get("frete_pct") or 0)*100:.2f}'.replace(".", ","),
                        f'{(r.get("pct_acum") or 0)*100:.2f}'.replace(".", ","),
                        r.get("classe") or "",
                        f'{(r.get("delta_pct") or 0)*100:.2f}'.replace(".", ","),
                    ])
                data = io.BytesIO(mem.getvalue().encode("utf-8-sig"))
                return send_file(data, mimetype="text/csv", as_attachment=True, download_name=fname)
            else:
                data = abc_to_xlsx(rows, titulo=f"Curva ABC ({modo}) {start} a {end}")
                return send_file(data,
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    as_attachment=True, download_name=fname)

        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@ml_vendas_bp.route("/api/ai/analyze-order", methods=['POST'])
def ai_analyze_order():
        """Análise de IA para pedidos específicos"""
        try:
            order_data = request.json.get('order_data', {})

            # Análise básica (pode ser expandida com ML real)
            analysis = {
                'insights': [],
                'recommendations': [],
                'risk_score': 0,
                'opportunity_score': 0
            }

            # Lógica de análise simples
            profit_margin = order_data.get('lucro_real_percent', 0)
            order_value = order_data.get('bruto_rs', 0)

            if profit_margin < 0.1:
                analysis['insights'].append({
                    'type': 'warning',
                    'title': 'Margem Crítica',
                    'description': f'Margem de {(profit_margin * 100):.1f}% abaixo do limite ideal',
                    'action': 'Revisar precificação urgente',
                    'priority': 'high'
                })
                analysis['risk_score'] = 8

            if order_value > 1000:
                analysis['insights'].append({
                    'type': 'success',
                    'title': 'Pedido Premium',
                    'description': f'Pedido de alto valor (R$ {order_value:.2f})',
                    'action': 'Priorizar e replicar',
                    'priority': 'medium'
                })
                analysis['opportunity_score'] = 9

            return jsonify(analysis)

        except Exception as e:
            return jsonify({"error": str(e)}), 500
