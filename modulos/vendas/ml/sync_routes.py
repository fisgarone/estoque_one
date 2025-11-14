# modulos/vendas/ml/sync_routes.py
from flask import Blueprint, jsonify, request
from datetime import datetime
import threading
from .ml_sync_service import sync_service, sync_status

# Blueprint para rotas de sincronização - NOME DIFERENTE para evitar conflito
ml_sync_bp = Blueprint("ml_sync", __name__)


@ml_sync_bp.route("/api/sync/status")
def get_sync_status():
    """Retorna status atual da sincronização"""
    try:
        service_status = sync_service.get_sync_status()

        return jsonify({
            'service_status': service_status,
            'sync_status': sync_status,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@ml_sync_bp.route("/api/sync/now", methods=['POST'])
def sync_now():
    """Força uma sincronização manual"""
    try:
        if sync_status['is_syncing']:
            return jsonify({
                'error': 'Sincronização já em andamento',
                'current_operation': sync_status['current_operation']
            }), 409

        # Parâmetros
        data = request.get_json() or {}
        days_back = data.get('days_back', 30)
        force_refresh = data.get('force_refresh', False)

        def sync_task():
            try:
                sync_status['is_syncing'] = True
                sync_status['current_operation'] = f'Sincronizando {days_back} dias'

                stats = sync_service.sync_all_accounts(days_back=days_back)

                sync_status['last_sync'] = datetime.now().isoformat()
                sync_status['last_stats'] = stats
                sync_status['is_syncing'] = False
                sync_status['current_operation'] = None

            except Exception as e:
                sync_status['is_syncing'] = False
                sync_status['current_operation'] = f'Erro: {str(e)}'

        # Executa em thread separada para não bloquear
        thread = threading.Thread(target=sync_task)
        thread.daemon = True
        thread.start()

        return jsonify({
            'message': 'Sincronização iniciada',
            'days_back': days_back,
            'force_refresh': force_refresh,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@ml_sync_bp.route("/api/sync/accounts")
def get_accounts_status():
    """Retorna status detalhado das contas"""
    try:
        status = sync_service.get_sync_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({"error": str(e)}), 500