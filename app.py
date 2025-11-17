# /app.py (MANTENDO SEU PADRÃO EXISTENTE)

import os

from flask import Flask

from extensions import db, babel


def create_app():
    """Cria e configura a instância principal da aplicação (ERP)."""
    app = Flask(__name__)

    # --- CONFIGURAÇÕES GLOBAIS ---
    basedir = os.path.abspath(os.path.dirname(__file__))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'fisgarone.db')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = 'sua-chave-secreta-muito-forte-para-o-erp'
    app.config['BABEL_DEFAULT_LOCALE'] = 'pt_BR'
    app.config['BABEL_DEFAULT_TIMEZONE'] = 'UTC'

    # --- INICIALIZAÇÃO DAS EXTENSÕES GLOBAIS ---
    db.init_app(app)
    babel.init_app(app)

    # --- REGISTRO DINÂMICO DE MÓDULOS (BLUEPRINTS) ---
    _register_blueprints(app)

    # --- INICIALIZAÇÃO DE SERVIÇOS ---
    _initialize_services(app)

    # --- CRIAÇÃO DO BANCO DE DADOS ---
    with app.app_context():
        db.create_all()

    return app


def _register_blueprints(app):
    """Registra todos os blueprints de forma organizada"""
    # --- SEUS BLUEPRINTS EXISTENTES ---
    from modulos.home.routes import home_bp
    app.register_blueprint(home_bp)

    from modulos.estoque.routes import estoque_bp
    app.register_blueprint(estoque_bp)

    # Este é o seu blueprint de IMPORTAÇÃO de NF-e, ele permanece igual
    from modulos.nfe.routes import nfe_bp
    app.register_blueprint(nfe_bp)

    # --- NOSSO NOVO BLUEPRINT DE EMISSÃO ---
    # Adicione estas duas linhas
    # from modulos.nfe.emissao_routes import nfe_emissao_bp
    # app.register_blueprint(nfe_emissao_bp)
    # -----------------------------------------

    from modulos.nfe.routes_importador import bp_importador
    app.register_blueprint(bp_importador)

    # Vendas - ML
    from modulos.vendas.ml.vendas_routes import ml_vendas_bp
    from modulos.vendas.ml.anuncios_routes import ml_anuncios_bp
    from modulos.vendas.ml.sync_routes import ml_sync_bp

    app.register_blueprint(ml_vendas_bp, url_prefix="/vendas/ml")
    app.register_blueprint(ml_anuncios_bp, url_prefix="/vendas/ml/anuncios")
    app.register_blueprint(ml_sync_bp, url_prefix="/vendas/ml")

    # Vendas - Outras Plataformas
    from modulos.vendas.shopee.shopee_blueprint import shopee_bp
    from modulos.vendas.dashboard_vendas import dashboard_vendas_bp

    app.register_blueprint(dashboard_vendas_bp, url_prefix='/vendas')
    app.register_blueprint(shopee_bp, url_prefix='/modulos/vendas/shopee')

    from modulos.vendas.ml.promocoes_ml import promocoes_ml_bp
    app.register_blueprint(promocoes_ml_bp, url_prefix='/vendas/ml')


def _initialize_services(app):
    """Inicializa serviços em background"""
    try:
        from modulos.vendas.ml.ml_sync_service import sync_service

        if os.getenv('ML_AUTO_SYNC', 'true').lower() == 'true':
            sync_service.start_auto_sync()
            print("✅ Sincronização automática ML iniciada")
    except Exception as e:
        print(f"⚠️  Serviço de sincronização ML não inicializado: {e}")


# --- PONTO DE ENTRADA DA APLICAÇÃO ---
if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, port=5000)
