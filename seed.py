import random
import os
# AQUI ESTÁ A MUDANÇA PRINCIPAL: importamos timezone
from datetime import datetime, timedelta, timezone
from app import create_app, db
from modulos.estoque.models import Produto, MovimentacaoEstoque

# --- Configuração (sem alterações aqui) ---
NUM_PRODUTOS = 20
MOVIMENTACOES_POR_PRODUTO = 50
NOMES_PRODUTOS = [
    "Fone de Ouvido Bluetooth TWS", "Smartwatch X-2000", "Teclado Mecânico Gamer RGB",
    "Mouse Óptico Sem Fio", "Monitor LED 24' Full HD", "Cadeira Gamer Ergonômica",
    "Webcam 1080p com Microfone", "SSD NVMe 1TB", "Memória RAM DDR4 16GB",
    "Placa de Vídeo RTX 4060", "Processador Core i7", "Placa-Mãe B550M",
    "Gabinete Mid-Tower com Vidro", "Fonte de Alimentação 750W", "Cooler para CPU",
    "Kit de Fans RGB", "Mousepad Gamer Estendido", "Caixa de Som Bluetooth Portátil",
    "Carregador Portátil 20000mAh", "Hub USB-C 7 em 1"
]


def resetar_banco(app):
    """Apaga e recria as tabelas do banco de dados dentro do contexto da app."""
    with app.app_context():
        print("Resetando o banco de dados...")
        db.drop_all()
        db.create_all()
        print("Banco de dados resetado com sucesso.")


def criar_produtos():
    """Cria produtos de exemplo no banco de dados."""
    print(f"Criando {NUM_PRODUTOS} produtos de exemplo...")
    produtos_criados = []
    for i in range(NUM_PRODUTOS):
        nome_produto = NOMES_PRODUTOS[i % len(NOMES_PRODUTOS)]
        if Produto.query.filter_by(nome=nome_produto).first():
            nome_produto = f"{nome_produto} #{i + 1}"

        produto = Produto(
            nome=nome_produto,
            sku=f"SKU-{random.randint(10000, 99999)}-{i + 1}",
            custo_unitario=round(random.uniform(50.0, 1500.0), 2),
            ponto_reposicao=random.randint(15, 30),
            estoque_seguranca=random.randint(5, 10),
            lead_time_dias=random.randint(3, 15)
        )
        produtos_criados.append(produto)

    db.session.add_all(produtos_criados)
    db.session.commit()
    print(f"{len(produtos_criados)} produtos criados.")
    return produtos_criados


def criar_movimentacoes(produtos):
    """Cria movimentações de entrada e saída para os produtos."""
    print(f"Criando movimentações de estoque...")
    total_movs = 0
    for produto in produtos:
        quantidade_atual = 0
        for _ in range(MOVIMENTACOES_POR_PRODUTO):
            if random.random() < 0.4:
                tipo = 'entrada'
                quantidade = random.randint(20, 100)
                motivo = "Compra de fornecedor"
                quantidade_atual += quantidade
            else:
                tipo = 'saida'
                if quantidade_atual > 0:
                    quantidade = random.randint(1, min(15, quantidade_atual))
                    motivo = f"Venda #{random.randint(1000, 9999)}"
                    quantidade_atual -= quantidade
                else:
                    continue

            mov = MovimentacaoEstoque(
                produto_id=produto.id,
                quantidade=quantidade,
                tipo=tipo,
                motivo=motivo,
                # LINHA CORRIGIDA: Usando timezone.utc em vez de datetime.UTC
                data_movimentacao=(datetime.now(timezone.utc) - timedelta(days=random.randint(0, 90))).replace(
                    tzinfo=None)
            )
            db.session.add(mov)
            total_movs += 1

        produto.quantidade_atual = quantidade_atual

    db.session.commit()
    print(f"{total_movs} movimentações criadas e quantidades de produtos atualizadas.")


if __name__ == '__main__':
    app = create_app()

    with app.app_context():
        resetar_banco(app)
        lista_produtos = criar_produtos()
        criar_movimentacoes(lista_produtos)
        print("\n✅ Script de povoamento concluído com sucesso!")
        print(f"O banco de dados '{app.config['SQLALCHEMY_DATABASE_URI']}' foi populado.")
