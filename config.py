import sqlite3
from pathlib import Path

# Diretório raiz do projeto (onde está o app.py e o fisgarone.db)
BASE_DIR = Path(__file__).resolve().parent

# Caminho do banco oficial do ERP
DB_PATH = BASE_DIR / "fisgarone.db"


def get_conn():
    """
    Abre uma conexão com o banco principal do ERP (fisgarone.db).
    Sempre use esta função em vez de chamar sqlite3.connect direto.
    """
    return sqlite3.connect(DB_PATH)
