# Execute no terminal Python para resetar o banco:
import sqlite3
import os
DB_PATH = 'fisgarone.db'
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("✅ Banco de dados removido completamente!")