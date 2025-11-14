# /extensions.py
from flask_sqlalchemy import SQLAlchemy
from flask_babel import Babel  # Importe o Babel

db = SQLAlchemy()
babel = Babel()  # Crie a instância do Babel
