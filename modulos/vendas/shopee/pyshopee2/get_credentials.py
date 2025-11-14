import os
from dotenv import load_dotenv

load_dotenv()  # Carregar as variáveis de ambiente do arquivo .env

def get_shopee_credentials(account_type):
    """Retorna as credenciais da Shopee com base no tipo de conta (comercial ou toys)."""
    if account_type == 'comercial':
        shop_id = os.getenv('SHOP_COMERCIAL_ID')
        access_token = os.getenv('SHOP_COMERCIAL_TOKEN')
    elif account_type == 'toys':
        shop_id = os.getenv('SHOP_TOYS_ID')
        access_token = os.getenv('SHOP_TOYS_TOKEN')
    else:
        raise ValueError("Tipo de conta desconhecido.")
    
    return shop_id, access_token
