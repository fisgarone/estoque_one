import time
import hmac
import hashlib
import requests
import json

def get_token_shop_level():
    # Dados fornecidos
    code = "634f435847524f6d6d744a714b46524a"  # 🔁 Substitua pelo seu code real
    partner_id = 2009954
    partner_key = "shpk775675716e56724d56636f584e756f6264644b794e7654596272474b426b"
    shop_id = 383162027

    # === Configuração de autenticação
    timestamp = int(time.time())
    host = "https://partner.shopeemobile.com"
    path = "/api/v2/auth/token/get"

    # === Corpo da requisição
    body = {
        "code": code,
        "shop_id": shop_id,
        "partner_id": partner_id
    }

    # === Geração da assinatura
    base_string = f"{partner_id}{path}{timestamp}"
    sign = hmac.new(
        partner_key.encode(),
        base_string.encode(),
        hashlib.sha256
    ).hexdigest()

    # === URL com query
    url = f"{host}{path}?partner_id={partner_id}&timestamp={timestamp}&sign={sign}"

    headers = {"Content-Type": "application/json"}

    # === Requisição POST com JSON
    response = requests.post(url, json=body, headers=headers)
    print("Status:", response.status_code)
    print("Resposta:", response.json())

    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")
        return access_token, refresh_token
    else:
        return None, None

# Executar
access_token, refresh_token = get_token_shop_level()
