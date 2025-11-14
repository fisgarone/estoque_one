import os
import hmac
import hashlib
import requests
import time
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()


def generate_signature(partner_id, path, timestamp, partner_key):
    """Gera assinatura HMAC-SHA256 para autenticação."""
    base_string = f"{partner_id}{path}{timestamp}"
    return hmac.new(
        partner_key.encode(),
        base_string.encode(),
        hashlib.sha256
    ).hexdigest()


def get_access_token_shop_level(shop_id, partner_id, partner_key, refresh_token):
    """Obtém novos tokens de acesso da API Shopee."""
    url = "https://partner.shopeemobile.com/api/v2/auth/access_token/get"
    timestamp = int(time.time())
    path = "/api/v2/auth/access_token/get"

    headers = {"Content-Type": "application/json"}
    params = {
        "partner_id": partner_id,
        "timestamp": timestamp,
        "sign": generate_signature(partner_id, path, timestamp, partner_key)
    }
    body = {
        "shop_id": int(shop_id),
        "refresh_token": refresh_token,
        "partner_id": int(partner_id)
    }

    response = requests.post(url, headers=headers, json=body, params=params, timeout=30)
    if response.status_code == 200:
        data = response.json()
        if "access_token" in data and "refresh_token" in data:
            return data["access_token"], data["refresh_token"]
    raise Exception(f"Erro na API: {response.text}")


def update_env_file(new_tokens):
    """Atualiza o arquivo .env mantendo comentários e formatação."""
    with open('.env', 'r+') as f:
        lines = f.readlines()
        f.seek(0)

        for line in lines:
            updated = False
            for account, tokens in new_tokens.items():
                if line.startswith(f"SHOPEE_ACCESS_TOKEN_{account}="):
                    f.write(f"SHOPEE_ACCESS_TOKEN_{account}={tokens['access_token']}\n")
                    updated = True
                elif line.startswith(f"SHOPEE_REFRESH_TOKEN_{account}="):
                    f.write(f"SHOPEE_REFRESH_TOKEN_{account}={tokens['refresh_token']}\n")
                    updated = True

            if not updated:
                f.write(line)
        f.truncate()


def main():
    accounts = {
        "COMERCIAL": {
            "partner_id": os.getenv("SHOPEE_PARTNER_ID_COMERCIAL"),
            "partner_key": os.getenv("SHOPEE_PARTNER_KEY_COMERCIAL"),
            "shop_id": os.getenv("SHOPEE_SHOP_ID_COMERCIAL"),
            "refresh_token": os.getenv("SHOPEE_REFRESH_TOKEN_COMERCIAL")
        },
        "TOYS": {
            "partner_id": os.getenv("SHOPEE_PARTNER_ID_TOYS"),
            "partner_key": os.getenv("SHOPEE_PARTNER_KEY_TOYS"),
            "shop_id": os.getenv("SHOPEE_SHOP_ID_TOYS"),
            "refresh_token": os.getenv("SHOPEE_REFRESH_TOKEN_TOYS")
        }
    }

    new_tokens = {}
    print("=== INÍCIO DA ATUALIZAÇÃO DE TOKENS ===")

    for account, config in accounts.items():
        try:
            print(f"\n▶ Processando conta {account}:")
            access_token, refresh_token = get_access_token_shop_level(
                config["shop_id"],
                config["partner_id"],
                config["partner_key"],
                config["refresh_token"]
            )

            new_tokens[account] = {
                "access_token": access_token,
                "refresh_token": refresh_token
            }

            print(f"✅ Novo ACCESS_TOKEN_{account}:")
            print(access_token)
            print(f"\n✅ Novo REFRESH_TOKEN_{account}:")
            print(refresh_token)

        except Exception as e:
            print(f"❌ Erro na conta {account}: {str(e)}")
            continue

    if new_tokens:
        update_env_file(new_tokens)
        print("\n=== RESUMO DA ATUALIZAÇÃO ===")
        for account, tokens in new_tokens.items():
            print(f"\nConta {account} atualizada:")
            print(f"ACCESS_TOKEN: {tokens['access_token'][:15]}...")
            print(f"REFRESH_TOKEN: {tokens['refresh_token'][:15]}...")

        print("\n⚠️ ATENÇÃO: Os tokens completos foram salvos no arquivo .env")
        print("=== ATUALIZAÇÃO CONCLUÍDA COM SUCESSO ===")
    else:
        print("\n❌ Nenhum token foi atualizado. Verifique os erros acima.")


if __name__ == "__main__":
    main()