# -*- coding: utf-8 -*-
"""
Mercado Libre Anúncios Fetcher
================================

Este script implementa um coletor assíncrono para recuperar anúncios de
contas do Mercado Libre. Ele foi pensado para servir como base para um
sistema de vendas e dashboard analítico, armazenando todos os campos
relevantes de cada anúncio em um banco SQLite. O script utiliza os
seguintes pontos da API do Mercado Libre:

* **Listagem de IDs de anúncios**: o endpoint
  ``/users/{user_id}/items/search`` retorna os IDs de anúncios de um
  vendedor. Esse recurso aceita filtros de busca e é apropriado para
  recuperar anúncios ativos ou pausados. Um cliente Ruby de terceiros
  demonstra seu uso em ``search_my_item_ids``, que chama
  ``get_request("/users/#{user_id}/items/search", filters)`` para
  retornar os identificadores:contentReference[oaicite:0]{index=0}.

* **Recuperação detalhada de anúncios**: o recurso ``/items`` permite
  recuperar dados completos de um ou vários anúncios. O wrapper Ruby
  ``get_items`` faz uma chamada ``get_request("/items", attrs.merge(...))``
  passando uma lista de IDs para obter todos os detalhes de uma vez:contentReference[oaicite:1]{index=1}.

Além desses recursos, o script implementa atualização automática de
``access_token`` através do ``refresh_token``. Isto é necessário porque
o token expira rapidamente; o wrapper Python demonstra como
atualizar tokens através de ``refresh_token``:contentReference[oaicite:2]{index=2}.

### Uso

1. **Preencha o arquivo `.env` com suas credenciais**. O script lê
   ``CLIENT_ID_*``, ``CLIENT_SECRET_*``, ``ACCESS_TOKEN_*``,
   ``REFRESH_TOKEN_*`` e ``SELLER_ID_*`` de cada conta. Esses nomes
   seguem o padrão usado no arquivo fornecido pelo usuário.
2. **Execute o script** com Python 3.9+. O comando básico é:

   ``python3 mercadolibre_ads_fetcher.py``

3. **Tabela de saída**: os resultados são gravados em
   ``C:\estoque_one\fisgarone.db`` dentro da tabela ``anuncios_ml``.

### Observações

* O script filtra anúncios criados ou atualizados nos últimos 60 dias
  comparando ``start_time`` e ``last_updated``.
* Os anúncios com status ``active`` ou ``paused`` são incluídos. Para
  outros status você pode ajustar o código.
* A recuperação utiliza paginação via ``scroll_id`` quando
  ``search_type=scan`` para contornar o limite de 1000 itens por
  consulta. Se ``scroll_id`` não estiver presente, a API ainda
  retorna até 50 resultados por consulta e um campo ``paging`` com
  ``offset`` e ``limit``:contentReference[oaicite:3]{index=3}.
* O script utiliza rotinas assíncronas para acelerar downloads,
  processando múltiplos itens em paralelo.

* A tabela ``anuncios_ml`` inclui colunas ``main_picture_url`` e
  ``picture_urls``. A primeira contém a URL segura (``secure_url``) da
  primeira imagem do anúncio (ou ``url`` caso ``secure_url`` não
  esteja disponível). A segunda armazena uma lista JSON com todas as
  URLs das imagens. Essas colunas são derivadas do campo
  ``pictures`` retornado pelo endpoint ``/items``.

* As datas ``start_time``, ``stop_time`` e ``last_updated`` são
  convertidas para o fuso horário de São Paulo e armazenadas no
  formato brasileiro ``dd/mm/aaaa HH:MM:SS``. O preço também é
  armazenado em duas formas: valor numérico original e uma coluna
  ``price_formatted`` com o símbolo ``R$`` e formatação com vírgula
  como separador decimal, quando ``currency_id`` for ``BRL``.

* **NOVO**: o script reconhece automaticamente **nomes de colunas em português**
  (padrão Brasil) ou o esquema antigo (inglês) e faz UPSERT de acordo, **sem alterar sua tabela**.

* **NOVO**: inclui **SKU** no nível do item e por variação:
  - `sku` (atributo `SELLER_SKU` no item),
  - `sku_interno` (`seller_custom_field`),
  - `skus_variacoes` (JSON com `id_variacao`, `sku`, `sku_interno`).

"""

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import sqlite3
import httpx

# Leitura manual de variáveis de ambiente do arquivo .env
def _read_env_file(path: str) -> Dict[str, str]:
    """Lê um arquivo .env simples (chave=valor).

    Esta função não suporta expansões complexas, mas é suficiente para
    arquivos no formato padrão. Comentários iniciados por ``#`` são
    ignorados. Espaços em branco ao redor das chaves ou valores são
    removidos.

    Args:
        path: caminho para o arquivo .env.

    Returns:
        Dicionário com as variáveis carregadas.
    """
    env: Dict[str, str] = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\'\"")
            env[key] = value
    return env


API_BASE_URL = "https://api.mercadolibre.com"


@dataclass
class AccountConfig:
    """Representa as credenciais e configurações de uma conta Mercado Libre."""

    client_id: str
    client_secret: str
    access_token: str
    refresh_token: str
    seller_id: str


def load_accounts_from_env() -> List[AccountConfig]:
    """Lê o arquivo .env e retorna uma lista de configurações de conta.

    Espera que as chaves sigam o padrão ``CLIENT_ID_NOME``,
    ``CLIENT_SECRET_NOME``, ``ACCESS_TOKEN_NOME``, ``REFRESH_TOKEN_NOME`` e
    ``SELLER_ID_NOME``. Todas as contas com ``CLIENT_ID_`` serão
    consideradas.

    Returns:
        Lista de `AccountConfig`.
    """
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    # Leitura manual do arquivo .env
    config = _read_env_file(env_path)
    accounts: List[AccountConfig] = []
    for key, client_id in config.items():
        if key.startswith("CLIENT_ID_"):
            suffix = key[len("CLIENT_ID_"):]
            client_secret = config.get(f"CLIENT_SECRET_{suffix}")
            access_token = config.get(f"ACCESS_TOKEN_{suffix}")
            refresh_token = config.get(f"REFRESH_TOKEN_{suffix}")
            seller_id = config.get(f"SELLER_ID_{suffix}")
            if client_secret and access_token and refresh_token and seller_id:
                accounts.append(
                    AccountConfig(
                        client_id=client_id,
                        client_secret=client_secret,
                        access_token=access_token.strip("'"),
                        refresh_token=refresh_token.strip("'"),
                        seller_id=seller_id,
                    )
                )
    return accounts


async def refresh_access_token(account: AccountConfig, client: httpx.AsyncClient) -> None:
    """Atualiza o access_token de uma conta utilizando o refresh_token.

    O endpoint de autenticação aceita um POST para
    ``/oauth/token`` com ``grant_type=refresh_token``, ``client_id``,
    ``client_secret`` e ``refresh_token``. A resposta contém um novo
    ``access_token`` e ``refresh_token``, conforme demonstrado na
    biblioteca Python oficial:contentReference[oaicite:4]{index=4}.

    Args:
        account: conta a atualizar.
        client: cliente HTTP compartilhado.
    """
    data = {
        "grant_type": "refresh_token",
        "client_id": account.client_id,
        "client_secret": account.client_secret,
        "refresh_token": account.refresh_token,
    }
    try:
        response = await client.post(
            f"{API_BASE_URL}/oauth/token",
            data=data,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        new_access_token = payload.get("access_token")
        new_refresh_token = payload.get("refresh_token")
        if new_access_token:
            account.access_token = new_access_token
        if new_refresh_token:
            account.refresh_token = new_refresh_token
    except Exception as exc:
        print(f"Falha ao atualizar token para conta {account.seller_id}: {exc}")


async def fetch_item_ids(
    account: AccountConfig,
    status: str,
    client: httpx.AsyncClient,
    created_after: datetime,
) -> List[str]:
    """Recupera IDs de anúncios de um vendedor com o status especificado.

    Utiliza o endpoint ``/users/{user_id}/items/search`` em modo de
    varredura (search_type=scan) para contornar o limite de 1000
    resultados. Em cada consulta inicial é retornado um ``scroll_id``
    (quando suportado) que deve ser enviado nas requisições seguintes
    para obter a próxima página. Se ``scroll_id`` não for retornado,
    utiliza o ``offset`` tradicional com 50 itens por chamada, até
    cobrir todos os resultados:contentReference[oaicite:5]{index=5}.

    Args:
        account: conta a ser consultada.
        status: "active" ou "paused".
        client: cliente HTTP a ser reutilizado.
        created_after: data mínima (UTC) para considerar anúncios.

    Returns:
        Lista de IDs de anúncios.
    """
    ids: List[str] = []
    params: Dict[str, Any] = {
        "status": status,
        "search_type": "scan",
        "limit": 50,
        "access_token": account.access_token,
    }
    url = f"{API_BASE_URL}/users/{account.seller_id}/items/search"
    scroll_id: Optional[str] = None
    offset = 0
    while True:
        query_params = params.copy()
        if scroll_id:
            query_params["scroll_id"] = scroll_id
            query_params.pop("offset", None)
        else:
            query_params["offset"] = offset
        resp = await client.get(url, params=query_params, timeout=60)
        if resp.status_code == 401:
            await refresh_access_token(account, client)
            query_params["access_token"] = account.access_token
            resp = await client.get(url, params=query_params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        for item in results:
            if isinstance(item, str):
                ids.append(item)
            elif isinstance(item, dict) and "id" in item:
                ids.append(str(item["id"]))
        scroll_id = data.get("scroll_id")
        paging = data.get("paging", {})
        total = paging.get("total", 0)
        if scroll_id:
            if not results:
                break
            continue
        else:
            offset += query_params.get("limit", 50)
            if offset >= total or not results:
                break
    return ids


async def fetch_items_details(
    account: AccountConfig,
    item_ids: List[str],
    client: httpx.AsyncClient,
) -> List[Dict[str, Any]]:
    """Recupera detalhes de vários itens de uma só vez.

    Utiliza o endpoint ``/items`` com o parâmetro ``ids`` para
    otimizar a chamada, conforme a biblioteca Ruby mostra:contentReference[oaicite:6]{index=6}. A API
    permite até 20 IDs por solicitação; portanto, dividimos a lista
    em lotes de 20.

    Args:
        account: conta usada para autenticação.
        item_ids: lista de IDs a consultar.
        client: cliente HTTP reutilizado.

    Returns:
        Lista de dicionários contendo as informações completas dos itens.
    """
    items: List[Dict[str, Any]] = []
    batch_size = 20
    for i in range(0, len(item_ids), batch_size):
        batch = item_ids[i : i + batch_size]
        ids_param = ",".join(batch)
        params = {
            "ids": ids_param,
            "access_token": account.access_token,
        }
        resp = await client.get(f"{API_BASE_URL}/items", params=params, timeout=60)
        if resp.status_code == 401:
            await refresh_access_token(account, client)
            params["access_token"] = account.access_token
            resp = await client.get(f"{API_BASE_URL}/items", params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for item_wrapper in data:
            body = item_wrapper.get("body")
            if body:
                items.append(body)
    return items


# =========================
# >>> NOVO BLOCO: Compatibilidade PT/EN de colunas + UPSERT dinâmico
# =========================

def _get_existing_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cols = []
    cur = conn.execute(f"PRAGMA table_info({table})")
    for cid, name, ctype, notnull, dflt, pk in cur.fetchall():
        cols.append(name)
    return cols

def _logical_to_physical_map(existing: List[str]) -> Dict[str, str]:
    """
    Mapa lógico -> nome físico de coluna, priorizando PT-BR.
    Se a coluna PT não existir, cai para o nome EN antigo.
    """
    pref = {
        # id
        "id": ("id_anuncio", "item_id"),
        "seller": ("id_vendedor", "seller_id"),
        "status": ("status", "status"),
        "title": ("titulo", "title"),
        "category": ("id_categoria", "category_id"),
        "price": ("preco", "price"),
        "price_fmt": ("preco_formatado", "price_formatted"),
        "currency": ("moeda", "currency_id"),
        "qty_avail": ("quantidade_disponivel", "available_quantity"),
        "qty_sold": ("quantidade_vendida", "sold_quantity"),
        "buying_mode": ("modo_compra", "buying_mode"),
        "listing_type": ("tipo_anuncio", "listing_type_id"),
        "condition": ("condicao", "condition"),
        "permalink": ("link_permanente", "permalink"),
        "thumb": ("miniatura", "thumbnail"),
        "main_pic": ("url_imagem_principal", "main_picture_url"),
        "pics_urls": ("urls_imagens", "picture_urls"),
        "start": ("inicio", "start_time"),
        "stop": ("fim", "stop_time"),
        "updated": ("atualizado_em", "last_updated"),
        "ship_mode": ("modo_envio", "shipping_mode"),
        "log_type": ("tipo_logistica", "logistic_type"),
        "store_id": ("id_loja_oficial", "official_store_id"),
        "vars": ("variacoes", "variations"),
        "attrs": ("atributos", "attributes"),
        "pics_json": ("imagens_json", "pictures"),
        "warranty": ("garantia", "warranty"),
        "video": ("id_video", "video_id"),
        "listing_src": ("origem_listagem", "listing_source"),
        "seller_nick": ("apelido_vendedor", "seller_nickname"),
        # SKUs
        "sku": ("sku", "sku"),  # PT e EN idênticos (se existir)
        "sku_interno": ("sku_interno", "sku_interno"),
        "skus_variacoes": ("skus_variacoes", "skus_variacoes"),
    }
    out: Dict[str, str] = {}
    s = set(existing)
    for logical, (pt, en) in pref.items():
        if pt in s:
            out[logical] = pt
        elif en in s:
            out[logical] = en
        else:
            # coluna não existe nessa base; ignora mais tarde
            out[logical] = None
    return out

def _get_seller_sku_from_attributes(attrs: Optional[List[Dict[str, Any]]]) -> Optional[str]:
    if not attrs:
        return None
    for a in attrs:
        if (a.get("id") == "SELLER_SKU") or (a.get("name") == "SELLER_SKU"):
            return a.get("value_name") or a.get("value_id") or a.get("value")
    return None

def _to_br_datetime_str(iso_utc: Optional[str]) -> Optional[str]:
    if not iso_utc:
        return None
    SAO_PAULO = ZoneInfo("America/Sao_Paulo")
    try:
        dt = datetime.fromisoformat(str(iso_utc).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_sp = dt.astimezone(SAO_PAULO)
        return dt_sp.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return iso_utc

def _brl(value: Optional[float], currency: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if currency != "BRL":
        return None
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def _parse_item_generic(item: Dict[str, Any]) -> Dict[str, Any]:
    """Extrai todos os campos e forma um dicionário lógico (não mapeado ao nome de coluna ainda)."""
    shipping = item.get("shipping", {}) or {}
    seller = item.get("seller_id")
    seller_nick = item.get("seller_nickname") or (item.get("seller") or {}).get("nickname")

    # Imagens
    pictures = item.get("pictures", []) or []
    urls: List[str] = []
    for pic in pictures:
        if isinstance(pic, dict):
            u = pic.get("secure_url") or pic.get("url")
            if u:
                urls.append(str(u))
    main_url: Optional[str] = urls[0] if urls else None

    # Datas BR
    start_time = item.get("start_time") or item.get("date_created")
    stop_time = item.get("stop_time")
    last_updated = item.get("last_updated") or item.get("last_update") or item.get("last_modified")

    inicio = _to_br_datetime_str(start_time)
    fim = _to_br_datetime_str(stop_time)
    atualizado_em = _to_br_datetime_str(last_updated)

    # Preço
    price_val = item.get("price")
    currency = item.get("currency_id")
    preco_fmt = _brl(price_val, currency)

    # Variações / Atributos
    variations = item.get("variations") or []
    attributes = item.get("attributes") or []

    # SKU
    sku_item = _get_seller_sku_from_attributes(attributes)
    sku_interno = item.get("seller_custom_field")
    skus_variacoes_list = []
    for v in variations:
        sku_var = _get_seller_sku_from_attributes(v.get("attributes")) \
                  or _get_seller_sku_from_attributes(v.get("attribute_combinations"))
        skus_variacoes_list.append({
            "id_variacao": v.get("id"),
            "sku": sku_var,
            "sku_interno": v.get("seller_custom_field")
        })

    logical = {
        "id": item.get("id"),
        "seller": seller,
        "status": item.get("status"),
        "title": item.get("title"),
        "category": item.get("category_id"),
        "price": price_val,
        "price_fmt": preco_fmt,
        "currency": currency,
        "qty_avail": item.get("available_quantity"),
        "qty_sold": item.get("sold_quantity"),
        "buying_mode": item.get("buying_mode"),
        "listing_type": item.get("listing_type_id"),
        "condition": item.get("condition"),
        "permalink": item.get("permalink"),
        "thumb": item.get("thumbnail"),
        "main_pic": main_url,
        "pics_urls": json.dumps(urls, ensure_ascii=False),
        "start": inicio,
        "stop": fim,
        "updated": atualizado_em,
        "ship_mode": shipping.get("mode"),
        "log_type": shipping.get("logistic_type"),
        "store_id": item.get("official_store_id"),
        "vars": json.dumps(variations, ensure_ascii=False),
        "attrs": json.dumps(attributes, ensure_ascii=False),
        "pics_json": json.dumps(pictures, ensure_ascii=False),
        "warranty": item.get("warranty"),
        "video": item.get("video_id"),
        "listing_src": item.get("listing_source"),
        "seller_nick": seller_nick,
        "sku": sku_item,
        "sku_interno": sku_interno,
        "skus_variacoes": json.dumps(skus_variacoes_list, ensure_ascii=False),
    }
    return logical

def _row_to_physical(logical_row: Dict[str, Any], colmap: Dict[str, Optional[str]]) -> Dict[str, Any]:
    """Converte o dicionário lógico para o dicionário com nomes físicos existentes na tabela."""
    out: Dict[str, Any] = {}
    for logical_key, value in logical_row.items():
        phys = colmap.get(logical_key)
        if phys:  # só inclui se a coluna existir nessa base
            out[phys] = value
    return out

def _build_upsert_sql(table: str, row_keys: List[str], pk: str) -> str:
    """
    Monta SQL de INSERT ... ON CONFLICT com as colunas presentes.
    row_keys: nomes físicos de colunas.
    pk: nome físico da PK (id_anuncio ou item_id).
    """
    cols = ", ".join(row_keys)
    vals = ", ".join([f":{k}" for k in row_keys])
    sets = ", ".join([f"{k}=excluded.{k}" for k in row_keys if k != pk])
    sql = f"""
    INSERT INTO {table} ({cols}) VALUES ({vals})
    ON CONFLICT({pk}) DO UPDATE SET
      {sets}
    """
    return sql

async def upsert_items_dynamic(conn: sqlite3.Connection, items: List[Dict[str, Any]]) -> None:
    """
    UPSERT dinâmico: detecta as colunas existentes (PT-BR ou EN),
    mapeia os campos e grava **sem alterar sua tabela**.
    """
    if not items:
        return
    existing = _get_existing_columns(conn, "anuncios_ml")
    colmap = _logical_to_physical_map(existing)

    # chave primária física
    pk = colmap["id"]  # id_anuncio OU item_id (um deles precisa existir)
    if not pk:
        print("Tabela anuncios_ml não tem id_anuncio nem item_id. Nada a fazer.")
        return

    # monta linhas físicas
    phys_rows: List[Dict[str, Any]] = []
    # quais chaves físicas serão usadas (ordem estável)
    keys_order: List[str] = []

    for item in items:
        logical = _parse_item_generic(item)
        phys = _row_to_physical(logical, colmap)
        phys_rows.append(phys)

    # define a ordem de colunas a partir da primeira linha
    if phys_rows:
        keys_order = list(phys_rows[0].keys())

    if not keys_order:
        print("Nenhuma coluna compatível encontrada para inserir.")
        return

    sql = _build_upsert_sql("anuncios_ml", keys_order, pk)

    def _exec_many():
        conn.executemany(sql, phys_rows)
        conn.commit()
    await asyncio.to_thread(_exec_many)

# =========================
# >>> FIM DO NOVO BLOCO
# =========================


async def upsert_items(conn: sqlite3.Connection, items: List[Dict[str, Any]]) -> None:
    """[LEGADO] Insere ou atualiza na tabela anúncios no esquema antigo (inglês).
    Mantido do seu original. **Não removido**.
    """
    query = """
        INSERT INTO anuncios_ml (
            item_id, seller_id, status, title, category_id, price,
            price_formatted,
            currency_id, available_quantity, sold_quantity, buying_mode,
            listing_type_id, condition, permalink, thumbnail,
            main_picture_url, picture_urls,
            start_time, stop_time, last_updated, shipping_mode,
            logistic_type, official_store_id, variations, attributes,
            pictures, warranty, video_id, listing_source, seller_nickname
        ) VALUES (
            :item_id, :seller_id, :status, :title, :category_id, :price,
            :price_formatted,
            :currency_id, :available_quantity, :sold_quantity, :buying_mode,
            :listing_type_id, :condition, :permalink, :thumbnail,
            :main_picture_url, :picture_urls,
            :start_time, :stop_time, :last_updated, :shipping_mode,
            :logistic_type, :official_store_id, :variations, :attributes,
            :pictures, :warranty, :video_id, :listing_source, :seller_nickname
        )
        ON CONFLICT(item_id) DO UPDATE SET
            status=excluded.status,
            title=excluded.title,
            category_id=excluded.category_id,
            price=excluded.price,
            price_formatted=excluded.price_formatted,
            currency_id=excluded.currency_id,
            available_quantity=excluded.available_quantity,
            sold_quantity=excluded.sold_quantity,
            buying_mode=excluded.buying_mode,
            listing_type_id=excluded.listing_type_id,
            condition=excluded.condition,
            permalink=excluded.permalink,
            thumbnail=excluded.thumbnail,
            main_picture_url=excluded.main_picture_url,
            picture_urls=excluded.picture_urls,
            start_time=excluded.start_time,
            stop_time=excluded.stop_time,
            last_updated=excluded.last_updated,
            shipping_mode=excluded.shipping_mode,
            logistic_type=excluded.logistic_type,
            official_store_id=excluded.official_store_id,
            variations=excluded.variations,
            attributes=excluded.attributes,
            pictures=excluded.pictures,
            warranty=excluded.warranty,
            video_id=excluded.video_id,
            listing_source=excluded.listing_source,
            seller_nickname=excluded.seller_nickname
    """
    def _exec_many():
        conn.executemany(query, [parse_item_to_row(it) for it in items])
        conn.commit()
    await asyncio.to_thread(_exec_many)


def parse_item_to_row(item: Dict[str, Any]) -> Dict[str, Any]:
    """[LEGADO] Converte o dicionário retornado pela API em dict no esquema antigo (inglês).
    Mantido do seu original. **Não removido**.
    """
    shipping = item.get("shipping", {}) or {}
    seller = item.get("seller_id")
    seller_nick = item.get("seller_nickname") or None
    # Extrai as imagens
    pictures = item.get("pictures", []) or []
    urls: List[str] = []
    for pic in pictures:
        if isinstance(pic, dict):
            url = pic.get("secure_url") or pic.get("url")
            if url:
                urls.append(str(url))
    main_url: Optional[str] = urls[0] if urls else None

    def format_datetime(dt_str: Optional[str]) -> Optional[str]:
        if not dt_str:
            return None
        try:
            dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
            sp_tz = ZoneInfo("America/Sao_Paulo")
            local_dt = dt.astimezone(sp_tz)
            return local_dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            return str(dt_str)

    price_val = item.get("price")
    currency = item.get("currency_id")
    formatted_price: Optional[str] = None
    if isinstance(price_val, (int, float)):
        try:
            if currency == "BRL":
                formatted_price = f"R$ {price_val:,.2f}"
                formatted_price = formatted_price.replace(",", "X").replace(".", ",").replace("X", ".")
            else:
                formatted_price = f"{price_val:.2f} {currency}" if currency else f"{price_val:.2f}"
        except Exception:
            formatted_price = None

    return {
        "item_id": item.get("id"),
        "seller_id": seller,
        "status": item.get("status"),
        "title": item.get("title"),
        "category_id": item.get("category_id"),
        "price": item.get("price"),
        "price_formatted": formatted_price,
        "currency_id": item.get("currency_id"),
        "available_quantity": item.get("available_quantity"),
        "sold_quantity": item.get("sold_quantity"),
        "buying_mode": item.get("buying_mode"),
        "listing_type_id": item.get("listing_type_id"),
        "condition": item.get("condition"),
        "permalink": item.get("permalink"),
        "thumbnail": item.get("thumbnail"),
        "main_picture_url": main_url,
        "picture_urls": json.dumps(urls),
        "start_time": format_datetime(item.get("start_time")),
        "stop_time": format_datetime(item.get("stop_time")),
        "last_updated": format_datetime(item.get("last_updated")),
        "shipping_mode": shipping.get("mode"),
        "logistic_type": shipping.get("logistic_type"),
        "official_store_id": item.get("official_store_id"),
        "variations": json.dumps(item.get("variations", [])),
        "attributes": json.dumps(item.get("attributes", [])),
        "pictures": json.dumps(pictures),
        "warranty": item.get("warranty"),
        "video_id": item.get("video_id"),
        "listing_source": item.get("listing_source"),
        "seller_nickname": seller_nick,
    }


async def ensure_table(conn: sqlite3.Connection) -> None:
    """[LEGADO] Cria a tabela anuncios_ml se não existir (esquema antigo).
    Mantido do seu original. **Não removido**.
    """
    def _create():
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS anuncios_ml (
                item_id TEXT PRIMARY KEY,
                seller_id INTEGER,
                status TEXT,
                title TEXT,
                category_id TEXT,
                price REAL,
                price_formatted TEXT,
                currency_id TEXT,
                available_quantity INTEGER,
                sold_quantity INTEGER,
                buying_mode TEXT,
                listing_type_id TEXT,
                condition TEXT,
                permalink TEXT,
                thumbnail TEXT,
                main_picture_url TEXT,
                picture_urls TEXT,
                start_time TEXT,
                stop_time TEXT,
                last_updated TEXT,
                shipping_mode TEXT,
                logistic_type TEXT,
                official_store_id INTEGER,
                variations TEXT,
                attributes TEXT,
                pictures TEXT,
                warranty TEXT,
                video_id TEXT,
                listing_source TEXT,
                seller_nickname TEXT
            )
            """
        )
        existing_cols = [row[1] for row in conn.execute("PRAGMA table_info(anuncios_ml)").fetchall()]
        for col, coltype in [
            ("main_picture_url", "TEXT"),
            ("picture_urls", "TEXT"),
            ("price_formatted", "TEXT"),
        ]:
            if col not in existing_cols:
                try:
                    conn.execute(f"ALTER TABLE anuncios_ml ADD COLUMN {col} {coltype}")
                except Exception:
                    pass
        conn.commit()
    await asyncio.to_thread(_create)


async def process_account(account: AccountConfig, conn: sqlite3.Connection) -> None:
    """Processa uma conta: busca IDs, recupera detalhes e insere no banco."""
    async with httpx.AsyncClient() as client:
        await refresh_access_token(account, client)
        created_after = datetime.now(timezone.utc) - timedelta(days=60)
        all_ids: List[str] = []
        for status in ["active", "paused"]:
            ids = await fetch_item_ids(account, status, client, created_after)
            all_ids.extend(ids)
        all_ids = list(dict.fromkeys(all_ids))
        items = await fetch_items_details(account, all_ids, client)

        # >>> NOVO: tente usar UPSERT dinâmico (PT/EN).
        # Se por algum motivo falhar (ex.: permissão/coluna ausente), cai no legado.
        try:
            await upsert_items_dynamic(conn, items)
            print(f"[DINÂMICO] Conta {account.seller_id}: {len(items)} anúncios processados (PT/EN compat).")
        except Exception as e:
            print(f"[DINÂMICO] Falhou ({e}). Usando UPSERT legado (EN).")
            # Filtragem para 60 dias no legado (igual ao seu original)
            filtered_items: List[Dict[str, Any]] = []
            for item in items:
                include = False
                for dt_str in [item.get("start_time"), item.get("last_updated")]:
                    try:
                        if dt_str:
                            dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
                            if dt >= created_after:
                                include = True
                                break
                    except Exception:
                        continue
                if include:
                    filtered_items.append(item)
            await upsert_items(conn, filtered_items)
            print(f"[LEGADO] Conta {account.seller_id}: {len(filtered_items)} anúncios gravados")


async def main() -> None:
    accounts = load_accounts_from_env()
    if not accounts:
        print("Nenhuma conta configurada no .env")
        sys.exit(1)
    db_path = os.path.join("C:\\estoque_one", "fisgarone.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    # cria conexão síncrona com sqlite3
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        await ensure_table(conn)
        for account in accounts:
            await process_account(account, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
