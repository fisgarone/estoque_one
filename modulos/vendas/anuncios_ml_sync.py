# modulos/vendas/anuncios_ml_sync_async.py
import os
import json
import asyncio
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

import aiohttp
import aiosqlite
from dotenv import load_dotenv

# =========================
# Configurações / Constantes
# =========================
load_dotenv()  # lê .env na raiz

# Raiz do projeto: ...\modulos\vendas\anuncios_ml_sync_async.py -> sobe 2 níveis
ROOT = Path(__file__).resolve().parents[2]
DB_PATH = os.environ.get("FISGARONE_DB", str(ROOT / "fisgarone.db"))
API_URL = os.environ.get("API_URL", "https://api.mercadolibre.com")

# Contas habilitadas (sufixos no .env)
CONTAS = ["TOYS", "COMERCIAL", "PESCA", "CAMPING"]

# Concorrência
ACCOUNT_CONCURRENCY = 4        # nº de contas em paralelo
DETAILS_CONCURRENCY = 20       # nº de itens detalhando em paralelo
PAGE_LIMIT = 50                 # varredura scan/scroll
HTTP_TIMEOUT = 30
REFRESH_MARGIN_SEC = 300        # renova 5 min antes
RETRY_MAX = 5                   # tentativas em 429/5xx
RETRY_BASE_SLEEP = 0.5          # backoff base

# =========================
# Datas (UTC-aware)
# =========================
def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def now_br() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def iso_in(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# =========================
# DDL
# =========================
DDL_ANUNCIOS = """
CREATE TABLE IF NOT EXISTS anuncios_canais (
  id INTEGER PRIMARY KEY,
  canal TEXT NOT NULL,
  conta TEXT NOT NULL,
  anuncio_id TEXT NOT NULL,
  sku_canal TEXT,
  titulo TEXT,
  status TEXT,
  preco REAL,
  preco_promocional REAL,
  moeda TEXT DEFAULT 'BRL',
  url TEXT,
  variacoes_json TEXT,
  atributos_json TEXT,
  imagens_json TEXT,
  estoque_canal REAL,
  politica_estoque TEXT DEFAULT 'centralizado',
  peso_gramas REAL,
  largura_cm REAL, altura_cm REAL, profundidade_cm REAL,
  data_publicacao_iso TEXT,
  ultima_captura_iso TEXT,
  ultima_captura_br  TEXT,
  raw_json TEXT,
  CONSTRAINT u_anuncio UNIQUE (canal, conta, anuncio_id)
);
"""

DDL_ANUNCIOS_IDX = """
CREATE INDEX IF NOT EXISTS idx_anuncios_lookup
  ON anuncios_canais (canal, conta, status);
"""

DDL_PRECOS_HIST = """
CREATE TABLE IF NOT EXISTS anuncios_precos_historico (
  id INTEGER PRIMARY KEY,
  canal TEXT NOT NULL,
  conta TEXT NOT NULL,
  anuncio_id TEXT NOT NULL,
  sku_canal TEXT,
  preco REAL NOT NULL,
  moeda TEXT DEFAULT 'BRL',
  capturado_em_iso TEXT DEFAULT (datetime('now'))
);
"""

DDL_PRECOS_HIST_IDX = """
CREATE INDEX IF NOT EXISTS idx_precos_hist_busca
  ON anuncios_precos_historico (canal, conta, anuncio_id, capturado_em_iso);
"""

DDL_TOKENS = """
CREATE TABLE IF NOT EXISTS ml_tokens (
  conta TEXT PRIMARY KEY,
  client_id TEXT NOT NULL,
  client_secret TEXT NOT NULL,
  seller_id TEXT NOT NULL,
  access_token TEXT,
  refresh_token TEXT NOT NULL,
  expires_at_iso TEXT,
  updated_at_iso TEXT
);
"""


async def ensure_schema(conn: aiosqlite.Connection) -> None:
    await conn.execute(DDL_ANUNCIOS)
    await conn.execute(DDL_ANUNCIOS_IDX)
    await conn.execute(DDL_PRECOS_HIST)
    await conn.execute(DDL_PRECOS_HIST_IDX)
    await conn.execute(DDL_TOKENS)
    await conn.commit()


# =========================
# .ENV → Config da Conta
# =========================
def load_conta_from_env(sufixo: str) -> Optional[Dict[str, str]]:
    def get(base: str) -> Optional[str]:
        return os.environ.get(f"{base}_{sufixo}")

    client_id = get("CLIENT_ID")
    client_secret = get("CLIENT_SECRET")
    seller_id = get("SELLER_ID")
    refresh_token = get("REFRESH_TOKEN")
    access_token = get("ACCESS_TOKEN")

    if not client_id or not client_secret or not seller_id or not refresh_token:
        return None

    return {
        "conta": sufixo,
        "client_id": client_id,
        "client_secret": client_secret,
        "seller_id": seller_id,
        "refresh_token": (refresh_token or "").strip("'\" "),
        "access_token": (access_token or "").strip("'\" ")
    }


async def seed_or_update_token_row(conn: aiosqlite.Connection, cfg: Dict[str, str]) -> None:
    async with conn.execute("SELECT conta FROM ml_tokens WHERE conta = ?", (cfg["conta"],)) as cur:
        row = await cur.fetchone()
    if not row:
        await conn.execute("""
            INSERT INTO ml_tokens (conta, client_id, client_secret, seller_id, access_token, refresh_token, expires_at_iso, updated_at_iso)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
        """, (cfg["conta"], cfg["client_id"], cfg["client_secret"], cfg["seller_id"],
              cfg.get("access_token") or None, cfg["refresh_token"], now_iso()))
        await conn.commit()
    else:
        await conn.execute("""
            UPDATE ml_tokens
               SET client_id=?, client_secret=?, seller_id=?
             WHERE conta=?
        """, (cfg["client_id"], cfg["client_secret"], cfg["seller_id"], cfg["conta"]))
        await conn.commit()


async def fetch_token_row(conn: aiosqlite.Connection, conta: str) -> Optional[Dict[str, Any]]:
    async with conn.execute("""
        SELECT conta, client_id, client_secret, seller_id, access_token, refresh_token, expires_at_iso
          FROM ml_tokens WHERE conta = ?
    """, (conta,)) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    keys = ["conta","client_id","client_secret","seller_id","access_token","refresh_token","expires_at_iso"]
    return dict(zip(keys, row))


def needs_refresh(expires_at_iso: Optional[str]) -> bool:
    if not expires_at_iso:
        return True
    try:
        exp = datetime.fromisoformat(expires_at_iso.replace("Z",""))
        return (exp - datetime.now(timezone.utc)).total_seconds() < REFRESH_MARGIN_SEC
    except Exception:
        return True


async def store_token_refresh(conn: aiosqlite.Connection, conta: str, access_token: str, refresh_token: str, expires_in: int) -> None:
    expires_at = iso_in(max(0, expires_in - 120))
    await conn.execute("""
        UPDATE ml_tokens
           SET access_token=?, refresh_token=?, expires_at_iso=?, updated_at_iso=?
         WHERE conta=?
    """, (access_token, refresh_token, expires_at, now_iso(), conta))
    await conn.commit()


async def oauth_refresh(session: aiohttp.ClientSession, client_id: str, client_secret: str, refresh_token: str) -> Tuple[str,str,int]:
    url = f"{API_URL}/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token
    }
    async with session.post(url, data=data, timeout=HTTP_TIMEOUT) as resp:
        txt = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"Refresh OAuth falhou: {resp.status} {txt}")
        payload = await resp.json()
    return payload["access_token"], payload.get("refresh_token", refresh_token), int(payload.get("expires_in", 21600))


class TokenProvider:
    """Gerencia access_token por conta com lock para refresh único."""
    def __init__(self, conn: aiosqlite.Connection, conta_cfgs: Dict[str, Dict[str,str]]):
        self.conn = conn
        self.cfgs = conta_cfgs
        self.locks: Dict[str, asyncio.Lock] = {c: asyncio.Lock() for c in conta_cfgs}

    async def get_access_token(self, session: aiohttp.ClientSession, conta: str) -> Tuple[str,str]:
        cfg = self.cfgs[conta]
        async with self.locks[conta]:
            row = await fetch_token_row(self.conn, conta)
            if not row:
                raise RuntimeError(f"Conta {conta} inexistente em ml_tokens")
            if row.get("access_token") and not needs_refresh(row.get("expires_at_iso")):
                return row["access_token"], row["seller_id"]

            at, rt, exp = await oauth_refresh(session, row["client_id"], row["client_secret"], row["refresh_token"])
            await store_token_refresh(self.conn, conta, at, rt, exp)
            return at, row["seller_id"]


# =========================
# HTTP util (retry/backoff)
# =========================
async def http_get_json(session: aiohttp.ClientSession, url: str, headers: Dict[str,str], params: Optional[Dict[str,Any]]=None) -> Any:
    delay = RETRY_BASE_SLEEP
    for attempt in range(1, RETRY_MAX+1):
        async with session.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT) as resp:
            if resp.status in (429, 500, 502, 503, 504):
                # backoff
                await asyncio.sleep(delay)
                delay *= 2
                continue
            txt = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"GET {url} -> {resp.status}: {txt}")
            return await resp.json()


# =========================
# Mercado Livre: scan/scroll + detalhes
# =========================
async def listar_item_ids(session: aiohttp.ClientSession, access_token: str, seller_id: str) -> List[str]:
    headers = {"Authorization": f"Bearer {access_token}"}
    base_url = f"{API_URL}/users/{seller_id}/items/search"

    item_ids: List[str] = []
    params = {"search_type": "scan", "limit": PAGE_LIMIT}
    data = await http_get_json(session, base_url, headers, params)
    results = data.get("results", []) or []
    scroll_id = data.get("scroll_id")
    item_ids.extend(results)

    while True:
        if not scroll_id:
            break
        await asyncio.sleep(0.12)
        data = await http_get_json(session, base_url, headers, {"search_type":"scan","limit":PAGE_LIMIT,"scroll_id":scroll_id})
        results = data.get("results", []) or []
        if not results:
            break
        item_ids.extend(results)
        scroll_id = data.get("scroll_id")
        if len(results) < PAGE_LIMIT:
            break

    return item_ids


def _parse_item(item: Dict[str,Any]) -> Dict[str,Any]:
    preco = item.get("price")
    moeda = item.get("currency_id") or "BRL"
    status = item.get("status")
    permalink = item.get("permalink")
    date_created = item.get("date_created")

    variacoes = item.get("variations") or []
    variacoes_json = json.dumps([
        {
            "id": v.get("id"),
            "sku": v.get("seller_custom_field"),
            "atributos": v.get("attribute_combinations", []),
            "preco": v.get("price") or preco,
            "disponivel": v.get("available_quantity"),
        } for v in variacoes
    ], ensure_ascii=False)

    atributos_json = json.dumps(item.get("attributes") or [], ensure_ascii=False)
    imagens_json = json.dumps([p.get("url") for p in (item.get("pictures") or [])], ensure_ascii=False)

    return {
        "anuncio_id": item.get("id"),
        "sku_canal": item.get("seller_custom_field"),
        "titulo": item.get("title"),
        "status": status,
        "preco": preco,
        "preco_promocional": None,
        "moeda": moeda,
        "url": permalink,
        "variacoes_json": variacoes_json,
        "atributos_json": atributos_json,
        "imagens_json": imagens_json,
        "estoque_canal": item.get("available_quantity"),
        "peso_gramas": None,
        "largura_cm": None,
        "altura_cm": None,
        "profundidade_cm": None,
        "data_publicacao_iso": date_created,
        "raw_json": json.dumps(item, ensure_ascii=False)
    }


async def carregar_item(session: aiohttp.ClientSession, access_token: str, item_id: str) -> Dict[str,Any]:
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{API_URL}/items/{item_id}"
    data = await http_get_json(session, url, headers)
    return _parse_item(data)


async def upsert_anuncio(conn: aiosqlite.Connection, conta: str, canal: str, reg: Dict[str,Any]) -> None:
    await conn.execute("""
    INSERT INTO anuncios_canais (
      canal, conta, anuncio_id, sku_canal, titulo, status, preco, preco_promocional, moeda, url,
      variacoes_json, atributos_json, imagens_json, estoque_canal, politica_estoque,
      peso_gramas, largura_cm, altura_cm, profundidade_cm,
      data_publicacao_iso, ultima_captura_iso, ultima_captura_br, raw_json
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
      ?, ?, ?, ?, 'centralizado',
      ?, ?, ?, ?,
      ?, ?, ?, ?
    )
    ON CONFLICT(canal, conta, anuncio_id) DO UPDATE SET
      sku_canal=excluded.sku_canal,
      titulo=excluded.titulo,
      status=excluded.status,
      preco=excluded.preco,
      preco_promocional=excluded.preco_promocional,
      moeda=excluded.moeda,
      url=excluded.url,
      variacoes_json=excluded.variacoes_json,
      atributos_json=excluded.atributos_json,
      imagens_json=excluded.imagens_json,
      estoque_canal=excluded.estoque_canal,
      peso_gramas=excluded.peso_gramas,
      largura_cm=excluded.largura_cm,
      altura_cm=excluded.altura_cm,
      profundidade_cm=excluded.profundidade_cm,
      data_publicacao_iso=excluded.data_publicacao_iso,
      ultima_captura_iso=excluded.ultima_captura_iso,
      ultima_captura_br=excluded.ultima_captura_br,
      raw_json=excluded.raw_json
    """, (
        canal, conta, reg["anuncio_id"], reg.get("sku_canal"), reg.get("titulo"), reg.get("status"),
        reg.get("preco"), reg.get("preco_promocional"), reg.get("moeda"), reg.get("url"),
        reg.get("variacoes_json"), reg.get("atributos_json"), reg.get("imagens_json"),
        reg.get("estoque_canal"),
        reg.get("peso_gramas"), reg.get("largura_cm"), reg.get("altura_cm"), reg.get("profundidade_cm"),
        reg.get("data_publicacao_iso"), now_iso(), now_br(), reg.get("raw_json")
    ))
    # histórico
    await conn.execute("""
        INSERT INTO anuncios_precos_historico (canal, conta, anuncio_id, sku_canal, preco, moeda)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ("ML", conta, reg["anuncio_id"], reg.get("sku_canal"), reg.get("preco"), reg.get("moeda")))
    await conn.commit()


async def sync_conta_ml(conn: aiosqlite.Connection, session: aiohttp.ClientSession, tokens: TokenProvider, conta: str) -> None:
    print(f"[ML][{conta}] Iniciando…")
    access_token, seller_id = await tokens.get_access_token(session, conta)

    # 1) Listar item_ids (scan/scroll)
    item_ids = await listar_item_ids(session, access_token, seller_id)
    total = len(item_ids)
    print(f"[ML][{conta}] Itens: {total}")

    # 2) Buscar detalhes em paralelo (com semáforo)
    sem = asyncio.Semaphore(DETAILS_CONCURRENCY)
    processados = 0

    async def worker(item_id: str):
        nonlocal processados, access_token
        async with sem:
            try:
                reg = await carregar_item(session, access_token, item_id)
            except Exception as e:
                # tentativa única de refresh se 401/aparente expiração
                msg = str(e).lower()
                if "401" in msg or "expired" in msg:
                    access_token, _ = await tokens.get_access_token(session, conta)
                    reg = await carregar_item(session, access_token, item_id)
                else:
                    raise
            await upsert_anuncio(conn, conta, "ML", reg)
            processados += 1
            if processados % 50 == 0:
                print(f"[ML][{conta}] {processados}/{total}…")

    tasks = [asyncio.create_task(worker(i)) for i in item_ids]
    # Executa e reporta erros sem travar o restante
    results = await asyncio.gather(*tasks, return_exceptions=True)
    erros = sum(1 for r in results if isinstance(r, Exception))
    if erros:
        print(f"[ML][{conta}] Finalizado com {erros} erros (itens ignorados).")
    print(f"[ML][{conta}] Concluído. Processados: {processados}/{total}")


# =========================
# Orquestração
# =========================
async def main_async(conta_especifica: Optional[str] = None):
    # log do DB
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    print(f"[DB] usando: {DB_PATH}")

    # Abre DB e garante schema
    async with aiosqlite.connect(DB_PATH) as conn:
        await ensure_schema(conn)

        # Carrega contas do .env e garante ml_tokens
        contas_cfg: Dict[str, Dict[str,str]] = {}
        for s in CONTAS:
            cfg = load_conta_from_env(s)
            if cfg:
                contas_cfg[s] = cfg
                await seed_or_update_token_row(conn, cfg)

        if not contas_cfg:
            raise RuntimeError("Nenhuma conta válida encontrada no .env.")

        # Filtro por conta, se solicitado
        alvos = [conta_especifica] if conta_especifica else list(contas_cfg.keys())
        alvos = [c for c in alvos if c in contas_cfg]
        if not alvos:
            raise RuntimeError("Conta solicitada não está configurada no .env.")

        tokens = TokenProvider(conn, contas_cfg)

        timeout = aiohttp.ClientTimeout(total=None, connect=HTTP_TIMEOUT)
        connector = aiohttp.TCPConnector(limit_per_host=DETAILS_CONCURRENCY * len(alvos))
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # roda contas em paralelo
            sem_accounts = asyncio.Semaphore(ACCOUNT_CONCURRENCY)

            async def run_conta(c: str):
                async with sem_accounts:
                    try:
                        await sync_conta_ml(conn, session, tokens, c)
                    except Exception as e:
                        print(f"[ML][{c}] ERRO geral: {e}")

            await asyncio.gather(*(run_conta(c) for c in alvos))

    print("[ML] Sincronização geral finalizada.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sincroniza anúncios do Mercado Livre (assíncrono, multi-conta).")
    parser.add_argument("--conta", help="Opcional: TOYS | COMERCIAL | PESCA | CAMPING", default=None)
    args = parser.parse_args()
    asyncio.run(main_async(args.conta))
