# -*- coding: utf-8 -*-
"""
Importador de NF (XML/PDF/Imagem/Planilha) — Tela nova e independente.
NÃO altera telas existentes. Blueprint: bp_importador (url_prefix="/nfe/importador").

Rotas:
- GET  /nfe/importador/                   -> tela HTML (template global)
- GET  /nfe/importador/listar_arquivos    -> lista arquivos em pasta_xml/
- POST /nfe/importador/parse_path         -> parse de arquivo existente (em pasta_xml/) e move p/ processados
- POST /nfe/importador/upload_json        -> upload (XML/PDF/IMG) com retorno JSON; move p/ processados
- GET  /nfe/importador/modelo_planilha    -> baixa XLSX modelo
- POST /nfe/importador/upload_planilha_json -> importa XLSX/CSV, valida e retorna JSON p/ pré-visualizar
- POST /nfe/importador/salvar             -> grava ProdutoNF(status='Pendente') a partir da grade

Requisitos:
- extensions.py (raiz) exportando: db = SQLAlchemy()
- ProdutoNF em modulos/estoque/models.py
- (Opcional) ocr_support.py no raiz (OCR de PDF/Imagem); caso ausente, apenas aviso.
"""

from __future__ import annotations
import sys
import io
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Iterable

import pandas as pd
from flask import (
    Blueprint, render_template, request, jsonify, current_app,
    redirect, url_for, flash, send_file
)
from werkzeug.utils import secure_filename

# ---------------------------------------------------------------------
# Caminhos base do projeto
# ---------------------------------------------------------------------
THIS = Path(__file__).resolve()
# Estrutura presumida: <raiz>/modulos/nfe/routes_importador.py
ROOT = THIS.parents[2]   # sobe 2 níveis até a raiz do projeto
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ---------------------------------------------------------------------
# DB e Modelos
# ---------------------------------------------------------------------
try:
    from extensions import db
except Exception as e:
    raise ImportError("extensions.py (db = SQLAlchemy()) não encontrado no raiz.") from e

try:
    # ProdutoNF já está no seu módulo de estoque
    from modulos.estoque.models import ProdutoNF
except Exception as e:
    raise ImportError("ProdutoNF não encontrado em modulos/estoque/models.py") from e

# ---------------------------------------------------------------------
# OCR (opcional)
# ---------------------------------------------------------------------
_OCR_AVAILABLE = False
try:
    import ocr_support  # se existir no raiz
    _OCR_AVAILABLE = True
except Exception:
    _OCR_AVAILABLE = False

# ---------------------------------------------------------------------
# Constantes e Pastas
# ---------------------------------------------------------------------
ALLOWED_EXTENSIONS = {".xml", ".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"}

SCAN_DIR = ROOT / "pasta_xml"
SCAN_DIR.mkdir(parents=True, exist_ok=True)

PROCESSED_DIR = SCAN_DIR / "processados"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_UPLOAD_DIR = ROOT / "uploads" / "nfe_importador"
DEFAULT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Blueprint (DEFINIDO AQUI — evita 'Unresolved reference')
# ---------------------------------------------------------------------
bp_importador = Blueprint(
    "nfe_importador",
    __name__,
    url_prefix="/nfe/importador",
    # templates globais (sobe 2 níveis até raiz, onde há /templates)
    template_folder='../../templates'
)

# ---------------------------------------------------------------------
# Helpers de número/strings/tempo
# ---------------------------------------------------------------------
REQ_COLS = ["sku", "descricao", "unidade_compra", "quantidade", "valor_unitario"]
OPT_COLS = ["ipi_percentual", "ncm", "cfop"]
ALL_COLS = REQ_COLS + OPT_COLS

def _allowed_file(name: str) -> bool:
    return Path(name).suffix.lower() in ALLOWED_EXTENSIONS

def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _now_br() -> str:
    return datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")

def _to_float_br(v, default=None):
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return default
    # aceita "5%" "1.234,56" "1234,56"
    s = s.replace('%', '').strip()
    if s.count(',') == 1 and s.count('.') >= 1:
        s = s.replace('.', '').replace(',', '.')
    elif s.count(',') == 1 and s.count('.') == 0:
        s = s.replace(',', '.')
    try:
        return float(s)
    except Exception:
        return default

def _move_to_processed(file_path: Path) -> Path:
    """
    Move arquivo para pasta_xml/processados/ adicionando timestamp.
    Se falhar (permissão/bloqueio), retorna original.
    """
    try:
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        dst = PROCESSED_DIR / f"{file_path.stem}__{ts}{file_path.suffix.lower()}"
        file_path.rename(dst)
        return dst
    except Exception:
        return file_path

def _collect_files() -> List[Dict]:
    files = []
    if not SCAN_DIR.exists():
        return files
    for p in SCAN_DIR.iterdir():
        if p.is_file() and _allowed_file(p.name):
            st = p.stat()
            files.append({
                "name": p.name,
                "ext": p.suffix.lower(),
                "mtime": st.st_mtime,
                "size": st.st_size,
            })
    files.sort(key=lambda x: x["mtime"], reverse=True)
    return files

# ---------------------------------------------------------------------
# Parsers (XML + PDF/Imagem via OCR)
# ---------------------------------------------------------------------
def _parse_xml_items(xml_path: Path) -> Tuple[List[Dict], Dict]:
    """
    Retorna (itens, meta).
    itens: sku, descricao, unidade_compra, quantidade, valor_unitario, ipi_percentual, ncm, cfop
    meta : chave, emitente, numero, serie
    """
    import xml.etree.ElementTree as ET
    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    tree = ET.parse(str(xml_path))
    root = tree.getroot()

    itens: List[Dict] = []
    for det in root.findall('.//nfe:det', ns):
        prod = det.find('nfe:prod', ns)
        imposto = det.find('nfe:imposto', ns)
        if prod is None:
            continue

        cProd = (prod.findtext('nfe:cProd', '', ns) or '').strip()
        xProd = (prod.findtext('nfe:xProd', '', ns) or '').strip()
        uCom  = (prod.findtext('nfe:uCom', '', ns) or 'UN').strip().upper() or 'UN'
        qCom  = _to_float_br(prod.findtext('nfe:qCom', '', ns), 0) or 0.0
        vUn   = _to_float_br(prod.findtext('nfe:vUnCom','', ns), 0) or 0.0
        ncm   = (prod.findtext('nfe:NCM', '', ns) or '').strip()
        cfop  = (prod.findtext('nfe:CFOP','', ns) or '').strip()

        ipi_percent = 0.0
        if imposto is not None:
            ipi = imposto.find('nfe:IPI', ns)
            if ipi is not None:
                pIPI = ipi.find('.//nfe:pIPI', ns)
                if pIPI is not None and (pIPI.text or '').strip():
                    ipi_percent = _to_float_br(pIPI.text, 0) or 0.0

        itens.append(dict(
            sku=cProd or xProd or 'SEM-SKU',
            descricao=xProd,
            unidade_compra=uCom,
            quantidade=float(qCom),
            valor_unitario=float(vUn),
            ipi_percentual=float(ipi_percent),
            ncm=ncm,
            cfop=cfop
        ))

    meta: Dict = {}
    try:
        inf = root.find('.//nfe:infNFe', ns)
        if inf is not None:
            meta['chave'] = (inf.attrib.get('Id', '') or '').replace('NFe','').strip()
    except Exception:
        pass
    try:
        meta['emitente'] = (root.findtext('.//nfe:emit/nfe:xNome','',ns) or '').strip()
    except Exception:
        meta['emitente'] = ''
    meta['numero'] = (root.findtext('.//nfe:ide/nfe:nNF','',ns) or '').strip()
    meta['serie']  = (root.findtext('.//nfe:ide/nfe:serie','',ns) or '').strip()

    return itens, meta

def _parse_pdf_or_image(path: Path) -> Tuple[List[Dict], Dict, List[str]]:
    warnings: List[str] = []
    itens: List[Dict] = []
    meta: Dict = {}

    if not _OCR_AVAILABLE:
        return itens, meta, ["OCR indisponível (ocr_support.py não carregado)."]

    parsed = None
    try:
        if hasattr(ocr_support, "extract_nfe_items"):
            parsed = ocr_support.extract_nfe_items(str(path))
        elif hasattr(ocr_support, "extract_lines"):
            parsed = ocr_support.extract_lines(str(path))
        elif hasattr(ocr_support, "parse_pdf"):
            parsed = ocr_support.parse_pdf(str(path))
    except Exception as e:
        warnings.append(f"OCR falhou: {e!r}")

    raw_items = []
    if isinstance(parsed, dict):
        raw_items = parsed.get("items") or parsed.get("itens") or []
        meta = parsed.get("meta") or {}
    elif isinstance(parsed, list):
        raw_items = parsed

    for li in raw_items:
        sku = (li.get('sku') or li.get('codigo') or li.get('cProd') or '').strip()
        desc = (li.get('descricao') or li.get('xProd') or '').strip()
        und  = (li.get('unidade') or li.get('uCom') or 'UN').strip().upper()
        qtd  = _to_float_br(li.get('quantidade') or li.get('qCom') or 0, 0) or 0.0
        vu   = _to_float_br(li.get('valor_unitario') or li.get('vUnCom') or 0, 0) or 0.0
        ncm  = (li.get('ncm') or '').strip()
        cfop = (li.get('cfop') or '').strip()
        ipi  = _to_float_br(li.get('ipi') or li.get('ipi_percentual') or li.get('pIPI') or 0, 0) or 0.0

        if not sku and not desc:
            continue

        itens.append(dict(
            sku=sku or desc or 'SEM-SKU',
            descricao=desc,
            unidade_compra=und,
            quantidade=float(qtd),
            valor_unitario=float(vu),
            ipi_percentual=float(ipi),
            ncm=ncm,
            cfop=cfop
        ))

    return itens, meta, warnings

# ---------------------------------------------------------------------
# Leitura de Planilha
# ---------------------------------------------------------------------
def _read_any_table(file_storage) -> pd.DataFrame:
    name = (file_storage.filename or "").lower()
    data = file_storage.read()
    bio = io.BytesIO(data)
    if name.endswith(".csv"):
        df = pd.read_csv(bio, dtype=str, encoding="utf-8-sig")
    else:
        df = pd.read_excel(bio, dtype=str)
    # normaliza nomes de colunas
    df.columns = [c.strip().lower() for c in df.columns]
    missing = [c for c in REQ_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltam colunas obrigatórias: {missing}. Presentes: {list(df.columns)}")
    for c in OPT_COLS:
        if c not in df.columns:
            df[c] = None
    # remove linhas sem SKU
    df = df[~df["sku"].fillna("").astype(str).str.strip().eq("")]
    return df[ALL_COLS]

# ---------------------------------------------------------------------
# ROTAS
# ---------------------------------------------------------------------
@bp_importador.get("/")
def tela():
    """Renderiza a tela do importador (template global)."""
    return render_template("nfe/nfe_importador.html")

@bp_importador.get("/listar_arquivos")
def listar_arquivos():
    return jsonify(ok=True, files=_collect_files())

@bp_importador.post("/parse_path")
def parse_path():
    data = request.get_json(silent=True) or {}
    filename = (data.get("filename") or "").strip()
    if not filename:
        return jsonify(ok=False, message="Informe 'filename'."), 400
    if Path(filename).name != filename:
        return jsonify(ok=False, message="Nome inválido."), 400

    file_path = SCAN_DIR / filename
    if not file_path.exists():
        return jsonify(ok=False, message="Arquivo não encontrado em pasta_xml."), 404
    if not _allowed_file(file_path.name):
        return jsonify(ok=False, message="Extensão não suportada."), 400

    ext = file_path.suffix.lower()
    try:
        if ext == ".xml":
            itens, meta = _parse_xml_items(file_path)
            warnings = []
        else:
            itens, meta, warnings = _parse_pdf_or_image(file_path)
    except Exception as e:
        return jsonify(ok=False, message=f"Falha ao extrair itens: {e}"), 500

    if not itens:
        msg = "Nenhum item detectado."
        if warnings: msg += " " + " ".join(warnings)
        return jsonify(ok=False, message=msg, itens=[], meta={}, warnings=warnings), 200

    moved = _move_to_processed(file_path)
    return jsonify(ok=True, itens=itens, meta=meta, warnings=warnings, moved=Path(moved).name), 200

@bp_importador.post("/upload_json")
def upload_json():
    """Upload de XML/PDF/Imagem com retorno JSON e move para processados após parse."""
    f = request.files.get("arquivo")
    if not f or not f.filename:
        return jsonify(ok=False, message="Selecione um arquivo."), 400
    if not _allowed_file(f.filename):
        return jsonify(ok=False, message="Extensão não suportada."), 400

    filename = secure_filename(f.filename)
    upload_dir = Path(current_app.config.get("UPLOAD_FOLDER_NFE", DEFAULT_UPLOAD_DIR))
    upload_dir.mkdir(parents=True, exist_ok=True)
    file_path = upload_dir / filename
    f.save(str(file_path))

    ext = file_path.suffix.lower()
    try:
        if ext == ".xml":
            itens, meta = _parse_xml_items(file_path)
            warnings = []
        else:
            itens, meta, warnings = _parse_pdf_or_image(file_path)
    except Exception as e:
        return jsonify(ok=False, message=f"Falha ao extrair itens: {e}"), 500

    if not itens:
        msg = "Nenhum item detectado."
        if warnings: msg += " " + " ".join(warnings)
        return jsonify(ok=False, message=msg, itens=[], meta={}, warnings=warnings), 200

    moved = _move_to_processed(file_path)
    return jsonify(ok=True, itens=itens, meta=meta, warnings=warnings, moved=Path(moved).name), 200

@bp_importador.get("/modelo_planilha")
def modelo_planilha():
    """Baixa XLSX de modelo com colunas corretas + 1 linha de exemplo."""
    out = io.BytesIO()
    df = pd.DataFrame([{
        "sku": "ABC-123",
        "descricao": "Parafuso Inox 3mm",
        "unidade_compra": "CX",
        "quantidade": "1.000,00",
        "valor_unitario": "0,57",
        "ipi_percentual": "5",
        "ncm": "73181500",
        "cfop": "1102"
    }], columns=ALL_COLS)
    with pd.ExcelWriter(out) as writer:
        df.to_excel(writer, index=False, sheet_name="ImportacaoNF")
    out.seek(0)
    return send_file(
        out, as_attachment=True,
        download_name="modelo_importacao_nf.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

@bp_importador.post("/upload_planilha_json")
def upload_planilha_json():
    """Recebe XLSX/CSV (campo 'planilha'), valida e retorna itens normalizados para pré-visualização."""
    f = request.files.get("planilha")
    if not f or not f.filename:
        return jsonify(ok=False, message="Envie a planilha (XLSX/CSV)."), 400
    try:
        df = _read_any_table(f)
        itens: List[Dict] = []
        for _, row in df.iterrows():
            sku = (row["sku"] or "").strip()
            desc = (row["descricao"] or "").strip()
            und  = (row["unidade_compra"] or "UN").strip().upper()
            qtd  = _to_float_br(row["quantidade"], 0) or 0.0
            vlu  = _to_float_br(row["valor_unitario"], 0) or 0.0
            ipi  = _to_float_br(row["ipi_percentual"], 0) or 0.0
            if not sku or not und or qtd <= 0 or vlu <= 0:
                continue
            itens.append(dict(
                sku=sku, descricao=desc, unidade_compra=und,
                quantidade=float(qtd), valor_unitario=float(vlu),
                ipi_percentual=float(ipi),
                ncm=(row.get("ncm") or "").strip(),
                cfop=(row.get("cfop") or "").strip()
            ))
        if not itens:
            return jsonify(ok=False, message="Planilha sem linhas válidas."), 400
        return jsonify(ok=True, itens=itens, meta={"fonte":"planilha"}, warnings=[]), 200
    except Exception as e:
        return jsonify(ok=False, message=f"Erro ao ler planilha: {e}"), 400

@bp_importador.post("/salvar")
def salvar():
    """
    Salva as linhas visíveis na grade em ProdutoNF como documento 'MAN-<timestamp>'.
    """
    data = request.get_json(silent=True) or {}
    itens = data.get("itens") or []
    if not isinstance(itens, list) or len(itens) == 0:
        return jsonify(ok=False, message="Nenhum item para salvar."), 400

    chave = "MAN-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    dt_iso, dt_br = _now_iso(), _now_br()

    count = 0
    for it in itens:
        sku = (it.get("sku") or "").strip()
        und = (it.get("unidade_compra") or "UN").strip().upper()
        qtd = _to_float_br(it.get("quantidade"), 0) or 0.0
        vu  = _to_float_br(it.get("valor_unitario"), 0) or 0.0
        if not sku or not und or qtd <= 0 or vu <= 0:
            continue

        item = ProdutoNF(
            chave_nfe=chave,
            data_emissao=dt_iso,
            numero_nfe=chave,
            serie_nfe="MANUAL",
            fornecedor_nome="Entrada Manual",
            fornecedor_cnpj=None,
            fornecedor="Entrada Manual",

            produto_nome=(it.get("descricao") or sku),
            produto_sku=sku,
            ncm=(it.get("ncm") or "").strip(),
            cfop=(it.get("cfop") or "").strip(),

            unidade_compra=und,
            quantidade_compra=float(qtd),
            valor_unitario_compra=float(vu),

            ipi_percentual=_to_float_br(it.get("ipi_percentual"), 0) or 0.0,
            valor_ipi=None,

            valor_total_item=None,
            valor_total_compra=None,

            status="Pendente",
            data_emissao_iso=dt_iso,
            data_emissao_br=dt_br,
            data_criacao_iso=dt_iso,
            data_criacao_br=dt_br,
            data_criacao=dt_br,
        )
        db.session.add(item)
        count += 1

    if count == 0:
        return jsonify(ok=False, message="Nenhuma linha válida (SKU/Un/Qtd/Vlr obrigatórios)."), 400

    db.session.commit()
    return jsonify(ok=True, message=f"{count} itens salvos. Chave: {chave}", chave=chave), 200
