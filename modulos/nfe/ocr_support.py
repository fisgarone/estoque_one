# -*- coding: utf-8 -*-
"""
OCR utilities for NF‑e (não depende de rotas ou DB).
- Usa Tesseract instalado no sistema via subprocess.
- Usa PyMuPDF para ler PDFs.
- Converte texto OCR em itens para ProdutoNF.
"""

import os
import re
import subprocess
import tempfile
from datetime import datetime

import fitz  # PyMuPDF

# Regexes e unidades
_NUM = re.compile(r'^\d{1,3}(?:\.\d{3})*,\d{2}$|^\d+(?:[.,]\d+)?$')
_UN_SET = {"UN","UND","UNID","PC","PÇ","PCE","PCT","CX","CJ","PAR","KG","G","MG","L",
           "LT","ML","M","MT","RL","ROLO","FD","SC","SAC"}

def _br2f(s: str) -> float:
    s = (s or '').replace('\xa0', ' ').strip()
    return float(s.replace('.', '').replace(',', '.')) if s else 0.0

def _clean_line(l: str) -> str:
    l = l.replace('\xa0', ' ').replace('R$', '')
    l = re.sub(r'[‘’´`“”]', '', l)
    return re.sub(r'\s+', ' ', l).strip()

def _find_tesseract_cmd() -> str:
    candidates = [
        os.environ.get("TESSERACT_CMD"),
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        "/usr/bin/tesseract", "/usr/local/bin/tesseract",
        "/opt/homebrew/bin/tesseract", "/usr/local/opt/tesseract/bin/tesseract",
        "tesseract",
    ]
    for c in candidates:
        if c and (c == "tesseract" or os.path.exists(c)):
            return c
    raise RuntimeError("Tesseract não encontrado. Instale-o ou defina TESSERACT_CMD")

def _tesseract_ocr_image(path_img: str, lang: str = 'por+eng') -> str:
    cmd = _find_tesseract_cmd()
    textos = []
    for psm in ('6','4','11'):  # linhas, coluna única e layout disperso
        res = subprocess.run(
            [cmd, path_img, "stdout", "-l", lang,
             "--oem", "1", "--psm", psm,
             "-c", "preserve_interword_spaces=1"],
            capture_output=True, encoding='utf-8', errors='ignore'
        )
        if res.returncode == 0 and res.stdout:
            textos.append(res.stdout)
    return "\n".join(textos)

def _ocr_text_from_pdf(path_pdf: str, lang: str = 'por+eng') -> str:
    textos = []
    with fitz.open(path_pdf) as doc:
        for page in doc:
            # Tenta extrair texto vetorial
            t = page.get_text() or ""
            if t.strip():
                textos.append(t)
            # Rasteriza e roda Tesseract
            pix = page.get_pixmap(dpi=400)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(pix.tobytes("png"))
                tmp_path = tmp.name
            try:
                textos.append(_tesseract_ocr_image(tmp_path, lang=lang))
            finally:
                os.remove(tmp_path)
    return "\n".join(textos)

def _parse_items_from_text(txt: str):
    items = []
    linhas = [_clean_line(l) for l in txt.splitlines() if l.strip()]

    # Passo 1: Regex com SKU opcional
    pat = re.compile(
        r'^(?:(?P<sku>[A-Za-z0-9][A-Za-z0-9\-\./]*)\s+)?'
        r'(?P<nome>.+?)\s+'
        r'(?P<qtd>\d+(?:[.,]\d+)?)\s+'
        r'(?P<un>[A-Za-zÇç]{1,6})\s+'
        r'(?P<vunit>\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)\s+'
        r'(?P<vtotal>\d{1,3}(?:\.\d{3})*,\d{2}|\d+(?:[.,]\d+)?)\s*$'
    )

    seq = 0
    for ln in linhas:
        m = pat.search(ln)
        if not m:
            continue
        qtd = _br2f(m.group('qtd'))
        vun = _br2f(m.group('vunit'))
        vtot = _br2f(m.group('vtotal'))
        un  = (m.group('un') or '').upper().replace('Ç','C')[:6]
        if len(un) <= 6 and un not in _UN_SET:
            _UN_SET.add(un)
        # Checagem de sanidade
        if qtd <= 0 or vun < 0 or vtot < 0:
            continue
        if not (qtd*vun*0.4 <= vtot <= qtd*vun*2.5):
            continue
        sku = m.group('sku') or f"OCR{seq+1:04d}"
        seq += 1
        items.append({
            'sku': sku[:60],
            'nome': m.group('nome')[:120],
            'qtd': qtd,
            'un': un,
            'valor_unit': vun,
            'valor_total': vtot
        })
    if items:
        return items

    # Passo 2: Heurística – últimos dois números são valor unitário e total
    for ln in linhas:
        toks = ln.split()
        if len(toks) < 6:
            continue
        num_idx = [i for i,t in enumerate(toks) if _NUM.match(t)]
        if len(num_idx) < 2:
            continue
        i2,i1 = num_idx[-1], num_idx[-2]
        v1,v2 = _br2f(toks[i1]), _br2f(toks[i2])
        vunit,vtotal = (v1, v2) if v2 >= v1 else (v2, v1)

        # Procura a unidade (UN) antes dos números
        un_idx = None; un_val = None
        for j in range(min(i1,i2)-1, -1, -1):
            cand = toks[j].upper().replace('Ç','C')
            if 1 <= len(cand) <= 6 and cand.isalpha():
                un_idx,un_val = j,cand
                break
        if un_idx is None:
            continue
        # Procura quantidade antes da unidade
        qtd_idx = None
        for j in range(un_idx-1, -1, -1):
            if _NUM.match(toks[j]):
                qtd_idx = j
                break
        if qtd_idx is None:
            continue
        nome = " ".join(toks[:qtd_idx]).strip()
        if not nome:
            continue
        qtd = _br2f(toks[qtd_idx])
        if qtd <= 0 or not (qtd*vunit*0.4 <= vtotal <= qtd*vunit*2.5):
            continue
        seq += 1
        items.append({
            'sku': f"OCR{seq:04d}",
            'nome': nome[:120],
            'qtd': qtd,
            'un': (un_val or 'UN')[:6],
            'valor_unit': vunit,
            'valor_total': vtotal
        })
    return items

def importar_ocr_para_produtonf(filepath, db_session, ProdutoNFModel):
    """Executa OCR em PDF/imagem e grava itens no ProdutoNF."""
    ext = os.path.splitext(filepath)[1].lower()
    texto = _ocr_text_from_pdf(filepath, lang='por+eng') if ext == '.pdf' else _tesseract_ocr_image(filepath, lang='por+eng')
    itens = _parse_items_from_text(texto)
    if not itens:
        raise RuntimeError("OCR executado, mas não encontrei linhas de itens. Verifique qualidade/zoom.")
    chave = f"OCR-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    for it in itens:
        db_session.add(ProdutoNFModel(
            chave_nfe=chave,
            produto_nome=it['nome'],
            produto_sku=it['sku'],
            quantidade_compra=it['qtd'],
            unidade_compra=it['un'],
            valor_unitario_compra=it['valor_unit'],
            status='Pendente'
        ))
    db_session.commit()
    return chave, len(itens)
