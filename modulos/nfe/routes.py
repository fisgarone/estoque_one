# -*- coding: utf-8 -*-
# /modulos/nfe/routes.py  (NF-e com cards + upload + OCR)

import io
import os
import re
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime

from flask import Blueprint, render_template, flash, redirect, url_for, request, jsonify
from sqlalchemy import text

from extensions import db
from modulos.estoque.models import ProdutoNF

nfe_bp = Blueprint(
    'nfe',
    __name__,
    template_folder='../templates',
    url_prefix='/nfe'
)

# Pastas (mantém sua estrutura)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
PASTA_RAIZ_XML = os.path.join(BASE_DIR, 'pasta_xml')
PASTA_PROCESSADOS_XML = os.path.join(PASTA_RAIZ_XML, 'processados')
PASTA_UPLOADS = os.path.join(PASTA_RAIZ_XML, 'uploads')  # PDFs/Imagens recebidos
os.makedirs(PASTA_RAIZ_XML, exist_ok=True)
os.makedirs(PASTA_PROCESSADOS_XML, exist_ok=True)
os.makedirs(PASTA_UPLOADS, exist_ok=True)

ALLOWED_EXTS = {'.xml', '.pdf', '.png', '.jpg', '.jpeg', '.tif', '.tiff', '.bmp', '.webp'}


# -----------------------
# PARSER XML (seu fluxo)
# -----------------------
def ler_dados_nfe(caminho_arquivo):
    """Extrai dados detalhados do XML da NF-e, incluindo IPI, NCM e dados do fornecedor."""
    try:
        tree = ET.parse(caminho_arquivo)
        root = tree.getroot()
        ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
        infNFe = root.find('nfe:NFe/nfe:infNFe', ns)
        ide = infNFe.find('nfe:ide', ns)
        emit = infNFe.find('nfe:emit', ns)

        chave_nfe = (infNFe.get('Id') or '').replace('NFe', '')
        numero_nfe = ide.find('nfe:nNF', ns).text if ide is not None else None
        data_emissao_str = ide.find('nfe:dhEmi', ns).text if ide is not None else None
        data_emissao = None
        if data_emissao_str:
            try:
                data_emissao = datetime.fromisoformat(data_emissao_str)
            except Exception:
                data_emissao = None

        nome_fornecedor = emit.find('nfe:xNome', ns).text if emit is not None else None
        cnpj_fornecedor = emit.find('nfe:CNPJ', ns).text if emit is not None and emit.find('nfe:CNPJ',
                                                                                           ns) is not None else None

        itens = []
        for det in root.findall('nfe:NFe/nfe:infNFe/nfe:det', ns):
            prod = det.find('nfe:prod', ns)
            imposto = det.find('nfe:imposto', ns)
            ipi = imposto.find('nfe:IPI/nfe:IPITrib', ns) if (
                    imposto is not None and imposto.find('nfe:IPI', ns) is not None) else None

            valor_ipi = float(ipi.find('nfe:vIPI', ns).text) if ipi is not None and ipi.find('nfe:vIPI',
                                                                                             ns) is not None else 0.0
            ncm = prod.find('nfe:NCM', ns).text if prod is not None and prod.find('nfe:NCM', ns) is not None else None
            cest = prod.find('nfe:CEST', ns).text if prod is not None and prod.find('nfe:CEST',
                                                                                    ns) is not None else None

            itens.append({
                'nome': (prod.find('nfe:xProd', ns).text if prod is not None else '').strip(),
                'sku': (prod.find('nfe:cProd', ns).text if prod is not None else '').strip(),
                'qtd': float(prod.find('nfe:qCom', ns).text) if prod is not None and prod.find('nfe:qCom',
                                                                                               ns) is not None else 0.0,
                'un': (prod.find('nfe:uCom', ns).text if prod is not None else 'UN'),
                'valor_unit': float(prod.find('nfe:vUnCom', ns).text) if prod is not None and prod.find('nfe:vUnCom',
                                                                                                        ns) is not None else 0.0,
                'valor_total': float(prod.find('nfe:vProd', ns).text) if prod is not None and prod.find('nfe:vProd',
                                                                                                        ns) is not None else 0.0,
                'valor_ipi': valor_ipi,
                'ncm': ncm,
                'cest': cest
            })

        return {
            'chave': chave_nfe,
            'numero': numero_nfe,
            'data': data_emissao,
            'fornecedor': nome_fornecedor,
            'cnpj_fornecedor': cnpj_fornecedor,
            'itens': itens,
            'nome_arquivo': os.path.basename(caminho_arquivo)
        }
    except Exception as e:
        print(f"[NFE][XML][ERRO] {os.path.basename(caminho_arquivo)}: {e}")
        return None


# -----------------------
# OCR (PDF/Imagem) – grava direto em ProdutoNF
# -----------------------
def _import_ocr_deps():
    try:
        import pytesseract  # noqa
        from PIL import Image  # noqa
    except Exception as e:
        raise RuntimeError("OCR indisponível. Instale dependências: tesseract-ocr, pytesseract e Pillow.") from e
    try:
        import fitz  # PyMuPDF
        return 'pymupdf'
    except Exception:
        try:
            from pdf2image import convert_from_path  # noqa
            return 'pdf2image'
        except Exception as e:
            raise RuntimeError("Para PDF, instale 'pymupdf' (recomendado) ou 'pdf2image' + 'poppler'.") from e


def _ocr_text_from_image(path):
    import pytesseract
    from PIL import Image
    img = Image.open(path)
    return pytesseract.image_to_string(img, lang='por')


def _ocr_text_from_pdf(path, pdf_mode):
    if pdf_mode == 'pymupdf':
        import fitz
        text_all = []
        with fitz.open(path) as doc:
            for page in doc:
                text = page.get_text() or ""
                if text.strip():
                    text_all.append(text)
                # fallback OCR
                pix = page.get_pixmap(dpi=250)
                img_bytes = pix.tobytes("png")
                try:
                    import pytesseract
                    from PIL import Image
                    im = Image.open(io.BytesIO(img_bytes))
                    ocr = pytesseract.image_to_string(im, lang='por')
                    if ocr.strip():
                        text_all.append(ocr)
                except Exception:
                    pass
        return "\n".join(text_all)
    else:
        from pdf2image import convert_from_path
        import pytesseract
        pages = convert_from_path(path, dpi=250)
        text_all = []
        for im in pages:
            text_all.append(pytesseract.image_to_string(im, lang='por'))
        return "\n".join(text_all)


def _parse_items_from_text(txt):
    """
    Heurística simples para extrair linhas de item de um DANFE impresso.
    Procura padrões: código (alfa-num), descrição, qtd, un, v.unit, v.total.
    """
    items = []
    lines = [re.sub(r'\s+', ' ', l).strip() for l in txt.splitlines() if l.strip()]
    pattern = re.compile(
        r'(?P<sku>[A-Za-z0-9\-\./]+)\s+'
        r'(?P<nome>[A-Za-z0-9ÁÉÍÓÚÂÊÔÃÕÇ áéíóúâêôãõç\-\./,]+?)\s+'
        r'(?P<qtd>\d+[.,]?\d*)\s+'
        r'(?P<un>[A-Za-z]{1,4})\s+'
        r'(?P<vunit>\d{1,3}(?:\.\d{3})*,\d{2}|\d+[.,]?\d*)\s+'
        r'(?P<vtotal>\d{1,3}(?:\.\d{3})*,\d{2}|\d+[.,]?\d*)$'
    )

    def br2float(s):
        s = s.replace('.', '').replace(',', '.')
        try:
            return float(s)
        except:
            return 0.0

    for ln in lines:
        m = pattern.search(ln)
        if m:
            sku = (m.group('sku') or '').strip()
            if not sku:
                # NÃO inventa SKU (regra sua): ignora linhas sem código.
                continue
            items.append({
                'sku': sku[:60],
                'nome': m.group('nome')[:120],
                'qtd': br2float(m.group('qtd')),
                'un': m.group('un')[:6].upper(),
                'valor_unit': br2float(m.group('vunit')),
                'valor_total': br2float(m.group('vtotal')),
            })
    return items


def importar_ocr_para_produtonf(filepath):
    pdf_mode = _import_ocr_deps()
    ext = os.path.splitext(filepath)[1].lower()
    txt = _ocr_text_from_pdf(filepath, pdf_mode) if ext == '.pdf' else _ocr_text_from_image(filepath)

    itens = _parse_items_from_text(txt)
    if not itens:
        raise RuntimeError("OCR executado, mas não encontrei linhas de itens no DANFE. Ajuste a foto/qualidade.")

    chave = f"OCR-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    for it in itens:
        novo = ProdutoNF(
            chave_nfe=chave,
            produto_nome=it['nome'],
            produto_sku=it['sku'],
            quantidade_compra=it['qtd'],
            unidade_compra=it['un'],
            valor_unitario_compra=it['valor_unit'],
            status='Pendente'
        )
        db.session.add(novo)
    db.session.commit()
    return chave, len(itens)


# -----------------------
# CARDS / KPIs
# -----------------------
@nfe_bp.get('/api/cards')
def api_cards():
    xml_na_pasta = len([f for f in os.listdir(PASTA_RAIZ_XML)
                        if os.path.isfile(os.path.join(PASTA_RAIZ_XML, f)) and f.lower().endswith('.xml')])

    row = db.session.execute(text("""
                                  SELECT COALESCE((SELECT COUNT(DISTINCT chave_nfe)
                                                   FROM produto_nf
                                                   WHERE status = 'Pendente'), 0)                                              AS notas_pendentes,
                                         COALESCE((SELECT COUNT(1) FROM produto_nf WHERE status = 'Pendente'),
                                                  0)                                                                           AS itens_pendentes,
                                         COALESCE((SELECT SUM(quantidade_compra * valor_unitario_compra)
                                                   FROM produto_nf
                                                   WHERE status = 'Pendente'),
                                                  0.0)                                                                         AS valor_pendente,
                                         COALESCE((SELECT COUNT(1)
                                                   FROM produto_nf
                                                   WHERE status = 'Processado' AND date (data_criacao) >= date ('now',
                                                                                                                '-7 day')), 0) AS itens_proc_7d
                                  """)).fetchone()

    def brl(v): return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    return jsonify({
        "xml_na_pasta": xml_na_pasta,
        "notas_pendentes": int(row.notas_pendentes or 0),
        "itens_pendentes": int(row.itens_pendentes or 0),
        "valor_pendente": brl(row.valor_pendente or 0.0),
        "itens_processados_7d": int(row.itens_proc_7d or 0),
    })


# -----------------------
# PAINEL
# -----------------------
@nfe_bp.route('/painel')
def painel_nfe():
    arquivos_xml = [f for f in os.listdir(PASTA_RAIZ_XML)
                    if os.path.isfile(os.path.join(PASTA_RAIZ_XML, f)) and f.lower().endswith('.xml')]

    notas_fiscais = []
    for nome_arquivo in arquivos_xml:
        caminho_completo = os.path.join(PASTA_RAIZ_XML, nome_arquivo)
        dados_nfe = ler_dados_nfe(caminho_completo)
        if dados_nfe:
            item_processado = ProdutoNF.query.filter_by(chave_nfe=dados_nfe['chave']).first()
            dados_nfe['status'] = 'Processado' if item_processado else 'Pendente'
            notas_fiscais.append(dados_nfe)

    notas_fiscais.sort(key=lambda x: (x.get('data') or datetime.min), reverse=True)
    return render_template('nfe/painel_nfe.html', notas=notas_fiscais)


# -----------------------
# PROCESSAR XML (seu fluxo original, agora batendo com o model)
# -----------------------
@nfe_bp.route('/processar-xml/<nome_arquivo>', methods=['POST'])
def processar_xml(nome_arquivo):
    """
    Processa um XML, insere seus itens na tabela produto_nf e move o arquivo.
    Mantém TODOS os campos ricos que você extrai.
    """
    caminho_origem = os.path.join(PASTA_RAIZ_XML, nome_arquivo)
    if not os.path.exists(caminho_origem):
        flash(f'Arquivo {nome_arquivo} não encontrado.', 'error')
        return redirect(url_for('nfe.painel_nfe'))

    dados_nfe = ler_dados_nfe(caminho_origem)
    if not dados_nfe:
        flash(f'Erro ao ler o arquivo XML {nome_arquivo}. Verifique sua estrutura.', 'error')
        return redirect(url_for('nfe.painel_nfe'))

    try:
        itens_adicionados = 0
        for item in dados_nfe['itens']:
            existe = ProdutoNF.query.filter_by(
                chave_nfe=dados_nfe['chave'], produto_sku=item['sku']
            ).first()

            if not existe:
                novo = ProdutoNF(
                    chave_nfe=dados_nfe['chave'],
                    fornecedor=dados_nfe.get('fornecedor'),
                    fornecedor_cnpj=dados_nfe.get('cnpj_fornecedor'),
                    produto_nome=item.get('nome'),
                    produto_sku=item.get('sku'),
                    ncm=item.get('ncm'),
                    cest=item.get('cest'),
                    quantidade_compra=item.get('qtd'),
                    unidade_compra=item.get('un'),
                    valor_unitario_compra=item.get('valor_unit'),
                    valor_total_compra=item.get('valor_total'),
                    valor_ipi=item.get('valor_ipi'),
                    numero_nfe=dados_nfe.get('numero'),
                    data_emissao=(dados_nfe.get('data').strftime('%Y-%m-%d') if dados_nfe.get('data') else None),
                    status='Pendente'
                )
                db.session.add(novo)
                itens_adicionados += 1

        if itens_adicionados > 0:
            db.session.commit()
            caminho_destino = os.path.join(PASTA_PROCESSADOS_XML, nome_arquivo)
            shutil.move(caminho_origem, caminho_destino)
            flash(f'{itens_adicionados} item(ns) foram enviados para a fila de processamento do estoque.', 'success')
        else:
            caminho_destino = os.path.join(PASTA_PROCESSADOS_XML, nome_arquivo)
            shutil.move(caminho_origem, caminho_destino)
            flash('Todos os itens desta NF-e já haviam sido enviados. Arquivo movido.', 'info')

    except Exception as e:
        db.session.rollback()
        print("\n" + "!" * 50)
        print("ERRO CRÍTICO DURANTE O COMMIT DO BANCO DE DADOS:")
        print(f"Tipo do Erro: {type(e)}")
        print(f"Argumentos do Erro: {e.args}")
        print("!" * 50 + "\n")
        flash('ERRO DE BANCO: Não foi possível salvar os itens. Verifique o console.', 'error')

    return redirect(url_for('estoque.processar_entradas_lista'))


# -----------------------
# UPLOAD (XML / PDF / IMAGEM)
# -----------------------
@nfe_bp.route('/upload', methods=['POST'])
def upload_nfe():
    if 'arquivos' not in request.files:
        flash('Nenhum arquivo enviado.', 'error')
        return redirect(url_for('nfe.painel_nfe'))

    files = request.files.getlist('arquivos')
    recebidos = criados = ocr_ok = ocr_fail = erros = 0

    for f in files:
        if not f or f.filename.strip() == '':
            continue
        ext = os.path.splitext(f.filename)[1].lower()
        if ext not in ALLOWED_EXTS:
            erros += 1
            continue

        recebidos += 1
        safe_name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{re.sub('[^A-Za-z0-9_.-]', '_', f.filename)}"
        save_path = os.path.join(PASTA_UPLOADS, safe_name)
        f.save(save_path)

        try:
            if ext == '.xml':
                destino_xml = os.path.join(PASTA_RAIZ_XML, safe_name)
                shutil.copyfile(save_path, destino_xml)
                dados = ler_dados_nfe(destino_xml)
                if not dados:
                    erros += 1
                else:
                    added = 0
                    for item in dados['itens']:
                        existe = ProdutoNF.query.filter_by(chave_nfe=dados['chave'], produto_sku=item['sku']).first()
                        if not existe:
                            novo = ProdutoNF(
                                chave_nfe=dados['chave'],
                                fornecedor=dados.get('fornecedor'),
                                fornecedor_cnpj=dados.get('cnpj_fornecedor'),
                                produto_nome=item['nome'],
                                produto_sku=item['sku'],
                                ncm=item.get('ncm'),
                                cest=item.get('cest'),
                                quantidade_compra=item['qtd'],
                                unidade_compra=item['un'],
                                valor_unitario_compra=item['valor_unit'],
                                valor_total_compra=item.get('valor_total'),
                                valor_ipi=item.get('valor_ipi'),
                                numero_nfe=dados.get('numero'),
                                data_emissao=(dados.get('data').strftime('%Y-%m-%d') if dados.get('data') else None),
                                status='Pendente'
                            )
                            db.session.add(novo);
                            added += 1
                    if added > 0:
                        db.session.commit()
                    shutil.move(destino_xml, os.path.join(PASTA_PROCESSADOS_XML, safe_name))
                    criados += added
            else:
                try:
                    chave, qtd = importar_ocr_para_produtonf(save_path)
                    ocr_ok += qtd
                except Exception as e:
                    print(f"[NFE][UPLOAD][OCR][ERRO] {safe_name}: {e}")
                    ocr_fail += 1
        except Exception as e:
            print(f"[NFE][UPLOAD][ERRO] {safe_name}: {e}")
            erros += 1

    msgs = []
    if recebidos: msgs.append(f"{recebidos} arquivo(s) recebido(s)")
    if criados:   msgs.append(f"{criados} linha(s) XML enviadas ao estoque")
    if ocr_ok:    msgs.append(f"{ocr_ok} linha(s) detectadas via OCR")
    if ocr_fail:  msgs.append(f"{ocr_fail} arquivo(s) sem leitura OCR")
    if erros:     msgs.append(f"{erros} arquivo(s) com erro")
    flash("; ".join(msgs) or "Nenhum arquivo válido.", 'info')

    return redirect(url_for('nfe.painel_nfe'))


@nfe_bp.route('/processar')
def processar_documento():
    """
    Recebe ?doc=MAN-... (ou qualquer chave_nfe) e redireciona
    para a tela de processamento de entradas do estoque.
    A lista do estoque já consulta ProdutoNF(status='Pendente'),
    então não precisa filtrar aqui.
    """
    doc = request.args.get('doc', '').strip()

    # Opcional: valida se existe pelo menos um ProdutoNF com essa chave
    if doc:
        existe = ProdutoNF.query.filter_by(chave_nfe=doc).first()
        if not existe:
            flash(f'Nenhum item pendente encontrado para o documento {doc}.', 'warning')
            # volta pro painel da NFe se não achar nada
            return redirect(url_for('nfe.painel_nfe'))

    # Com ou sem doc, joga pra tela padrão de processar entradas
    return redirect(url_for('estoque.processar_entradas_lista'))
