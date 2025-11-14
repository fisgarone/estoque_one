(function(){
  const urlBase = "/nfe/importador";
  const tblBody = () => document.querySelector("#tbl-itens tbody");
  const pillsBox = () => document.getElementById("meta-pills");
  const drop = document.getElementById("dropzone");

  // cria pelo menos 1 linha ao abrir
  addRow();

  // Botões principais
  document.getElementById("btn-scan").addEventListener("click", scanPasta);
  document.getElementById("btn-add").addEventListener("click", addRow);
  document.getElementById("btn-save").addEventListener("click", salvar);
  document.getElementById("btn-save-bottom").addEventListener("click", salvar);

  // Upload de arquivo (XML/PDF/IMG)
  const fileInput = document.getElementById("file-input");
  fileInput.addEventListener("change", async (e)=>{
    const f = e.target.files[0];
    if (!f) return;
    await uploadAjax(f);
    fileInput.value = "";
  });

  // Upload de planilha
  const planilhaInput = document.getElementById("planilha-input");
  planilhaInput.addEventListener("change", async (e)=>{
    const f = e.target.files[0];
    if (!f) return;
    await importarPlanilha(f);
    planilhaInput.value = "";
  });

  // Drag & Drop (aceita ambos)
  drop.addEventListener("dragover", e => { e.preventDefault(); drop.classList.add("dragover"); });
  drop.addEventListener("dragleave", () => drop.classList.remove("dragover"));
  drop.addEventListener("drop", async e => {
    e.preventDefault(); drop.classList.remove("dragover");
    const f = e.dataTransfer.files && e.dataTransfer.files[0];
    if (!f) return;
    const name = (f.name || "").toLowerCase();
    if (name.endsWith(".xlsx") || name.endsWith(".csv")) {
      await importarPlanilha(f);
    } else {
      await uploadAjax(f);
    }
  });

  // Auto-scan dos mais recentes (silencioso)
  document.addEventListener("DOMContentLoaded", async ()=>{
    try{ await scanPasta(true); }catch(_){}
  });

  // --------- Funções ----------
  async function scanPasta(silent=false){
    const res = await fetch(`${urlBase}/listar_arquivos`);
    const js = await res.json();
    if(!js.ok){ if(!silent) notify("Falha ao listar arquivos.", "error"); return false; }
    const files = js.files || [];
    if(files.length === 0){ if(!silent) notify("Nenhum arquivo em pasta_xml.", "warning"); return false; }
    const top = files[0].name;
    const ok = await parsePath(top, silent);
    return ok;
  }

  async function parsePath(filename, silent=false){
    const res = await fetch(`${urlBase}/parse_path`, {
      method: "POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify({ filename })
    });
    const js = await res.json();
    if(!js.ok){
      if(!silent) notify(js.message || "Falha ao ler arquivo.", "error");
      return false;
    }
    fillItems(js.itens || []);
    renderPills(js.meta||{}, filename, js.warnings || [], js.moved);
    notify(`Itens carregados: ${filename}`, "success");
    return true;
  }

  async function uploadAjax(file){
    const fd = new FormData();
    fd.append("arquivo", file);
    const res = await fetch(`${urlBase}/upload_json`, { method:"POST", body: fd });
    const js = await res.json();
    if(!js.ok){
      notify(js.message || "Falha ao processar.", "error");
      return;
    }
    fillItems(js.itens || []);
    renderPills(js.meta||{}, file.name, js.warnings||[], js.moved);
    notify(`Itens processados: ${file.name}`, "success");
  }

  async function importarPlanilha(file){
    const fd = new FormData();
    fd.append("planilha", file);
    const res = await fetch(`${urlBase}/upload_planilha_json`, { method:"POST", body: fd });
    const js = await res.json();
    if(!js.ok){
      notify(js.message || "Falha ao importar planilha.", "error");
      return;
    }
    fillItems(js.itens || []);
    renderPills(js.meta||{}, file.name, js.warnings||[], null);
    notify(`Planilha importada: ${file.name}`, "success");
  }

  function fillItems(items){
    const tb = tblBody();
    tb.innerHTML = "";
    if(!items || items.length===0){ addRow(); return; }
    for(const it of items){
      const tr = document.createElement("tr");
      tr.innerHTML = rowHtml(
        it.sku || "", it.descricao || "", it.unidade_compra || "UN",
        toNum(it.quantidade) || "", toPlain(it.valor_unitario),
        toNum(it.ipi_percentual) || 0, it.ncm || "", it.cfop || ""
      );
      tb.appendChild(tr);
    }
  }

  function addRow(){
    const tr = document.createElement("tr");
    tr.innerHTML = rowHtml("", "", "UN", "", "", 0, "", "");
    tblBody().appendChild(tr);
  }

  function rowHtml(sku, desc, und, qtd, vlr, ipi, ncm, cfop){
    return `
      <td><input value="${esc(sku)}" data-f="sku" required></td>
      <td><input value="${esc(desc)}" data-f="descricao"></td>
      <td><input value="${esc(und)}" data-f="unidade_compra" required></td>
      <td><input value="${esc(qtd)}" data-f="quantidade" type="number" step="0.0001" min="0.0001" required></td>
      <td><input value="${esc(vlr)}" data-f="valor_unitario" type="text" placeholder="9,99" required></td>
      <td><input value="${esc(ipi)}" data-f="ipi_percentual" type="number" step="0.01" min="0"></td>
      <td><input value="${esc(ncm)}" data-f="ncm"></td>
      <td><input value="${esc(cfop)}" data-f="cfop"></td>
      <td><button class="btnx ghost" onclick="this.closest('tr').remove()">🗑</button></td>
    `;
  }

  async function salvar(){
    const itens = [];
    const rows = tblBody().querySelectorAll("tr");
    for(const tr of rows){
      const g = f => tr.querySelector(`[data-f="${f}"]`)?.value?.trim() || "";
      const sku = g("sku");
      const und = (g("unidade_compra") || "UN").toUpperCase();
      const qtd = parseNumber(g("quantidade"));
      const vlr = parseNumber(g("valor_unitario"));
      if(!sku || !und || !qtd || !vlr) continue;
      itens.push({
        sku, descricao: g("descricao"), unidade_compra: und,
        quantidade: qtd, valor_unitario: vlr,
        ipi_percentual: parseNumber(g("ipi_percentual")) || 0,
        ncm: g("ncm"), cfop: g("cfop")
      });
    }
    if(itens.length===0){ notify("Nenhuma linha válida (SKU/Un/Qtd/Vlr obrigatórios).", "warning"); return; }

    const res = await fetch(`${urlBase}/salvar`, {
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({ itens })
    });
    const js = await res.json();
    if(!js.ok){ notify(js.message || "Falha ao salvar.", "error"); return; }
    notify(js.message || "Salvo.", "success");
    const chave = js.chave;
    setTimeout(()=> {
      const url = tryUrlForProcessar(chave);
      if(url) location.href = url;
    }, 600);
  }

  function renderPills(meta, filename, warnings, moved){
    const box = pillsBox();
    const pills = [];
    if(filename) pills.push(pill(`<i class="ri-file-text-line"></i> ${esc(filename)}`));
    if(moved)    pills.push(pill(`<i class="ri-check-double-line"></i> movido para processados (${esc(moved)})`));
    if(meta?.chave)    pills.push(pill(`<i class="ri-key-2-line"></i> ${esc(meta.chave)}`));
    if(meta?.emitente) pills.push(pill(`<i class="ri-building-2-line"></i> ${esc(meta.emitente)}`));
    if(meta?.numero)   pills.push(pill(`<i class="ri-hashtag"></i> Nº ${esc(meta.numero)}`));
    if(meta?.serie)    pills.push(pill(`<i class="ri-stack-line"></i> Série ${esc(meta.serie)}`));
    if(warnings && warnings.length) {
      const txt = warnings.map(esc).join(" | ");
      pills.push(pill(`<i class="ri-alert-line"></i> ${txt}`));
    }
    box.innerHTML = pills.join(" ");
    box.style.display = pills.length ? "flex" : "none";
  }

  function pill(html){ return `<span class="pill">${html}</span>`; }
  function notify(msg, type="success"){
    if(typeof showNotification === "function"){ showNotification(msg, type); }
    else { console.log(`[${type}]`, msg); }
  }

  function esc(s){ return String(s||"").replace(/[&<>"']/g, m=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[m])); }
  function toNum(x){ const n = Number(x); return isFinite(n)? n : ""; }
  function toPlain(x){ return (x===0||x) ? String(x).replace('.', ',') : ""; }
  function parseNumber(s){
    if(!s) return 0;
    s = String(s).trim();
    if(s.indexOf(",")>=0 && s.indexOf(".")>=0){
      s = s.replace(/\./g, "").replace(",", ".");
    }else if(s.indexOf(",")>=0){
      s = s.replace(",", ".");
    }
    const n = Number(s);
    return isFinite(n)? n : 0;
  }

  function tryUrlForProcessar(chave){
    // Ajuste aqui se quiser redirecionar para sua tela de processar entradas
    const candidates = [
      `/nfe/processar?doc=${encodeURIComponent(chave)}`,
      `/nfe/processar?chave=${encodeURIComponent(chave)}`,
      `/estoque/processar-entradas?doc=${encodeURIComponent(chave)}`,
      `/estoque/processar-entradas?chave=${encodeURIComponent(chave)}`,
      `/estoque/processar-entradas/lista`
    ];
    return candidates[0]; // deixe fixo no seu alvo real, se souber
  }
})();
