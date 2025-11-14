/* Painel NF-e (sincronizado com /nfe/*)
 * - Lista XMLs da pasta COMPRAS_XML
 * - Visualiza detalhes no modal topo
 * - Processa/salva XML -> nfe_processadas + produtos_processados
 * - Processamento em massa (loop unitário; sem endpoint /processar-lote)
 */

(() => {
  "use strict";

  // ---------- Helpers DOM ----------
  const $  = (s, r=document) => r.querySelector(s);
  const $$ = (s, r=document) => Array.from(r.querySelectorAll(s));

  // ---------- Toasts simples ----------
  function ensureToastHost(){
    if (!$("#fisgar-toasts")) {
      const host = document.createElement("div");
      host.id = "fisgar-toasts";
      host.style.position = "fixed";
      host.style.top = "16px";
      host.style.right = "16px";
      host.style.zIndex = "99999";
      host.style.display = "flex";
      host.style.flexDirection = "column";
      host.style.gap = "8px";
      document.body.appendChild(host);
    }
  }
  function toast(msg, type="info", timeout=3200){
    ensureToastHost();
    const el = document.createElement("div");
    el.textContent = msg;
    el.style.padding = "10px 14px";
    el.style.borderRadius = "10px";
    el.style.boxShadow = "0 6px 18px rgba(0,0,0,.25)";
    el.style.color = "#fff"; el.style.fontWeight = "700";
    el.style.backdropFilter = "blur(6px)";
    el.style.border = "1px solid rgba(255,255,255,.18)";
    el.style.transition = "opacity .25s ease";
    el.style.opacity = "0.95"; el.style.maxWidth = "460px";
    const bg = {
      info:"linear-gradient(180deg,#06b6d4,#0ea5e9)",
      success:"linear-gradient(180deg,#22c55e,#16a34a)",
      warning:"linear-gradient(180deg,#f59e0b,#d97706)",
      error:"linear-gradient(180deg,#ef4444,#b91c1c)",
    };
    el.style.background = bg[type] || bg.info;
    $("#fisgar-toasts").appendChild(el);
    setTimeout(()=>{ el.style.opacity="0"; el.style.transform="translateY(-6px)"; }, timeout);
    setTimeout(()=> el.remove(), timeout+300);
  }

  // ---------- Formatação ----------
  const fmtMoeda = (n) => Number(n||0).toLocaleString("pt-BR",{style:"currency",currency:"BRL"});
  const fmtData = (iso) => {
    if(!iso) return "";
    const d = new Date(iso);
    return isNaN(d) ? iso : d.toLocaleString("pt-BR");
  };
  const normalize = (s) => String(s??"").toLowerCase();
  const sum = (arr, sel) => arr.reduce((a,x)=>a+Number(sel(x)||0),0);

  // ---------- Refs ----------
  const elCardsContainer   = $("#fisgar-cards-container");
  const elListaContainer   = $("#fisgar-lista-container");
  const elTabelaContainer  = $("#fisgar-tabela-container");

  const elBtnCards         = $("#btn-modo-cards");
  const elBtnLista         = $("#btn-modo-lista");
  const elBtnTabela        = $("#btn-modo-tabela");

  const elBtnProcessarMassa= $("#btn-processar-massa");
  const elFiltroGlobal     = $("#filtro-global");
  const elFiltroTipo       = $("#filtro-tipo");

  const elKpiNfeCount      = $("#card-nfe-count .num");
  const elKpiNfeTotal      = $("#card-nfe-total .num");
  const elKpiItensCount    = $("#card-itens-count .num");
  const elKpiFornecCount   = $("#card-fornecedores .num");

  const elModal            = $("#modalDetalhesNfe");
  const elModalBody        = $("#modal-body-detalhes");
  const elBtnModalProcessar= $("#btn-processar-todos");
  const elBtnModalBaixar   = $("#btn-baixar-xml");
  const elModalTitle       = $("#modalDetalhesNfeLabel");

  // ---------- Estado ----------
  let ARQUIVOS = [];
  let FILTRADOS = [];
  let SELECIONADOS = new Set();
  let MODO = "cards";
  let ARQUIVO_EM_FOCO = null;

  // ---------- Backend ----------
  async function apiGET(url){
    const r = await fetch(url, {headers: {"Accept":"application/json"}});
    if(!r.ok) throw new Error(`HTTP ${r.status} -> ${url}`);
    return r.json();
  }
  async function apiPOST(url, body){
    const r = await fetch(url, {
      method: "POST",
      headers: {"Content-Type":"application/json","Accept":"application/json"},
      body: JSON.stringify(body||{})
    });
    const data = await r.json().catch(()=> ({}));
    if(!r.ok) throw new Error(data?.message || `HTTP ${r.status} -> ${url}`);
    return data;
  }

  async function listarArquivos(){
    const data = await apiGET("/nfe/listar-arquivos");
    const arr = data?.arquivos || [];
    return arr.map(x => ({
      arquivo:     x.arquivo,
      chave:       x.chave || "",
      fornecedor:  x.fornecedor || "",
      total_itens: Number(x.total_itens || 0),
      valor_total: Number(x.valor_total || 0),
      status:      x.status || "Não processado"
    }));
  }
  async function visualizarXML(arquivo){
    const data = await apiGET(`/nfe/visualizar-xml?arquivo=${encodeURIComponent(arquivo)}`);
    if(data?.status!=="success") throw new Error(data?.message || "Falha ao visualizar");
    return data;
  }
  async function salvarXML(arquivo){
    const data = await apiPOST("/nfe/salvar-xml",{arquivo});
    if(!["success","warning"].includes(data?.status)) throw new Error(data?.message || "Falha ao salvar/processar");
    return data;
  }

  // ---------- UI helpers ----------
  const badgeStatus = (status) => {
    const s = String(status || "");
    const isOK = s.toLowerCase().includes("process");
    // novas + compat para seu CSS antigo
    const cls = isOK ? "ok status-processado" : "pendente status-pendente";
    return `<span class="badge-status ${cls}">${status || (isOK ? "Processado" : "Não processado")}</span>`;
  };

  function cardNF(n){
    const isOK = String(n.status || "").toLowerCase().includes("process");
    const stateCls = isOK ? "is-processado" : "is-pendente";
    const id = `chk-${btoa(n.arquivo).replace(/=/g,"")}`;
    return `
      <div class="fisgar-card ${stateCls}">
        <div class="fisgar-card-head">
          <div class="left">
            ${badgeStatus(n.status)}
            <strong class="arquivo" title="${n.arquivo}">${n.arquivo}</strong>
          </div>
          <div class="right">
            <input type="checkbox" class="sel-arquivo" id="${id}" data-arquivo="${n.arquivo}">
          </div>
        </div>
        <div class="fisgar-card-body">
          <div><i class="fas fa-barcode"></i> ${n.chave || "-"}</div>
          <div><i class="fas fa-truck"></i> ${n.fornecedor || "-"}</div>
          <div><i class="fas fa-cubes"></i> Itens: <strong>${n.total_itens}</strong></div>
          <div><i class="fas fa-coins"></i> Total: <strong>${fmtMoeda(n.valor_total)}</strong></div>
        </div>
        <div class="fisgar-card-actions">
          <button class="neon-btn neon-animate btn-ver" data-arquivo="${n.arquivo}">
            <i class="fas fa-eye"></i> Ver
          </button>
          <button class="neon-btn neon-animate btn-processar" data-arquivo="${n.arquivo}">
            <i class="fas fa-bolt"></i> Processar
          </button>
          <button class="neon-btn neon-animate btn-download" data-arquivo="${n.arquivo}">
            <i class="fas fa-download"></i> Baixar XML
          </button>
        </div>
      </div>
    `;
  }
  function listaNF(lista){
    return `
      <ul class="fisgar-lista">
        ${lista.map(n=>`
          <li class="fisgar-lista-item ${String(n.status||'').toLowerCase().includes('process') ? 'is-processado' : 'is-pendente'}">
            <div class="col-status">${badgeStatus(n.status)}</div>
            <div class="col-arquivo" title="${n.arquivo}">${n.arquivo}</div>
            <div class="col-forn">${n.fornecedor || "-"}</div>
            <div class="col-itens">${n.total_itens}</div>
            <div class="col-total">${fmtMoeda(n.valor_total)}</div>
            <div class="col-acoes">
              <input type="checkbox" class="sel-arquivo" data-arquivo="${n.arquivo}">
              <button class="btn-xs btn-ver" data-arquivo="${n.arquivo}"><i class="fas fa-eye"></i></button>
              <button class="btn-xs btn-processar" data-arquivo="${n.arquivo}"><i class="fas fa-bolt"></i></button>
              <button class="btn-xs btn-download" data-arquivo="${n.arquivo}"><i class="fas fa-download"></i></button>
            </div>
          </li>
        `).join("")}
      </ul>
    `;
  }
  function tabelaNF(lista){
    return `
      <table class="fisgar-table">
        <thead>
          <tr>
            <th></th><th>Status</th><th>Arquivo</th><th>Fornecedor</th><th>Itens</th><th>Total</th><th>Ações</th>
          </tr>
        </thead>
        <tbody>
          ${lista.map(n=>`
            <tr class="${String(n.status||'').toLowerCase().includes('process') ? 'is-processado' : 'is-pendente'}">
              <td><input type="checkbox" class="sel-arquivo" data-arquivo="${n.arquivo}"></td>
              <td>${badgeStatus(n.status)}</td>
              <td title="${n.arquivo}">${n.arquivo}</td>
              <td>${n.fornecedor || "-"}</td>
              <td>${n.total_itens}</td>
              <td>${fmtMoeda(n.valor_total)}</td>
              <td>
                <button class="btn-xs btn-ver" data-arquivo="${n.arquivo}"><i class="fas fa-eye"></i></button>
                <button class="btn-xs btn-processar" data-arquivo="${n.arquivo}"><i class="fas fa-bolt"></i></button>
                <button class="btn-xs btn-download" data-arquivo="${n.arquivo}"><i class="fas fa-download"></i></button>
              </td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    `;
  }

  function atualizarKPIs(lista){
    elKpiNfeCount.textContent = String(lista.length);
    elKpiNfeTotal.textContent = fmtMoeda(sum(lista, x=>x.valor_total));
    elKpiItensCount.textContent = String(sum(lista, x=>x.total_itens));
    elKpiFornecCount.textContent = String(new Set(lista.map(x=>(x.fornecedor||"").trim()).filter(Boolean)).size);
  }

  function render(){
    elCardsContainer.style.display  = (MODO==="cards") ? "" : "none";
    elListaContainer.style.display  = (MODO==="lista") ? "" : "none";
    elTabelaContainer.style.display = (MODO==="tabela")? "" : "none";

    [elBtnCards, elBtnLista, elBtnTabela].forEach(b=>b.classList.remove("active"));
    (MODO==="cards"?elBtnCards : MODO==="lista"?elBtnLista : elBtnTabela).classList.add("active");

    if(MODO==="cards") elCardsContainer.innerHTML = FILTRADOS.map(cardNF).join("");
    else if(MODO==="lista") elListaContainer.innerHTML = listaNF(FILTRADOS);
    else elTabelaContainer.innerHTML = tabelaNF(FILTRADOS);

    rebindItemHandlers();
    atualizarKPIs(FILTRADOS);
  }

  function rebindItemHandlers(){
    $$(".sel-arquivo").forEach(chk=>{
      chk.addEventListener("change", e=>{
        const arq = e.target.dataset.arquivo;
        if(e.target.checked) SELECIONADOS.add(arq); else SELECIONADOS.delete(arq);
        elBtnProcessarMassa.disabled = SELECIONADOS.size===0;
      });
    });
    $$(".btn-ver").forEach(btn=>{
      btn.addEventListener("click", async ()=>{ await abrirModalDetalhes(btn.dataset.arquivo); });
    });
    $$(".btn-processar").forEach(btn=>{
      btn.addEventListener("click", async ()=>{ await processarUm(btn.dataset.arquivo); });
    });
    $$(".btn-download").forEach(btn=>{
      btn.addEventListener("click", ()=>{ window.open(`/nfe/baixar-xml?arquivo=${encodeURIComponent(btn.dataset.arquivo)}`,"_blank"); });
    });
  }

  function aplicarFiltros(){
    const q = normalize(elFiltroGlobal.value);
    let base = [...ARQUIVOS];

    const tipo = elFiltroTipo.value||"";
    if (tipo==="processados") base = base.filter(x=>normalize(x.status).includes("process"));
    else if (tipo==="nao-processados") base = base.filter(x=>!normalize(x.status).includes("process"));
    else if (tipo==="fornecedor") {
      const f = prompt("Filtrar por fornecedor (contém):") || "";
      if (f) base = base.filter(x=>normalize(x.fornecedor).includes(normalize(f)));
    }
    // data: no-op por enquanto (depende do payload na listagem)

    if (q) {
      base = base.filter(x=>{
        const pool = [x.arquivo, x.chave, x.fornecedor, String(x.total_itens), String(x.valor_total)]
          .map(normalize).join(" ");
        return pool.includes(q);
      });
    }

    FILTRADOS = base;
    render();
  }

  async function abrirModalDetalhes(arquivo){
    try{
      elModalTitle.innerHTML = `<i class="fas fa-info-circle"></i> Detalhes da NF-e — ${arquivo}`;
      elModalBody.innerHTML = `<div class="loading">Carregando...</div>`;
      ARQUIVO_EM_FOCO = arquivo;
      const data = await visualizarXML(arquivo);
      const d = data?.dados || {};
      const emitNome = d?.emitente?.nome || "-";
      const emitCNPJ = d?.emitente?.cnpj || "";
      const chave    = d?.chave || "";
      const emissao  = d?.data_emissao || "";
      const vNF      = d?.valor_total || 0;
      const itens    = Array.isArray(d?.produtos) ? d.produtos : [];

      const header = `
        <div class="detalhe-cabeca">
          <div><strong>Emitente:</strong> ${emitNome}${emitCNPJ?` — CNPJ: ${emitCNPJ}`:""}</div>
          <div><strong>Chave:</strong> ${chave || "-"}</div>
          <div><strong>Emissão:</strong> ${fmtData(emissao)}</div>
          <div><strong>Total NF-e:</strong> ${fmtMoeda(vNF)}</div>
        </div>
      `;
      const tabela = `
        <table class="fisgar-table itens">
          <thead>
            <tr><th>Código</th><th>Descrição</th><th>NCM</th><th>Qtde</th><th>Unid</th><th>V.Unit</th><th>V.Total</th></tr>
          </thead>
          <tbody>
            ${itens.map(p=>`
              <tr>
                <td>${p.codigo||""}</td>
                <td>${p.descricao||""}</td>
                <td>${p.ncm||""}</td>
                <td>${p.quantidade??""}</td>
                <td>${p.unidade||""}</td>
                <td>${fmtMoeda(p.valor_unitario||0)}</td>
                <td>${fmtMoeda(p.valor_total||0)}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
      elModalBody.innerHTML = header + tabela;

      $("#btn-processar-todos").onclick = async ()=>{ if(ARQUIVO_EM_FOCO) await processarUm(ARQUIVO_EM_FOCO); };
      $("#btn-baixar-xml").onclick = ()=>{ if(ARQUIVO_EM_FOCO) window.open(`/nfe/baixar-xml?arquivo=${encodeURIComponent(ARQUIVO_EM_FOCO)}`,"_blank"); };

      if (typeof bootstrap!=="undefined" && bootstrap.Modal){
        const modal = bootstrap.Modal.getOrCreateInstance(elModal);
        modal.show();
      } else {
        elModal.style.display = "block";
      }
    }catch(err){
      console.error(err);
      toast(`Erro ao carregar detalhes: ${err.message}`,"error");
      elModalBody.innerHTML = `<div class="error">Falha ao carregar.</div>`;
    }
  }

  async function processarUm(arquivo){
    try{
      $$(`[data-arquivo="${arquivo}"]`).forEach(b=> b.disabled = true);
      toast(`Processando ${arquivo}...`,"info");
      const res = await salvarXML(arquivo);
      toast(`OK: ${arquivo} — ${res?.message || "Processado"}`,"success");
      const it = ARQUIVOS.find(x=>x.arquivo===arquivo);
      if (it) it.status = "Processado";
      await recarregarLista();
    }catch(err){
      console.error(err);
      toast(`Falha em ${arquivo}: ${err.message}`,"error");
    }finally{
      $$(`[data-arquivo="${arquivo}"]`).forEach(b=> b.disabled = false);
      render();
    }
  }

  async function processarMassa(){
    if (SELECIONADOS.size===0) return;
    elBtnProcessarMassa.disabled = true;

    const fila = Array.from(SELECIONADOS);
    let ok=0, fail=0;

    for (const arquivo of fila){
      try{
        await processarUm(arquivo);
        ok++;
      }catch(e){ fail++; }
    }

    if (fail===0) toast(`Lote concluído: ${ok} OK`, "success");
    else toast(`Lote parcial: ${ok} OK, ${fail} falhas`, "warning");

    SELECIONADOS.clear();
    elBtnProcessarMassa.disabled = true;
    await recarregarLista();
  }

  async function recarregarLista(){
    try{
      ARQUIVOS = await listarArquivos();
      FILTRADOS = [...ARQUIVOS];
      aplicarFiltros(); // também chama render()
    }catch(err){
      console.error(err);
      toast(`Erro ao listar arquivos: ${err.message}`,"error");
    }
  }

  function bindEventosUI(){
    elBtnCards?.addEventListener("click", ()=>{ MODO="cards"; render(); });
    elBtnLista?.addEventListener("click", ()=>{ MODO="lista"; render(); });
    elBtnTabela?.addEventListener("click", ()=>{ MODO="tabela"; render(); });

    elFiltroGlobal?.addEventListener("input", aplicarFiltros);
    elFiltroTipo?.addEventListener("change", aplicarFiltros);

    elBtnProcessarMassa?.addEventListener("click", processarMassa);
  }

  document.addEventListener("DOMContentLoaded", async ()=>{
    bindEventosUI();
    await recarregarLista();
    elBtnProcessarMassa.disabled = true;
  });
})();
