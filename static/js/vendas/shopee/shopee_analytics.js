// ===== Shopee Analytics – Cards/Charts clicáveis, modais, tabelas ordenáveis + filtro =====



const API_ROOT     = "/modulos/vendas/shopee";
const API_BASE     = `${API_ROOT}/api/shopee`;
const API_DASH     = `${API_BASE}/dashboard-data`;
const API_FILTROS  = `${API_BASE}/filtros`;
const API_EXPORT   = `${API_BASE}/export`;
const API_TOP      = `${API_BASE}/top-produtos`;
const API_TRANS    = `${API_BASE}/transportadoras`;
const API_EVOL     = `${API_BASE}/evolucao-vendas`;
const API_ABC      = `${API_BASE}/curva-abc`;
const URL_CURVA    = `${API_ROOT}/curva-abc`;

const fmtBRL = (v) => Number(v || 0).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
const fmtInt = (v) => Number(v || 0).toLocaleString("pt-BR");
const fmtPct = (v) => (v === null || v === undefined ? "—" : `${Number(v).toFixed(1)}%`);
const nz = (v, d=0) => Number(v ?? d);

let LAST = { metrics:{}, charts:{}, tables:{}, comparativos:{} };
let charts = {};

function setVisible(el, show){ if(!el) return; el.style.display = show ? "" : "none"; }
function destroyCharts(){ Object.values(charts).forEach(c=>{ try{c.destroy();}catch(e){} }); charts = {}; }
function setLastUpdate(){
  const el = document.getElementById("last-update-time");
  if(el){
    const now=new Date();
    el.textContent = now.toLocaleTimeString("pt-BR",{hour12:false});
  }
}

function pillDelta(container, value){
  if(!container) return;
  container.classList.remove("negative","neutral");
  const icon = container.querySelector("i");
  const span = container.querySelector("span") || container;
  if(value===null || value===undefined){
    container.classList.add("neutral");
    if(icon) icon.className="";
    span.textContent="—";
    return;
  }
  const n = Number(value);
  if(n>0){
    container.classList.remove("negative","neutral");
    if(icon) icon.className="fas fa-arrow-up";
    span.textContent = fmtPct(n);
  } else if(n<0){
    container.classList.add("negative");
    if(icon) icon.className="fas fa-arrow-down";
    span.textContent = fmtPct(Math.abs(n));
  } else {
    container.classList.add("neutral");
    if(icon) icon.className="";
    span.textContent="= mês ant.";
  }
}

function getFiltroParams(){
  const p = new URLSearchParams();
  const di = document.getElementById("dataInicio")?.value;
  const df = document.getElementById("dataFim")?.value;
  const transportadora = document.getElementById("transportadora")?.value;
  const tipo = document.getElementById("tipoConta")?.value;
  const status = document.getElementById("statusPedido")?.value; // NOVO

  if(di) p.append("data_inicio", di);
  if(df) p.append("data_fim", df);
  if(transportadora) p.append("transportadora", transportadora);
  if(tipo) p.append("tipo_conta", tipo);
  if(status && status.toLowerCase() !== 'todas') p.append("status_pedido", status); // NOVO

  return p;
}

async function carregarFiltros(){
  try{
    const res = await fetch(API_FILTROS);
    const data = await res.json();
    const selT = document.getElementById("transportadora");
    const selC = document.getElementById("tipoConta");
    const selS = document.getElementById("statusPedido"); // NOVO

    if(selT && Array.isArray(data.transportadoras)){
      selT.innerHTML = `<option value="">Todas</option>` + data.transportadoras.map(t=>`<option value="${t}">${t}</option>`).join("");
    }
    if(selC && Array.isArray(data.tipos_conta)){
      selC.innerHTML = `<option value="">Todos</option>` + data.tipos_conta.map(t=>`<option value="${t}">${t}</option>`).join("");
    }
    // NOVO: Preencher o select de status
    if(selS && Array.isArray(data.status_pedido)){
      selS.innerHTML = `<option value="todas">Todos</option>` + data.status_pedido.map(s=>`<option value="${s}">${s}</option>`).join("");
    }
  }catch(e){ console.error("Filtros:", e); }
}

/* ==================== CARDS ==================== */
function atualizarCards(metrics, comparativos){
  LAST.metrics = metrics; LAST.comparativos = comparativos || {};
  const d = (k)=>document.getElementById(k);
  d("totalVendas").textContent       = fmtInt(metrics.total_vendas || 0);
  d("receitaTotal").textContent      = fmtBRL(metrics.receita_total || 0);
  d("lucroTotal").textContent        = fmtBRL(metrics.lucro_total || 0);
  d("margemMedia").textContent       = fmtPct(metrics.margem_liquida || 0);
  d("ticketMedio").textContent       = fmtBRL(metrics.ticket_medio || 0);
  d("produtosDiferentes").textContent= fmtInt(metrics.qtd_produtos_diferentes || 0);

  const deltas = (comparativos && comparativos.deltas_percent) || {};
  pillDelta(d("variacaoVendas")?.closest(".metric-change"),   deltas.total_vendas);
  pillDelta(d("variacaoReceita")?.closest(".metric-change"),  deltas.receita_total);
  pillDelta(d("variacaoLucro")?.closest(".metric-change"),    deltas.lucro_total);
  pillDelta(d("variacaoMargem")?.closest(".metric-change"),   deltas.margem_liquida);
  pillDelta(d("variacaoTicket")?.closest(".metric-change"),   deltas.ticket_medio);
  pillDelta(d("variacaoProdutos")?.closest(".metric-change"), deltas.qtd_produtos_diferentes);
}

/* ==================== GRÁFICOS ==================== */
function desenharGraficos(chartsData){
  destroyCharts();
  LAST.charts = chartsData || {};

  const salesEl = document.getElementById("salesChart");
  if(salesEl){
    charts.sales = new Chart(salesEl.getContext("2d"), {
      type: "bar",
      data: { labels: chartsData.sales?.labels||[], datasets: [{ label:"Receita", data: chartsData.sales?.values||[], borderWidth:1 }] },
      options: { responsive:true, maintainAspectRatio:false, onClick: ()=>openSalesModal() }
    });
  }

  const mEl = document.getElementById("marginsChart");
  if(mEl){
    charts.margins = new Chart(mEl.getContext("2d"), {
      type: "bar",
      data: { labels: chartsData.margins?.labels||[], datasets: [{ label:"MC %", data: chartsData.margins?.values||[], borderWidth:1 }] },
      options: { responsive:true, maintainAspectRatio:false, onClick: ()=>openProductsModal("margem") }
    });
  }

  const aEl = document.getElementById("abcChart");
  if(aEl){
    charts.abc = new Chart(aEl.getContext("2d"), {
      type: "doughnut",
      data: { labels: chartsData.abc?.labels||["A","B","C"], datasets: [{ data: chartsData.abc?.values||[0,0,0] }] },
      options: { responsive:true, maintainAspectRatio:false, onClick: ()=>openAbcModal() }
    });
  }

  const tEl = document.getElementById("topProductsChart");
  if(tEl){
    charts.top = new Chart(tEl.getContext("2d"), {
      type: "bar",
      data: { labels: chartsData.topProducts?.labels||[], datasets: [{ label:"Lucro", data: chartsData.topProducts?.values||[] }] },
      options: { responsive:true, maintainAspectRatio:false, onClick: ()=>openProductsModal("lucro") }
    });
  }

  const trEl = document.getElementById("transportChart");
  if(trEl){
    charts.transport = new Chart(trEl.getContext("2d"), {
      type: "bar",
      data: { labels: chartsData.transport?.labels||[], datasets: [{ label:"Qtd", data: chartsData.transport?.values||[] }] },
      options: { responsive:true, maintainAspectRatio:false, onClick: ()=>openTransportModal() }
    });
  }
}

/* ==================== TABELAS – ferramentas (sort + filter) ==================== */
function makeSortable(table){
  if(!table) return;
  const ths = table.querySelectorAll("thead th");
  ths.forEach((th, idx)=>{
    th.addEventListener("click", ()=>{
      const tbody = table.querySelector("tbody"); if(!tbody) return;
      const rows = Array.from(tbody.querySelectorAll("tr"));
      const type = th.dataset.type || "text";
      const current = th.classList.contains("sort-asc") ? "asc" :
                      th.classList.contains("sort-desc") ? "desc" : null;
      ths.forEach(x=>x.classList.remove("sort-asc","sort-desc"));
      const next = current==="asc" ? "desc" : "asc";
      th.classList.add(next==="asc" ? "sort-asc" : "sort-desc");
      const parse = (txt)=>{
        const t = (txt||"").toString().replace(/\./g,"").replace(",",".").replace(/[^\d\.\-]/g,"");
        const n = parseFloat(t);
        return isNaN(n) ? 0 : n;
      };
      rows.sort((a,b)=>{
        const A = a.children[idx]?.textContent.trim() || "";
        const B = b.children[idx]?.textContent.trim() || "";
        if(type==="number"||type==="currency"||type==="percent"){
          const nA = parse(A); const nB = parse(B);
          return next==="asc" ? nA-nB : nB-nA;
        } else {
          return next==="asc" ? A.localeCompare(B) : B.localeCompare(A);
        }
      });
      rows.forEach(r=>tbody.appendChild(r));
    });
  });
}

function makeFilterable(table, inputEl){
  if(!table || !inputEl) return;
  const tbody = table.querySelector("tbody"); if(!tbody) return;
  const allRows = () => Array.from(tbody.querySelectorAll("tr"));
  const apply = ()=>{
    const q = (inputEl.value||"").toLowerCase().trim();
    allRows().forEach(tr=>{
      const text = tr.textContent.toLowerCase();
      tr.style.display = q==="" || text.includes(q) ? "" : "none";
    });
  };
  inputEl.addEventListener("input", apply);
}

/* ==================== TABELAS – página principal ==================== */
function preencherTabelas(tables){
  LAST.tables = tables || {};
  // Produtos
  const pTbody = document.getElementById("productsTable");
  if(pTbody){
    const list = Array.isArray(tables?.products) ? tables.products : [];
    if(!list.length){ pTbody.innerHTML = `<tr><td colspan="7" class="text-center">Sem dados</td></tr>`; }
    else{
      pTbody.innerHTML = list.map(r=>{
        const mc = Number(r.mc_percentual||0);
        const status =
          (nz(r.LUCRO_REAL)<=0 || mc<0) ? `<span class="badge bg-danger">Ruim</span>` :
          (mc<10) ? `<span class="badge bg-warning">Médio</span>` :
                    `<span class="badge bg-success">Bom</span>`;
        return `
          <tr>
            <td>${(r.produto || r.SKU || r.id_venda || "-")}</td>
            <td>${r.SKU || "-"}</td>
            <td>${fmtInt(r.QTD_COMPRADA || 0)}</td>
            <td>${fmtBRL(r.VALOR_TOTAL || 0)}</td>
            <td>${fmtBRL(r.LUCRO_REAL || 0)}</td>
            <td>${fmtPct(mc)}</td>
            <td>${status}</td>
          </tr>
        `;
      }).join("");
    }
    // sort + filter na página
    makeSortable(document.getElementById("productsTableWrap"));
    makeFilterable(document.getElementById("productsTableWrap"), document.getElementById("productsSearch"));
  }

  // Transportadoras
  const tTbody = document.getElementById("transportTable");
  if(tTbody){
    const list = Array.isArray(tables?.transport) ? tables.transport : [];
    if(!list.length){ tTbody.innerHTML = `<tr><td colspan="7" class="text-center">Sem dados</td></tr>`; }
    else{
      tTbody.innerHTML = list.map(r=>{
        const mc = Number(r.mc_media||0);
        const perf = (r.vendas_lucrativas||0) - (r.vendas_prejuizo||0);
        const badge = perf>0 ? `<span class="badge bg-success">Boa</span>` : perf<0 ? `<span class="badge bg-danger">Ruim</span>` : `<span class="badge bg-warning">Neutra</span>`;
        return `
          <tr>
            <td>${r.TRANSPORTADORA || "-"}</td>
            <td>${fmtInt(r.quantidade || 0)}</td>
            <td>${fmtBRL(r.receita_total || 0)}</td>
            <td>${fmtBRL(r.lucro_total || 0)}</td>
            <td>${fmtBRL(r.ticket_medio || 0)}</td>
            <td>${fmtPct(mc)}</td>
            <td>${badge}</td>
          </tr>
        `;
      }).join("");
    }
    // sort + filter na página
    makeSortable(document.getElementById("transportTableWrap"));
    makeFilterable(document.getElementById("transportTableWrap"), document.getElementById("transportSearch"));
  }

  // botões "Detalhar" que abrem modal
  document.getElementById("expandProducts")?.addEventListener("click", ()=>openProductsModal("lucro"));
  document.getElementById("expandTransport")?.addEventListener("click", openTransportModal);
}

/* ==================== MODAIS – botões e helpers ==================== */
function modalBtns(extra=[]){
  const base = [
    { text:"Exportar CSV", icon:"ri-download-line", type:"export", action: ()=> window.open(`${API_EXPORT}?format=csv&${getFiltroParams().toString()}`,"_blank") },
    { text:"Exportar JSON", icon:"ri-code-line", type:"export", action: ()=> window.open(`${API_EXPORT}?format=json&${getFiltroParams().toString()}`,"_blank") },
  ];
  return [...base, ...extra];
}
function makeModalFilter(tableId, inputId){
  const table = document.getElementById(tableId);
  const input = document.getElementById(inputId);
  makeSortable(table);
  makeFilterable(table, input);
}

/* ==================== MODAIS – conteúdos ==================== */
async function openSalesModal(){
  const params = getFiltroParams();
  const active = document.querySelector(".btn-period.active")?.dataset?.period || "30d";
  params.append("periodo", active);

  const res = await fetch(`${API_EVOL}?${params.toString()}`);
  const data = await res.json();
  const rows = Array.isArray(data.evolucao) ? data.evolucao : [];

  const html = `
    <div>
      <div class="table-header-row" style="margin-bottom:8px;">
        <h3 style="margin:0;color:var(--primary-bright);">Evolução de Vendas</h3>
        <div class="table-toolbar">
          <input id="modalSearchSales" class="table-search" type="text" placeholder="Filtrar..." />
        </div>
      </div>
      <div class="table-responsive">
        <table class="table" id="modalSalesTable">
          <thead>
            <tr>
              <th data-type="text">Dia</th>
              <th data-type="currency">Receita</th>
              <th data-type="currency">Lucro</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map(r=>`
              <tr>
                <td>${r.dia || "-"}</td>
                <td>${fmtBRL(r.receita)}</td>
                <td>${fmtBRL(r.lucro)}</td>
              </tr>`).join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
  abrirModalPremium("Evolução de Vendas", html, { buttons: modalBtns() });
  makeModalFilter("modalSalesTable","modalSearchSales");
}

async function openProductsModal(orderBy="lucro"){
  const params = getFiltroParams();
  params.append("limit","100");
  params.append("order_by", orderBy);

  const res = await fetch(`${API_TOP}?${params.toString()}`);
  const data = await res.json();
  const list = Array.isArray(data.produtos) ? data.produtos : [];

  const html = `
    <div>
      <div class="table-header-row" style="margin-bottom:8px;">
        <h3 style="margin:0;color:var(--primary-bright);">Produtos (${orderBy})</h3>
        <div class="table-toolbar">
          <input id="modalSearchProducts" class="table-search" type="text" placeholder="Filtrar..." />
        </div>
      </div>
      <div class="table-responsive">
        <table class="table" id="modalProductsTable">
          <thead>
            <tr>
              <th data-type="text">Produto</th>
              <th data-type="text">SKU</th>
              <th data-type="number">Qtd</th>
              <th data-type="currency">Receita</th>
              <th data-type="currency">Lucro</th>
              <th data-type="percent">MC%</th>
              <th data-type="text">Qualidade</th>
            </tr>
          </thead>
          <tbody>
            ${list.map(r=>{
              const mc = Number(r.mc_percentual||0);
              const bad = (nz(r.LUCRO_REAL)<=0 || mc<0) ? "bg-danger" : (mc<10) ? "bg-warning" : "bg-success";
              const tag = bad==="bg-danger" ? "Ruim" : bad==="bg-warning" ? "Médio" : "Bom";
              return `
                <tr>
                  <td>${(r.produto || r.SKU || r.id_venda || "-")}</td>
                  <td>${r.SKU || "-"}</td>
                  <td>${fmtInt(r.QTD_COMPRADA || 0)}</td>
                  <td>${fmtBRL(r.VALOR_TOTAL || 0)}</td>
                  <td>${fmtBRL(r.LUCRO_REAL || 0)}</td>
                  <td>${fmtPct(mc)}</td>
                  <td><span class="badge ${bad}">${tag}</span></td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
  abrirModalPremium("Top Produtos", html, { buttons: modalBtns() });
  makeModalFilter("modalProductsTable","modalSearchProducts");
}

async function openTransportModal(){
  const params = getFiltroParams();
  const res = await fetch(`${API_TRANS}?${params.toString()}`);
  const data = await res.json();
  const list = Array.isArray(data.transportadoras) ? data.transportadoras : [];

  const html = `
    <div>
      <div class="table-header-row" style="margin-bottom:8px;">
        <h3 style="margin:0;color:var(--primary-bright);">Análise por Transportadora</h3>
        <div class="table-toolbar">
          <input id="modalSearchTrans" class="table-search" type="text" placeholder="Filtrar..." />
        </div>
      </div>
      <div class="table-responsive">
        <table class="table" id="modalTransTable">
          <thead>
            <tr>
              <th data-type="text">Transportadora</th>
              <th data-type="number">Quantidade</th>
              <th data-type="currency">Receita Total</th>
              <th data-type="currency">Lucro Total</th>
              <th data-type="currency">Ticket Médio</th>
              <th data-type="percent">Margem Média</th>
              <th data-type="text">Performance</th>
            </tr>
          </thead>
          <tbody>
            ${list.map(r=>{
              const mc = Number(r.mc_media||0);
              const perf = (r.vendas_lucrativas||0) - (r.vendas_prejuizo||0);
              const badge = perf>0 ? `<span class="badge bg-success">Boa</span>` : perf<0 ? `<span class="badge bg-danger">Ruim</span>` : `<span class="badge bg-warning">Neutra</span>`;
              return `
                <tr>
                  <td>${r.TRANSPORTADORA || "-"}</td>
                  <td>${fmtInt(r.quantidade || 0)}</td>
                  <td>${fmtBRL(r.receita_total || 0)}</td>
                  <td>${fmtBRL(r.lucro_total || 0)}</td>
                  <td>${fmtBRL(r.ticket_medio || 0)}</td>
                  <td>${fmtPct(mc)}</td>
                  <td>${badge}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
  abrirModalPremium("Análise por Transportadora", html, { buttons: modalBtns() });
  makeModalFilter("modalTransTable","modalSearchTrans");
}

async function openAbcModal(){
  const params = getFiltroParams();
  params.append("criterio", "receita"); // ABC padrão por receita

  const res = await fetch(`${API_ABC}?${params.toString()}`);
  const data = await res.json();
  const list = Array.isArray(data.curva_abc) ? data.curva_abc : [];

  const html = `
    <div>
      <div class="table-header-row" style="margin-bottom:8px;">
        <h3 style="margin:0;color:var(--primary-bright);">Curva ABC Completa</h3>
        <div class="table-toolbar">
          <input id="modalSearchAbc" class="table-search" type="text" placeholder="Filtrar..." />
        </div>
      </div>
      <div class="table-responsive">
        <table class="table" id="modalAbcTable">
          <thead>
            <tr>
              <th data-type="number">Posição</th>
              <th data-type="text">Classe</th>
              <th data-type="text">Produto</th>
              <th data-type="currency">Receita</th>
              <th data-type="percent">% Individual</th>
              <th data-type="percent">% Acumulado</th>
              <th data-type="percent">MC%</th>
              <th data-type="currency">Lucro</th>
            </tr>
          </thead>
          <tbody>
            ${list.map(r=>{
              const badgeClass = r.classe_abc === 'A' ? 'bg-success' : (r.classe_abc === 'B' ? 'bg-warning' : 'bg-danger');
              return `
                <tr>
                  <td>${r.ranking || "-"}</td>
                  <td><span class="badge ${badgeClass}">${r.classe_abc || "-"}</span></td>
                  <td>${r.produto || "-"}</td>
                  <td>${fmtBRL(r.VALOR_TOTAL || 0)}</td>
                  <td>${fmtPct(r.percentual)}</td>
                  <td>${fmtPct(r.percentual_acumulado)}</td>
                  <td>${fmtPct(r.mc_percentual)}</td>
                  <td>${fmtBRL(r.LUCRO_REAL || 0)}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
  abrirModalPremium("Curva ABC", html, { buttons: modalBtns() });
  makeModalFilter("modalAbcTable","modalSearchAbc");
}

/* ==================== CARREGAMENTO PRINCIPAL ==================== */
async function carregarDashboard(){
  setVisible(document.getElementById("loadingSpinner"), true);
  setVisible(document.getElementById("errorMessage"), false);
  setLastUpdate();

  try{
    const params = getFiltroParams();
    const res = await fetch(`${API_DASH}?${params.toString()}`);
    const data = await res.json();

    if(!res.ok || !data.success) throw new Error(data.message || "Erro ao carregar dados do dashboard.");

    atualizarCards(data.metrics, data.comparativos);
    desenharGraficos(data.charts);
    preencherTabelas(data.tables);

  } catch(e){
    console.error("Erro no dashboard:", e);
    document.getElementById("errorText").textContent = `Erro: ${e.message || "Verifique sua conexão ou os dados."}`;
    setVisible(document.getElementById("errorMessage"), true);
  } finally {
    setVisible(document.getElementById("loadingSpinner"), false);
  }
}

/* ==================== INICIALIZAÇÃO ==================== */
function wireButtons(){
  document.getElementById("refreshBtn")?.addEventListener("click", carregarDashboard);
  document.getElementById("exportBtn")?.addEventListener("click", ()=>{
    window.open(`${API_EXPORT}?format=csv&${getFiltroParams().toString()}`,"_blank");
  });

  // Período
  document.querySelectorAll(".btn-period").forEach(btn=>{
    btn.addEventListener("click", function(){
      document.querySelectorAll(".btn-period").forEach(b=>b.classList.remove("active"));
      this.classList.add("active");
      const period = this.dataset.period;
      const today = new Date();
      let startDate = new Date();

      if(period === "7d") startDate.setDate(today.getDate() - 6);
      else if(period === "30d") startDate.setDate(today.getDate() - 29);
      else if(period === "90d") startDate.setDate(today.getDate() - 89);
      else if(period === "1y") startDate.setFullYear(today.getFullYear() - 1);

      document.getElementById("dataInicio").value = startDate.toISOString().slice(0,10);
      document.getElementById("dataFim").value = today.toISOString().slice(0,10);
      carregarDashboard();
    });
  });

  // Filtros
  ["dataInicio","dataFim","transportadora","tipoConta", "statusPedido"].forEach(id=>{
    const el = document.getElementById(id);
    el && el.addEventListener("change", carregarDashboard);
  });

  // Tema
  document.getElementById("themeToggle")?.addEventListener("click", ()=>{
    document.body.classList.toggle("dark-theme");
    const icon = document.getElementById("themeIcon");
    if(document.body.classList.contains("dark-theme")){
      icon.classList.remove("fa-moon");
      icon.classList.add("fa-sun");
      localStorage.setItem("theme","dark");
    } else {
      icon.classList.remove("fa-sun");
      icon.classList.add("fa-moon");
      localStorage.setItem("theme","light");
    }
    // Recarregar gráficos para aplicar tema
    desenharGraficos(LAST.charts);
  });
}

function loadTheme(){
  const theme = localStorage.getItem("theme");
  if(theme === "dark"){
    document.body.classList.add("dark-theme");
    const icon = document.getElementById("themeIcon");
    if(icon){
      icon.classList.remove("fa-moon");
      icon.classList.add("fa-sun");
    }
  }
}

document.addEventListener("DOMContentLoaded", async ()=>{
  loadTheme();
  wireButtons();
  await carregarFiltros();

  // Define datas padrão (últimos 30 dias) na inicialização
  const today = new Date();
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(today.getDate() - 29);
  document.getElementById("dataInicio").value = thirtyDaysAgo.toISOString().slice(0,10);
  document.getElementById("dataFim").value = today.toISOString().slice(0,10);

  carregarDashboard();
});

// Objeto global para acesso externo (ex: de outros scripts ou console)
const analytics = {
  carregarDashboard: carregarDashboard,
  updateChart: (chartName, mode) => {
    if (charts[chartName]) {
      if (chartName === 'sales') {
        // Lógica para atualizar o gráfico de vendas (receita/lucro)
        const labels = LAST.charts.sales.labels;
        const data = (mode === 'receita') ? LAST.charts.sales.values : LAST.charts.sales.values.map(v => {
          // Isso é um placeholder. Você precisaria de dados de lucro para cada ponto.
          // Por simplicidade, vamos apenas simular uma variação.
          return v * 0.3; // Exemplo: lucro é 30% da receita
        });
        charts[chartName].data.datasets[0].label = mode === 'receita' ? 'Receita' : 'Lucro';
        charts[chartName].data.datasets[0].data = data;
        charts[chartName].update();
      }
    }
  }
};