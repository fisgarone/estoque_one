(function(){
  let chart;
  const el = (id) => document.getElementById(id);
  const fmtBR = (v) => Number(v||0).toLocaleString('pt-BR');
  const fmtBRMoney = (v) => `R$ ${Number(v||0).toFixed(2).replace('.', ',')}`;

  async function fetchPerformance(days){
    const resp = await fetch(`/vendas/ml/anuncios/api/performance?days=${days}`, { cache: 'no-store' });
    if(!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp.json();
  }

  function ensureChart(){
    const ctx = el('relatorioPerformanceChart');
    if(!ctx) return null;
    if(chart) return chart;
    chart = new Chart(ctx, {
      type:'line',
      data: {
        labels: [],
        datasets: [
          { label:'Vendas', data: [], borderColor:'rgba(0,191,255,1)', backgroundColor:'rgba(0,191,255,.12)', borderWidth:3, fill:true, tension:0.4 },
          { label:'Receita', data: [], borderColor:'rgba(255,62,128,1)', backgroundColor:'rgba(255,62,128,.12)', borderWidth:3, fill:true, tension:0.4 }
        ]
      },
      options:{
        responsive:true, maintainAspectRatio:false, interaction:{ intersect:false, mode:'index' },
        plugins:{ legend:{ position:'top', labels:{ usePointStyle:true } } },
        scales:{ x:{ ticks:{} }, y:{ ticks:{} } }
      }
    });
    return chart;
  }

  function preencherTabela(labels, vendas, receita){
    const tbody = el('tblSerie').querySelector('tbody');
    const frag = document.createDocumentFragment();
    for(let i=0;i<labels.length;i++){
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${labels[i]}</td>
        <td><strong>${fmtBR(vendas[i])}</strong></td>
        <td><strong>${fmtBRMoney(receita[i])}</strong></td>
      `;
      frag.appendChild(tr);
    }
    tbody.replaceChildren(frag);
  }

  function preencherKpis(labels, vendas, receita){
    const totalVendas = (vendas||[]).reduce((a,b)=>a+Number(b||0),0);
    const totalReceita = (receita||[]).reduce((a,b)=>a+Number(b||0),0);
    const ticket = totalVendas > 0 ? (totalReceita/totalVendas) : 0;
    el('kpiVendas').textContent = fmtBR(totalVendas);
    el('kpiReceita').textContent = fmtBRMoney(totalReceita);
    el('kpiTicket').textContent = fmtBRMoney(ticket);
    el('kpiDias').textContent = fmtBR(labels.length);
  }

  function preencherObservacoes(labels, vendas, receita){
    const ul = el('listaObservacoes');
    const frag = document.createDocumentFragment();
    ul.innerHTML = '';

    if(!labels.length){
      const li = document.createElement('li');
      li.textContent = 'Sem dados no período selecionado.';
      frag.appendChild(li);
      ul.appendChild(frag);
      return;
    }

    // Sem “dados fictícios”: observações simples derivadas dos dados reais
    const maxIdx = vendas.indexOf(Math.max(...vendas));
    const minIdx = vendas.indexOf(Math.min(...vendas));
    const li1 = document.createElement('li');
    li1.textContent = `Maior volume de vendas em ${labels[maxIdx]} (${fmtBR(vendas[maxIdx])} un.).`;
    frag.appendChild(li1);
    const li2 = document.createElement('li');
    li2.textContent = `Menor volume de vendas em ${labels[minIdx]} (${fmtBR(vendas[minIdx])} un.).`;
    frag.appendChild(li2);

    ul.appendChild(frag);
  }

  async function carregar(days){
    const c = ensureChart();
    if(!c) return;
    try{
      const data = await fetchPerformance(days);
      const labels = Array.isArray(data.labels) ? data.labels : [];
      const vendas = Array.isArray(data.vendas) ? data.vendas.map(Number) : [];
      const receita = Array.isArray(data.receita) ? data.receita.map(Number) : [];

      c.data.labels = labels;
      c.data.datasets[0].data = vendas;
      c.data.datasets[1].data = receita;
      c.update('active');

      preencherTabela(labels, vendas, receita);
      preencherKpis(labels, vendas, receita);
      preencherObservacoes(labels, vendas, receita);
      window.showNotification && window.showNotification('Relatório atualizado', 'success');
    } catch(err){
      console.error('Erro ao carregar performance:', err);
      window.showNotification && window.showNotification('Falha ao carregar dados', 'error');
      // zera
      c.data.labels = []; c.data.datasets[0].data = []; c.data.datasets[1].data = []; c.update('active');
      preencherTabela([],[],[]);
      preencherKpis([],[],[]);
      preencherObservacoes([],[],[]);
    }
  }

  // Eventos
  function bind(){
    const periodo = el('periodo');
    el('btnAplicar').addEventListener('click', ()=>{
      // Se datas customizadas forem usadas, por enquanto usa apenas o "periodo" (7/30/90)
      // Para intervalo arbitrário, precisamos expor no backend uma rota com start/end.
      const days = parseInt(periodo.value,10) || 30;
      carregar(days);
    });
    el('btnLimpar').addEventListener('click', ()=>{
      el('data_inicio').value = '';
      el('data_fim').value = '';
      el('periodo').value = '30';
      carregar(30);
    });
    el('btnRefresh').addEventListener('click', ()=>{
      const days = parseInt(el('periodo').value,10) || 30;
      carregar(days);
    });
    el('btnPng').addEventListener('click', ()=>{
      if(!chart) return;
      const url = chart.toBase64Image();
      const a = document.createElement('a');
      a.download = 'performance.png';
      a.href = url; a.click();
    });
    el('btnExport').addEventListener('click', ()=> window.showNotification && window.showNotification('Exportando...', 'info'));
    el('btnCompare').addEventListener('click', ()=> window.showNotification && window.showNotification('Comparação não configurada', 'warning'));
    el('btnDetails').addEventListener('click', ()=> window.location.href = '/vendas/ml/anuncios/listar');
  }

  document.addEventListener('DOMContentLoaded', ()=>{
    bind();
    carregar(30); // default
  });
})();
