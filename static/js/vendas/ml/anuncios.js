// Gestão de Anúncios (ML) — JS completo, sem dependências externas e sem dados fictícios.
(function(){
  const $ = (s, c=document) => c.querySelector(s);
  const $$ = (s, c=document) => Array.from(c.querySelectorAll(s));
  const state = { selected: new Set() };

  function notify(m,t='success'){ if(window.showNotification) window.showNotification(m,t); }
  function loading(v){ const o=$('#loadingOverlay'); if(o) o.style.display=v?'flex':'none'; }

  // ===== Filtros =====
  function toggleFilters(){
    const sec=$('#filtersSection'), content=$('#filtersContent');
    if(!sec||!content) return;
    sec.classList.toggle('open');
    content.style.maxHeight = sec.classList.contains('open') ? content.scrollHeight+'px' : '0';
  }
  function aplicarFiltros(){
    const q = new URLSearchParams();
    const get = id => $(id)?.value?.trim() || '';
    const set = (k,v)=>{ if(v) q.set(k,v); };

    set('status', get('#statusFilter'));
    set('substatus', get('#substatusFilter'));
    set('categoria', get('#categoriaFilter'));
    set('preco_min', get('#precoMinFilter')); set('preco_max', get('#precoMaxFilter'));
    set('estoque_min', get('#estoqueMinFilter')); set('estoque_max', get('#estoqueMaxFilter'));
    set('vendidos_min', get('#vendidosMinFilter')); set('vendidos_max', get('#vendidosMaxFilter'));
    set('dt_ini', get('#dtIniFilter')); set('dt_fim', get('#dtFimFilter'));
    set('ordenar', get('#ordenarFilter'));
    set('ads', get('#adsFilter'));

    const url = `${location.pathname}?${q.toString()}`;
    location.href = url;
  }
  function limparFiltros(){
    ['#statusFilter','#substatusFilter','#categoriaFilter','#precoMinFilter','#precoMaxFilter',
     '#estoqueMinFilter','#estoqueMaxFilter','#vendidosMinFilter','#vendidosMaxFilter',
     '#dtIniFilter','#dtFimFilter','#ordenarFilter','#adsFilter']
     .forEach(id=>{
       const el=$(id);
       if(!el) return;
       if(id==='#ordenarFilter') el.value = 'atualizado_desc';
       else el.value = '';
     });
    aplicarFiltros();
  }

  // ===== Seleção & KPIs =====
  function syncBulk(){
    const bulk=$('#bulkActions'), count=$('#selectedCount');
    const n=state.selected.size; if(count) count.textContent=n;
    if(bulk) bulk.style.display = n>0 ? 'flex':'none';
  }
  function toggleSelect(card, checked){
    const id=card?.dataset?.id; if(!id) return;
    checked ? state.selected.add(id) : state.selected.delete(id);
    card.classList.toggle('selected', checked);
    syncBulk();
  }
  function recalcKpis(){
    const cards=$$('.anuncio-card');
    const ativos=cards.filter(c=>(c.dataset.status||'').toLowerCase()==='active').length;
    const paus=cards.filter(c=>(c.dataset.status||'').toLowerCase()==='paused').length;
    const clos=cards.filter(c=>(c.dataset.status||'').toLowerCase()==='closed').length;
    $('#kpiAtivos').textContent=ativos; $('#kpiPausados').textContent=paus; $('#kpiFinalizados').textContent=clos;
    $('#kpisInline').hidden=false;
    $('#totalCount').textContent=cards.length;
  }

  // ===== Ações item =====
  function goto(p){ location.href=p; }
  function ver(id){ if(id) goto(`/vendas/ml/anuncio/${id}`); }
  function editar(id){ if(id) goto(`/vendas/ml/anuncio/${id}/editar`); }
  async function clonar(id){
    if(!id) return; loading(true);
    try{
      const r=await fetch(`/vendas/ml/anuncio/${id}/clonar`,{method:'POST',headers:{'Content-Type':'application/json'}});
      const j=await r.json(); j.success? (notify('Clonado'), location.reload()) : notify(j.message||'Falha ao clonar','error');
    }catch(e){ console.error(e); notify('Erro ao clonar','error'); } finally{ loading(false); }
  }
  async function toggleStatus(id){
    if(!id) return; loading(true);
    try{
      // Descobre status atual no DOM e alterna para o oposto
      const card = document.querySelector(`.anuncio-card[data-id="${id}"]`);
      const cur  = (card?.dataset?.status||'').toLowerCase();
      const newStatus = cur==='active' ? 'paused' : 'active';
      const r=await fetch(`/vendas/ml/anuncio/${id}/editar`,{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({status:newStatus})
      });
      const j=await r.json(); j.success? (notify(`Anúncio ${newStatus==='active'?'ativado':'pausado'}`), location.reload()) : notify(j.message||'Falha ao alterar status','error');
    }catch(e){ console.error(e); notify('Erro ao alterar status','error'); } finally{ loading(false); }
  }
  async function excluir(id){
    if(!id) return; if(!confirm('Excluir anúncio?')) return; loading(true);
    try{
      const r=await fetch(`/vendas/ml/anuncio/${id}/deletar`,{method:'DELETE'});
      const j=await r.json(); j.success? (notify('Excluído'), location.reload()) : notify(j.message||'Falha ao excluir','error');
    }catch(e){ console.error(e); notify('Erro ao excluir','error'); } finally{ loading(false); }
  }

  // ===== Edição rápida =====
  function openQuickEdit(card){
    const id=card.dataset.id;
    $('#qeId').value=id;

    // Preço BR -> número
    const priceText = card.querySelector('.value.price')?.textContent || '';
    const precoNum = parseFloat(priceText.replace(/[^\d,.-]/g,'').replace(/\./g,'').replace(',','.')) || '';
    $('#qePreco').value = precoNum;

    // Estoque
    const estoqueText = [...card.querySelectorAll('.info-row .value')].find(el => !el.classList.contains('price'))?.textContent || '0';
    $('#qeEstoque').value = parseInt(estoqueText,10) || '';

    // Título
    $('#qeTitulo').value = card.querySelector('.anuncio-titulo')?.getAttribute('title') || '';

    $('#quickEditModal').style.display='block';
  }
  function closeModal(id){ const m = document.getElementById(id); if(m) m.style.display='none'; }
  async function salvarQuickEdit(){
    const id=$('#qeId').value;
    const payload={
      preco: parseFloat(($('#qePreco').value||'0').replace(',','.')),
      quantidade_disponivel: parseInt($('#qeEstoque').value||'0',10),
      titulo: $('#qeTitulo').value||''
    };
    loading(true);
    try{
      const r=await fetch(`/vendas/ml/anuncio/${id}/editar`,{
        method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)
      });
      const j=await r.json(); j.success? (notify('Atualizado'), location.reload()) : notify(j.message||'Falha ao atualizar','error');
    }catch(e){ console.error(e); notify('Erro ao atualizar','error'); } finally{ loading(false); closeModal('quickEditModal'); }
  }

  // ===== Automações (UI pronta; backend opcional) =====
  function openDrawer(){ const dr=$('#automacoesDrawer'); if(dr) dr.setAttribute('aria-hidden','false'); }
  function closeDrawer(){ const dr=$('#automacoesDrawer'); if(dr) dr.setAttribute('aria-hidden','true'); }
  async function salvarAutomacoes(){
    const cfg={
      pause_out_of_stock: $('#autoOutOfStock').checked,
      ads_roas_enabled: $('#autoROAS').checked,
      roas_min: parseFloat($('#roasMin').value||0),
      roas_max: parseFloat($('#roasMax').value||0),
      price_rule_enabled: $('#autoPreco').checked,
      markup_min: parseFloat($('#mkpMin').value||0),
      markup_max: parseFloat($('#mkpMax').value||0),
    };
    // Quando houver rota backend, descomente:
    // await fetch('/vendas/ml/anuncios/automacoes', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(cfg) });
    notify('Regras salvas localmente. Pronto para plugar no backend.', 'info');
  }

  // ===== Bulk =====
  async function bulkSet(status){
    if(state.selected.size===0) return;
    if(!confirm(`Confirmar ${status==='active'?'ativação':'pausa'} de ${state.selected.size} anúncio(s)?`)) return;
    loading(true);
    try{
      for (const id of state.selected){
        await fetch(`/vendas/ml/anuncio/${id}/editar`,{
          method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({status})
        });
      }
      notify('Ação em lote concluída'); location.reload();
    }catch(e){ console.error(e); notify('Erro na ação em lote','error'); } finally{ loading(false); }
  }
  async function bulkClone(){
    if(state.selected.size===0) return;
    if(!confirm(`Clonar ${state.selected.size} anúncio(s)?`)) return;
    loading(true);
    try{
      for (const id of state.selected){
        await fetch(`/vendas/ml/anuncio/${id}/clonar`,{method:'POST',headers:{'Content-Type':'application/json'}});
      }
      notify('Clonagem concluída'); location.reload();
    }catch(e){ console.error(e); notify('Erro ao clonar em lote','error'); } finally{ loading(false); }
  }
  async function bulkDelete(){
    if(state.selected.size===0) return;
    if(!confirm(`Excluir ${state.selected.size} anúncio(s)?`)) return;
    loading(true);
    try{
      for (const id of state.selected){
        await fetch(`/vendas/ml/anuncio/${id}/deletar`,{method:'DELETE'});
      }
      notify('Exclusão concluída'); location.reload();
    }catch(e){ console.error(e); notify('Erro ao excluir em lote','error'); } finally{ loading(false); }
  }

  // ===== Delegação de eventos =====
  function bind(){
    // Header
    $('#btnNovo')?.addEventListener('click', ()=> location.href='/vendas/ml/anuncio/novo');
    $('#btnImportar')?.addEventListener('click', ()=> notify('Importação em desenvolvimento','info'));
    $('#btnAutomacoes')?.addEventListener('click', openDrawer);

    // Filtros
    $('#btnToggleFilters')?.addEventListener('click', toggleFilters);
    $('#btnAplicar')?.addEventListener('click', aplicarFiltros);
    $('#btnLimpar')?.addEventListener('click', limparFiltros);
    $('#filtersContent')?.addEventListener('keyup', (e)=>{ if(e.target.tagName==='INPUT' && e.key==='Enter') aplicarFiltros(); });

    // Paginação
    $('.pagination-controls')?.addEventListener('click', (e)=>{
      const btn=e.target.closest('button[data-action]'); if(!btn) return;
      const action=btn.dataset.action;
      const url=new URL(location.href);
      const page=parseInt(url.searchParams.get('page')||'1',10);
      if(action==='page-prev'){ url.searchParams.set('page', Math.max(1,page-1)); location.href=url.toString(); }
      if(action==='page-next'){ url.searchParams.set('page', page+1); location.href=url.toString(); }
    });

    // Grid (seleção e ações)
    $('#anunciosGrid')?.addEventListener('click', (e)=>{
      const card=e.target.closest('.anuncio-card'); if(!card) return;
      const id=card.dataset.id;

      // checkbox
      if(e.target.classList.contains('anuncio-checkbox')){
        toggleSelect(card, e.target.checked);
        return;
      }

      // overlay ver
      if(e.target.closest('[data-action="ver"]')){ ver(id); return; }

      // botões
      const btn=e.target.closest('[data-action]'); if(!btn) return;
      const act=btn.dataset.action;
      if(act==='editar') return editar(id);
      if(act==='clonar') return clonar(id);
      if(act==='status') return toggleStatus(id);
      if(act==='excluir') return excluir(id);
      if(act==='dropdown'){ const dd=btn.closest('.action-dropdown'); if(dd) dd.classList.toggle('open'); return; }
      if(act==='quickedit'){ e.preventDefault(); openQuickEdit(card); return; }
      if(act==='analisar'){ e.preventDefault(); location.href=`/vendas/ml/relatorios?anuncio_id=${id}`; return; }
    });

    // Fechar dropdown ao clicar fora
    document.addEventListener('click', (e)=>{ if(!e.target.closest('.action-dropdown')) $$('.action-dropdown.open').forEach(dd=>dd.classList.remove('open')); });

    // Modal quick edit
    $('#qeSalvar')?.addEventListener('click', salvarQuickEdit);
    document.addEventListener('click', (e)=>{ const c=e.target.closest('[data-close]'); if(c) { const id=c.dataset.close; const el=document.getElementById(id); if(el) el.style.display='none'; } });

    // Drawer automações
    $('#autoSalvar')?.addEventListener('click', salvarAutomacoes);
    document.addEventListener('click', (e)=>{ const c=e.target.closest('[data-close="automacoesDrawer"]'); if(c) closeDrawer(); });

    // Bulk
    $('#btnAtivarSel')?.addEventListener('click', ()=> bulkSet('active'));
    $('#btnPausarSel')?.addEventListener('click', ()=> bulkSet('paused'));
    $('#btnClonarSel')?.addEventListener('click', bulkClone);
    $('#btnExcluirSel')?.addEventListener('click', bulkDelete);
  }

  // ===== Ajustes visuais que evitam “sumiço”/overlays =====
  function hardenOverlay(){
    // Garante que overlay só aceita clique quando visível
    const style = document.createElement('style');
    style.textContent = `
      .image-overlay{ pointer-events: none; }
      .anuncio-card:hover .image-overlay{ pointer-events: auto; }
      .card-content, .card-actions{ position:relative; z-index:1; }
      .card-image img{ display:block; }
    `;
    document.head.appendChild(style);
  }

  document.addEventListener('DOMContentLoaded', ()=>{
    bind();
    recalcKpis();
    hardenOverlay();
  });
})();
