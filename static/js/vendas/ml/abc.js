/* Curva ABC — Anúncios (Dashboard Integrado) — versão 4 */
(function () {
  "use strict";

  // NUNCA colida com 'BASE' global de outros arquivos.
  // Se existir window.ML_BASE ou window.ML?.BASE, usa; senão padroniza.
  const API_BASE =
    (typeof window !== "undefined" && (window.ML_BASE || (window.ML && window.ML.BASE))) ||
    "/vendas/ml";

  let abcChart = null;
  let abcRows = [];
  let tableSort = { key: null, dir: -1 }; // -1 desc
  let graphClassFilter = "all"; // all | A | B | C

  /* ================= Helpers ================= */
  function fmtMoney(v) {
    if (v === null || v === undefined || isNaN(v)) return "—";
    return Number(v).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
  }
  function fmtPct(v) {
    if (v === null || v === undefined || isNaN(v)) return "—";
    return (Number(v) * 100).toFixed(2) + "%";
  }
  function toISO(d) {
    const z = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${z(d.getMonth() + 1)}-${z(d.getDate())}`;
  }
  function isDark() {
    try {
      return (
        document.documentElement.getAttribute("data-theme") === "dark" ||
        document.body.classList.contains("dark") ||
        document.body.classList.contains("dark-theme")
      );
    } catch {
      return false;
    }
  }
  function axisColors() {
    return isDark()
      ? { grid: "rgba(255,255,255,0.12)", ticks: "#cfe9f2", line: "#63d7ff" }
      : { grid: "rgba(0,0,0,0.08)", ticks: "#083a55", line: "#00bfff" };
  }
  function hexToRgba(hex, a) {
    const h = (hex || "#00bfff").replace("#", "");
    const n = parseInt(h, 16);
    const r = (n >> 16) & 255,
      g = (n >> 8) & 255,
      b = n & 255;
    return `rgba(${r},${g},${b},${a})`;
  }

  /* ================= Filtros ================= */
  async function loadFilters() {
    try {
      const res = await fetch(`${API_BASE}/api/filters`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      const conta = document.getElementById("f-conta");
      if (conta && Array.isArray(data.contas)) {
        conta.innerHTML = `<option value="">Todas</option>`;
        for (const c of data.contas) {
          const o = document.createElement("option");
          o.value = c;
          o.textContent = c;
          conta.appendChild(o);
        }
      }

      const now = new Date();
      const start = new Date(now.getFullYear(), now.getMonth(), 1);
      const fStart = document.getElementById("f-start");
      const fEnd = document.getElementById("f-end");
      if (fStart) fStart.value = toISO(start);
      if (fEnd) fEnd.value = toISO(now);
    } catch (e) {
      console.error("[ABC] Falha ao carregar filtros:", e);
    }
  }

  function buildQuery() {
    const p = new URLSearchParams();
    const conta = document.getElementById("f-conta")?.value || "";
    const start = document.getElementById("f-start")?.value || "";
    const end = document.getElementById("f-end")?.value || "";
    const modo = document.getElementById("f-modo")?.value || "lucro"; // lucro|faturamento|unidades|margem
    const q = document.getElementById("f-q")?.value || "";
    if (conta) p.set("conta", conta);
    if (start) p.set("start", start);
    if (end) p.set("end", end);
    if (modo) p.set("mode", modo);
    if (q) p.set("q", q);
    return "?" + p.toString();
  }

  /* ================= Export ================= */
  function exportABC(fmt) {
    const base = `${API_BASE}/api/abc` + buildQuery();
    const url = base + (base.includes("?") ? "&" : "?") + "format=" + fmt;
    window.open(url, "_blank");
  }

  /* ============ Carregamento principal ============ */
  async function loadABC() {
    try {
      const url = `${API_BASE}/api/abc` + buildQuery();
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const js = await res.json();

      const rows = Array.isArray(js) ? js : Array.isArray(js.rows) ? js.rows : [];
      abcRows = rows.map(coerceRow);

      const empty = document.getElementById("abc-empty");
      if (empty) empty.style.display = abcRows.length ? "none" : "block";

      renderTable();
      renderPareto();
    } catch (e) {
      console.error("[ABC] Falha ao carregar ABC:", e);
      const empty = document.getElementById("abc-empty");
      if (empty) empty.style.display = "block";
    }
  }

  function coerceRow(r) {
    const unidades = Number(r.unidades ?? r.qty ?? 0);
    const faturamento = Number(r.faturamento_rs ?? r.faturamento ?? r.revenue_rs ?? 0);
    const lucro = Number(r.lucro_rs ?? r.lucro ?? r.profit_rs ?? 0);
    const margem_pct = Number(r.margem_pct ?? r.margin_pct ?? r.margem ?? 0);
    const frete_seller_rs = Number(r.frete_seller_rs ?? r.seller_shipping_rs ?? 0);
    const frete_pct = Number(r.frete_pct ?? r.shipping_pct ?? 0);
    const pct_acum = Number(r.pct_acum ?? r.cumulated_pct ?? 0);
    const ticket_medio_rs = unidades > 0 ? faturamento / unidades : 0;
    const delta_pct = Number(r.delta_pct ?? 0);

    return {
      sku: r.sku ?? r.SKU ?? "",
      mlb: r.mlb ?? r.MLB ?? "",
      titulo: r.titulo ?? r.title ?? "",
      unidades,
      ticket_medio_rs,
      faturamento_rs: faturamento,
      lucro_rs: lucro,
      margem_pct,
      frete_seller_rs,
      frete_pct,
      pct_acum,
      classe: r.classe ?? r.class ?? "",
      delta_pct
    };
  }

  /* ================= Tabela (sortable) ================= */
  let tableBound = false;

  function bindTableSort() {
    if (tableBound) return;
    const keys = [
      "sku",
      "mlb",
      "titulo",
      "unidades",
      "ticket_medio_rs",
      "faturamento_rs",
      "lucro_rs",
      "margem_pct",
      "frete_seller_rs",
      "frete_pct",
      "pct_acum",
      "classe"
    ];
    document.querySelectorAll("#tbl-abc thead th").forEach((th, i) => {
      th.style.cursor = "pointer";
      th.addEventListener("click", () => {
        const key = keys[i];
        if (tableSort.key === key) {
          tableSort.dir = -tableSort.dir;
        } else {
          tableSort.key = key;
          tableSort.dir = -1;
        }
        renderTable();
      });
    });
    tableBound = true;
  }

  function getValue(row, key) {
    if (key === "ticket_medio_rs") return Number(row.ticket_medio_rs || 0);
    const numeric =
      key.endsWith("_rs") || key.endsWith("_pct") || key === "unidades" || key === "pct_acum";
    return numeric ? Number(row[key] || 0) : row[key] ?? "";
  }

  function renderTable() {
    const tb = document.querySelector("#tbl-abc tbody");
    if (!tb) return;
    let rows = abcRows.slice();

    if (tableSort.key) {
      const dir = tableSort.dir,
        key = tableSort.key;
      rows.sort((a, b) => {
        const av = getValue(a, key);
        const bv = getValue(b, key);
        if (av === bv) return 0;
        return (av < bv ? -1 : 1) * dir;
      });
    }

    const idx80 = findParetoIndex(abcRows);
    const key80 = idx80 >= 0 ? abcRows[idx80].sku || abcRows[idx80].mlb || "" : "";

    tb.innerHTML = "";
    for (const r of rows) {
      const tr = document.createElement("tr");
      const is80 = key80 && key80 === (r.sku || r.mlb || "");
      if (is80) {
        tr.style.outline = "2px dashed #00bfff";
        tr.style.outlineOffset = "-2px";
      }
      tr.innerHTML = `
        <td>${r.sku || "—"}</td>
        <td>${r.mlb || "—"}</td>
        <td>${r.titulo || "—"}</td>
        <td>${Number(r.unidades || 0)}</td>
        <td>${fmtMoney(r.ticket_medio_rs || 0)}</td>
        <td>${fmtMoney(r.faturamento_rs || 0)}</td>
        <td>${fmtMoney(r.lucro_rs || 0)}</td>
        <td>${fmtPct(r.margem_pct || 0)}</td>
        <td>${fmtMoney(r.frete_seller_rs || 0)}</td>
        <td>${fmtPct(r.frete_pct || 0)}</td>
        <td>${fmtPct(r.pct_acum || 0)}</td>
        <td><span class="badge-cls badge-${r.classe}">${r.classe || "—"}</span></td>
      `;
      tb.appendChild(tr);
    }
  }

  function findParetoIndex(rows) {
    for (let i = 0; i < rows.length; i++) {
      if (Number(rows[i].pct_acum || 0) >= 0.8) return i;
    }
    return -1;
  }
  function findParetoIndex95(rows) {
    for (let i = 0; i < rows.length; i++) {
      if (Number(rows[i].pct_acum || 0) >= 0.95) return i;
    }
    return -1;
  }

  /* ================= Gráfico Pareto ================= */
  function ensureClassTabs() {
    const header =
      document.querySelector("#ml-ABC .card-header") ||
      document.querySelector("#ml-abc .card-header") ||
      document.querySelector("#chart-abc")?.closest(".card")?.querySelector(".card-header");

    if (!header || header.querySelector(".abc-tabs")) return;

    const wrap = document.createElement("div");
    wrap.className = "abc-tabs";
    wrap.style.display = "flex";
    wrap.style.gap = "8px";
    wrap.style.marginLeft = "auto";

    const mk = (val, label) => {
      const b = document.createElement("button");
      b.type = "button";
      b.dataset.abc = val;
      b.textContent = label;
      b.style.borderRadius = "999px";
      b.style.padding = "6px 12px";
      b.style.fontWeight = "800";
      b.style.border = "1px solid #00bfff";
      b.style.background = "transparent";
      b.style.cursor = "pointer";
      b.addEventListener("click", () => {
        graphClassFilter = val;
        [...wrap.querySelectorAll("button")].forEach((x) => (x.style.background = "transparent"));
        b.style.background = "#00bfff22";
        renderPareto();
      });
      return b;
    };

    wrap.appendChild(mk("all", "Todos"));
    wrap.appendChild(mk("A", "Classe A"));
    wrap.appendChild(mk("B", "Classe B"));
    wrap.appendChild(mk("C", "Classe C"));
    header.appendChild(wrap);
    wrap.querySelector('button[data-abc="all"]').style.background = "#00bfff22";
  }

  function getMode() {
    return document.getElementById("f-modo")?.value || "lucro";
  }
  function valueByMode(r) {
    const m = getMode();
    if (m === "faturamento") return Number(r.faturamento_rs || 0);
    if (m === "unidades") return Number(r.unidades || 0);
    if (m === "margem") return Number(r.margem_pct || 0) * Number(r.faturamento_rs || 0);
    return Number(r.lucro_rs || 0); // lucro
  }
  function classAlpha(r) {
    return r.classe === "A" ? 1 : r.classe === "B" ? 0.75 : 0.55;
  }
  function colorByDelta(r) {
    const d = typeof r.delta_pct === "number" ? r.delta_pct : 0;
    const base = d > 0.02 ? "#00bfff" : d < -0.02 ? "#ff8a65" : "#ffe600";
    return hexToRgba(base, classAlpha(r));
  }

  function renderPareto() {
    ensureClassTabs();

    const el = document.getElementById("chart-abc");
    if (!el) return;
    el.style.height = "420px";
    el.setAttribute("height", "420");

    let rows = abcRows.slice(); // ranking original
    if (graphClassFilter !== "all") rows = rows.filter((r) => r.classe === graphClassFilter);

    const modo = getMode();
    const labels = rows.map(
      (r) => `${r.sku || r.mlb || "(sem id)"}${r.classe ? ` (${r.classe})` : ""}`
    );
    const valores = rows.map((r) => valueByMode(r));

    // acumulado em %
    const total = valores.reduce((s, v) => s + Number(v || 0), 0) || 1;
    let run = 0;
    const acumulado = valores.map((v) => {
      run += Number(v || 0);
      return (run / total) * 100;
    });

    // índices A/B/C do conjunto original
    const idx80 = findParetoIndex(abcRows);
    const idx95 = findParetoIndex95(abcRows);

    const colors = rows.map((r) => colorByDelta(r));
    const axes = axisColors();

    // plugin de faixas + linha 80%
    const bandsPlugin = {
      id: "abcBands",
      beforeDatasetsDraw(chart) {
        if (graphClassFilter !== "all") return;
        if (idx80 < 0) return;
        const { ctx, chartArea, scales } = chart;
        const x = scales.x;
        const left = chartArea.left,
          right = chartArea.right,
          top = chartArea.top,
          bottom = chartArea.bottom;
        const toX = (i) => x.getPixelForTick(Math.max(0, Math.min(i, x.ticks.length - 1)));

        ctx.save();
        // A
        ctx.fillStyle = isDark() ? "rgba(0,191,255,0.08)" : "rgba(0,191,255,0.10)";
        ctx.fillRect(left, top, toX(idx80) - left, bottom - top);
        // B
        if (idx95 > idx80) {
          ctx.fillStyle = isDark() ? "rgba(255,230,0,0.08)" : "rgba(255,230,0,0.12)";
          ctx.fillRect(toX(idx80 + 1), top, toX(idx95) - toX(idx80 + 1), bottom - top);
        }
        // C
        ctx.fillStyle = isDark() ? "rgba(255,138,101,0.06)" : "rgba(255,138,101,0.08)";
        ctx.fillRect(toX(idx95 + 1), top, right - toX(idx95 + 1), bottom - top);

        // linha 80%
        const x80 = x.getPixelForTick(Math.min(idx80, x.ticks.length - 1));
        ctx.setLineDash([6, 6]);
        ctx.strokeStyle = axes.line;
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(x80, top);
        ctx.lineTo(x80, bottom);
        ctx.stroke();

        // ponto 80% na escala direita
        const y = scales.y2 || scales.y1 || scales.y;
        const y80 = y.getPixelForValue(80);
        ctx.setLineDash([]);
        ctx.fillStyle = axes.line;
        ctx.beginPath();
        ctx.arc(x80, y80, 4, 0, 2 * Math.PI);
        ctx.fill();

        ctx.restore();
      }
    };

    // Chart.js presente?
    if (typeof Chart === "undefined") {
      console.error("[ABC] Chart.js não foi carregado antes do abc.js.");
      return;
    }

    if (abcChart) abcChart.destroy();
    abcChart = new Chart(el, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            type: "bar",
            label:
              modo === "faturamento"
                ? "Faturamento (R$)"
                : modo === "unidades"
                ? "Unidades"
                : modo === "margem"
                ? "Margem ponderada"
                : "Lucro (R$)",
            data: valores,
            backgroundColor: colors,
            yAxisID: "y1",
            barThickness: 24,
            borderWidth: 0
          },
          {
            type: "line",
            label: "Acumulado (%)",
            data: acumulado,
            yAxisID: "y2",
            tension: 0.3,
            borderWidth: 2,
            pointRadius: 2,
            borderColor: axes.line,
            pointBackgroundColor: axes.line
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y1: {
            position: "left",
            grid: { color: axes.grid },
            ticks: {
              color: axes.ticks,
              callback: (v) => (getMode() === "unidades" ? v : fmtMoney(v))
            }
          },
          y2: {
            position: "right",
            min: 0,
            max: 100,
            grid: { display: false },
            ticks: { color: axes.ticks, callback: (v) => v + "%" }
          },
          x: {
            grid: { display: false },
            ticks: { color: axes.ticks, autoSkip: false, maxRotation: 40, minRotation: 0 }
          }
        },
        plugins: {
          legend: { position: "top", labels: { color: axes.ticks } },
          tooltip: {
            callbacks: {
              label: (ctx) => {
                const ds = ctx.dataset;
                const raw = ctx.raw;
                if (ds.yAxisID === "y2") return `${ds.label}: ${Number(raw).toFixed(0)}%`;
                const row = abcRows.filter(
                  (r) =>
                    `${r.sku || r.mlb || "(sem id)"}${r.classe ? ` (${r.classe})` : ""}` ===
                    ctx.label
                )[0];
                const delta = ((row?.delta_pct || 0) * 100).toFixed(0);
                const deltaTxt = (row?.delta_pct || 0) > 0 ? `+${delta}%` : `${delta}%`;
                const modo = getMode();
                if (modo === "unidades") return `${ds.label}: ${raw} (Δ ${deltaTxt})`;
                if (modo === "margem") return `${ds.label}: ${Number(raw).toFixed(2)} (Δ ${deltaTxt})`;
                return `${ds.label}: ${fmtMoney(raw)} (Δ ${deltaTxt})`;
              },
              title: (items) => {
                const label = items[0].label;
                const row = abcRows.filter(
                  (r) => `${r.sku || r.mlb || "(sem id)"}${r.classe ? ` (${r.classe})` : ""}` === label
                )[0];
                return `${row?.sku || row?.mlb || "(sem id)"} — Classe ${row?.classe || "?"}`;
              }
            }
          }
        }
      },
      plugins: [bandsPlugin]
    });
  }

  /* ================= Topbar ================= */
  function bindTopbar() {
    document.getElementById("btn-aplicar")?.addEventListener("click", loadABC);
    document.getElementById("f-q")?.addEventListener("keyup", (e) => {
      if (e.key === "Enter") loadABC();
    });
    document.getElementById("btn-export-abc-csv")?.addEventListener("click", () => exportABC("csv"));
    document
      .getElementById("btn-export-abc-xlsx")
      ?.addEventListener("click", () => exportABC("xlsx"));
  }

  /* ========== Observa tema (MutationObserver) ========== */
  function watchTheme() {
    try {
      const obs = new MutationObserver((muts) => {
        for (const m of muts) {
          if (m.type === "attributes" && m.attributeName === "class") {
            renderPareto();
            break;
          }
        }
      });
      obs.observe(document.body, { attributes: true });
    } catch {
      /* ignora */
    }
  }

  /* ================= Boot ================= */
  async function init() {
    await loadFilters();
    bindTopbar();
    bindTableSort();
    watchTheme();
    await loadABC();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
