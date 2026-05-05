/* Data Mining Pro UI polish and productivity layer */
(function () {
  const moduleCopy = {
    ai: {
      nav: "AI Data Analyst",
      icon: "AI",
      eyebrow: "AI-Powered Analytics",
      title: "Upload, clean, and analyze with AI.",
      desc: "Auto-cleaning, Plotly visualizations, ML metrics, and data chat in one workspace.",
      dropTitle: "Drop CSV or Excel data",
      dropDesc: "Auto-cleaning, outlier detection, and instant insight cards.",
      demo: "Run Demo"
    },
    etl: {
      nav: "Smart ETL",
      icon: "ETL",
      eyebrow: "Automated ETL Pipeline",
      title: "Extract, transform, load, and model.",
      desc: "Detect schema, clean data, engineer features, train models, and export clean CSV.",
      dropTitle: "Drop a CSV file",
      dropDesc: "Encoding detection, imputation, profiling, and SQLite output.",
      demo: "Run Demo"
    },
    bi: {
      nav: "BI Reporter",
      icon: "BI",
      eyebrow: "Business Intelligence",
      title: "Turn raw data into reports.",
      desc: "Create executive KPIs, charts, ML prediction notes, and a downloadable Excel report.",
      dropTitle: "Drop CSV or Excel data",
      dropDesc: "Formatted workbook, charts, statistics, and Power BI-ready sheets.",
      demo: "Generate Demo Report"
    },
    sql: {
      nav: "SQL Analytics",
      icon: "SQL",
      eyebrow: "SQL Analytics Engine",
      title: "Load files and query them live.",
      desc: "Auto-schema detection, SQLite loading, generated analytical queries, and a SQL editor.",
      dropTitle: "Drop CSV or Excel data",
      dropDesc: "Auto-load into SQLite with query templates and exports.",
      demo: "Run Demo Dataset"
    }
  };

  const originalAlert = window.alert;
  window.alert = function (message) {
    toast(String(message || "Something happened"), "danger");
    if (window.console) console.warn(message);
  };

  document.addEventListener("DOMContentLoaded", () => {
    document.title = "Data Mining Pro | AI Analytics, ETL, BI Reports, SQL";
    polishStaticCopy();
    addHeaderTools();
    addSidebarWorkflow();
    addMobileModuleBar();
    enhanceDropZones();
    patchTabs();
    watchTables();
    installKeyboardShortcuts();
    fetchProjectStatus();
  });

  function polishStaticCopy() {
    const logo = document.querySelector(".logo-name");
    if (logo) logo.textContent = "Data Mining Pro";
    const tag = document.querySelector(".logo-tag");
    if (tag) tag.textContent = "UNIFIED";
    const byline = document.querySelector(".header-right span");
    if (byline) byline.textContent = "Production analytics workspace";

    Object.entries(moduleCopy).forEach(([id, copy]) => {
      const nav = document.getElementById("nav-" + id);
      if (nav) nav.innerHTML = `<span class="sb-icon">${copy.icon}</span>${copy.nav}`;

      const mod = document.getElementById("mod-" + id);
      if (!mod) return;
      const hero = mod.querySelector(".upload-hero");
      const drop = mod.querySelector(".drop-zone");
      const demoBtn = mod.querySelector(".upload-wrap > .btn-primary");
      if (hero) {
        const eyebrow = hero.querySelector(".eyebrow");
        const h1 = hero.querySelector("h1");
        const p = hero.querySelector("p");
        if (eyebrow) eyebrow.textContent = copy.eyebrow;
        if (h1) h1.textContent = copy.title;
        if (p) p.textContent = copy.desc;
      }
      if (drop) {
        const icon = drop.querySelector(".drop-icon");
        const h2 = drop.querySelector("h2");
        const p = drop.querySelector("p");
        if (icon) icon.textContent = copy.icon;
        if (h2) h2.textContent = copy.dropTitle;
        if (p) p.textContent = copy.dropDesc;
      }
      if (demoBtn) demoBtn.textContent = copy.demo;
    });

    const infoItems = document.querySelectorAll(".sidebar .sb-divider ~ .sb-item");
    if (infoItems[0]) infoItems[0].innerHTML = `<span class="sb-icon">FILES</span> CSV, XLSX, XLS`;
    if (infoItems[1]) infoItems[1].innerHTML = `<span class="sb-icon">MAX</span> 500MB upload`;
  }

  function addHeaderTools() {
    const right = document.querySelector(".header-right");
    if (!right) return;
    right.insertAdjacentHTML("beforeend", `
      <button class="icon-btn" id="theme-toggle" title="Switch theme" type="button">Theme</button>
      <div class="system-pill" id="system-pill"><span class="pulse"></span><span>Checking</span></div>
    `);
    document.getElementById("theme-toggle").addEventListener("click", () => {
      document.body.classList.toggle("light-mode");
      localStorage.setItem("dmp-theme", document.body.classList.contains("light-mode") ? "light" : "dark");
    });
    if (localStorage.getItem("dmp-theme") === "light") document.body.classList.add("light-mode");
  }

  function addSidebarWorkflow() {
    const sidebar = document.querySelector(".sidebar");
    if (!sidebar) return;
    sidebar.insertAdjacentHTML("beforeend", `
      <div class="sb-divider"></div>
      <div class="sb-label">Workflow</div>
      <div class="workflow-card">
        <div><strong>1</strong><span>Upload</span></div>
        <div><strong>2</strong><span>Profile</span></div>
        <div><strong>3</strong><span>Analyze</span></div>
        <div><strong>4</strong><span>Export</span></div>
      </div>
      <div class="data-files" id="data-files"></div>
    `);
  }

  function addMobileModuleBar() {
    const layout = document.querySelector(".app-layout");
    if (!layout) return;
    const bar = document.createElement("div");
    bar.className = "mobile-modules";
    bar.innerHTML = Object.entries(moduleCopy).map(([id, copy]) =>
      `<button type="button" data-module="${id}" class="${id === "ai" ? "active" : ""}">${copy.icon}</button>`
    ).join("");
    layout.appendChild(bar);
    bar.addEventListener("click", (event) => {
      const btn = event.target.closest("[data-module]");
      if (!btn) return;
      switchModule(btn.dataset.module);
      syncMobileBar(btn.dataset.module);
    });

    const oldSwitch = window.switchModule;
    window.switchModule = function (mod) {
      oldSwitch(mod);
      syncMobileBar(mod);
    };
  }

  function syncMobileBar(mod) {
    document.querySelectorAll(".mobile-modules button").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.module === mod);
    });
  }

  function enhanceDropZones() {
    document.querySelectorAll(".drop-zone").forEach((zone) => {
      if (zone.querySelector(".file-meta")) return;
      zone.insertAdjacentHTML("beforeend", `<div class="file-meta">No file selected</div>`);
      const input = zone.querySelector("input[type=file]");
      if (!input) return;
      input.addEventListener("change", () => {
        const file = input.files && input.files[0];
        const meta = zone.querySelector(".file-meta");
        if (file && meta) meta.textContent = `${file.name} | ${formatBytes(file.size)}`;
      });
    });
  }

  function patchTabs() {
    document.querySelectorAll(".tabs").forEach((tabs) => {
      tabs.setAttribute("role", "tablist");
      tabs.querySelectorAll(".tab").forEach((tab) => tab.setAttribute("type", "button"));
    });
  }

  function watchTables() {
    const observer = new MutationObserver(() => enhanceTables());
    observer.observe(document.body, { childList: true, subtree: true });
    enhanceTables();
  }

  function enhanceTables() {
    document.querySelectorAll(".tbl-wrap").forEach((wrap) => {
      if (wrap.dataset.enhanced || !wrap.querySelector("table")) return;
      wrap.dataset.enhanced = "true";
      const toolbar = document.createElement("div");
      toolbar.className = "table-tools";
      toolbar.innerHTML = `
        <input type="search" placeholder="Search table" aria-label="Search table">
        <button type="button">Export CSV</button>
      `;
      wrap.parentNode.insertBefore(toolbar, wrap);
      const input = toolbar.querySelector("input");
      const button = toolbar.querySelector("button");
      input.addEventListener("input", () => filterTable(wrap, input.value));
      button.addEventListener("click", () => exportTable(wrap));
    });
  }

  function filterTable(wrap, term) {
    const q = term.trim().toLowerCase();
    wrap.querySelectorAll("tbody tr").forEach((row) => {
      row.style.display = !q || row.textContent.toLowerCase().includes(q) ? "" : "none";
    });
  }

  function exportTable(wrap) {
    const table = wrap.querySelector("table");
    if (!table) return;
    const rows = Array.from(table.querySelectorAll("tr")).map((row) =>
      Array.from(row.children).map((cell) => `"${cell.textContent.replace(/"/g, '""')}"`).join(",")
    );
    const blob = new Blob([rows.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `data-mining-pro-${Date.now()}.csv`;
    link.click();
    URL.revokeObjectURL(url);
    toast("Table exported as CSV", "success");
  }

  function installKeyboardShortcuts() {
    document.addEventListener("keydown", (event) => {
      if (event.target.matches("input, textarea, select")) return;
      const map = { "1": "ai", "2": "etl", "3": "bi", "4": "sql" };
      if (map[event.key]) switchModule(map[event.key]);
    });
  }

  function fetchProjectStatus() {
    fetch("/api/project-status")
      .then((res) => res.json())
      .then((data) => {
        const pill = document.getElementById("system-pill");
        if (pill) pill.innerHTML = `<span class="pulse"></span><span>Ready</span>`;
        renderDataFiles(data.local_data_files || []);
      })
      .catch(() => {
        const pill = document.getElementById("system-pill");
        if (pill) {
          pill.classList.add("warn");
          pill.innerHTML = `<span class="pulse"></span><span>Offline</span>`;
        }
      });
  }

  function renderDataFiles(files) {
    const box = document.getElementById("data-files");
    if (!box) return;
    if (!files.length) {
      box.innerHTML = `<div class="data-file-empty">No local CSV/DB files found.</div>`;
      return;
    }
    box.innerHTML = `<div class="sb-label mini">Local Data</div>` + files.slice(0, 4).map((file) =>
      `<div class="data-file"><span>${file.name}</span><em>${file.size_mb} MB</em></div>`
    ).join("");
  }

  function toast(message, type) {
    let tray = document.querySelector(".toast-tray");
    if (!tray) {
      tray = document.createElement("div");
      tray.className = "toast-tray";
      document.body.appendChild(tray);
    }
    const item = document.createElement("div");
    item.className = `toast ${type || ""}`;
    item.textContent = message;
    tray.appendChild(item);
    setTimeout(() => item.classList.add("show"), 20);
    setTimeout(() => {
      item.classList.remove("show");
      setTimeout(() => item.remove(), 250);
    }, 3600);
  }

  function formatBytes(bytes) {
    if (!bytes) return "0 B";
    const units = ["B", "KB", "MB", "GB"];
    const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    return `${(bytes / Math.pow(1024, i)).toFixed(i ? 1 : 0)} ${units[i]}`;
  }
})();
