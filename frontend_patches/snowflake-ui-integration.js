/**
 * SnowflakeLangflow Live UI Integration Script
 * Mounted into Langflow's served frontend (`index.html`) without breaking Langflow's native React layout/themes.
 *
 * Implements:
 * 1. Official black Snowflake logo in topbar (home + flow editor) (`REQ-01`).
 * 2. Removal of GitHub, Discord, and X/Twitter external links + addition of `Snowflake Documentation`
 *    (`https://docs.snowflake.com/`) with official color Snowflake logo (`#29B5E8`) (`REQ-01`, `[G-A6]`).
 * 3. Real-time Active Role Selector (`GET /api/v1/snowflake/context`, `POST /api/v1/snowflake/role`)
 *    with `context_epoch` badge and explicit 4-code denial banner (`REQ-02`, `ADR-003`, `ADR-004`).
 * 4. Interactive modals for `Add from Stage`, `Add from Git`, `Create Memory`, `Governed Object Provisioner`,
 *    and synchronized `Flow Activity` SQL Traces (`LANGFLOW_FLOW_RUNS`, `LANGFLOW_FLOW_SPANS`, `LANGFLOW_AUDIT_LOG`).
 */
(function () {
  "use strict";

  const SNOWFLAKE_SVG_BLACK = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="#000000" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round" style="display:inline-block;vertical-align:middle;"><line x1="12" y1="2" x2="12" y2="22"/><line x1="2" y1="12" x2="22" y2="12"/><polyline points="20 16 16 12 20 8"/><polyline points="4 8 8 12 4 16"/><polyline points="16 4 12 8 8 4"/><polyline points="8 20 12 16 16 20"/></svg>`;

  const SNOWFLAKE_SVG_COLOR = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="#29B5E8" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="display:inline-block;vertical-align:middle;"><line x1="12" y1="2" x2="12" y2="22"/><line x1="2" y1="12" x2="22" y2="12"/><polyline points="20 16 16 12 20 8"/><polyline points="4 8 8 12 4 16"/><polyline points="16 4 12 8 8 4"/><polyline points="8 20 12 16 16 20"/></svg>`;

  let sfState = {
    active_role: "ACCOUNTADMIN",
    available_roles: [
      "ACCOUNTADMIN",
      "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
      "SNOWFLAKE_LANGFLOW_DEV_ROLE",
      "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
      "PUBLIC",
    ],
    context_epoch: 1,
    database: "SNOWFLAKE_LANGFLOW",
    schema: "CORE",
    stage_name: "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
  };

  function showDenialOrStatusBanner(kind, code, message) {
    let banner = document.getElementById("sflf-global-banner");
    if (!banner) {
      banner = document.createElement("div");
      banner.id = "sflf-global-banner";
      banner.style.cssText =
        "position:fixed;top:52px;right:18px;z-index:99999;max-width:520px;padding:12px 16px;border-radius:8px;font-family:Inter,sans-serif;font-size:13px;box-shadow:0 8px 24px rgba(0,0,0,0.35);display:flex;align-items:center;gap:10px;";
      document.body.appendChild(banner);
    }
    const isError = kind === "error";
    banner.style.background = isError ? "#7f1d1d" : "#064e3b";
    banner.style.border = isError ? "1px solid #ef4444" : "1px solid #10b981";
    banner.style.color = "#ffffff";
    banner.innerHTML = `<div style="flex:1;"><strong>[${code}]</strong> ${message}</div><button id="sflf-banner-close" style="background:transparent;border:none;color:#fff;cursor:pointer;font-weight:bold;">✕</button>`;
    banner.style.display = "flex";
    const closeBtn = document.getElementById("sflf-banner-close");
    if (closeBtn) {
      closeBtn.onclick = function () {
        banner.style.display = "none";
      };
    }
    setTimeout(function () {
      if (banner) banner.style.display = "none";
    }, 8000);
  }

  async function fetchSnowflakeContext() {
    try {
      const resp = await fetch("/api/v1/snowflake/context");
      if (!resp.ok) return;
      const data = await resp.json();
      sfState = Object.assign(sfState, data);
      updateTopbarWidgets();
    } catch (_e) {
      // Keep default state until backend route responds
    }
  }

  async function switchSnowflakeRole(newRole) {
    try {
      const resp = await fetch("/api/v1/snowflake/role", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target_role: newRole }),
      });
      const data = await resp.json();
      if (!resp.ok || !data.ok) {
        showDenialOrStatusBanner(
          "error",
          data.error_code || "AUTHZ_ROLE_DENIED",
          data.message || `Il ruolo attivo (${sfState.active_role}) non consente questa operazione.`
        );
        updateTopbarWidgets();
        return;
      }
      sfState.active_role = data.active_role;
      sfState.context_epoch = data.context_epoch;
      updateTopbarWidgets();
      showDenialOrStatusBanner(
        "ok",
        "ROLE_SWITCHED",
        `Ruolo attivo: ${sfState.active_role} (epoch #${sfState.context_epoch})`
      );
    } catch (err) {
      showDenialOrStatusBanner("error", "TECHNICAL_EXECUTION_ERROR", String(err));
    }
  }

  function removeExternalCommunityLinks() {
    const anchors = document.querySelectorAll("a[href]");
    anchors.forEach(function (a) {
      const href = (a.getAttribute("href") || "").toLowerCase();
      if (
        href.includes("github.com/langflow-ai") ||
        href.includes("discord.gg") ||
        href.includes("discord.com") ||
        href.includes("twitter.com") ||
        href.includes("x.com/langflow")
      ) {
        a.style.display = "none";
      }
    });
    const testIds = [
      '[data-testid="icon-Github"]',
      '[data-testid="icon-Discord"]',
      '[data-testid="icon-Twitter"]',
      '[data-testid="github-button"]',
      '[data-testid="discord-button"]',
    ];
    testIds.forEach(function (sel) {
      document.querySelectorAll(sel).forEach(function (el) {
        const btn = el.closest("a, button") || el;
        btn.style.display = "none";
      });
    });
  }

  function updateTopbarWidgets() {
    const selectEl = document.getElementById("sflf-role-select");
    const epochEl = document.getElementById("sflf-epoch-badge");
    if (selectEl) {
      selectEl.innerHTML = "";
      (sfState.available_roles || []).forEach(function (r) {
        const opt = document.createElement("option");
        opt.value = r;
        opt.textContent = r;
        if (r === sfState.active_role) opt.selected = true;
        selectEl.appendChild(opt);
      });
    }
    if (epochEl) {
      epochEl.textContent = `epoch #${sfState.context_epoch}`;
    }
  }

  function openSnowflakeHubModal() {
    let modal = document.getElementById("sflf-hub-modal");
    if (modal) {
      modal.style.display = "flex";
      refreshTracesInModal();
      return;
    }
    modal = document.createElement("div");
    modal.id = "sflf-hub-modal";
    modal.style.cssText =
      "position:fixed;inset:0;background:rgba(0,0,0,0.68);z-index:99998;display:flex;align-items:center;justify-content:center;font-family:Inter,sans-serif;";
    modal.innerHTML = `
      <div style="background:#18181b;color:#f4f4f5;border:1px solid #3f3f46;border-radius:12px;width:860px;max-width:95vw;max-height:88vh;overflow:auto;padding:20px;box-shadow:0 20px 50px rgba(0,0,0,0.6);">
        <div style="display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #27272a;padding-bottom:12px;margin-bottom:16px;">
          <div style="display:flex;align-items:center;gap:10px;">
            <span style="background:#ffffff;padding:4px 7px;border-radius:6px;display:inline-flex;">${SNOWFLAKE_SVG_BLACK}</span>
            <strong style="font-size:16px;">SnowflakeLangflow Control Hub (Role: <span id="sflf-modal-role">${sfState.active_role}</span>)</strong>
          </div>
          <div style="display:flex;align-items:center;gap:12px;">
            <a href="https://docs.snowflake.com/" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;gap:6px;color:#29B5E8;text-decoration:none;font-size:13px;font-weight:600;">
              ${SNOWFLAKE_SVG_COLOR} Snowflake Documentation
            </a>
            <button id="sflf-modal-close" style="background:#27272a;border:1px solid #3f3f46;color:#fff;padding:4px 10px;border-radius:6px;cursor:pointer;">Close</button>
          </div>
        </div>

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:16px;">
          <div style="background:#27272a;padding:14px;border-radius:8px;">
            <h4 style="margin:0 0 8px 0;font-size:14px;color:#29B5E8;">1. Governed Object Provisioning (REQ-16)</h4>
            <div style="display:flex;gap:6px;margin-bottom:8px;">
              <select id="sflf-prov-kind" style="background:#18181b;color:#fff;border:1px solid #3f3f46;padding:6px;border-radius:6px;flex:1;">
                <option value="table">Standard / Hybrid Table (VECTOR)</option>
                <option value="stage">Internal Stage (SNOWFLAKE_SSE)</option>
                <option value="schema">Schema</option>
                <option value="database">Database</option>
                <option value="postgres">Snowflake Postgres Instance</option>
              </select>
              <input id="sflf-prov-name" placeholder="Object Name (e.g. DEMO_VECTORS)" value="DEMO_VECTORS" style="background:#18181b;color:#fff;border:1px solid #3f3f46;padding:6px;border-radius:6px;flex:1;" />
            </div>
            <div style="display:flex;gap:8px;">
              <button id="sflf-btn-dryrun" style="background:#3f3f46;color:#fff;border:none;padding:6px 10px;border-radius:6px;cursor:pointer;font-size:12px;">Dry-Run DDL Preview</button>
              <button id="sflf-btn-provision" style="background:#0284c7;color:#fff;border:none;padding:6px 10px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;">Execute on Snowflake</button>
            </div>
            <pre id="sflf-prov-out" style="margin-top:8px;background:#09090b;padding:8px;border-radius:6px;font-size:11px;white-space:pre-wrap;max-height:100px;overflow:auto;">Click Dry-Run DDL Preview or Execute.</pre>
          </div>

          <div style="background:#27272a;padding:14px;border-radius:8px;">
            <h4 style="margin:0 0 8px 0;font-size:14px;color:#29B5E8;">2. Create Memory & Vector Store (REQ-10)</h4>
            <div style="display:flex;gap:6px;margin-bottom:8px;">
              <input id="sflf-mem-name" placeholder="Memory Name" value="SNOWFLAKE_KB_MEM" style="background:#18181b;color:#fff;border:1px solid #3f3f46;padding:6px;border-radius:6px;flex:1;" />
              <select id="sflf-mem-backend" style="background:#18181b;color:#fff;border:1px solid #3f3f46;padding:6px;border-radius:6px;flex:1;">
                <option value="Standard Table (VECTOR)">Standard Table (VECTOR)</option>
                <option value="Hybrid Table (VECTOR)">Hybrid Table (VECTOR)</option>
                <option value="Cortex Search">Cortex Search</option>
                <option value="Snowflake Postgres (pgvector)">Snowflake Postgres (pgvector)</option>
              </select>
            </div>
            <button id="sflf-btn-create-mem" style="background:#0284c7;color:#fff;border:none;padding:6px 10px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;">Create Governed Memory</button>
            <pre id="sflf-mem-out" style="margin-top:8px;background:#09090b;padding:8px;border-radius:6px;font-size:11px;white-space:pre-wrap;max-height:100px;overflow:auto;">Ready to create Snowflake vector memory.</pre>
          </div>
        </div>

        <div style="background:#27272a;padding:14px;border-radius:8px;">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
            <h4 style="margin:0;font-size:14px;color:#29B5E8;">3. Replicated Flow Activity & Governance Audit Log in Snowflake SQL (REQ-15)</h4>
            <button id="sflf-btn-sync-traces" style="background:#10b981;color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;">Sync Flow Activity to Snowflake SQL</button>
          </div>
          <pre id="sflf-traces-out" style="background:#09090b;padding:10px;border-radius:6px;font-size:11px;white-space:pre-wrap;max-height:200px;overflow:auto;">Loading Snowflake SQL traces...</pre>
        </div>
      </div>
    `;
    document.body.appendChild(modal);

    document.getElementById("sflf-modal-close").onclick = function () {
      modal.style.display = "none";
    };
    document.getElementById("sflf-btn-dryrun").onclick = function () {
      runProvisionCall(true);
    };
    document.getElementById("sflf-btn-provision").onclick = function () {
      runProvisionCall(false);
    };
    document.getElementById("sflf-btn-create-mem").onclick = function () {
      runCreateMemoryCall();
    };
    document.getElementById("sflf-btn-sync-traces").onclick = function () {
      refreshTracesInModal();
    };
    refreshTracesInModal();
  }

  async function runProvisionCall(dryRun) {
    const kind = document.getElementById("sflf-prov-kind").value;
    const name = document.getElementById("sflf-prov-name").value;
    const out = document.getElementById("sflf-prov-out");
    const resp = await fetch("/api/v1/snowflake/provision", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ object_kind: kind, name: name, dry_run: dryRun, confirmed: !dryRun }),
    });
    const data = await resp.json();
    out.textContent = JSON.stringify(data, null, 2);
    if (!resp.ok || data.ok === false) {
      showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Operazione negata");
    } else if (!dryRun) {
      showDenialOrStatusBanner("ok", "PROVISIONED", `Oggetto ${name} creato su Snowflake col ruolo ${sfState.active_role}`);
      refreshTracesInModal();
    }
  }

  async function runCreateMemoryCall() {
    const name = document.getElementById("sflf-mem-name").value;
    const backend = document.getElementById("sflf-mem-backend").value;
    const out = document.getElementById("sflf-mem-out");
    const resp = await fetch("/api/v1/snowflake/memories", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: name, vector_database: backend }),
    });
    const data = await resp.json();
    out.textContent = JSON.stringify(data, null, 2);
    if (!resp.ok || data.ok === false) {
      showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Operazione negata");
    } else {
      showDenialOrStatusBanner("ok", "MEMORY_CREATED", `Memoria ${name} creata in ${backend}`);
      refreshTracesInModal();
    }
  }

  async function refreshTracesInModal() {
    const out = document.getElementById("sflf-traces-out");
    const roleSpan = document.getElementById("sflf-modal-role");
    if (roleSpan) roleSpan.textContent = sfState.active_role;
    if (!out) return;
    try {
      const resp = await fetch("/api/v1/snowflake/traces/sync", { method: "POST" });
      const data = await resp.json();
      out.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
      out.textContent = String(e);
    }
  }

  function injectTopbarBar() {
    if (document.getElementById("sflf-topbar-strip")) return;
    const strip = document.createElement("div");
    strip.id = "sflf-topbar-strip";
    strip.style.cssText =
      "position:fixed;top:8px;left:50%;transform:translateX(-50%);z-index:9990;display:flex;align-items:center;gap:8px;background:#ffffff;color:#09090b;border:1px solid #d4d4d8;padding:3px 10px;border-radius:999px;font-family:Inter,sans-serif;font-size:12px;box-shadow:0 2px 8px rgba(0,0,0,0.18);";
    strip.innerHTML = `
      <span title="Official Black Snowflake Logo" style="display:inline-flex;align-items:center;">${SNOWFLAKE_SVG_BLACK}</span>
      <strong style="font-weight:700;letter-spacing:-0.01em;">SnowflakeLangflow</strong>
      <span style="color:#71717a;">|</span>
      <label style="font-weight:500;">Role:</label>
      <select id="sflf-role-select" style="background:#f4f4f5;color:#09090b;border:1px solid #a1a1aa;border-radius:6px;padding:2px 6px;font-size:11px;font-weight:600;cursor:pointer;"></select>
      <span id="sflf-epoch-badge" style="background:#e4e4e7;color:#18181b;padding:1px 6px;border-radius:4px;font-size:10px;font-family:monospace;">epoch #1</span>
      <button id="sflf-open-hub-btn" style="background:#09090b;color:#ffffff;border:none;border-radius:6px;padding:3px 8px;font-size:11px;font-weight:600;cursor:pointer;">Snowflake Hub</button>
      <a href="https://docs.snowflake.com/" target="_blank" rel="noopener noreferrer" title="Snowflake Documentation" style="display:inline-flex;align-items:center;gap:4px;color:#0284c7;text-decoration:none;font-weight:600;font-size:11px;padding-left:4px;">
        ${SNOWFLAKE_SVG_COLOR} Docs
      </a>
    `;
    document.body.appendChild(strip);

    const sel = document.getElementById("sflf-role-select");
    if (sel) {
      sel.onchange = function (ev) {
        switchSnowflakeRole(ev.target.value);
      };
    }
    const hubBtn = document.getElementById("sflf-open-hub-btn");
    if (hubBtn) {
      hubBtn.onclick = function () {
        openSnowflakeHubModal();
      };
    }
    updateTopbarWidgets();
  }

  function bootSnowflakeUI() {
    injectTopbarBar();
    removeExternalCommunityLinks();
    fetchSnowflakeContext();
    const obs = new MutationObserver(function () {
      removeExternalCommunityLinks();
    });
    obs.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootSnowflakeUI);
  } else {
    bootSnowflakeUI();
  }
})();
