/**
 * SnowflakeLangflow Live UI Integration Script (v0.3.3 — 100% React 18 Virtual-DOM Safe)
 *
 * CRITICAL INVARIANT:
 * Never call `appendChild`, `insertBefore`, `removeChild`, or mutate `innerHTML` on ANY node
 * inside `<div id="root">`. React 18 owns `#root` exclusively. All native logos, links, and
 * i18n labels inside `#root` are compiled into `assets/index-*.js` at build time, while the
 * interactive Role Selector bar and Snowflake Hub modal are mounted directly on `document.body`
 * outside `#root`.
 */
(function () {
  "use strict";

  // Guard Node.prototype.removeChild and Node.prototype.insertBefore so browser extensions,
  // page translators, or portal transitions can never trigger React 18 NotFoundError crashes.
  if (typeof Node === "function" && Node.prototype && !Node.prototype.__sflfPatched) {
    const _origRemoveChild = Node.prototype.removeChild;
    Node.prototype.removeChild = function (child) {
      if (child && child.parentNode !== this) {
        if (child.parentNode) {
          return _origRemoveChild.call(child.parentNode, child);
        }
        return child;
      }
      return _origRemoveChild.apply(this, arguments);
    };
    const _origInsertBefore = Node.prototype.insertBefore;
    Node.prototype.insertBefore = function (newNode, referenceNode) {
      if (referenceNode && referenceNode.parentNode !== this) {
        return this.appendChild(newNode);
      }
      return _origInsertBefore.apply(this, arguments);
    };
    Node.prototype.__sflfPatched = true;
  }

  const FORK_GITHUB_URL = "https://github.com/fbattaglieri-snowflake/SnowflakeLangflow";
  const SNOWFLAKE_DOCS_URL = "https://docs.snowflake.com/";

  // Intercept window.open globally so any React button calling window.open("https://github.com/langflow-ai/...")
  // is redirected to the user's fork and any Discord/X popup is blocked.
  const _origWindowOpen = window.open;
  window.open = function (url, target, features) {
    if (typeof url === "string") {
      if (url.includes("github.com/langflow-ai/langflow")) {
        url = url.replace("https://github.com/langflow-ai/langflow", FORK_GITHUB_URL);
      }
      if (url.includes("discord.com") || url.includes("discord.gg") || url.includes("x.com/langflow")) {
        return null;
      }
    }
    return _origWindowOpen.call(window, url, target, features);
  };

  // Capture-phase click listener on document (never mutates React DOM nodes)
  document.addEventListener(
    "click",
    function (ev) {
      const target = ev.target && ev.target.closest ? ev.target.closest("a, button") : null;
      if (!target) return;
      const testId = target.getAttribute("data-testid") || "";
      const href = target.getAttribute("href") || "";
      if (
        testId === "empty_page_github_button" ||
        testId === "menu_github_button" ||
        href.includes("github.com/langflow-ai/langflow")
      ) {
        ev.preventDefault();
        ev.stopPropagation();
        _origWindowOpen.call(window, FORK_GITHUB_URL, "_blank", "noopener,noreferrer");
      }
    },
    true
  );

  const OFFICIAL_SNOWFLAKE_PATH =
    "M9.86 15.298l13.008 7.8a3.72 3.72 0 0 0 4.589-.601 4.01 4.01 0 0 0 1.227-2.908V3.956a3.81 3.81 0 0 0-1.861-3.42 3.81 3.81 0 0 0-3.893 0 3.81 3.81 0 0 0-1.861 3.42v8.896l-7.387-4.43a3.79 3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 1.844-1.057 1.9-.44 4.28 1.4 5.422m31.27 7.8l13.008-7.8c1.84-1.143 2.458-3.533 1.4-5.424a3.75 3.75 0 0 0-5.22-1.452l-7.3 4.37v-8.84a3.81 3.81 0 1 0-7.615 0v15.323a4.08 4.08 0 0 0 .494 2.367c.482.903 1.314 1.57 2.3 1.844a3.71 3.71 0 0 0 2.922-.4M29.552 31.97c.013-.25.108-.5.272-.68l1.52-1.58a1.06 1.06 0 0 1 .658-.282h.057a1.05 1.05 0 0 1 .656.282l1.52 1.58a1.12 1.12 0 0 1 .272.681v.06a1.13 1.13 0 0 1-.272.683l-1.52 1.58a1.04 1.04 0 0 1-.656.284h-.057c-.246-.014-.48-.115-.658-.284l-1.52-1.58a1.13 1.13 0 0 1-.272-.683zm-4.604-.65v1.364a1.54 1.54 0 0 0 .372.93l5.16 5.357a1.42 1.42 0 0 0 .895.386h1.312a1.42 1.42 0 0 0 .895-.386l5.16-5.357a1.54 1.54 0 0 0 .372-.93V31.32a1.54 1.54 0 0 0-.372-.93l-5.16-5.357a1.42 1.42 0 0 0-.895-.386h-1.312a1.42 1.42 0 0 0-.895.386L25.32 30.4a1.55 1.55 0 0 0-.372.93M3.13 27.62l7.365 4.417L3.13 36.45a4.06 4.06 0 0 0-1.399 5.424 3.75 3.75 0 0 0 2.3 1.844c.986.274 2.042.133 2.922-.392l13.008-7.8c1.2-.762 1.9-2.078 1.9-3.492a4.16 4.16 0 0 0-1.9-3.492l-13.008-7.8a3.79 3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 1.844-1.057 1.9-.44 4.278 1.4 5.422m38.995 4.442a4 4 0 0 0 1.91 3.477l13 7.8c.88.524 1.934.666 2.92.392s1.817-.94 2.3-1.843a4.05 4.05 0 0 0-1.4-5.424L53.5 32.038l7.365-4.417c1.84-1.143 2.457-3.53 1.4-5.422a3.74 3.74 0 0 0-2.3-1.844c-.987-.274-2.042-.134-2.92.4l-13 7.8a4 4 0 0 0-1.91 3.507M25.48 40.508a3.7 3.7 0 0 0-2.611.464l-13.008 7.8c-1.84 1.143-2.456 3.53-1.4 5.422.483.903 1.314 1.57 2.3 1.843a3.75 3.75 0 0 0 2.922-.392l7.387-4.43v8.83a3.81 3.81 0 1 0 7.614 0V44.4a3.91 3.91 0 0 0-3.205-3.903m28.66 8.276l-13.008-7.8a3.75 3.75 0 0 0-2.922-.392 3.74 3.74 0 0 0-2.3 1.843 4.09 4.09 0 0 0-.494 2.37v15.25a3.81 3.81 0 1 0 7.614 0V51.28l7.287 4.37a3.79 3.79 0 0 0 2.922.4c.986-.265 1.818-.94 2.3-1.844 1.057-1.9.44-4.28-1.4-5.422";

  function buildOfficialSnowflakeSvg(fill, sizePx) {
    const sz = sizePx || 20;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${sz}" height="${sz}" viewBox="0 0 64 64" fill="${fill}" style="display:inline-block;vertical-align:middle;flex-shrink:0;"><path d="${OFFICIAL_SNOWFLAKE_PATH}"/></svg>`;
  }

  const SNOWFLAKE_SVG_BLACK = buildOfficialSnowflakeSvg("#000000", 16);
  const SNOWFLAKE_SVG_COLOR = buildOfficialSnowflakeSvg("#29b5e8", 15);

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
        "position:fixed;top:54px;right:18px;z-index:99999;max-width:520px;padding:12px 16px;border-radius:8px;font-family:Inter,sans-serif;font-size:13px;box-shadow:0 8px 24px rgba(0,0,0,0.35);display:flex;align-items:center;gap:10px;";
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

  // Mount Role Selector bar directly on document.body (OUTSIDE #root) so React's Virtual DOM reconciler
  // never encounters foreign child nodes during route transitions!
  function mountFixedTopbarControls() {
    if (document.getElementById("sflf-body-topbar-controls")) return;
    const ctrl = document.createElement("div");
    ctrl.id = "sflf-body-topbar-controls";
    ctrl.style.cssText =
      "position:fixed;top:10px;right:192px;z-index:25;display:inline-flex;align-items:center;gap:5px;background:transparent;color:#09090b;padding:1px 4px;font-family:Inter,sans-serif;font-size:11px;height:28px;";
    ctrl.innerHTML = `
      <span style="display:inline-flex;align-items:center;">${buildOfficialSnowflakeSvg("#000000", 14)}</span>
      <label style="font-weight:600;color:#18181b;font-size:11px;">Role:</label>
      <select id="sflf-role-select" style="background:#ffffff;color:#09090b;border:1px solid #d4d4d8;border-radius:5px;padding:1px 6px;font-size:11px;font-weight:600;cursor:pointer;max-width:142px;height:24px;text-overflow:ellipsis;"></select>
      <button id="sflf-open-hub-btn" style="background:#09090b;color:#ffffff;border:none;border-radius:5px;padding:2px 8px;height:24px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap;">Snowflake Hub</button>
    `;
    document.body.appendChild(ctrl);

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
      <div style="background:#18181b;color:#f4f4f5;border:1px solid #3f3f46;border-radius:10px;width:580px;max-width:90vw;max-height:76vh;overflow:auto;padding:14px 16px;box-shadow:0 20px 50px rgba(0,0,0,0.6);font-size:12px;">
        <div style="display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #27272a;padding-bottom:10px;margin-bottom:12px;">
          <div style="display:flex;align-items:center;gap:8px;">
            <span style="display:inline-flex;">${buildOfficialSnowflakeSvg("#29b5e8", 18)}</span>
            <strong style="font-size:13px;">Snowflake Langflow Hub (Role: <span id="sflf-modal-role">${sfState.active_role}</span> · <span id="sflf-epoch-badge">epoch #${sfState.context_epoch}</span>)</strong>
          </div>
          <div style="display:flex;align-items:center;gap:10px;">
            <a href="${FORK_GITHUB_URL}" target="_blank" rel="noopener noreferrer" style="color:#a1a1aa;text-decoration:none;font-size:11px;">Fork GitHub</a>
            <a href="${SNOWFLAKE_DOCS_URL}" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;gap:4px;color:#29B5E8;text-decoration:none;font-size:11px;font-weight:600;">
              ${SNOWFLAKE_SVG_COLOR} Docs
            </a>
            <button id="sflf-modal-close" style="background:#27272a;border:1px solid #3f3f46;color:#fff;padding:3px 8px;border-radius:5px;font-size:11px;cursor:pointer;">Close</button>
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

  function bootSnowflakeUI() {
    mountFixedTopbarControls();
    fetchSnowflakeContext();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootSnowflakeUI);
  } else {
    bootSnowflakeUI();
  }
})();
