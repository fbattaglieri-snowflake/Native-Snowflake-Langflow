/**
 * SnowflakeLangflow Live UI Integration Script (v0.3.0)
 *
 * Uses the exact official Snowflake brand vector mark (`viewBox="0 0 64 64"`) extracted from
 * Langflow's official `SnowflakeIcon` asset (`index-X9auSuYC.js`):
 * - Black (`#000000`) in top-left header (`header_left_section_wrapper`) and Welcome card.
 * - Official Snowflake Blue (`#29b5e8`) for `Snowflake Documentation` (`https://docs.snowflake.com/`).
 * - Replaces all upstream GitHub links with `https://github.com/fbattaglieri-snowflake/SnowflakeLangflow`.
 * - Removes all Discord and X/Twitter buttons and cards.
 * - Mounts the Active Role Selector & `Snowflake Hub` button cleanly in `header_right_section_wrapper`.
 */
(function () {
  "use strict";

  const FORK_GITHUB_URL = "https://github.com/fbattaglieri-snowflake/SnowflakeLangflow";
  const SNOWFLAKE_DOCS_URL = "https://docs.snowflake.com/";

  const OFFICIAL_SNOWFLAKE_PATH =
    "M9.86 15.298l13.008 7.8a3.72 3.72 0 0 0 4.589-.601 4.01 4.01 0 0 0 1.227-2.908V3.956a3.81 3.81 0 0 0-1.861-3.42 3.81 3.81 0 0 0-3.893 0 3.81 3.81 0 0 0-1.861 3.42v8.896l-7.387-4.43a3.79 3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 1.844-1.057 1.9-.44 4.28 1.4 5.422m31.27 7.8l13.008-7.8c1.84-1.143 2.458-3.533 1.4-5.424a3.75 3.75 0 0 0-5.22-1.452l-7.3 4.37v-8.84a3.81 3.81 0 1 0-7.615 0v15.323a4.08 4.08 0 0 0 .494 2.367c.482.903 1.314 1.57 2.3 1.844a3.71 3.71 0 0 0 2.922-.4M29.552 31.97c.013-.25.108-.5.272-.68l1.52-1.58a1.06 1.06 0 0 1 .658-.282h.057a1.05 1.05 0 0 1 .656.282l1.52 1.58a1.12 1.12 0 0 1 .272.681v.06a1.13 1.13 0 0 1-.272.683l-1.52 1.58a1.04 1.04 0 0 1-.656.284h-.057c-.246-.014-.48-.115-.658-.284l-1.52-1.58a1.13 1.13 0 0 1-.272-.683zm-4.604-.65v1.364a1.54 1.54 0 0 0 .372.93l5.16 5.357a1.42 1.42 0 0 0 .895.386h1.312a1.42 1.42 0 0 0 .895-.386l5.16-5.357a1.54 1.54 0 0 0 .372-.93V31.32a1.54 1.54 0 0 0-.372-.93l-5.16-5.357a1.42 1.42 0 0 0-.895-.386h-1.312a1.42 1.42 0 0 0-.895.386L25.32 30.4a1.55 1.55 0 0 0-.372.93M3.13 27.62l7.365 4.417L3.13 36.45a4.06 4.06 0 0 0-1.399 5.424 3.75 3.75 0 0 0 2.3 1.844c.986.274 2.042.133 2.922-.392l13.008-7.8c1.2-.762 1.9-2.078 1.9-3.492a4.16 4.16 0 0 0-1.9-3.492l-13.008-7.8a3.79 3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 1.844-1.057 1.9-.44 4.278 1.4 5.422m38.995 4.442a4 4 0 0 0 1.91 3.477l13 7.8c.88.524 1.934.666 2.92.392s1.817-.94 2.3-1.843a4.05 4.05 0 0 0-1.4-5.424L53.5 32.038l7.365-4.417c1.84-1.143 2.457-3.53 1.4-5.422a3.74 3.74 0 0 0-2.3-1.844c-.987-.274-2.042-.134-2.92.4l-13 7.8a4 4 0 0 0-1.91 3.507M25.48 40.508a3.7 3.7 0 0 0-2.611.464l-13.008 7.8c-1.84 1.143-2.456 3.53-1.4 5.422.483.903 1.314 1.57 2.3 1.843a3.75 3.75 0 0 0 2.922-.392l7.387-4.43v8.83a3.81 3.81 0 1 0 7.614 0V44.4a3.91 3.91 0 0 0-3.205-3.903m28.66 8.276l-13.008-7.8a3.75 3.75 0 0 0-2.922-.392 3.74 3.74 0 0 0-2.3 1.843 4.09 4.09 0 0 0-.494 2.37v15.25a3.81 3.81 0 1 0 7.614 0V51.28l7.287 4.37a3.79 3.79 0 0 0 2.922.4c.986-.265 1.818-.94 2.3-1.844 1.057-1.9.44-4.28-1.4-5.422";

  function buildOfficialSnowflakeSvg(fill, sizePx) {
    const sz = sizePx || 20;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${sz}" height="${sz}" viewBox="0 0 64 64" fill="${fill}" style="display:inline-block;vertical-align:middle;flex-shrink:0;"><path d="${OFFICIAL_SNOWFLAKE_PATH}"/></svg>`;
  }

  const SNOWFLAKE_SVG_BLACK = buildOfficialSnowflakeSvg("#000000", 20);
  const SNOWFLAKE_SVG_COLOR = buildOfficialSnowflakeSvg("#29b5e8", 16);

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

  function applyDomCustomizations() {
    // 1. Top-left header: inject Official Black Snowflake Logo right next to Langflow icon
    const headerLeft = document.querySelector('[data-testid="header_left_section_wrapper"]');
    if (headerLeft && !document.getElementById("sflf-header-left-logo")) {
      const badge = document.createElement("div");
      badge.id = "sflf-header-left-logo";
      badge.style.cssText =
        "display:inline-flex;align-items:center;gap:6px;padding:2px 8px;border-radius:6px;background:#ffffff;border:1px solid #e4e4e7;";
      badge.innerHTML = `${SNOWFLAKE_SVG_BLACK}<span style="color:#000000;font-weight:700;font-size:12px;letter-spacing:-0.01em;">Snowflake</span>`;
      headerLeft.appendChild(badge);
    }

    // 2. Top-right header: mount Role Selector & Snowflake Hub next to notifications/profile
    const headerRight = document.querySelector('[data-testid="header_right_section_wrapper"]');
    if (headerRight && !document.getElementById("sflf-header-right-controls")) {
      const ctrl = document.createElement("div");
      ctrl.id = "sflf-header-right-controls";
      ctrl.style.cssText =
        "display:inline-flex;align-items:center;gap:6px;background:#f4f4f5;color:#09090b;border:1px solid #d4d4d8;padding:3px 8px;border-radius:8px;font-size:12px;";
      ctrl.innerHTML = `
        <span style="display:inline-flex;align-items:center;">${buildOfficialSnowflakeSvg("#000000", 15)}</span>
        <label style="font-weight:600;color:#18181b;">Role:</label>
        <select id="sflf-role-select" style="background:#ffffff;color:#09090b;border:1px solid #a1a1aa;border-radius:5px;padding:2px 6px;font-size:11px;font-weight:600;cursor:pointer;"></select>
        <span id="sflf-epoch-badge" style="background:#e4e4e7;color:#18181b;padding:1px 5px;border-radius:4px;font-size:10px;font-family:monospace;">epoch #${sfState.context_epoch}</span>
        <button id="sflf-open-hub-btn" style="background:#09090b;color:#ffffff;border:none;border-radius:5px;padding:3px 8px;font-size:11px;font-weight:600;cursor:pointer;">Snowflake Hub</button>
        <a href="${SNOWFLAKE_DOCS_URL}" target="_blank" rel="noopener noreferrer" title="Snowflake Documentation" style="display:inline-flex;align-items:center;gap:4px;color:#0284c7;text-decoration:none;font-weight:600;font-size:11px;padding-left:2px;">
          ${SNOWFLAKE_SVG_COLOR} Docs
        </a>
      `;
      headerRight.insertBefore(ctrl, headerRight.firstChild);

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

    // 3. Remove ALL Discord & X/Twitter buttons/cards everywhere (topbar, welcome page, profile menu)
    const discordSelectors = [
      '[data-testid="empty_page_discord_button"]',
      '[data-testid="menu_discord_button"]',
      '[data-testid="menu_twitter_button"]',
      'a[href*="discord.com"]',
      'a[href*="discord.gg"]',
      'a[href*="x.com"]',
      'a[href*="twitter.com"]',
    ];
    discordSelectors.forEach(function (sel) {
      document.querySelectorAll(sel).forEach(function (el) {
        const wrapper = el.closest("a") || el;
        wrapper.style.display = "none";
      });
    });

    // Also hide topbar Discord button (identifiable by sr-only text or svg inside header_right_section_wrapper)
    document.querySelectorAll("button").forEach(function (btn) {
      const txt = (btn.textContent || "").trim().toLowerCase();
      if (txt === "discord" || txt.includes("go to discord") || txt.includes("join builders, ask questions")) {
        btn.style.display = "none";
      }
    });

    // 4. Update Welcome page GitHub card & topbar GitHub links to point to fbattaglieri-snowflake/SnowflakeLangflow
    const ghCard = document.querySelector('[data-testid="empty_page_github_button"]');
    if (ghCard && !ghCard.getAttribute("data-sflf-patched")) {
      ghCard.setAttribute("data-sflf-patched", "true");
      const titleSpans = ghCard.querySelectorAll("span");
      titleSpans.forEach(function (sp) {
        if (sp.textContent === "GitHub") {
          sp.textContent = "GitHub Fork — fbattaglieri-snowflake/SnowflakeLangflow";
        } else if (sp.classList.contains("font-mono")) {
          sp.textContent = "";
        } else if (sp.textContent && sp.textContent.includes("Follow development")) {
          sp.textContent =
            "Unofficial personal fork by Francesco Battaglieri (not official Snowflake or Langflow) — Under Active Testing.";
        }
      });
      ghCard.onclick = function (ev) {
        ev.preventDefault();
        ev.stopPropagation();
        window.open(FORK_GITHUB_URL, "_blank", "noopener,noreferrer");
      };
    }

    document.querySelectorAll('a[href*="github.com/langflow-ai"]').forEach(function (a) {
      a.setAttribute("href", FORK_GITHUB_URL);
    });

    // 5. Enhance Welcome page center logo with official black Snowflake mark
    const welcomeTitle = document.querySelector('[data-testid="mainpage_title"]');
    if (welcomeTitle && !document.getElementById("sflf-welcome-snowflake-logo")) {
      welcomeTitle.textContent = "Welcome to SnowflakeLangflow";
      const logoHolder = document.createElement("div");
      logoHolder.id = "sflf-welcome-snowflake-logo";
      logoHolder.style.cssText =
        "display:inline-flex;align-items:center;justify-content:center;gap:12px;background:#ffffff;border:1px solid #e4e4e7;padding:10px 18px;border-radius:14px;box-shadow:0 6px 20px rgba(0,0,0,0.08);margin-bottom:4px;";
      logoHolder.innerHTML = `${buildOfficialSnowflakeSvg("#000000", 36)}<span style="font-size:18px;font-weight:700;color:#000000;">SnowflakeLangflow</span>`;
      welcomeTitle.parentNode.insertBefore(logoHolder, welcomeTitle);
    }

    // 6. Profile menu Docs entry -> Snowflake Documentation with official color Snowflake icon
    const menuDocs = document.getElementById("menu_docs_button");
    if (menuDocs && !menuDocs.getAttribute("data-sflf-docs")) {
      menuDocs.setAttribute("data-sflf-docs", "true");
      menuDocs.innerHTML = `<span style="display:inline-flex;align-items:center;gap:8px;">${SNOWFLAKE_SVG_COLOR}<span>Snowflake Documentation</span></span>`;
      const anchor = menuDocs.closest("a");
      if (anchor) {
        anchor.setAttribute("href", SNOWFLAKE_DOCS_URL);
      }
    }
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
            <a href="${FORK_GITHUB_URL}" target="_blank" rel="noopener noreferrer" style="color:#a1a1aa;text-decoration:none;font-size:12px;">Fork: fbattaglieri-snowflake/SnowflakeLangflow</a>
            <a href="${SNOWFLAKE_DOCS_URL}" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;gap:6px;color:#29B5E8;text-decoration:none;font-size:13px;font-weight:600;">
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

  function bootSnowflakeUI() {
    applyDomCustomizations();
    fetchSnowflakeContext();
    const obs = new MutationObserver(function () {
      applyDomCustomizations();
    });
    obs.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootSnowflakeUI);
  } else {
    bootSnowflakeUI();
  }
})();
