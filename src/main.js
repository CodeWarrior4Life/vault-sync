const { invoke } = window.__TAURI__.core;
const { open } = window.__TAURI__.dialog;
const { listen } = window.__TAURI__.event;

const form = document.querySelector("#pairing-form");
const nexusUrlEl = document.querySelector("#nexus-url");
const tokenEl = document.querySelector("#token");
const vaultRootEl = document.querySelector("#vault-root");
const browseBtn = document.querySelector("#browse-btn");
const pairBtn = document.querySelector("#pair-btn");
const statusEl = document.querySelector("#status");
const successPanel = document.querySelector("#success-panel");
const resultSubscriberIdEl = document.querySelector("#result-subscriber-id");
const resultModeEl = document.querySelector("#result-mode");
const resultVaultsRootEl = document.querySelector("#result-vaults-root");
const resultDetectedVaultsEl = document.querySelector("#result-detected-vaults");
const closeBtn = document.querySelector("#close-btn");
const editSettingsBtn = document.querySelector("#edit-settings-btn");

// S477 §3.2: cache the current paired config for the Edit Settings restore
// path. Populated after a successful pair (or on startup if a config is
// already on disk). Shape: { nexusUrl, vaultsRoot, mode, subscriberId }.
window.__currentPairedConfig = null;

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );
}

// v0.3 copy helpers: replace internal jargon with user-facing meaning.
function describeMode(mode) {
  switch (mode) {
    case "live":
      return "Live (writes go straight into your vault)";
    case "shadow":
      return "Shadow (writes to a sidecar tree — preview/test mode, NOT your real vault)";
    case "disabled":
      return "Disabled (no writes)";
    default:
      return mode || "unknown";
  }
}

function describeScopeRoots(roots) {
  if (!roots || roots.length === 0) {
    return "Everything in the vault (no folder restriction)";
  }
  return roots.join(", ");
}

// S477 §3.2: render the Paired panel from a current-config snapshot. Calls
// the Tauri `list_vault_folders` command to surface detected Obsidian vaults
// under the configured root.
async function populatePairedPanel({ subscriberId, vaultsRoot, mode }) {
  resultSubscriberIdEl.textContent = subscriberId || "—";
  resultVaultsRootEl.textContent = vaultsRoot || "—";
  resultModeEl.textContent = describeMode(mode);

  if (!vaultsRoot) {
    resultDetectedVaultsEl.textContent = "—";
    return;
  }

  try {
    const folders = await invoke("list_vault_folders", { vaultsRoot });
    const detected = (folders || []).filter((f) => f.has_obsidian);
    if (detected.length === 0) {
      resultDetectedVaultsEl.innerHTML =
        "<em>(no Obsidian vaults detected — daemon will still sync loose files)</em>";
    } else {
      resultDetectedVaultsEl.innerHTML = detected
        .map((f) => `<code>${escapeHtml(f.name)}</code>`)
        .join(", ");
    }
  } catch (e) {
    resultDetectedVaultsEl.textContent =
      "(error enumerating: " + (e && e.message ? e.message : String(e)) + ")";
  }
}

// v0.3 — true persistence means the field is FILLED in the UI on reopen.
// Pre-populate nexus_url, vaults_root AND the bearer token from disk so
// the user sees their active pairing state immediately.
(async () => {
  try {
    const cfg = await invoke("load_current_config");
    if (cfg) {
      if (cfg.nexus_url) nexusUrlEl.value = cfg.nexus_url;
      if (cfg.vaults_root) vaultRootEl.value = cfg.vaults_root;
    }
    const tok = await invoke("load_current_token");
    if (tok) {
      tokenEl.value = tok;
      // Indicate active pairing — change the button to "Re-pair"
      // since hitting Pair again with the same token would be a no-op.
      pairBtn.textContent = "Re-pair";
      pairBtn.classList.add("re-pair");
      const note = document.createElement("p");
      note.className = "status info";
      note.id = "active-pairing-note";
      note.innerHTML = "✓ Already paired with " + (cfg && cfg.nexus_url ? cfg.nexus_url : "your Nexus") + ". Edit fields to re-pair or pair with a different server.";
      form.insertBefore(note, form.firstChild);

      // v0.3.2: pre-select the current materializer_mode radio so the
      // user sees the live state instead of an always-defaults-to-shadow
      // form. Fetch directly from the server's /api/sync/health (which
      // returns the subscriber row's current mode).
      let liveMode = null;
      try {
        const r = await fetch(cfg.nexus_url.replace(/\/$/, "") + "/api/sync/health", {
          headers: { Authorization: "Bearer " + tok },
        });
        if (r.ok) {
          const h = await r.json();
          liveMode = (h && h.materializer_mode) || null;
          const mode = liveMode || "shadow";
          const radio = document.querySelector('input[name="materializer-mode"][value="' + mode + '"]');
          if (radio) radio.checked = true;
        }
      } catch (_e) {
        // Network / health failure — leave the default-checked radio alone.
      }

      // S477 §3.2: cache the current paired config so the Edit Settings
      // button on the Paired panel (after a re-pair OR after the user
      // navigates to the Paired panel via tray) has the prior state to
      // restore. Subscriber_id comes from load_current_config.
      if (cfg && cfg.subscriber_id) {
        window.__currentPairedConfig = {
          nexusUrl: cfg.nexus_url || "",
          vaultsRoot: cfg.vaults_root || "",
          mode: liveMode || "shadow",
          subscriberId: cfg.subscriber_id,
        };
      }
    }
  } catch (_e) {
    // First-run / no config — leave form blank.
  }
})();

// --- Verify & Repair progress panel (driven by backend events) ---
const verifyPanel = document.querySelector("#verify-panel");
const verifyLoading = document.querySelector("#verify-loading");
const verifyResults = document.querySelector("#verify-results");
const verifyError = document.querySelector("#verify-error");

function showVerifyPanel() {
  // Hide every other panel, show the verify panel in loading state.
  form.classList.add("hidden");
  document.querySelector("#success-panel").classList.add("hidden");
  document.querySelector("#status").classList.add("hidden");
  verifyResults.classList.add("hidden");
  verifyError.classList.add("hidden");
  verifyLoading.classList.remove("hidden");
  verifyPanel.classList.remove("hidden");
}

listen("verify-progress", () => {
  showVerifyPanel();
});

listen("verify-result", (event) => {
  const r = event.payload || {};
  document.querySelector("#vr-scanned").textContent = r.files_scanned ?? 0;
  document.querySelector("#vr-insync").textContent = r.files_in_sync ?? 0;
  document.querySelector("#vr-push").textContent = r.modify_count ?? 0;
  document.querySelector("#vr-pull").textContent = r.add_count ?? 0;
  document.querySelector("#vr-substrate").textContent = r.substrate_refused_count ?? 0;
  document.querySelector("#vr-elapsed").textContent = ((r.elapsed_ms ?? 0) / 1000).toFixed(1) + " s";
  const pushCount = r.modify_count ?? 0;
  document.querySelector("#vr-note").textContent = pushCount > 0
    ? `${pushCount} file(s) queued for upload — syncing in the background now.`
    : "Everything is in sync.";
  verifyLoading.classList.add("hidden");
  verifyError.classList.add("hidden");
  verifyResults.classList.remove("hidden");
  verifyPanel.classList.remove("hidden");
});

listen("verify-error", (event) => {
  document.querySelector("#vr-error-msg").textContent =
    "Verify and repair failed: " + (event.payload || "unknown error");
  verifyLoading.classList.add("hidden");
  verifyResults.classList.add("hidden");
  verifyError.classList.remove("hidden");
  verifyPanel.classList.remove("hidden");
});

// S477 §3.5 (v0.3.7): Linux inotify watch-limit detection. Daemon emits this
// event when `notify::ErrorKind::MaxFilesWatch` trips during FileWatcher
// start (i.e. the kernel rejected our recursive watch because the per-user
// inotify limit was already exhausted). Payload is the current sysctl value
// read from /proc/sys/fs/inotify/max_user_watches (0 if unreadable).
listen("inotify_limit_exceeded", (event) => {
  const banner = document.querySelector("#inotify-banner");
  if (!banner) return;
  const current = event && event.payload;
  const strong = banner.querySelector("strong");
  if (strong) {
    if (typeof current === "number" && current > 0) {
      strong.textContent =
        "Linux inotify watch limit exceeded (current=" + current + ").";
    } else {
      strong.textContent = "Linux inotify watch limit exceeded.";
    }
  }
  banner.classList.remove("hidden");
});

document.querySelector("#vr-close-btn").addEventListener("click", () => {
  verifyPanel.classList.add("hidden");
  form.classList.remove("hidden");
});
document.querySelector("#vr-error-close-btn").addEventListener("click", () => {
  verifyPanel.classList.add("hidden");
  form.classList.remove("hidden");
});

function showStatus(msg, isError) {
  statusEl.textContent = msg;
  statusEl.className = "status" + (isError ? " error" : " info");
}

function hideStatus() {
  statusEl.className = "status hidden";
  statusEl.textContent = "";
}

browseBtn.addEventListener("click", async () => {
  try {
    const selected = await open({ directory: true, multiple: false, title: "Select Vault Root" });
    if (selected) {
      vaultRootEl.value = selected;
    }
  } catch (e) {
    showStatus("Could not open folder picker: " + e, true);
  }
});

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  hideStatus();
  pairBtn.disabled = true;
  pairBtn.textContent = "Pairing…";

  const nexusUrl = nexusUrlEl.value.trim();
  const token = tokenEl.value.trim();
  const vaultsRoot = vaultRootEl.value.trim();
  const selectedMode = document.querySelector('input[name="materializer-mode"]:checked');
  const mode = selectedMode ? selectedMode.value : null;

  const isEdit = !!window.__currentPairedConfig;
  const tokenProvided = token.length > 0;

  try {
    if (isEdit && !tokenProvided) {
      // S477 §3.2: edit-mode without token rotation — PATCH existing subscriber.
      // This call is currently a stub (returns Err) pending Phase F server
      // coordination. The error is surfaced to the user verbatim.
      await invoke("patch_self_subscriber", {
        nexusUrl,
        newVaultsRoot: vaultsRoot,
        newMode: mode,
      });
      // On success (post-Phase-F): refresh cache + re-render Paired panel.
      window.__currentPairedConfig.nexusUrl = nexusUrl;
      window.__currentPairedConfig.vaultsRoot = vaultsRoot;
      window.__currentPairedConfig.mode = mode;
      form.classList.add("hidden");
      successPanel.classList.remove("hidden");
      await populatePairedPanel({
        subscriberId: window.__currentPairedConfig.subscriberId,
        vaultsRoot,
        mode,
      });
      pairBtn.disabled = false;
      pairBtn.textContent = "Pair";
    } else {
      // Fresh pair OR token rotation: full POST.
      const result = await invoke("pair", {
        input: {
          nexus_url: nexusUrl,
          token,
          vaults_root: vaultsRoot,
          materializer_mode: mode,
        },
      });

      // Cache for subsequent Edit Settings restores.
      window.__currentPairedConfig = {
        nexusUrl,
        vaultsRoot,
        mode: result.materializer_mode || mode,
        subscriberId: result.subscriber_id,
      };

      // Show success panel, hide form.
      form.classList.add("hidden");
      successPanel.classList.remove("hidden");
      await populatePairedPanel({
        subscriberId: result.subscriber_id,
        vaultsRoot,
        mode: result.materializer_mode || mode,
      });
    }
  } catch (err) {
    showStatus("Pairing failed: " + err, true);
    pairBtn.disabled = false;
    pairBtn.textContent = "Pair";
  }
});

// S477 §3.2: Close button hides the wizard window. Daemon keeps running
// in the tray; window can be reopened via tray menu / second-launch.
closeBtn.addEventListener("click", async () => {
  try {
    const w = window.__TAURI__.window || window.__TAURI__.webviewWindow;
    if (w && typeof w.getCurrentWindow === "function") {
      await w.getCurrentWindow().hide();
    } else if (w && typeof w.getCurrent === "function") {
      await w.getCurrent().hide();
    } else {
      // Final fallback: best-effort window.close (may be intercepted by the
      // existing close-handler that hides instead of quits).
      window.close();
    }
  } catch (e) {
    showStatus("Could not hide window: " + e, true);
  }
});

// S477 §3.2: Edit Settings restores the pair-form with current values
// pre-filled. Token stays blank so the user can either leave it (PATCH
// path) or paste a new one (re-pair path).
editSettingsBtn.addEventListener("click", () => {
  const cfg = window.__currentPairedConfig;
  if (cfg) {
    if (cfg.nexusUrl) nexusUrlEl.value = cfg.nexusUrl;
    if (cfg.vaultsRoot) vaultRootEl.value = cfg.vaultsRoot;
    tokenEl.value = "";
    if (cfg.mode) {
      const modeRadio = document.querySelector(
        'input[name="materializer-mode"][value="' + cfg.mode + '"]'
      );
      if (modeRadio) modeRadio.checked = true;
    }
  }
  hideStatus();
  pairBtn.disabled = false;
  pairBtn.textContent = "Save";
  successPanel.classList.add("hidden");
  form.classList.remove("hidden");
});
