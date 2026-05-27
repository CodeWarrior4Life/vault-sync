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
const resultScopeRootsEl = document.querySelector("#result-scope-roots");

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

document.querySelector("#vr-close-btn").addEventListener("click", () => {
  verifyPanel.classList.add("hidden");
});
document.querySelector("#vr-error-close-btn").addEventListener("click", () => {
  verifyPanel.classList.add("hidden");
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

  try {
    const result = await invoke("pair", {
      input: {
        nexus_url: nexusUrlEl.value.trim(),
        token: tokenEl.value.trim(),
        vaults_root: vaultRootEl.value.trim(),
      },
    });

    // Show success panel, hide form.
    form.classList.add("hidden");
    resultSubscriberIdEl.textContent = result.subscriber_id;
    resultModeEl.textContent = describeMode(result.materializer_mode);
    resultScopeRootsEl.textContent = describeScopeRoots(result.scope_roots);
    successPanel.classList.remove("hidden");
  } catch (err) {
    showStatus("Pairing failed: " + err, true);
    pairBtn.disabled = false;
    pairBtn.textContent = "Pair";
  }
});
