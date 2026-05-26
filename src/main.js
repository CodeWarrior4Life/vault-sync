const { invoke } = window.__TAURI__.core;
const { open } = window.__TAURI__.dialog;

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
        vault_root: vaultRootEl.value.trim(),
      },
    });

    // Show success panel, hide form.
    form.classList.add("hidden");
    resultSubscriberIdEl.textContent = result.subscriber_id;
    resultModeEl.textContent = result.materializer_mode;
    resultScopeRootsEl.textContent = result.scope_roots.join(", ") || "(none)";
    successPanel.classList.remove("hidden");
  } catch (err) {
    showStatus("Pairing failed: " + err, true);
    pairBtn.disabled = false;
    pairBtn.textContent = "Pair";
  }
});
