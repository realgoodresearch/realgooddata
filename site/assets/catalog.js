const storageKey = "rrg-data-portal-token";
const state = {
  items: [],
  token: localStorage.getItem(storageKey) || "",
  filters: {
    query: "",
    classification: "all",
    access: "all"
  }
};

const tokenInput = document.getElementById("token-input");
const tokenStatus = document.getElementById("token-status");
const catalogueNode = document.getElementById("catalogue");
const searchInput = document.getElementById("search-input");
const classificationFilter = document.getElementById("classification-filter");
const accessFilter = document.getElementById("access-filter");
const tokenTrigger = document.getElementById("token-trigger");
const tokenModal = document.getElementById("token-modal");
const tokenModalClose = document.getElementById("token-modal-close");

const metricTotal = document.getElementById("count-total");
const metricDownloadable = document.getElementById("count-downloadable");
const metricLocked = document.getElementById("count-locked");

if (tokenInput) {
  tokenInput.value = state.token;
}
syncTokenStatus();

document.getElementById("apply-token")?.addEventListener("click", () => {
  state.token = tokenInput.value.trim();
  if (state.token) {
    localStorage.setItem(storageKey, state.token);
  } else {
    localStorage.removeItem(storageKey);
  }
  syncTokenStatus();
  closeTokenModal();
  fetchCatalogue();
});

document.getElementById("clear-token")?.addEventListener("click", () => {
  state.token = "";
  tokenInput.value = "";
  localStorage.removeItem(storageKey);
  syncTokenStatus();
  closeTokenModal();
  fetchCatalogue();
});

tokenTrigger?.addEventListener("click", () => {
  openTokenModal();
});

tokenModalClose?.addEventListener("click", () => {
  closeTokenModal();
});

tokenModal?.addEventListener("click", (event) => {
  if (event.target instanceof HTMLElement && event.target.dataset.closeModal === "true") {
    closeTokenModal();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeTokenModal();
  }
});

searchInput?.addEventListener("input", (event) => {
  state.filters.query = event.target.value.toLowerCase().trim();
  render();
});

classificationFilter?.addEventListener("change", (event) => {
  state.filters.classification = event.target.value;
  render();
});

accessFilter?.addEventListener("change", (event) => {
  state.filters.access = event.target.value;
  render();
});

async function fetchCatalogue() {
  const headers = {};
  if (state.token) {
    headers["X-Access-Token"] = state.token;
  }

  try {
    const response = await fetch("/api/v1/catalog", { headers });
    const payload = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(payload.detail || "Failed to load catalogue.");
    }

    state.items = Array.isArray(payload.items) ? payload.items : [];
    render();
  } catch (error) {
    state.items = [];
    render();
    showStatus(error.message || "Failed to load catalogue.", true);
  }
}

function showStatus(message, isError = false) {
  let statusNode = document.getElementById("status");
  if (!statusNode) {
    statusNode = document.createElement("section");
    statusNode.id = "status";
    statusNode.setAttribute("aria-live", "polite");
    const shell = document.querySelector(".catalogue-shell");
    const catalogue = document.getElementById("catalogue");
    if (shell && catalogue) {
      shell.insertBefore(statusNode, catalogue);
    } else if (catalogue && catalogue.parentNode) {
      catalogue.parentNode.insertBefore(statusNode, catalogue);
    } else {
      return;
    }
  }
  statusNode.textContent = message;
  statusNode.className = isError ? "status-card error" : "status-card";
}

function clearStatus() {
  const statusNode = document.getElementById("status");
  if (statusNode) {
    statusNode.remove();
  }
}

function syncTokenStatus() {
  if (!tokenStatus) {
    return;
  }
  tokenStatus.textContent = state.token
    ? "Token saved in this browser and applied to catalogue requests."
    : "No token is currently applied.";
}

function openTokenModal() {
  if (!tokenModal) {
    return;
  }
  tokenModal.classList.remove("hidden");
  tokenModal.setAttribute("aria-hidden", "false");
  window.setTimeout(() => tokenInput?.focus(), 0);
}

function closeTokenModal() {
  if (!tokenModal) {
    return;
  }
  tokenModal.classList.add("hidden");
  tokenModal.setAttribute("aria-hidden", "true");
}

function render() {
  const visibleItems = state.items.filter(matchesFilters);
  clearStatus();
  if (metricTotal) {
    metricTotal.textContent = String(state.items.length);
  }
  if (metricDownloadable) {
    metricDownloadable.textContent = String(state.items.filter((item) => item.downloadable).length);
  }
  if (metricLocked) {
    metricLocked.textContent = String(state.items.filter((item) => !item.downloadable).length);
  }

  if (!catalogueNode) {
    return;
  }

  if (visibleItems.length === 0) {
    catalogueNode.innerHTML = '<div class="empty-state">No datasets match the current filters.</div>';
    return;
  }

  catalogueNode.innerHTML = "";
  for (const item of visibleItems) {
    catalogueNode.appendChild(renderCard(item));
  }
}

function matchesFilters(item) {
  if (state.filters.classification !== "all" && item.classification !== state.filters.classification) {
    return false;
  }

  if (state.filters.access === "downloadable" && !item.downloadable) {
    return false;
  }

  if (state.filters.access === "locked" && item.downloadable) {
    return false;
  }

  if (!state.filters.query) {
    return true;
  }

  const haystack = [
    item.title,
    item.summary || "",
    item.slug,
    ...(item.tags || [])
  ].join(" ").toLowerCase();

  return haystack.includes(state.filters.query);
}

function renderCard(item) {
  const card = document.createElement("article");
  card.className = "dataset-card";

  const published = item.published_at
    ? new Date(item.published_at).toLocaleDateString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric"
      })
    : "Unpublished";

  const sizeText = typeof item.file_size_bytes === "number"
    ? formatBytes(item.file_size_bytes)
    : "Size unavailable";

  const accessMessage = {
    public: "Available for immediate download.",
    token_granted: "Your current token allows this download.",
    token_required: "Listed here, but requires a valid token to download.",
    confidential_no_download: "Listed for awareness only. Download is disabled."
  }[item.access_reason] || "Access state unavailable.";

  const tagMarkup = (item.tags || [])
    .map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`)
    .join("");

  card.innerHTML = `
    <div class="dataset-top">
      <div>
        <h3>${escapeHtml(item.title)}</h3>
        <div class="slug">${escapeHtml(item.slug)}</div>
      </div>
      <span class="badge ${item.classification}">${escapeHtml(item.classification)}</span>
    </div>
    <p class="summary">${escapeHtml(item.summary || "No summary provided.")}</p>
    <div class="meta">
      <span>${escapeHtml(published)}</span>
      <span>${escapeHtml(sizeText)}</span>
      <span>${escapeHtml(item.mime_type || "Unknown format")}</span>
    </div>
    <div class="tag-row">${tagMarkup}</div>
    <div class="dataset-footer">
      <div class="access-note">${escapeHtml(accessMessage)}</div>
      <button class="button-primary download-button" type="button"${item.downloadable ? "" : " disabled"}>
        Download
      </button>
    </div>
  `;

  const button = card.querySelector(".download-button");
  if (item.downloadable) {
    button.addEventListener("click", () => handleDownload(item, button));
  }

  return card;
}

async function handleDownload(item, button) {
  button.disabled = true;
  const originalLabel = button.textContent;
  button.textContent = "Preparing…";

  const headers = {
    "Content-Type": "application/json"
  };

  if (state.token) {
    headers["X-Access-Token"] = state.token;
  }

  try {
    const response = await fetch("/api/v1/download-url", {
      method: "POST",
      headers,
      body: JSON.stringify({
        dataset_id: item.id
      })
    });
    const payload = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(payload.detail || "Download request failed.");
    }

    if (!payload.allowed || !payload.download_url) {
      throw new Error("This dataset is currently not downloadable.");
    }

    window.location.href = payload.download_url;
    clearStatus();
  } catch (error) {
    showStatus(error.message || "Download request failed.", true);
  } finally {
    button.disabled = false;
    button.textContent = originalLabel;
  }
}

function formatBytes(bytes) {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = units[0];
  for (const nextUnit of units) {
    value /= 1024;
    unit = nextUnit;
    if (value < 1024) {
      break;
    }
  }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${unit}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

fetchCatalogue();
