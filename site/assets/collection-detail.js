const storageKey = "rrg-data-portal-token";
const invalidTokenMessage = "Invalid or expired access token";
const state = {
  token: localStorage.getItem(storageKey) || "",
  collection: null
};
let detailStatusTimeout = null;

const tokenInput = document.getElementById("token-input");
const tokenModal = document.getElementById("token-modal");
const tokenModalClose = document.getElementById("token-modal-close");
const detailNode = document.getElementById("collection-detail");
const lockedIcon = `
  <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
    <path d="M5.5 6V4.75a2.5 2.5 0 0 1 5 0V6h.75A1.75 1.75 0 0 1 13 7.75v5.5A1.75 1.75 0 0 1 11.25 15h-6.5A1.75 1.75 0 0 1 3 13.25v-5.5A1.75 1.75 0 0 1 4.75 6zm1 0h3V4.75a1.5 1.5 0 0 0-3 0z"/>
  </svg>
`;
const unlockedIcon = `
  <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
    <path d="M11 6V4.75a3 3 0 1 0-6 0 .75.75 0 0 1-1.5 0 4.5 4.5 0 1 1 9 0V6h.25A1.75 1.75 0 0 1 14.5 7.75v5.5A1.75 1.75 0 0 1 12.75 15h-7.5A1.75 1.75 0 0 1 3.5 13.25v-5.5A1.75 1.75 0 0 1 5.25 6zm-5.75 1.5a.25.25 0 0 0-.25.25v5.5c0 .138.112.25.25.25h7.5a.25.25 0 0 0 .25-.25v-5.5a.25.25 0 0 0-.25-.25z"/>
  </svg>
`;

if (tokenInput) {
  tokenInput.value = state.token;
}

document.getElementById("apply-token")?.addEventListener("click", () => {
  state.token = tokenInput.value.trim();
  if (state.token) {
    localStorage.setItem(storageKey, state.token);
  } else {
    localStorage.removeItem(storageKey);
  }
  closeTokenModal();
  fetchCollection();
});

document.getElementById("clear-token")?.addEventListener("click", () => {
  state.token = "";
  if (tokenInput) {
    tokenInput.value = "";
  }
  localStorage.removeItem(storageKey);
  closeTokenModal();
  fetchCollection();
});

tokenModalClose?.addEventListener("click", closeTokenModal);
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

async function fetchCollection() {
  return fetchCollectionInternal(false);
}

async function fetchCollectionInternal(retriedAfterInvalidToken) {
  const slug = new URLSearchParams(window.location.search).get("slug");
  if (!slug) {
    showDetailMessage("No collection slug was provided.", true);
    return;
  }

  const headers = {};
  if (state.token) {
    headers["X-Access-Token"] = state.token;
  }

  try {
    const response = await fetch(`/api/v1/collections/${encodeURIComponent(slug)}`, {
      headers
    });
    const payload = await response.json().catch(() => ({}));
    if (
      response.status === 401 &&
      state.token &&
      !retriedAfterInvalidToken &&
      isInvalidTokenError(payload)
    ) {
      clearActiveToken();
      showTemporaryDetailStatus(invalidTokenMessage, true);
      return fetchCollectionInternal(true);
    }
    if (!response.ok) {
      throw new Error(payload.detail || "Failed to load collection.");
    }
    state.collection = payload;
    clearDetailStatus();
    renderCollection();
  } catch (error) {
    showDetailMessage(error.message || "Failed to load collection.", true);
  }
}

function isInvalidTokenError(payload) {
  const detail = String(payload?.detail || "").trim().toLowerCase();
  return detail === "invalid or expired access token.";
}

function clearActiveToken() {
  state.token = "";
  if (tokenInput) {
    tokenInput.value = "";
  }
  localStorage.removeItem(storageKey);
}

function renderCollection() {
  if (!detailNode || !state.collection) {
    return;
  }

  const collection = state.collection;
  const items = Array.isArray(collection.datasets) ? [...collection.datasets].sort(compareCollectionItems) : [];
  const published = collection.published_at
    ? new Date(collection.published_at).toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric"
    })
    : "Unpublished";
  const tags = Array.isArray(collection.tags) ? collection.tags.filter(Boolean) : [];
  const tagsHtml = tags.length
    ? `<div class="tag-list">${tags.map((tag) => `<span class="tag-chip">${escapeHtml(tag)}</span>`).join("")}</div>`
    : "";
  const panelHtml = renderCollectionItemsPanel(items);

  detailNode.innerHTML = `
    <section class="detail-hero">
      <div class="detail-hero-top">
        <div>
          <p class="eyebrow">Collection</p>
          <h1 class="hero-title">${escapeHtml(collection.title)}</h1>
        </div>
        <button
          id="detail-token-trigger"
          class="token-trigger button-secondary${state.token ? " is-unlocked" : ""}"
          type="button"
          aria-label="${state.token ? "Access token is applied. Click to change or clear it." : "Provide access token to unlock restricted data."}"
          title="${state.token ? "Access token is applied. Click to change or clear it." : "Provide access token to unlock restricted data."}">
          ${state.token ? unlockedIcon : lockedIcon}
        </button>
      </div>
      <p class="hero-copy">${escapeHtml(collection.summary || "No summary provided.")}</p>
      ${tagsHtml}
      <div class="meta">
        <span>${escapeHtml(published)}</span>
        <span>${collection.counts.total} files</span>
        <span>${collection.counts.downloadable} currently downloadable</span>
      </div>
      <div class="count-list">
        <span class="count-pill public">${collection.counts.public} public</span>
        <span class="count-pill restricted">${collection.counts.restricted} restricted</span>
        <span class="count-pill confidential">${collection.counts.confidential} confidential</span>
      </div>
      <div class="detail-hero-actions">
        <button
          id="download-all-trigger"
          class="button-primary"
          type="button"
          ${collection.counts.downloadable > 0 ? "" : "disabled"}>
          Download All
        </button>
      </div>
    </section>
    ${panelHtml}
  `;

  detailNode.querySelector("#detail-token-trigger")?.addEventListener("click", openTokenModal);
  detailNode.querySelector("#download-all-trigger")?.addEventListener("click", (event) => {
    downloadCollectionArchive(event.currentTarget);
  });
  detailNode.querySelectorAll("[data-open-dataset-id]").forEach((button) => {
    button.addEventListener("click", () => openDataset(button));
  });
}

function renderCollectionItemsPanel(items) {
  if (!items.length) {
    return `
      <section class="dataset-table-card">
        <div class="empty-state">No files are listed for this collection.</div>
      </section>
    `;
  }
  return `
    <section class="dataset-table-card">
      <div class="dataset-table-wrap">
        <table class="dataset-table">
          <thead>
            <tr>
              <th>File</th>
              <th>Size</th>
              <th></th>
            </tr>
          </thead>
          <tbody>${items.map(renderDatasetRow).join("")}</tbody>
        </table>
      </div>
    </section>
  `;
}

function compareCollectionItems(left, right) {
  const classificationDelta = classificationRank(left.classification) - classificationRank(right.classification);
  if (classificationDelta !== 0) {
    return classificationDelta;
  }
  const roleDelta = datasetRoleRank(left.dataset_role) - datasetRoleRank(right.dataset_role);
  if (roleDelta !== 0) {
    return roleDelta;
  }
  const leftSort = Number.isFinite(left.sort_order) ? left.sort_order : 0;
  const rightSort = Number.isFinite(right.sort_order) ? right.sort_order : 0;
  if (leftSort !== rightSort) {
    return leftSort - rightSort;
  }
  return String(left.title || "").localeCompare(String(right.title || ""));
}

function datasetRoleRank(role) {
  if (role === "documentation") {
    return 0;
  }
  if (role === "visuals") {
    return 1;
  }
  return 2;
}

function classificationRank(classification) {
  if (classification === "public") {
    return 0;
  }
  if (classification === "restricted") {
    return 1;
  }
  if (classification === "confidential") {
    return 2;
  }
  return 3;
}

function renderDatasetRow(item) {
  const roleLabel = datasetRoleLabel(item.dataset_role);
  const roleBadge = `<span class="badge role-badge role-${escapeHtml(item.dataset_role || "data")}">${escapeHtml(roleLabel)}</span>`;
  const classificationBadge = `<span class="badge ${escapeHtml(item.classification)}">${escapeHtml(item.classification)}</span>`;
  const actionButtons = item.classification === "confidential"
    ? ""
    : datasetRoleSupportsView(item.dataset_role)
    ? `
      <button
        class="button-primary"
        type="button"
        data-open-dataset-id="${escapeHtml(item.id)}"
        data-open-mode="new-tab"
        data-delivery-mode="inline"
        ${item.downloadable ? "" : "disabled"}>
        View
      </button>
      <button
        class="button-primary"
        type="button"
        data-open-dataset-id="${escapeHtml(item.id)}"
        data-open-mode="download"
        data-delivery-mode="download"
        ${item.downloadable ? "" : "disabled"}>
        Download
      </button>
    `
    : `
      <button
        class="button-primary"
        type="button"
        data-open-dataset-id="${escapeHtml(item.id)}"
        data-open-mode="download"
        data-delivery-mode="download"
        ${item.downloadable ? "" : "disabled"}>
        Download
      </button>
    `;
  return `
    <tr>
      <td>
        <strong>${escapeHtml(item.title)}</strong>
        <div class="filename">${escapeHtml(item.filename || "")}</div>
        <div class="summary">${escapeHtml(item.summary || "")}</div>
        <div class="dataset-badges">
          ${classificationBadge}
          ${roleBadge}
        </div>
      </td>
      <td>${escapeHtml(typeof item.file_size_bytes === "number" ? formatBytes(item.file_size_bytes) : "Unknown")}</td>
      <td class="table-actions">
        ${actionButtons}
      </td>
    </tr>
  `;
}

function datasetRoleLabel(role) {
  if (role === "documentation") {
    return "Documentation";
  }
  if (role === "visuals") {
    return "Visuals";
  }
  return "Data";
}

function datasetRoleSupportsView(role) {
  return role === "documentation" || role === "visuals";
}

async function openDataset(button) {
  const datasetId = button.dataset.openDatasetId;
  if (!datasetId) {
    return;
  }

  button.disabled = true;
  const originalLabel = button.textContent || "Open";
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
        dataset_id: datasetId,
        delivery_mode: button.dataset.deliveryMode || "download"
      })
    });
    const payload = await response.json().catch(() => ({}));
    if (response.status === 401 && state.token && isInvalidTokenError(payload)) {
      clearActiveToken();
      showTemporaryDetailStatus(invalidTokenMessage, true);
      closeTokenModal();
      fetchCollection();
      return;
    }
    if (!response.ok) {
      throw new Error(payload.detail || "Download request failed.");
    }
    if (!payload.allowed || !payload.download_url) {
      throw new Error("This file is currently not available.");
    }
    if (button.dataset.openMode === "new-tab") {
      window.open(payload.download_url, "_blank", "noopener");
    } else {
      window.location.href = payload.download_url;
    }
  } catch (error) {
    showDetailMessage(error.message || "Request failed.", true);
  } finally {
    button.disabled = false;
    button.textContent = originalLabel;
  }
}

async function downloadCollectionArchive(button) {
  if (!state.collection?.slug) {
    return;
  }

  button.disabled = true;
  const originalLabel = button.textContent || "Download All";
  button.textContent = "Preparing…";

  const headers = {};
  if (state.token) {
    headers["X-Access-Token"] = state.token;
  }

  try {
    const response = await fetch(`/api/v1/collections/${encodeURIComponent(state.collection.slug)}/download-all`, {
      headers
    });
    if (response.status === 401 && state.token) {
      const payload = await response.json().catch(() => ({}));
      if (isInvalidTokenError(payload)) {
        clearActiveToken();
        showTemporaryDetailStatus(invalidTokenMessage, true);
        closeTokenModal();
        fetchCollection();
        return;
      }
    }
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || "Collection download failed.");
    }
    const blob = await response.blob();
    const filename = parseDownloadFilename(response.headers.get("Content-Disposition")) || `${state.collection.slug}.zip`;
    const blobUrl = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = blobUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(blobUrl);
  } catch (error) {
    showDetailMessage(error.message || "Collection download failed.", true);
  } finally {
    button.disabled = false;
    button.textContent = originalLabel;
  }
}

function parseDownloadFilename(contentDisposition) {
  if (!contentDisposition) {
    return null;
  }
  const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    return decodeURIComponent(utf8Match[1]);
  }
  const quotedMatch = contentDisposition.match(/filename=\"([^\"]+)\"/i);
  if (quotedMatch?.[1]) {
    return quotedMatch[1];
  }
  const simpleMatch = contentDisposition.match(/filename=([^;]+)/i);
  return simpleMatch?.[1]?.trim() || null;
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

function showDetailMessage(message, isError = false) {
  if (!detailNode) {
    return;
  }
  detailNode.innerHTML = `<section class="status-card${isError ? " error" : ""}">${escapeHtml(message)}</section>`;
}

function showTemporaryDetailStatus(message, isError = false, durationMs = 4000) {
  let statusNode = document.getElementById("detail-status");
  if (!statusNode) {
    statusNode = document.createElement("section");
    statusNode.id = "detail-status";
    statusNode.setAttribute("aria-live", "polite");
    const shell = document.querySelector(".catalogue-shell");
    if (shell && detailNode) {
      shell.insertBefore(statusNode, detailNode);
    } else {
      return;
    }
  }
  statusNode.textContent = message;
  statusNode.className = isError ? "status-card error" : "status-card";
  if (detailStatusTimeout) {
    window.clearTimeout(detailStatusTimeout);
  }
  detailStatusTimeout = window.setTimeout(() => {
    clearDetailStatus();
  }, durationMs);
}

function clearDetailStatus() {
  if (detailStatusTimeout) {
    window.clearTimeout(detailStatusTimeout);
    detailStatusTimeout = null;
  }
  document.getElementById("detail-status")?.remove();
}

function formatBytes(bytes) {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes / 1024;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

fetchCollection();
