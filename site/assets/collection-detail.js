const storageKey = "rrg-data-portal-token";
const state = {
  token: localStorage.getItem(storageKey) || "",
  collection: null
};

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
    if (!response.ok) {
      throw new Error(payload.detail || "Failed to load collection.");
    }
    state.collection = payload;
    renderCollection();
  } catch (error) {
    showDetailMessage(error.message || "Failed to load collection.", true);
  }
}

function renderCollection() {
  if (!detailNode || !state.collection) {
    return;
  }

  const collection = state.collection;
  const published = collection.published_at
    ? new Date(collection.published_at).toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric"
    })
    : "Unpublished";

  const datasetRows = (collection.datasets || []).map(renderDatasetRow).join("");
  const readmePanel = collection.readme_url
    ? `
      <section class="readme-panel">
        <div class="readme-header">
          <h2>Collection README</h2>
          <a class="button-secondary" href="${escapeHtml(collection.readme_url)}" target="_blank" rel="noreferrer">
            Open ${escapeHtml(collection.readme_filename || "README.pdf")}
          </a>
        </div>
        <iframe class="readme-frame" src="${escapeHtml(collection.readme_url)}" title="Collection README PDF"></iframe>
      </section>
    `
    : "";

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
    </section>
    ${readmePanel}
    <section class="dataset-table-card">
      <div class="dataset-table-wrap">
        <table class="dataset-table">
          <thead>
            <tr>
              <th>File</th>
              <th>Classification</th>
              <th>Size</th>
              <th></th>
            </tr>
          </thead>
          <tbody>${datasetRows}</tbody>
        </table>
      </div>
    </section>
  `;

  detailNode.querySelector("#detail-token-trigger")?.addEventListener("click", openTokenModal);
  detailNode.querySelectorAll("[data-download-dataset-id]").forEach((button) => {
    button.addEventListener("click", () => handleDownload(button));
  });
}

function renderDatasetRow(item) {
  return `
    <tr>
      <td>
        <strong>${escapeHtml(item.title)}</strong>
        <div class="filename">${escapeHtml(item.filename || "")}</div>
        <div class="summary">${escapeHtml(item.summary || "")}</div>
      </td>
      <td><span class="badge ${item.classification}">${escapeHtml(item.classification)}</span></td>
      <td>${escapeHtml(typeof item.file_size_bytes === "number" ? formatBytes(item.file_size_bytes) : "Unknown")}</td>
      <td class="table-actions">
        <button
          class="button-primary"
          type="button"
          data-download-dataset-id="${escapeHtml(item.id)}"
          ${item.downloadable ? "" : "disabled"}>
          Download
        </button>
      </td>
    </tr>
  `;
}

async function handleDownload(button) {
  const datasetId = button.dataset.downloadDatasetId;
  if (!datasetId) {
    return;
  }

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
      body: JSON.stringify({ dataset_id: datasetId })
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.detail || "Download request failed.");
    }
    if (!payload.allowed || !payload.download_url) {
      throw new Error("This dataset is currently not downloadable.");
    }
    window.location.href = payload.download_url;
  } catch (error) {
    showDetailMessage(error.message || "Download request failed.", true);
  } finally {
    button.disabled = false;
    button.textContent = originalLabel;
  }
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
