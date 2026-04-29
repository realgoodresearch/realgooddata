const storageKey = "rrg-data-portal-token";
const invalidTokenMessage = "Invalid or expired access token";
const state = {
  items: [],
  token: localStorage.getItem(storageKey) || "",
  query: ""
};
let statusTimeout = null;

const searchInput = document.getElementById("search-input");
const collectionList = document.getElementById("collection-list");
const tokenInput = document.getElementById("token-input");
const tokenTrigger = document.getElementById("token-trigger");
const tokenModal = document.getElementById("token-modal");
const tokenModalClose = document.getElementById("token-modal-close");
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
syncTokenTrigger();

document.getElementById("apply-token")?.addEventListener("click", () => {
  state.token = tokenInput.value.trim();
  if (state.token) {
    localStorage.setItem(storageKey, state.token);
  } else {
    localStorage.removeItem(storageKey);
  }
  syncTokenTrigger();
  closeTokenModal();
  fetchCollections();
});

document.getElementById("clear-token")?.addEventListener("click", () => {
  state.token = "";
  if (tokenInput) {
    tokenInput.value = "";
  }
  localStorage.removeItem(storageKey);
  syncTokenTrigger();
  closeTokenModal();
  fetchCollections();
});

tokenTrigger?.addEventListener("click", openTokenModal);
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

searchInput?.addEventListener("input", (event) => {
  state.query = event.target.value.toLowerCase().trim();
  render();
});

async function fetchCollections() {
  return fetchCollectionsInternal(false);
}

async function fetchCollectionsInternal(retriedAfterInvalidToken) {
  const headers = {};
  if (state.token) {
    headers["X-Access-Token"] = state.token;
  }

  try {
    const response = await fetch("/api/v1/collections", { headers });
    const payload = await response.json().catch(() => ({}));
    if (
      response.status === 401 &&
      state.token &&
      !retriedAfterInvalidToken &&
      isInvalidTokenError(payload)
    ) {
      clearActiveToken();
      syncTokenTrigger();
      showTemporaryStatus(invalidTokenMessage, true);
      return fetchCollectionsInternal(true);
    }
    if (!response.ok) {
      throw new Error(payload.detail || "Failed to load collections.");
    }
    state.items = Array.isArray(payload.items) ? payload.items : [];
    clearStatus();
    render();
  } catch (error) {
    state.items = [];
    render();
    showStatus(error.message || "Failed to load collections.", true);
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

function render() {
  if (!collectionList) {
    return;
  }

  const visibleItems = state.items.filter((item) => {
    if (!state.query) {
      return true;
    }
    const haystack = [
      item.title,
      item.summary || "",
      item.slug,
      item.search_text || ""
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(state.query);
  });

  if (visibleItems.length === 0) {
    collectionList.innerHTML = '<div class="empty-state">No collections match the current search.</div>';
    return;
  }

  collectionList.innerHTML = "";
  for (const item of visibleItems) {
    collectionList.appendChild(renderCollectionCard(item));
  }
}

function renderCollectionCard(item) {
  const card = document.createElement("a");
  card.className = "collection-card";
  card.href = `collection.html?slug=${encodeURIComponent(item.slug)}`;
  const tags = Array.isArray(item.tags) ? item.tags.filter(Boolean) : [];
  const tagsHtml = tags.length
    ? `<div class="tag-list">${tags.map((tag) => `<span class="tag-chip">${escapeHtml(tag)}</span>`).join("")}</div>`
    : "";

  const published = item.published_at
    ? new Date(item.published_at).toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric"
    })
    : "Unpublished";

  card.innerHTML = `
    <div class="collection-card-top">
      <div>
        <h2>${escapeHtml(item.title)}</h2>
        <div class="slug">${escapeHtml(item.slug)}</div>
      </div>
    </div>
    <p class="summary">${escapeHtml(item.summary || "No summary provided.")}</p>
    ${tagsHtml}
    <div class="meta">
      <span>${escapeHtml(published)}</span>
      <span>${item.counts.total} files</span>
    </div>
    <div class="count-list">
      <span class="count-pill public">${item.counts.public} public</span>
      <span class="count-pill restricted">${item.counts.restricted} restricted</span>
      <span class="count-pill confidential">${item.counts.confidential} confidential</span>
    </div>
  `;

  return card;
}

function openTokenModal() {
  if (!tokenModal) {
    return;
  }
  tokenModal.classList.remove("hidden");
  tokenModal.setAttribute("aria-hidden", "false");
  window.setTimeout(() => tokenInput?.focus(), 0);
}

function syncTokenTrigger() {
  if (!tokenTrigger) {
    return;
  }
  const unlocked = Boolean(state.token);
  tokenTrigger.innerHTML = unlocked ? unlockedIcon : lockedIcon;
  tokenTrigger.classList.toggle("is-unlocked", unlocked);
  tokenTrigger.setAttribute(
    "aria-label",
    unlocked
      ? "Access token is applied. Click to change or clear it."
      : "Provide access token to unlock restricted data."
  );
  tokenTrigger.setAttribute(
    "title",
    unlocked
      ? "Access token is applied. Click to change or clear it."
      : "Provide access token to unlock restricted data."
  );
}

function closeTokenModal() {
  if (!tokenModal) {
    return;
  }
  tokenModal.classList.add("hidden");
  tokenModal.setAttribute("aria-hidden", "true");
}

function showStatus(message, isError = false) {
  let statusNode = document.getElementById("status");
  if (!statusNode) {
    statusNode = document.createElement("section");
    statusNode.id = "status";
    statusNode.setAttribute("aria-live", "polite");
    const shell = document.querySelector(".catalogue-shell");
    const list = document.getElementById("collection-list");
    if (shell && list) {
      shell.insertBefore(statusNode, list);
    } else {
      return;
    }
  }
  statusNode.textContent = message;
  statusNode.className = isError ? "status-card error" : "status-card";
}

function showTemporaryStatus(message, isError = false, durationMs = 4000) {
  showStatus(message, isError);
  if (statusTimeout) {
    window.clearTimeout(statusTimeout);
  }
  statusTimeout = window.setTimeout(() => {
    clearStatus();
    statusTimeout = null;
  }, durationMs);
}

function clearStatus() {
  if (statusTimeout) {
    window.clearTimeout(statusTimeout);
    statusTimeout = null;
  }
  document.getElementById("status")?.remove();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

fetchCollections();
