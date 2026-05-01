function initializeDatasetReordering() {
  const table = document.querySelector('[data-sortable-datasets="true"]');
  if (!table) {
    return;
  }

  const tbody = table.querySelector("tbody");
  const saveButton = document.querySelector("[data-save-order]");
  const statusNode = document.querySelector("[data-reorder-status]");
  const collectionId = table.dataset.collectionId;
  const classification = table.dataset.classification || "";
  const datasetRole = table.dataset.datasetRole || "";
  const reorderUrl = table.dataset.reorderUrl;
  if (!tbody || !saveButton || !statusNode || !collectionId || !reorderUrl) {
    return;
  }

  let draggedRow = null;
  let dirty = false;
  let saving = false;

  function rows() {
    return Array.from(tbody.querySelectorAll("tr[data-dataset-id]"));
  }

  function updateOrderNumbers() {
    rows().forEach((row, index) => {
      const orderNode = row.querySelector("[data-order-number]");
      if (orderNode) {
        orderNode.textContent = String(index + 1);
      }
    });
  }

  function setStatus(message) {
    statusNode.textContent = message;
  }

  function setDirty(nextDirty) {
    dirty = nextDirty;
    saveButton.disabled = !dirty || saving;
    if (!saving) {
      setStatus(dirty ? "Order changed. Save to apply." : "Order unchanged.");
    }
  }

  function clearDropTargets() {
    rows().forEach((row) => row.classList.remove("drop-target"));
  }

  rows().forEach((row) => {
    row.addEventListener("dragstart", () => {
      draggedRow = row;
      row.classList.add("dragging");
    });
    row.addEventListener("dragend", () => {
      row.classList.remove("dragging");
      clearDropTargets();
      draggedRow = null;
    });
    row.addEventListener("dragover", (event) => {
      if (!draggedRow || draggedRow === row) {
        return;
      }
      event.preventDefault();
      clearDropTargets();
      row.classList.add("drop-target");
    });
    row.addEventListener("dragleave", () => {
      row.classList.remove("drop-target");
    });
    row.addEventListener("drop", (event) => {
      if (!draggedRow || draggedRow === row) {
        return;
      }
      event.preventDefault();
      const rowBounds = row.getBoundingClientRect();
      const insertBefore = event.clientY < rowBounds.top + rowBounds.height / 2;
      if (insertBefore) {
        tbody.insertBefore(draggedRow, row);
      } else {
        tbody.insertBefore(draggedRow, row.nextSibling);
      }
      clearDropTargets();
      updateOrderNumbers();
      setDirty(true);
    });
  });

  saveButton.addEventListener("click", async () => {
    if (!dirty || saving) {
      return;
    }

    saving = true;
    saveButton.disabled = true;
    setStatus("Saving order...");

    const orderedIds = rows().map((row) => row.dataset.datasetId).join(",");
    const payload = new URLSearchParams({
      collection_id: collectionId,
      classification,
      dataset_role: datasetRole,
      ordered_ids: orderedIds,
    });

    try {
      const response = await fetch(reorderUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        },
        body: payload.toString(),
      });
      if (!response.ok) {
        throw new Error("save_failed");
      }
      setDirty(false);
      setStatus("Order saved.");
    } catch (_error) {
      setStatus("Unable to save order. Reload and try again.");
      saveButton.disabled = false;
    } finally {
      saving = false;
      if (dirty) {
        saveButton.disabled = false;
      }
    }
  });
}

document.addEventListener("DOMContentLoaded", initializeDatasetReordering);
