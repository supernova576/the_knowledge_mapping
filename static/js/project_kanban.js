(function () {
  const board = document.getElementById('kanban-board');
  if (!board) return;

  const projectName = board.dataset.projectName;
  const columns = ['Not Started', 'In Progress', 'Done'];
  const statusOptions = JSON.parse(board.dataset.statusOptions || '["Not Started","In Progress","Done"]');
  let refreshHandle = null;
  const deleteModalElement = document.getElementById('kanbanDeleteModal');
  const deleteMessage = document.getElementById('kanban-delete-message');
  const deleteConfirm = document.getElementById('kanban-delete-confirm');
  const deleteModal = deleteModalElement ? new bootstrap.Modal(deleteModalElement) : null;
  let pendingDelete = null;

  const escapeHtml = (value) => String(value || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const fetchJson = async (url, options) => {
    const response = await fetch(url, options);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || 'Request failed.');
    }
    return payload;
  };

  const statusSelect = (selectedValue) => `
    <select class="form-select form-select-sm" name="status">
      ${statusOptions.map((option) => `<option ${option === selectedValue ? 'selected' : ''}>${escapeHtml(option)}</option>`).join('')}
    </select>
  `;

  const scheduleRefresh = () => {
    window.clearTimeout(refreshHandle);
    refreshHandle = window.setTimeout(render, 30000);
  };

  const render = async () => {
    try {
      const payload = await fetchJson(`/api/projects/${encodeURIComponent(projectName)}/kanban`, {
        headers: { Accept: 'application/json' }
      });

      const groups = payload.columns || {};
      const deadlineMapping = payload.deadline_mapping || {};
      const boardColumns = columns.map((status) => {
        const cards = (groups[status] || []).map((item) => {
          const deadline = deadlineMapping[item.deliverable] || {};
          return `
            <div class="card mb-3 shadow-sm">
              <div class="card-body">
                <form class="km-kanban-item-form" data-item-id="${item.id}">
                  <div class="mb-2">
                    <label class="form-label small mb-1">Deliverable</label>
                    <input class="form-control form-control-sm" name="deliverable" maxlength="200" value="${escapeHtml(item.deliverable)}" required />
                  </div>
                  <div class="mb-2">
                    <label class="form-label small mb-1">Status</label>
                    ${statusSelect(item.status_normalized || item.status || status)}
                  </div>
                  <div class="mb-2">
                    <label class="form-label small mb-1">Due</label>
                    <input class="form-control form-control-sm" name="due" placeholder="DD.MM.YYYY" value="${escapeHtml(item.due)}" />
                  </div>
                  <div class="small text-body-secondary mb-3">Deadline: ${escapeHtml(deadline.name || '')}${deadline.date ? ` · ${escapeHtml(deadline.date)}` : ''}${deadline.time && deadline.time !== '-' ? ` · ${escapeHtml(deadline.time)}` : ''}</div>
                  <div class="d-flex gap-2">
                    <button class="btn btn-outline-primary btn-sm" type="submit">Save</button>
                    <button class="btn btn-outline-danger btn-sm" type="button" data-action="delete">Delete</button>
                  </div>
                </form>
              </div>
            </div>
          `;
        }).join('');

        return `<div class="col-12 col-md-4"><div class="card shadow-sm h-100"><div class="card-header fw-semibold">${escapeHtml(status)}</div><div class="card-body">${cards || '<p class="text-body-secondary mb-0">No items.</p>'}</div></div></div>`;
      }).join('');

      board.innerHTML = `
        <div class="card shadow-sm mb-3">
          <div class="card-body">
            <h2 class="h5">Add Deliverable</h2>
            <form id="kanban-create-form" class="row g-3">
              <div class="col-md-5">
                <label class="form-label" for="kanban-deliverable">Deliverable</label>
                <input class="form-control" id="kanban-deliverable" name="deliverable" maxlength="200" required />
              </div>
              <div class="col-md-3">
                <label class="form-label" for="kanban-status">Status</label>
                <select class="form-select" id="kanban-status" name="status">
                  ${statusOptions.map((option) => `<option ${option === 'Not Started' ? 'selected' : ''}>${escapeHtml(option)}</option>`).join('')}
                </select>
              </div>
              <div class="col-md-2">
                <label class="form-label" for="kanban-due">Due</label>
                <input class="form-control" id="kanban-due" name="due" placeholder="DD.MM.YYYY" />
              </div>
              <div class="col-md-2 d-grid">
                <label class="form-label d-none d-md-block">&nbsp;</label>
                <button class="btn btn-primary" type="submit">Add</button>
              </div>
            </form>
            <div id="kanban-feedback" class="small mt-3"></div>
          </div>
        </div>
        <div class="row g-3">${boardColumns}</div>
      `;

      bindEvents();
      scheduleRefresh();
    } catch (error) {
      board.innerHTML = `<div class="alert alert-danger mb-0">${escapeHtml(error.message || 'Failed to load Kanban.')}</div>`;
    }
  };

  const setFeedback = (message, isError) => {
    const feedback = document.getElementById('kanban-feedback');
    if (!feedback) return;
    feedback.className = `small mt-3 ${isError ? 'text-danger' : 'text-success'}`;
    feedback.textContent = message;
  };

  const openDeletePrompt = (itemId, deliverableLabel) => {
    if (!deleteModal || !deleteMessage || !deleteConfirm) {
      return false;
    }

    pendingDelete = { itemId, deliverableLabel };
    deleteMessage.textContent = deliverableLabel
      ? `Remove the deliverable "${deliverableLabel}"?`
      : 'Remove this deliverable?';
    deleteConfirm.disabled = false;
    deleteConfirm.textContent = 'Delete Deliverable';
    deleteModal.show();
    return true;
  };

  const bindEvents = () => {
    const createForm = document.getElementById('kanban-create-form');
    if (createForm) {
      createForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const formData = new FormData(createForm);
        try {
          await fetchJson(`/api/projects/${encodeURIComponent(projectName)}/kanban`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
            body: JSON.stringify({
              deliverable: formData.get('deliverable'),
              status: formData.get('status'),
              due: formData.get('due')
            })
          });
          createForm.reset();
          setFeedback('Deliverable added.', false);
          await render();
        } catch (error) {
          setFeedback(error.message || 'Failed to add deliverable.', true);
        }
      });
    }

    board.querySelectorAll('.km-kanban-item-form').forEach((form) => {
      form.addEventListener('submit', async (event) => {
        event.preventDefault();
        const formData = new FormData(form);
        try {
          await fetchJson(`/api/projects/${encodeURIComponent(projectName)}/kanban/${encodeURIComponent(form.dataset.itemId)}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
            body: JSON.stringify({
              deliverable: formData.get('deliverable'),
              status: formData.get('status'),
              due: formData.get('due')
            })
          });
          setFeedback('Deliverable updated.', false);
          await render();
        } catch (error) {
          setFeedback(error.message || 'Failed to update deliverable.', true);
        }
      });

      const deleteButton = form.querySelector('[data-action="delete"]');
      if (deleteButton) {
        deleteButton.addEventListener('click', async () => {
          const deliverableInput = form.querySelector('[name="deliverable"]');
          const deliverableLabel = String(deliverableInput?.value || '').trim();

          if (openDeletePrompt(form.dataset.itemId, deliverableLabel)) {
            return;
          }

          try {
            await fetchJson(`/api/projects/${encodeURIComponent(projectName)}/kanban/${encodeURIComponent(form.dataset.itemId)}/delete`, {
              method: 'POST',
              headers: { Accept: 'application/json' }
            });
            setFeedback('Deliverable removed.', false);
            await render();
          } catch (error) {
            setFeedback(error.message || 'Failed to remove deliverable.', true);
          }
        });
      }
    });
  };

  if (deleteConfirm && deleteModalElement) {
    deleteConfirm.addEventListener('click', async () => {
      if (!pendingDelete?.itemId) return;

      deleteConfirm.disabled = true;
      deleteConfirm.textContent = 'Deleting...';

      try {
        await fetchJson(`/api/projects/${encodeURIComponent(projectName)}/kanban/${encodeURIComponent(pendingDelete.itemId)}/delete`, {
          method: 'POST',
          headers: { Accept: 'application/json' }
        });
        deleteModal.hide();
        setFeedback('Deliverable removed.', false);
        await render();
      } catch (error) {
        deleteModal.hide();
        setFeedback(error.message || 'Failed to remove deliverable.', true);
      } finally {
        pendingDelete = null;
        deleteConfirm.disabled = false;
        deleteConfirm.textContent = 'Delete Deliverable';
      }
    });

    deleteModalElement.addEventListener('hidden.bs.modal', () => {
      pendingDelete = null;
      deleteConfirm.disabled = false;
      deleteConfirm.textContent = 'Delete Deliverable';
    });
  }

  render();
})();
