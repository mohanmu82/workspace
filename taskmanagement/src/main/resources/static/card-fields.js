// Shared "which fields show on task cards/rows" preference, saved per-view in localStorage.
// Each page picks a viewKey and a list of toggleable fields ({id, label, default}); title/actions
// are always shown and aren't part of this list.
const CardFields = (function () {
  const LS_PREFIX = 'taskDisplayPrefs:';
  const changeListeners = {};

  function readAll(viewKey) {
    try { return JSON.parse(localStorage.getItem(LS_PREFIX + viewKey) || '{}'); } catch (_) { return {}; }
  }

  function isOn(viewKey, fieldId, defaultOn) {
    const stored = readAll(viewKey);
    if (Object.prototype.hasOwnProperty.call(stored, fieldId)) return !!stored[fieldId];
    return defaultOn !== false;
  }

  function setOn(viewKey, fieldId, value) {
    const stored = readAll(viewKey);
    stored[fieldId] = value;
    localStorage.setItem(LS_PREFIX + viewKey, JSON.stringify(stored));
    if (changeListeners[viewKey]) changeListeners[viewKey].forEach(cb => cb());
  }

  function onChange(viewKey, cb) {
    (changeListeners[viewKey] = changeListeners[viewKey] || []).push(cb);
  }

  // fields: [{ id, label, default }]
  function renderButton(viewKey, fields) {
    const items = fields.map(f => {
      const checked = isOn(viewKey, f.id, f.default) ? 'checked' : '';
      return `<li><label class="dropdown-item d-flex align-items-center gap-2 mb-0" onclick="event.stopPropagation()">
        <input type="checkbox" class="form-check-input mt-0" ${checked} onchange="CardFields.setOn('${viewKey}','${f.id}', this.checked)">${f.label}
      </label></li>`;
    }).join('');
    return `<div class="dropdown d-inline-block">
      <button class="btn btn-outline-secondary btn-sm dropdown-toggle" type="button" data-bs-toggle="dropdown" data-bs-auto-close="outside" title="Choose which fields appear on task cards in this view">
        <i class="bi bi-eye me-1"></i>Display
      </button>
      <ul class="dropdown-menu dropdown-menu-end p-2" style="min-width:170px; z-index:1030">${items}</ul>
    </div>`;
  }

  return { isOn, setOn, onChange, renderButton };
})();
