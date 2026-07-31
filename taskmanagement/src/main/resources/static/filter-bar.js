// Shared "Add Filter" chip/popover component (field + value, AND-combined, persisted
// per-view in localStorage). Mirrors the ad-hoc filter bar originally built for
// due-dates.html; factored out here so every list-style view can offer the same
// quick, non-destructive filtering without duplicating the popover markup/logic.
const FilterBar = (function () {
  const instances = {}; // containerId -> { storageKey, fields, getItems, onChange, activeFilters }

  function esc(s) {
    if (!s) return '';
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  function escAttr(s) { return esc(s).replace(/'/g, '&#39;'); }

  function ensureStyles() {
    if (document.getElementById('filterBarStyles')) return;
    const style = document.createElement('style');
    style.id = 'filterBarStyles';
    style.textContent = `
      .filter-bar { display:flex; flex-wrap:wrap; gap:6px; align-items:center; }
      .filter-chip { display:inline-flex; align-items:center; gap:4px; background:#e9ecef; border-radius:10px;
                     padding:2px 4px 2px 8px; font-size:0.75rem; }
      .filter-chip .filter-chip-remove { border:none; background:none; color:#6c757d; padding:0 2px; line-height:1; cursor:pointer; }
      .filter-chip .filter-chip-remove:hover { color:#dc3545; }
      .filter-add-popover { position:absolute; z-index:1040; background:#fff; border:1px solid #dee2e6; border-radius:8px;
                            box-shadow:0 4px 12px rgba(0,0,0,0.12); padding:0.6rem; width:260px; display:none; }
    `;
    document.head.appendChild(style);
  }

  function loadStoredFilters(storageKey) {
    try {
      const parsed = JSON.parse(localStorage.getItem(storageKey) || '[]');
      return Array.isArray(parsed) ? parsed : [];
    } catch (e) { return []; }
  }

  function persistFilters(inst) {
    localStorage.setItem(inst.storageKey, JSON.stringify(inst.activeFilters));
  }

  // opts: { containerId, storageKey, fields: () => [{id,label}], getItems: () => [...], onChange: () => {} }
  function init(opts) {
    ensureStyles();
    const container = document.getElementById(opts.containerId);
    container.classList.add('position-relative');
    container.innerHTML = `
      <div class="filter-bar" id="${opts.containerId}__bar"></div>
      <div class="filter-add-popover" id="${opts.containerId}__popover">
        <div class="mb-2">
          <label class="form-label small fw-semibold mb-1">Field</label>
          <select class="form-select form-select-sm" id="${opts.containerId}__fieldSelect"></select>
        </div>
        <div class="mb-2">
          <label class="form-label small fw-semibold mb-1">Value</label>
          <input list="${opts.containerId}__valueOptions" class="form-control form-control-sm" id="${opts.containerId}__valueInput"/>
          <datalist id="${opts.containerId}__valueOptions"></datalist>
        </div>
        <div class="d-flex justify-content-end gap-1">
          <button class="btn btn-secondary btn-sm" type="button" id="${opts.containerId}__cancelBtn">Cancel</button>
          <button class="btn btn-primary btn-sm" type="button" id="${opts.containerId}__addBtn">Add</button>
        </div>
      </div>
    `;

    const inst = {
      containerId: opts.containerId,
      storageKey: opts.storageKey,
      fields: opts.fields,
      getItems: opts.getItems,
      onChange: opts.onChange || function () {},
      activeFilters: loadStoredFilters(opts.storageKey),
    };
    instances[opts.containerId] = inst;

    document.getElementById(`${opts.containerId}__fieldSelect`).addEventListener('change', () => onFieldChange(inst));
    document.getElementById(`${opts.containerId}__valueInput`).addEventListener('keydown', e => {
      if (e.key === 'Enter') { e.preventDefault(); addFilter(inst); }
    });
    document.getElementById(`${opts.containerId}__cancelBtn`).addEventListener('click', () => closePopover(inst));
    document.getElementById(`${opts.containerId}__addBtn`).addEventListener('click', () => addFilter(inst));

    renderBar(inst);

    return {
      matches: (item) => matches(inst, item),
      getFilters: () => inst.activeFilters.slice(),
      refresh: () => renderBar(inst),
      clear: () => { inst.activeFilters = []; persistFilters(inst); renderBar(inst); },
    };
  }

  function renderBar(inst) {
    const fields = inst.fields();
    const bar = document.getElementById(`${inst.containerId}__bar`);
    const chips = inst.activeFilters.map((f, idx) => {
      const label = (fields.find(x => x.id === f.field) || {}).label || f.field;
      return `<span class="filter-chip">${esc(label)}: ${esc(f.value)}
        <button class="filter-chip-remove" title="Remove filter" data-idx="${idx}"><i class="bi bi-x"></i></button>
      </span>`;
    }).join('');
    bar.innerHTML = chips + `<button class="btn btn-outline-secondary btn-sm" type="button" id="${inst.containerId}__toggleBtn"><i class="bi bi-funnel me-1"></i>Add Filter</button>`;
    bar.querySelectorAll('.filter-chip-remove').forEach(btn => {
      btn.addEventListener('click', () => removeFilter(inst, parseInt(btn.dataset.idx, 10)));
    });
    document.getElementById(`${inst.containerId}__toggleBtn`).addEventListener('click', () => togglePopover(inst));
  }

  function togglePopover(inst) {
    const pop = document.getElementById(`${inst.containerId}__popover`);
    const willOpen = pop.style.display !== 'block';
    if (willOpen) {
      const sel = document.getElementById(`${inst.containerId}__fieldSelect`);
      sel.innerHTML = inst.fields().map(f => `<option value="${escAttr(f.id)}">${esc(f.label)}</option>`).join('');
      onFieldChange(inst);
      document.getElementById(`${inst.containerId}__valueInput`).value = '';
    }
    pop.style.display = willOpen ? 'block' : 'none';
  }

  function closePopover(inst) {
    document.getElementById(`${inst.containerId}__popover`).style.display = 'none';
  }

  function onFieldChange(inst) {
    const field = document.getElementById(`${inst.containerId}__fieldSelect`).value;
    const values = new Set();
    (inst.getItems() || []).forEach(item => {
      const v = item[field];
      if (Array.isArray(v)) { v.forEach(x => x && values.add(String(x))); }
      else if (v !== undefined && v !== null && v !== '') { values.add(String(v)); }
    });
    document.getElementById(`${inst.containerId}__valueOptions`).innerHTML =
      [...values].sort().map(v => `<option value="${escAttr(v)}"></option>`).join('');
  }

  function addFilter(inst) {
    const field = document.getElementById(`${inst.containerId}__fieldSelect`).value;
    const value = document.getElementById(`${inst.containerId}__valueInput`).value.trim();
    if (!field || !value) return;
    inst.activeFilters.push({ field, value });
    persistFilters(inst);
    closePopover(inst);
    renderBar(inst);
    inst.onChange();
  }

  function removeFilter(inst, idx) {
    inst.activeFilters.splice(idx, 1);
    persistFilters(inst);
    renderBar(inst);
    inst.onChange();
  }

  function matches(inst, item) {
    return inst.activeFilters.every(f => {
      const v = item[f.field];
      if (Array.isArray(v)) return v.some(x => (x || '').toString().toLowerCase().includes(f.value.toLowerCase()));
      return String(v || '').toLowerCase().includes(f.value.toLowerCase());
    });
  }

  return { init };
})();
