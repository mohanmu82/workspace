/**
 * Reusable "load from a static dataset" panel — embed on any page that wants to source
 * its input rows from a Static Dataset (see /staticdatasets.html) instead of manual entry.
 *
 * Usage:
 *   <div id="dsWidget"></div>
 *   <script src="staticdataset-widget.js"></script>
 *   <script>
 *     StaticDatasetWidget.mount({
 *       containerId: 'dsWidget',
 *       fields: [
 *         { key: 'url',  label: 'URL',  guesses: ['url'],  required: true },
 *         { key: 'name', label: 'Name', guesses: ['name'], required: false },
 *         { key: 'type', label: 'Type', guesses: ['type'], required: false,
 *           fallback: { label: 'Default Type', default: 'http',
 *                       options: [{value:'http',label:'HTTP'}, {value:'prometheus',label:'Prometheus'}] } }
 *       ],
 *       loadLabel: 'Load Targets',
 *       onLoad(rows) {
 *         // rows: [{ url, name, type, _dataset, _row }, ...] — already filtered + mapped
 *         return rows.length + ' row(s) loaded.';
 *       }
 *     });
 *   </script>
 *
 * Each row's attribute values feed a saved "favorite" — a named, AND-ed combination of
 * filter conditions (attribute/operator/value) persisted on the dataset itself via
 * POST/DELETE /staticdataset/{name}/favorites, so it is reusable on every page that
 * mounts this widget against the same dataset.
 */
(function (global) {
  'use strict';

  const OPERATORS = [
    { value: 'equals',      label: 'equals' },
    { value: 'notEquals',   label: 'not equals' },
    { value: 'contains',    label: 'contains' },
    { value: 'notContains', label: 'not contains' },
    { value: 'startsWith',  label: 'starts with' },
    { value: 'endsWith',    label: 'ends with' }
  ];

  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function injectStyles() {
    if (document.getElementById('sdw-styles')) return;
    const style = document.createElement('style');
    style.id = 'sdw-styles';
    style.textContent = `
      .sdw-row { display: flex; gap: 10px; align-items: flex-end; flex-wrap: wrap; margin-bottom: 10px; }
      .sdw-row:last-child { margin-bottom: 0; }
      .sdw-field { display: flex; flex-direction: column; gap: 4px; }
      .sdw-field label {
        font-size: 0.72rem; font-weight: 700; color: #666;
        text-transform: uppercase; letter-spacing: .04em; font-family: 'Segoe UI', sans-serif;
      }
      .sdw-field select, .sdw-field input[type=text] {
        padding: 7px 10px; font-size: 0.85rem; border: 1px solid #ccc; border-radius: 4px;
        font-family: inherit; background: #fff;
      }
      .sdw-field select:focus, .sdw-field input:focus { outline: none; border-color: #0078d4; }
      .sdw-required::after { content: ' *'; color: #b02a37; }
      .sdw-cond-row { display: flex; gap: 8px; align-items: center; margin-bottom: 6px; }
      .sdw-btn {
        padding: 7px 16px; font-size: 0.85rem; font-weight: 600; border: none;
        border-radius: 4px; cursor: pointer; background: #0078d4; color: #fff; white-space: nowrap;
        font-family: 'Segoe UI', sans-serif;
      }
      .sdw-btn:hover:not(:disabled) { background: #005fa3; }
      .sdw-btn:disabled { background: #9ec5e8; cursor: not-allowed; }
      .sdw-btn.sdw-secondary { background: #6c757d; }
      .sdw-btn.sdw-secondary:hover { background: #545b62; }
      .sdw-btn.sdw-small { padding: 4px 10px; font-size: 0.72rem; }
      .sdw-btn.sdw-danger { background: none; color: #b02a37; padding: 2px 4px; font-size: 0.85rem; }
      .sdw-btn.sdw-danger:hover { background: none; text-decoration: underline; }
      .sdw-fav-chips { display: flex; gap: 6px; flex-wrap: wrap; }
      .sdw-chip {
        display: inline-flex; align-items: center; gap: 6px; padding: 3px 6px 3px 10px;
        border-radius: 12px; background: #eceff3; font-size: 0.75rem; font-family: 'Segoe UI', sans-serif;
      }
      .sdw-chip button.sdw-chip-apply {
        border: none; background: none; cursor: pointer; font-size: 0.75rem; font-weight: 600; color: #084298;
        padding: 0;
      }
      .sdw-chip button.sdw-chip-apply:hover { text-decoration: underline; }
      .sdw-status { font-size: 0.82rem; margin-top: 6px; font-family: 'Segoe UI', sans-serif; }
      .sdw-status.sdw-ok  { color: #0a5c36; }
      .sdw-status.sdw-err { color: #842029; }
      .sdw-count { font-size: 0.8rem; color: #666; font-family: 'Segoe UI', sans-serif; }
      .sdw-section-label {
        font-size: 0.7rem; font-weight: 700; color: #888; text-transform: uppercase;
        letter-spacing: .04em; margin: 10px 0 6px; font-family: 'Segoe UI', sans-serif;
      }
    `;
    document.head.appendChild(style);
  }

  function matchCondition(row, cond) {
    if (!cond.attribute) return true;
    const raw = row[cond.attribute] != null ? String(row[cond.attribute]) : '';
    const a = raw.toLowerCase(), b = String(cond.value || '').toLowerCase();
    switch (cond.op) {
      case 'equals':      return a === b;
      case 'notEquals':   return a !== b;
      case 'contains':    return a.includes(b);
      case 'notContains': return !a.includes(b);
      case 'startsWith':  return a.startsWith(b);
      case 'endsWith':    return a.endsWith(b);
      default:            return true;
    }
  }

  function mount(opts) {
    const container = document.getElementById(opts.containerId);
    if (!container) throw new Error('StaticDatasetWidget: container #' + opts.containerId + ' not found');
    injectStyles();

    const fields = opts.fields || [];
    const loadLabel = opts.loadLabel || 'Load';

    const state = {
      datasetName: null,
      attributes: [],
      rows: [],
      favorites: [],
      conditions: [{ attribute: '', op: 'equals', value: '' }]
    };

    container.innerHTML = `
      <div class="sdw-row">
        <div class="sdw-field" style="min-width:200px">
          <label>Dataset</label>
          <select data-role="dataset"><option value="">-- select a dataset --</option></select>
        </div>
        <div class="sdw-mapping" style="display:flex;gap:10px;flex-wrap:wrap"></div>
      </div>
      <div class="sdw-section-label">Filter (all conditions must match)</div>
      <div data-role="conditions"></div>
      <div class="sdw-row" style="margin-top:2px">
        <button type="button" class="sdw-btn sdw-secondary sdw-small" data-action="add-condition">+ Condition</button>
        <span class="sdw-count" data-role="count"></span>
      </div>
      <div class="sdw-section-label">Favorites</div>
      <div class="sdw-row">
        <div class="sdw-field" style="min-width:180px">
          <input type="text" data-role="fav-name" placeholder="Favorite name">
        </div>
        <button type="button" class="sdw-btn sdw-secondary sdw-small" data-action="save-favorite">Save as Favorite</button>
        <div class="sdw-fav-chips" data-role="fav-chips"></div>
      </div>
      <div class="sdw-row" style="margin-top:6px">
        <button type="button" class="sdw-btn" data-action="load">${escapeHtml(loadLabel)}</button>
      </div>
      <div class="sdw-status" data-role="status"></div>
    `;

    const el = {
      dataset:    container.querySelector('[data-role="dataset"]'),
      mapping:    container.querySelector('.sdw-mapping'),
      conditions: container.querySelector('[data-role="conditions"]'),
      count:      container.querySelector('[data-role="count"]'),
      favName:    container.querySelector('[data-role="fav-name"]'),
      favChips:   container.querySelector('[data-role="fav-chips"]'),
      status:     container.querySelector('[data-role="status"]')
    };

    function setStatus(msg, cls) {
      el.status.textContent = msg || '';
      el.status.className = 'sdw-status' + (cls ? ' ' + cls : '');
    }

    function guessAttribute(field) {
      const guesses = (field.guesses || []).map(g => g.toLowerCase());
      return state.attributes.find(a => guesses.includes(a.toLowerCase())) || '';
    }

    function renderMapping() {
      el.mapping.innerHTML = fields.map(f => {
        const attrOptions = state.attributes.map(a =>
          '<option value="' + escapeHtml(a) + '">' + escapeHtml(a) + '</option>').join('');
        const noneLabel = f.required ? '-- choose --' : '-- none --';
        let html = '<div class="sdw-field">' +
          '<label class="' + (f.required ? 'sdw-required' : '') + '">' + escapeHtml(f.label) + '</label>' +
          '<select data-role="map-' + f.key + '"><option value="">' + noneLabel + '</option>' + attrOptions + '</select>' +
          '</div>';
        if (f.fallback) {
          const fbOptions = f.fallback.options.map(o =>
            '<option value="' + escapeHtml(o.value) + '"' + (o.value === f.fallback.default ? ' selected' : '') + '>' +
            escapeHtml(o.label) + '</option>').join('');
          html += '<div class="sdw-field">' +
            '<label>' + escapeHtml(f.fallback.label) + '</label>' +
            '<select data-role="fallback-' + f.key + '">' + fbOptions + '</select>' +
            '</div>';
        }
        return html;
      }).join('');

      fields.forEach(f => {
        const sel = el.mapping.querySelector('[data-role="map-' + f.key + '"]');
        const guess = guessAttribute(f);
        if (guess) sel.value = guess;
      });
    }

    function renderConditions() {
      el.conditions.innerHTML = state.conditions.map((cond, idx) => {
        const attrOptions = state.attributes.map(a =>
          '<option value="' + escapeHtml(a) + '"' + (a === cond.attribute ? ' selected' : '') + '>' +
          escapeHtml(a) + '</option>').join('');
        const opOptions = OPERATORS.map(o =>
          '<option value="' + o.value + '"' + (o.value === cond.op ? ' selected' : '') + '>' + o.label + '</option>').join('');
        return '<div class="sdw-cond-row" data-idx="' + idx + '">' +
          '<select data-role="cond-attr" data-idx="' + idx + '"><option value="">-- none --</option>' + attrOptions + '</select>' +
          '<select data-role="cond-op" data-idx="' + idx + '">' + opOptions + '</select>' +
          '<input type="text" data-role="cond-val" data-idx="' + idx + '" placeholder="value" value="' + escapeHtml(cond.value) + '">' +
          '<button type="button" class="sdw-btn sdw-danger" data-action="remove-condition" data-idx="' + idx + '">&times;</button>' +
          '</div>';
      }).join('');
      updateCount();
    }

    function renderFavorites() {
      if (!state.favorites.length) { el.favChips.innerHTML = ''; return; }
      el.favChips.innerHTML = state.favorites.map(f =>
        '<span class="sdw-chip">' +
          '<button type="button" class="sdw-chip-apply" data-action="apply-favorite" data-fav="' + escapeHtml(f.name) + '">' +
            escapeHtml(f.name) +
          '</button>' +
          '<button type="button" class="sdw-btn sdw-danger" data-action="delete-favorite" data-fav="' + escapeHtml(f.name) + '">&times;</button>' +
        '</span>').join('');
    }

    function filteredRows() {
      const active = state.conditions.filter(c => c.attribute);
      if (!active.length) return state.rows;
      return state.rows.filter(row => active.every(c => matchCondition(row, c)));
    }

    function updateCount() {
      if (!state.datasetName) { el.count.textContent = ''; return; }
      el.count.textContent = filteredRows().length + ' of ' + state.rows.length + ' rows match';
    }

    async function onDatasetChange() {
      const name = el.dataset.value;
      state.datasetName = name || null;
      state.attributes = [];
      state.rows = [];
      state.favorites = [];
      state.conditions = [{ attribute: '', op: 'equals', value: '' }];
      renderMapping();
      renderConditions();
      renderFavorites();
      if (!name) { setStatus(''); return; }

      setStatus('Loading dataset…');
      try {
        const res = await fetch('/staticdataset/' + encodeURIComponent(name) + '/data');
        const data = await res.json();
        if (!res.ok) { setStatus(data.error || 'Failed to load dataset.', 'sdw-err'); return; }
        state.attributes = data.attributes || [];
        state.rows = data.rows || [];
        state.favorites = data.favorites || [];
        renderMapping();
        renderConditions();
        renderFavorites();
        setStatus(state.rows.length + ' rows available.', 'sdw-ok');
      } catch (e) {
        setStatus('Failed to load dataset: ' + e.message, 'sdw-err');
      }
    }

    async function saveFavorite() {
      if (!state.datasetName) { setStatus('Select a dataset first.', 'sdw-err'); return; }
      const name = el.favName.value.trim();
      if (!name) { setStatus('Enter a favorite name.', 'sdw-err'); return; }
      const conditions = state.conditions
        .filter(c => c.attribute)
        .map(c => ({ attribute: c.attribute, op: c.op, value: c.value }));
      if (!conditions.length) { setStatus('Add at least one condition to save.', 'sdw-err'); return; }

      try {
        const res = await fetch('/staticdataset/' + encodeURIComponent(state.datasetName) + '/favorites', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, conditions })
        });
        const data = await res.json();
        if (!res.ok) { setStatus(data.error || 'Failed to save favorite.', 'sdw-err'); return; }
        state.favorites = data.favorites || state.favorites;
        renderFavorites();
        el.favName.value = '';
        setStatus('Favorite "' + name + '" saved.', 'sdw-ok');
      } catch (e) {
        setStatus('Failed to save favorite: ' + e.message, 'sdw-err');
      }
    }

    function applyFavorite(name) {
      const fav = state.favorites.find(f => f.name === name);
      if (!fav) return;
      state.conditions = fav.conditions.length
        ? fav.conditions.map(c => ({ attribute: c.attribute, op: c.op, value: c.value }))
        : [{ attribute: '', op: 'equals', value: '' }];
      renderConditions();
      doLoad(); // clicking a favorite applies the filter and feeds the page immediately
    }

    async function deleteFavorite(name) {
      if (!state.datasetName) return;
      if (!confirm('Delete favorite "' + name + '"?')) return;
      try {
        await fetch('/staticdataset/' + encodeURIComponent(state.datasetName) +
          '/favorites/' + encodeURIComponent(name), { method: 'DELETE' });
      } catch (e) { /* ignore */ }
      state.favorites = state.favorites.filter(f => f.name !== name);
      renderFavorites();
    }

    function doLoad() {
      if (!state.datasetName) { setStatus('Select a dataset first.', 'sdw-err'); return; }

      const mapping = {};
      const fallbackValues = {};
      for (const f of fields) {
        const sel = el.mapping.querySelector('[data-role="map-' + f.key + '"]');
        mapping[f.key] = sel ? sel.value : '';
        if (f.fallback) {
          const fbSel = el.mapping.querySelector('[data-role="fallback-' + f.key + '"]');
          fallbackValues[f.key] = fbSel ? fbSel.value : f.fallback.default;
        }
        if (f.required && !mapping[f.key] && !fallbackValues[f.key]) {
          setStatus('Choose an attribute for "' + f.label + '".', 'sdw-err');
          return;
        }
      }

      const rows = filteredRows();
      if (!rows.length) { setStatus('No rows match the current filter.', 'sdw-err'); return; }

      const mappedRows = rows.map(row => {
        const out = { _dataset: state.datasetName, _row: row };
        fields.forEach(f => {
          const attr = mapping[f.key];
          let val = attr ? row[attr] : undefined;
          if ((val == null || val === '') && f.fallback) val = fallbackValues[f.key];
          out[f.key] = val;
        });
        return out;
      });

      let result;
      try {
        result = opts.onLoad ? opts.onLoad(mappedRows) : undefined;
      } catch (e) {
        setStatus('onLoad handler failed: ' + e.message, 'sdw-err');
        return;
      }
      setStatus(typeof result === 'string' ? result : (mappedRows.length + ' row(s) loaded.'), 'sdw-ok');
    }

    // ── Event wiring ──────────────────────────────────────────────────────
    el.dataset.addEventListener('change', onDatasetChange);

    container.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const action = btn.getAttribute('data-action');
      const idx = btn.getAttribute('data-idx');
      const fav = btn.getAttribute('data-fav');

      if (action === 'add-condition') {
        state.conditions.push({ attribute: '', op: 'equals', value: '' });
        renderConditions();
      } else if (action === 'remove-condition') {
        state.conditions.splice(Number(idx), 1);
        if (!state.conditions.length) state.conditions.push({ attribute: '', op: 'equals', value: '' });
        renderConditions();
      } else if (action === 'save-favorite') {
        saveFavorite();
      } else if (action === 'apply-favorite') {
        applyFavorite(fav);
      } else if (action === 'delete-favorite') {
        deleteFavorite(fav);
      } else if (action === 'load') {
        doLoad();
      }
    });

    container.addEventListener('change', (e) => {
      const t = e.target;
      if (t.matches('[data-role="cond-attr"]')) {
        state.conditions[Number(t.getAttribute('data-idx'))].attribute = t.value;
        updateCount();
      } else if (t.matches('[data-role="cond-op"]')) {
        state.conditions[Number(t.getAttribute('data-idx'))].op = t.value;
        updateCount();
      }
    });

    container.addEventListener('input', (e) => {
      const t = e.target;
      if (t.matches('[data-role="cond-val"]')) {
        state.conditions[Number(t.getAttribute('data-idx'))].value = t.value;
        updateCount();
      }
    });

    renderConditions();

    // ── Populate dataset list ───────────────────────────────────────────────
    (async function loadDatasetList() {
      try {
        const res = await fetch('/staticdataset');
        const list = await res.json();
        el.dataset.innerHTML = '<option value="">-- select a dataset --</option>' +
          list.map(d => '<option value="' + escapeHtml(d.name) + '">' + escapeHtml(d.name) +
                         ' (' + d.count + ' rows)</option>').join('');
      } catch (e) { /* static dataset feature optional — ignore */ }
    })();

    return {
      reload: onDatasetChange
    };
  }

  global.StaticDatasetWidget = { mount };
})(window);
