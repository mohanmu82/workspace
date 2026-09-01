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
 *       page: 'serviceDashboard', // stable page id — scopes the saved field mapping below
 *       fields: [
 *         { key: 'url',  label: 'URL',  guesses: ['url'],  required: true },
 *         { key: 'name', label: 'Name', guesses: ['name'], required: false },
 *         { key: 'type', label: 'Type', guesses: ['type'], required: false,
 *           fallback: { label: 'Default Type', default: 'http',
 *                       options: [{value:'http',label:'HTTP'}, {value:'prometheus',label:'Prometheus'}] } }
 *       ],
 *       loadLabel: 'Load Targets',
 *       onLoad(rows, meta) {
 *         // rows: [{ url, name, type, _dataset, _row }, ...] — already filtered + mapped
 *         // meta: { replace, dataset, conditions, mapping, fallbacks } — the filter that
 *         //       produced them, so a caller can ask the server to re-run it later
 *         return rows.length + ' row(s) loaded.';
 *       }
 *     });
 *   </script>
 *
 * Each row's attribute values feed a saved "favorite" — a named, AND-ed combination of
 * filter conditions (attribute/operator/value) persisted on the dataset itself via
 * POST/DELETE /staticdataset/{name}/favorites, so it is reusable on every page that
 * mounts this widget against the same dataset.
 *
 * The filtering itself happens server-side: this panel sends its conditions to
 * POST /staticdataset/{name}/query and is handed back only the rows that matched, rather than
 * pulling every row down to narrow it here. That keeps a large dataset off the wire when a page
 * wants a handful of rows out of it, and — because the condition is the thing being sent — lets
 * a caller with no UI at all (an App Catalog page, a scheduled check) ask for the same filter by
 * naming a favorite. The same query backs the live "23 of 500 rows match" count, debounced.
 *
 * The field -> attribute mapping (and any fallback values) is likewise persisted server-side,
 * keyed by `page` + dataset name, via GET/PUT /pagepreference — so once an operator maps
 * "url" to the "HTTPURL" column, that choice survives reloads and is shared with anyone else
 * opening the same page, instead of relying on the best-effort guess every time. Editable
 * directly from the Admin page. Pass no `page` to opt out and always fall back to guessing.
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
      .sdw-mapping-status {
        font-size: 0.72rem; color: #888; font-family: 'Segoe UI', sans-serif;
        align-self: center; margin-left: 2px;
      }
      .sdw-mapping-status.sdw-ok { color: #0a5c36; }
    `;
    document.head.appendChild(style);
  }

  // How long a keystroke in a condition settles before the match count is asked for again —
  // the count is a server call now, and one per character typed is a call per character wasted.
  const COUNT_DEBOUNCE = 250;

  /**
   * Asks the server which rows of `dataset` match — conditions are sent, rows come back, and the
   * browser never sees the ones that did not match. `countOnly` asks for the counts alone, which
   * is all the "23 of 500 rows match" line under the conditions needs.
   */
  async function queryDataset(dataset, conditions, countOnly) {
    const body = {
      conditions: (conditions || [])
        .filter(c => c.attribute)
        .map(c => ({ attribute: c.attribute, op: c.op, value: c.value })),
      countOnly: !!countOnly
    };
    const res = await fetch('/staticdataset/' + encodeURIComponent(dataset) + '/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data && data.error ? data.error : 'HTTP ' + res.status);
    return data;
  }

  function mount(opts) {
    const container = document.getElementById(opts.containerId);
    if (!container) throw new Error('StaticDatasetWidget: container #' + opts.containerId + ' not found');
    injectStyles();

    const fields = opts.fields || [];
    const loadLabel = opts.loadLabel || 'Load';
    const showLabel = opts.showLabel || null;
    const page = opts.page || null; // scopes the saved field mapping — omit to always guess, never persist

    const state = {
      datasetName: null,
      attributes: [],
      // How many rows the dataset holds in total. The rows themselves are deliberately not kept:
      // the server is the one that filters now, so the browser only ever sees what matched.
      total: 0,
      favorites: [],
      conditions: [{ attribute: '', op: 'equals', value: '' }]
    };
    let countTimer = null;   // debounces the match-count call while a condition is being typed
    let countToken = 0;      // guards against a slow count landing after a newer one

    container.innerHTML = `
      <div class="sdw-row">
        <div class="sdw-field" style="min-width:200px">
          <label>Dataset</label>
          <select data-role="dataset"><option value="">-- select a dataset --</option></select>
        </div>
        <div class="sdw-mapping" style="display:flex;gap:10px;flex-wrap:wrap"></div>
        <span class="sdw-mapping-status" data-role="mapping-status"></span>
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
        ${showLabel ? '<button type="button" class="sdw-btn sdw-secondary" data-action="load-replace">' + escapeHtml(showLabel) + '</button>' : ''}
      </div>
      <div class="sdw-status" data-role="status"></div>
    `;

    const el = {
      dataset:       container.querySelector('[data-role="dataset"]'),
      mapping:       container.querySelector('.sdw-mapping'),
      mappingStatus: container.querySelector('[data-role="mapping-status"]'),
      conditions:    container.querySelector('[data-role="conditions"]'),
      count:         container.querySelector('[data-role="count"]'),
      favName:       container.querySelector('[data-role="fav-name"]'),
      favChips:      container.querySelector('[data-role="fav-chips"]'),
      status:        container.querySelector('[data-role="status"]')
    };

    function setStatus(msg, cls) {
      el.status.textContent = msg || '';
      el.status.className = 'sdw-status' + (cls ? ' ' + cls : '');
    }

    function guessAttribute(field) {
      const guesses = (field.guesses || []).map(g => g.toLowerCase());
      return state.attributes.find(a => guesses.includes(a.toLowerCase())) || '';
    }

    // preset: { mapping: {fieldKey: attrName}, fallbacks: {fieldKey: value} } from a saved
    // page preference, or null to fall back to the best-effort attribute-name guess.
    function renderMapping(preset) {
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
        const savedAttr = preset && preset.mapping ? preset.mapping[f.key] : null;
        const value = (savedAttr && state.attributes.includes(savedAttr)) ? savedAttr : guessAttribute(f);
        if (value) sel.value = value;

        if (f.fallback) {
          const fbSel = el.mapping.querySelector('[data-role="fallback-' + f.key + '"]');
          const savedFallback = preset && preset.fallbacks ? preset.fallbacks[f.key] : null;
          if (savedFallback && fbSel.querySelector('option[value="' + CSS.escape(savedFallback) + '"]')) {
            fbSel.value = savedFallback;
          }
        }
      });

      setMappingStatus(preset ? 'Using saved mapping.' : '', preset ? 'sdw-ok' : '');
    }

    function setMappingStatus(msg, cls) {
      if (!el.mappingStatus) return;
      el.mappingStatus.textContent = msg || '';
      el.mappingStatus.className = 'sdw-mapping-status' + (cls ? ' ' + cls : '');
    }

    function readCurrentMapping() {
      const mapping = {};
      const fallbacks = {};
      fields.forEach(f => {
        const sel = el.mapping.querySelector('[data-role="map-' + f.key + '"]');
        mapping[f.key] = sel ? sel.value : '';
        if (f.fallback) {
          const fbSel = el.mapping.querySelector('[data-role="fallback-' + f.key + '"]');
          fallbacks[f.key] = fbSel ? fbSel.value : f.fallback.default;
        }
      });
      return { mapping, fallbacks };
    }

    async function loadMappingPreference() {
      if (!page || !state.datasetName) return null;
      try {
        const res = await fetch('/pagepreference/' + encodeURIComponent(page) + '/' + encodeURIComponent(state.datasetName));
        if (!res.ok) return null;
        return await res.json();
      } catch (e) {
        return null;
      }
    }

    async function saveMappingPreference() {
      if (!page || !state.datasetName) return;
      const { mapping, fallbacks } = readCurrentMapping();
      try {
        const res = await fetch('/pagepreference', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ page, datasetName: state.datasetName, mapping, fallbacks })
        });
        setMappingStatus(res.ok ? 'Mapping saved.' : 'Failed to save mapping.', res.ok ? 'sdw-ok' : '');
      } catch (e) {
        setMappingStatus('Failed to save mapping: ' + e.message, '');
      }
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

    /**
     * Refreshes the "23 of 500 rows match" line by asking the server, debounced so typing a
     * condition costs one call rather than one per keystroke. A stale answer is dropped rather
     * than painted: with the count coming over the network, a slow earlier call can otherwise land
     * after a faster later one and leave the wrong number under the conditions the user is looking at.
     */
    function updateCount() {
      if (!state.datasetName) { el.count.textContent = ''; return; }
      clearTimeout(countTimer);
      const token = ++countToken;
      countTimer = setTimeout(async () => {
        try {
          const data = await queryDataset(state.datasetName, state.conditions, true);
          if (token !== countToken) return;
          el.count.textContent = data.count + ' of ' + data.total + ' rows match';
        } catch (e) {
          if (token === countToken) el.count.textContent = '';
        }
      }, COUNT_DEBOUNCE);
    }

    async function onDatasetChange() {
      const name = el.dataset.value;
      state.datasetName = name || null;
      state.attributes = [];
      state.total = 0;
      state.favorites = [];
      state.conditions = [{ attribute: '', op: 'equals', value: '' }];
      countToken++;                       // anything already in flight belongs to the old dataset
      clearTimeout(countTimer);
      renderMapping();
      renderConditions();
      renderFavorites();
      if (!name) { setStatus(''); return; }

      setStatus('Loading dataset…');
      try {
        // The summary, not the rows: attributes and favorites are all this panel needs to offer a
        // filter, and the rows arrive later as the ones that matched it.
        const res = await fetch('/staticdataset/' + encodeURIComponent(name));
        const data = await res.json();
        if (!res.ok) { setStatus(data.error || 'Failed to load dataset.', 'sdw-err'); return; }
        state.attributes = data.attributes || [];
        state.total = data.count || 0;
        state.favorites = data.favorites || [];
        const preset = await loadMappingPreference();
        renderMapping(preset);
        renderConditions();
        renderFavorites();
        setStatus(state.total + ' rows available.', 'sdw-ok');
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

    async function doLoad(replace) {
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

      // The conditions go out and the matching rows come back — the ones that did not match are
      // never sent, which is the whole point of the filter living on the server.
      const conditions = state.conditions
        .filter(c => c.attribute)
        .map(c => ({ attribute: c.attribute, op: c.op, value: c.value }));

      let rows;
      setStatus('Filtering…');
      try {
        const data = await queryDataset(state.datasetName, conditions, false);
        rows = data.rows || [];
        state.total = data.total != null ? data.total : state.total;
        el.count.textContent = data.count + ' of ' + data.total + ' rows match';
      } catch (e) {
        setStatus('Failed to filter dataset: ' + e.message, 'sdw-err');
        return;
      }
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
        // The dataset name, the conditions and the field mapping ride along with the rows so a
        // page that wants to re-run this filter later — the dashboard re-checking what a filter
        // names, rather than the list it happened to load — can ask the server for it directly
        // instead of reconstructing it from the rows it was handed.
        result = opts.onLoad
          ? opts.onLoad(mappedRows, {
              replace: !!replace,
              dataset: state.datasetName,
              conditions,
              mapping,
              fallbacks: fallbackValues
            })
          : undefined;
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
        doLoad(false);
      } else if (action === 'load-replace') {
        doLoad(true);
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
      } else if (t.matches('.sdw-mapping select')) {
        saveMappingPreference();
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
        if (opts.autoSelectFirstDataset && list.length > 0 && !el.dataset.value) {
          el.dataset.value = list[0].name;
          onDatasetChange();
        }
      } catch (e) { /* static dataset feature optional — ignore */ }
    })();

    return {
      reload: onDatasetChange
    };
  }

  global.StaticDatasetWidget = { mount };
})(window);
