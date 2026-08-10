/**
 * Reusable read-only data grid for viewing an array of row objects with Excel-style
 * column filtering (click a header's funnel to pick which values stay) and single-column
 * sorting, plus a search box that matches across every column.
 *
 * Usage:
 *   <div id="gridMount" style="height:100%"></div>
 *   <script src="excelgrid.js"></script>
 *   <script>
 *     ExcelGrid.mount({ containerId: 'gridMount', attributes: ['name','url'], rows: [...] });
 *   </script>
 */
(function (global) {
  'use strict';

  const MAX_RENDERED_ROWS = 5000;

  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function injectStyles() {
    if (document.getElementById('eg-styles')) return;
    const style = document.createElement('style');
    style.id = 'eg-styles';
    style.textContent = `
      .eg-root { display: flex; flex-direction: column; height: 100%; gap: 8px; font-family: 'Segoe UI', sans-serif; }
      .eg-toolbar { display: flex; align-items: center; gap: 10px; }
      .eg-search { flex: 1; padding: 7px 10px; font-size: 0.85rem; border: 1px solid #ccc; border-radius: 4px; font-family: inherit; }
      .eg-search:focus { outline: none; border-color: #0078d4; }
      .eg-count { font-size: 0.8rem; color: #666; white-space: nowrap; }
      .eg-btn { padding: 6px 14px; font-size: 0.8rem; font-weight: 600; border: none; border-radius: 4px; cursor: pointer; background: #0078d4; color: #fff; white-space: nowrap; }
      .eg-btn:hover { background: #005fa3; }
      .eg-btn.eg-secondary { background: #6c757d; }
      .eg-btn.eg-secondary:hover { background: #545b62; }
      .eg-btn.eg-small { padding: 4px 10px; font-size: 0.72rem; }
      .eg-table-wrap { flex: 1; overflow: auto; border: 1px solid #dde1e7; border-radius: 4px; }
      .eg-table { border-collapse: collapse; width: 100%; font-size: 0.8rem; }
      .eg-table th, .eg-table td { padding: 6px 10px; border-bottom: 1px solid #eceff3; text-align: left; white-space: nowrap; }
      .eg-table thead th { background: #f8f9fb; position: sticky; top: 0; z-index: 1; border-bottom: 2px solid #dde1e7; }
      .eg-th { display: flex; align-items: center; justify-content: space-between; gap: 6px; position: relative; }
      .eg-th-label { cursor: pointer; user-select: none; }
      .eg-th-label:hover { color: #0078d4; }
      .eg-filter-btn { border: none; background: none; cursor: pointer; font-size: 0.7rem; color: #888; padding: 2px 4px; border-radius: 3px; line-height: 1; }
      .eg-filter-btn:hover { background: #e4e8ef; color: #333; }
      .eg-filter-active { color: #0078d4; font-weight: 700; }
      .eg-empty { text-align: center; color: #888; padding: 24px; white-space: normal; }
      .eg-popover {
        position: absolute; top: 100%; left: 0; margin-top: 4px; width: 230px; max-height: 300px;
        background: #fff; border: 1px solid #ccc; border-radius: 6px; box-shadow: 0 4px 14px rgba(0,0,0,.18);
        padding: 8px; display: flex; flex-direction: column; gap: 6px; z-index: 50;
        font-weight: 400; text-transform: none; letter-spacing: normal;
      }
      .eg-pop-search { padding: 5px 8px; font-size: 0.78rem; border: 1px solid #ccc; border-radius: 4px; font-family: inherit; }
      .eg-pop-all { font-size: 0.78rem; display: flex; gap: 6px; align-items: center; border-bottom: 1px solid #eceff3; padding-bottom: 4px; }
      .eg-pop-list { overflow-y: auto; max-height: 160px; display: flex; flex-direction: column; gap: 2px; }
      .eg-pop-item { font-size: 0.78rem; display: flex; gap: 6px; align-items: center; padding: 2px 0; white-space: normal; word-break: break-word; }
      .eg-pop-actions { display: flex; gap: 6px; padding-top: 4px; border-top: 1px solid #eceff3; }
    `;
    document.head.appendChild(style);
  }

  function mount(opts) {
    const container = document.getElementById(opts.containerId);
    if (!container) throw new Error('ExcelGrid: container #' + opts.containerId + ' not found');
    injectStyles();
    container.classList.add('eg-root');

    const attributes = opts.attributes || [];
    const allRows = opts.rows || [];

    const state = {
      search: '',
      filters: {},   // attribute -> Set of allowed string values (absent = no filter)
      sortAttr: null,
      sortDir: null  // 'asc' | 'desc'
    };

    container.innerHTML =
      '<div class="eg-toolbar">' +
        '<input type="text" class="eg-search" placeholder="Search all columns…">' +
        '<button type="button" class="eg-btn eg-secondary eg-small" data-action="clear-filters">Clear Filters</button>' +
        '<span class="eg-count" data-role="count"></span>' +
      '</div>' +
      '<div class="eg-table-wrap"><table class="eg-table"><thead><tr data-role="head-row"></tr></thead><tbody data-role="body"></tbody></table></div>';

    const el = {
      search: container.querySelector('.eg-search'),
      count: container.querySelector('[data-role="count"]'),
      headRow: container.querySelector('[data-role="head-row"]'),
      body: container.querySelector('[data-role="body"]')
    };

    function uniqueValues(attr) {
      const set = new Set();
      allRows.forEach(r => set.add(r[attr] == null || r[attr] === '' ? '' : String(r[attr])));
      return Array.from(set).sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }));
    }

    function matchesFilters(row) {
      for (const attr in state.filters) {
        const allowed = state.filters[attr];
        if (!allowed) continue;
        const v = row[attr] == null || row[attr] === '' ? '' : String(row[attr]);
        if (!allowed.has(v)) return false;
      }
      if (state.search) {
        const s = state.search.toLowerCase();
        const hit = attributes.some(a => row[a] != null && String(row[a]).toLowerCase().includes(s));
        if (!hit) return false;
      }
      return true;
    }

    function getFilteredSortedRows() {
      let rows = allRows.filter(matchesFilters);
      if (state.sortAttr && state.sortDir) {
        const attr = state.sortAttr, dir = state.sortDir === 'asc' ? 1 : -1;
        rows = rows.slice().sort((a, b) => {
          const av = a[attr] == null ? '' : String(a[attr]);
          const bv = b[attr] == null ? '' : String(b[attr]);
          return av.localeCompare(bv, undefined, { numeric: true, sensitivity: 'base' }) * dir;
        });
      }
      return rows;
    }

    let openPopoverAttr = null;

    function closePopover() {
      openPopoverAttr = null;
      const existing = container.querySelector('.eg-popover');
      if (existing) existing.remove();
    }

    function renderHead() {
      el.headRow.innerHTML = attributes.map(attr => {
        const hasFilter = !!state.filters[attr];
        const isSort = state.sortAttr === attr;
        const arrow = isSort ? (state.sortDir === 'asc' ? ' ▲' : ' ▼') : '';
        return '<th><div class="eg-th">' +
          '<span class="eg-th-label" data-role="sort" data-attr="' + escapeHtml(attr) + '">' + escapeHtml(attr) + arrow + '</span>' +
          '<button type="button" class="eg-filter-btn' + (hasFilter ? ' eg-filter-active' : '') + '" data-role="filter" data-attr="' + escapeHtml(attr) + '" title="Filter">&#9662;</button>' +
          '</div></th>';
      }).join('');
    }

    function renderBody() {
      const rows = getFilteredSortedRows();
      const shown = rows.slice(0, MAX_RENDERED_ROWS);
      if (!attributes.length || !allRows.length) {
        el.body.innerHTML = '<tr><td class="eg-empty" colspan="' + Math.max(attributes.length, 1) + '">No rows loaded.</td></tr>';
      } else if (!shown.length) {
        el.body.innerHTML = '<tr><td class="eg-empty" colspan="' + attributes.length + '">No rows match the current filters.</td></tr>';
      } else {
        el.body.innerHTML = shown.map(row =>
          '<tr>' + attributes.map(a => '<td>' + escapeHtml(row[a] != null ? row[a] : '') + '</td>').join('') + '</tr>'
        ).join('');
      }
      let countMsg = rows.length.toLocaleString() + ' of ' + allRows.length.toLocaleString() + ' row(s)';
      if (rows.length > shown.length) countMsg += ' (showing first ' + shown.length.toLocaleString() + ')';
      el.count.textContent = countMsg;
    }

    function renderAll() {
      renderHead();
      renderBody();
    }

    function openPopover(attr, anchorBtn) {
      closePopover();
      openPopoverAttr = attr;
      const values = uniqueValues(attr);
      const current = state.filters[attr]; // Set or undefined (undefined = all selected)
      const temp = new Set(current ? current : values);

      const pop = document.createElement('div');
      pop.className = 'eg-popover';
      pop.innerHTML =
        '<input type="text" class="eg-pop-search" placeholder="Search values…">' +
        '<label class="eg-pop-all"><input type="checkbox" data-role="select-all"> (Select All)</label>' +
        '<div class="eg-pop-list"></div>' +
        '<div class="eg-pop-actions">' +
          '<button type="button" class="eg-btn eg-small" data-role="apply">OK</button>' +
          '<button type="button" class="eg-btn eg-secondary eg-small" data-role="cancel">Cancel</button>' +
          '<button type="button" class="eg-btn eg-secondary eg-small" data-role="clear">Clear</button>' +
        '</div>';

      const listEl = pop.querySelector('.eg-pop-list');
      const selectAllEl = pop.querySelector('[data-role="select-all"]');
      const searchEl = pop.querySelector('.eg-pop-search');
      let visibleValues = values;

      function renderValueList(filterText) {
        const ft = (filterText || '').toLowerCase();
        visibleValues = values.filter(v => (v === '' ? '(blank)' : v).toLowerCase().includes(ft));
        listEl.innerHTML = visibleValues.map(v => {
          const label = v === '' ? '(blank)' : v;
          return '<label class="eg-pop-item"><input type="checkbox" data-val="' + escapeHtml(v) + '"' +
            (temp.has(v) ? ' checked' : '') + '> ' + escapeHtml(label) + '</label>';
        }).join('');
        selectAllEl.checked = visibleValues.length > 0 && visibleValues.every(v => temp.has(v));
      }
      renderValueList('');

      searchEl.addEventListener('input', () => renderValueList(searchEl.value));
      selectAllEl.addEventListener('change', () => {
        if (selectAllEl.checked) visibleValues.forEach(v => temp.add(v));
        else visibleValues.forEach(v => temp.delete(v));
        renderValueList(searchEl.value);
      });
      listEl.addEventListener('change', (e) => {
        const t = e.target;
        if (!t.matches('input[type=checkbox]')) return;
        const v = t.getAttribute('data-val');
        if (t.checked) temp.add(v); else temp.delete(v);
        selectAllEl.checked = visibleValues.length > 0 && visibleValues.every(vv => temp.has(vv));
      });

      pop.querySelector('[data-role="apply"]').addEventListener('click', () => {
        if (temp.size >= values.length) delete state.filters[attr];
        else state.filters[attr] = new Set(temp);
        closePopover();
        renderAll();
      });
      pop.querySelector('[data-role="cancel"]').addEventListener('click', closePopover);
      pop.querySelector('[data-role="clear"]').addEventListener('click', () => {
        delete state.filters[attr];
        closePopover();
        renderAll();
      });
      pop.addEventListener('click', e => e.stopPropagation());

      anchorBtn.parentElement.appendChild(pop); // anchored to .eg-th (position: relative)
      searchEl.focus();
    }

    el.search.addEventListener('input', () => {
      state.search = el.search.value.trim();
      renderBody();
    });

    container.addEventListener('click', (e) => {
      if (e.target.closest('[data-action="clear-filters"]')) {
        state.filters = {};
        renderAll();
        return;
      }
      const sortEl = e.target.closest('[data-role="sort"]');
      if (sortEl) {
        const attr = sortEl.getAttribute('data-attr');
        if (state.sortAttr !== attr) { state.sortAttr = attr; state.sortDir = 'asc'; }
        else if (state.sortDir === 'asc') { state.sortDir = 'desc'; }
        else { state.sortAttr = null; state.sortDir = null; }
        renderAll();
        return;
      }
      const filterBtn = e.target.closest('[data-role="filter"]');
      if (filterBtn) {
        e.stopPropagation();
        const attr = filterBtn.getAttribute('data-attr');
        if (openPopoverAttr === attr) { closePopover(); return; }
        openPopover(attr, filterBtn);
        return;
      }
      closePopover();
    });

    document.addEventListener('click', (e) => {
      if (!container.contains(e.target)) closePopover();
    });

    renderAll();
  }

  global.ExcelGrid = { mount };
})(window);
