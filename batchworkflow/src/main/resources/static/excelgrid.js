/**
 * Reusable data grid for viewing an array of row objects with Excel-style column filtering
 * (click a header's funnel to pick which values stay) and single-column sorting, plus a
 * search box that matches across every column.
 *
 * Read-only by default. With {@code editable: true} cells can be edited in place (click a
 * cell; Enter/Tab commits, Escape cancels) and rows/columns added or rows removed — the grid
 * works on its own copy of the rows, so nothing changes for the caller until it reads
 * {@code getData()} back.
 *
 * Usage:
 *   <div id="gridMount" style="height:100%"></div>
 *   <script src="excelgrid.js"></script>
 *   <script>
 *     const grid = ExcelGrid.mount({ containerId: 'gridMount', attributes: ['name','url'], rows: [...] });
 *     grid.getVisibleRows();   // rows after the current search/filters/sort
 *     grid.getData();          // { attributes, rows } — all rows, including edits
 *     grid.isDirty();          // true once an edit has been made (editable mode)
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
      .eg-toolbar { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
      .eg-search { flex: 1; min-width: 160px; padding: 7px 10px; font-size: 0.85rem; border: 1px solid #ccc; border-radius: 4px; font-family: inherit; }
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
      .eg-editable .eg-cell { cursor: text; min-width: 60px; }
      .eg-editable .eg-cell:hover { background: #f3f8fd; }
      .eg-table td.eg-cell-editing { padding: 0; }
      .eg-cell-input { width: 100%; min-width: 120px; padding: 5px 9px; font-size: 0.8rem; font-family: inherit; border: 2px solid #0078d4; border-radius: 0; outline: none; }
      .eg-table th.eg-row-actions, .eg-table td.eg-row-actions { width: 1%; padding: 4px 6px; text-align: center; }
      .eg-del-row { border: none; background: none; cursor: pointer; color: #b02a37; font-size: 0.85rem; padding: 2px 6px; border-radius: 3px; line-height: 1; }
      .eg-del-row:hover { background: #fbdcdc; }
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

    const editable = !!opts.editable;
    const onChange = typeof opts.onChange === 'function' ? opts.onChange : function () {};
    const attributes = (opts.attributes || []).slice();
    // Editable grids mutate their rows, so work on a copy rather than the caller's objects.
    const allRows = editable
      ? (opts.rows || []).map(r => Object.assign({}, r))
      : (opts.rows || []);
    let dirty = false;

    const state = {
      search: '',
      filters: {},   // attribute -> Set of allowed string values (absent = no filter)
      sortAttr: null,
      sortDir: null  // 'asc' | 'desc'
    };

    container.classList.toggle('eg-editable', editable);
    container.innerHTML =
      '<div class="eg-toolbar">' +
        '<input type="text" class="eg-search" placeholder="Search all columns…">' +
        '<button type="button" class="eg-btn eg-secondary eg-small" data-action="clear-filters">Clear Filters</button>' +
        (editable
          ? '<button type="button" class="eg-btn eg-small" data-action="add-row">+ Row</button>' +
            '<button type="button" class="eg-btn eg-small" data-action="add-column">+ Column</button>'
          : '') +
        '<span class="eg-count" data-role="count"></span>' +
      '</div>' +
      '<div class="eg-table-wrap"><table class="eg-table"><thead><tr data-role="head-row"></tr></thead><tbody data-role="body"></tbody></table></div>';

    const el = {
      search: container.querySelector('.eg-search'),
      count: container.querySelector('[data-role="count"]'),
      headRow: container.querySelector('[data-role="head-row"]'),
      body: container.querySelector('[data-role="body"]'),
      tableWrap: container.querySelector('.eg-table-wrap')
    };

    function markDirty() {
      dirty = true;
      onChange();
    }

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
      el.headRow.innerHTML = (editable ? '<th class="eg-row-actions"></th>' : '') + attributes.map(attr => {
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
      const colCount = Math.max(attributes.length, 1) + (editable ? 1 : 0);
      if (!attributes.length || !allRows.length) {
        el.body.innerHTML = '<tr><td class="eg-empty" colspan="' + colCount + '">' +
          (editable ? 'No rows yet — use + Row / + Column to add some.' : 'No rows loaded.') + '</td></tr>';
      } else if (!shown.length) {
        el.body.innerHTML = '<tr><td class="eg-empty" colspan="' + colCount + '">No rows match the current filters.</td></tr>';
      } else if (editable) {
        // Cells carry the row's position in allRows so an edit lands on the right object
        // whatever the current sort/filter order is.
        const rowIndex = new Map(allRows.map((r, i) => [r, i]));
        el.body.innerHTML = shown.map(row => {
          const ri = rowIndex.get(row);
          return '<tr>' +
            '<td class="eg-row-actions"><button type="button" class="eg-del-row" data-action="delete-row" data-ri="' + ri + '" title="Remove row">&#10005;</button></td>' +
            attributes.map((a, ci) =>
              '<td class="eg-cell" data-ri="' + ri + '" data-ci="' + ci + '">' + escapeHtml(row[a] != null ? row[a] : '') + '</td>'
            ).join('') +
            '</tr>';
        }).join('');
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

    // -------------------------------------------------------------------------
    // Editing (editable mode only)
    // -------------------------------------------------------------------------

    function startEdit(td) {
      if (!td || td.classList.contains('eg-cell-editing')) return;
      const row = allRows[Number(td.getAttribute('data-ri'))];
      const attr = attributes[Number(td.getAttribute('data-ci'))];
      if (!row || attr == null) return;

      const original = row[attr] == null ? '' : String(row[attr]);
      const input = document.createElement('input');
      input.type = 'text';
      input.className = 'eg-cell-input';
      input.value = original;
      td.classList.add('eg-cell-editing');
      td.textContent = '';
      td.appendChild(input);
      input.focus();
      input.select();

      let finished = false;
      function finish(commit) {
        if (finished) return;
        finished = true;
        // Tabs and line breaks would split the value when the rows are stored as delimited text.
        const value = input.value.replace(/[\t\r\n]+/g, ' ');
        if (commit && value !== original) {
          row[attr] = value;
          markDirty();
        }
        td.classList.remove('eg-cell-editing');
        td.textContent = commit ? value : original;
      }

      input.addEventListener('blur', () => finish(true));
      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          finish(true);
          startEdit(cellBelow(td));
        } else if (e.key === 'Escape') {
          e.preventDefault();
          e.stopPropagation(); // don't let the host page treat it as "close"
          finish(false);
        } else if (e.key === 'Tab') {
          e.preventDefault();
          finish(true);
          startEdit(e.shiftKey ? td.previousElementSibling : td.nextElementSibling);
        }
      });
    }

    function cellBelow(td) {
      const nextRow = td.parentElement.nextElementSibling;
      return nextRow ? nextRow.children[td.cellIndex] : null;
    }

    function addRow() {
      const row = {};
      attributes.forEach(a => { row[a] = ''; });
      allRows.push(row);
      // Clear anything that could hide a blank row, so the new row is visible at the bottom.
      state.search = '';
      el.search.value = '';
      state.filters = {};
      state.sortAttr = null;
      state.sortDir = null;
      markDirty();
      renderAll();
      el.tableWrap.scrollTop = el.tableWrap.scrollHeight;
      const lastRow = el.body.lastElementChild;
      if (lastRow) startEdit(lastRow.querySelector('.eg-cell'));
    }

    function addColumn() {
      const raw = prompt('New column name:');
      if (raw == null) return;
      const name = raw.trim();
      if (!name) { alert('Column name is required.'); return; }
      if (/[\t\r\n]/.test(name)) { alert('Column name cannot contain tabs or line breaks.'); return; }
      if (attributes.some(a => a.toLowerCase() === name.toLowerCase())) {
        alert('A column named "' + name + '" already exists.');
        return;
      }
      attributes.push(name);
      allRows.forEach(r => { if (r[name] == null) r[name] = ''; });
      markDirty();
      renderAll();
      el.tableWrap.scrollLeft = el.tableWrap.scrollWidth;
    }

    function deleteRow(ri) {
      if (!allRows[ri]) return;
      allRows.splice(ri, 1);
      markDirty();
      renderBody();
    }

    // -------------------------------------------------------------------------
    // Column filter popover
    // -------------------------------------------------------------------------

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
      if (editable) {
        if (e.target.closest('[data-action="add-row"]')) { closePopover(); addRow(); return; }
        if (e.target.closest('[data-action="add-column"]')) { closePopover(); addColumn(); return; }
        const delBtn = e.target.closest('[data-action="delete-row"]');
        if (delBtn) { closePopover(); deleteRow(Number(delBtn.getAttribute('data-ri'))); return; }
        const cell = e.target.closest('td.eg-cell');
        if (cell) { closePopover(); startEdit(cell); return; }
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

    return {
      /** Rows as currently shown — after search, column filters and sort (not capped at the render limit). */
      getVisibleRows: () => getFilteredSortedRows(),
      /** Every row (including edits) and the current column list. */
      getData: () => ({ attributes: attributes.slice(), rows: allRows }),
      isDirty: () => dirty
    };
  }

  global.ExcelGrid = { mount };
})(window);
