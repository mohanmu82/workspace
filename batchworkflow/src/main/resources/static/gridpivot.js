/*
 * The pivot behind Analyze, shared by the screen that lets an operator build one by hand
 * (gridanalyzer.html) and the page runner that applies a saved one as an action binds its rows
 * (apppage.html). One copy of the arithmetic is what makes a pivot added to a page from the analyzer
 * come out the same on the page as it did on the screen it was built on.
 *
 * Everything here is pure: rows and a configuration in, a model or a table out. Drawing is left to
 * whoever called.
 */
(function (global) {
    'use strict';

    /**
     * The one measure that is not a column: it counts rows rather than values of anything. Prefixed
     * with a control character so no column of any response can collide with it. A saved pivot
     * spells it {@code {agg: 'rows'}} instead — see {@link #fromSpec} — so the control character is
     * never written into a page.
     */
    const ROW_COUNT = 'rows';
    const ROW_COUNT_LABEL = '(row count)';
    const ROW_COUNT_AGG = 'rows';

    /**
     * What a cell can work out. Numeric ones right-align and take part in the heat map; min and max
     * fall back to comparing text when the column is not numbers. Mirrors AppPagePivot#AGGS.
     */
    const AGGS = {
        sum:    { label: 'Sum',     numeric: true },
        count:  { label: 'Count',   numeric: true },
        unique: { label: 'Unique',  numeric: true },
        avg:    { label: 'Average', numeric: true },
        min:    { label: 'Min' },
        max:    { label: 'Max' }
    };

    /**
     * What the parts of a multi-level key are joined with to index it. A control character rather
     * than anything readable: "A" + "B/C" and "A/B" + "C" must not land in the same bucket.
     */
    const SEP = '';

    /** A cell as the text it was shown as, objects and nulls included. */
    function cellText(row, column) {
        const cell = (row && typeof row === 'object') ? row[column] : row;
        if (cell === undefined || cell === null) return '';
        return typeof cell === 'object' ? JSON.stringify(cell) : String(cell);
    }

    function toNumber(text) {
        if (text === '' || text === null || text === undefined) return null;
        // Thousands separators and a trailing % or currency are ordinary in a response someone is
        // summing, and Number() alone rejects every one of them.
        const cleaned = String(text).replace(/[\s,]/g, '').replace(/^[$£€]/, '').replace(/%$/, '');
        if (cleaned === '' || isNaN(Number(cleaned))) return null;
        return Number(cleaned);
    }

    /** Numbers compare as numbers so a column of ids does not order 1, 10, 2. */
    function compareText(a, b) {
        const na = toNumber(a), nb = toNumber(b);
        if (na !== null && nb !== null) return na === nb ? 0 : (na < nb ? -1 : 1);
        return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' });
    }

    function fieldLabel(field) {
        return field === ROW_COUNT ? ROW_COUNT_LABEL : field;
    }

    function measureLabel(measure) {
        return measure.field === ROW_COUNT ? 'Row count'
            : (AGGS[measure.agg] || AGGS.count).label + ' of ' + measure.field;
    }

    /** A key part as it is shown — an empty cell is a group of its own and says so. */
    function keyLabel(part) {
        return part === '' ? '(blank)' : part;
    }

    // ── Aggregation ──────────────────────────────────────────────────────────
    // One accumulator per measure per cell, fed a single pass over the rows. Only what the chosen
    // aggregation needs is kept: a Set is built for "unique" and for nothing else.

    function newAcc(agg) {
        if (agg === 'unique') return { set: new Set() };
        if (agg === 'sum' || agg === 'avg') return { sum: 0, n: 0 };
        if (agg === 'count') return { n: 0 };
        return { best: undefined };
    }

    function accept(acc, measure, row, read) {
        const agg = measure.agg;
        if (measure.field === ROW_COUNT) { acc.n++; return; }

        const text = read(row, measure.field);
        if (agg === 'count') { if (text !== '') acc.n++; return; }
        if (agg === 'unique') { if (text !== '') acc.set.add(text); return; }
        if (agg === 'sum' || agg === 'avg') {
            const n = toNumber(text);
            if (n !== null) { acc.sum += n; acc.n++; }
            return;
        }
        if (text === '') return;
        if (acc.best === undefined || compareText(text, acc.best) * (agg === 'max' ? 1 : -1) > 0) acc.best = text;
    }

    /** The accumulator's answer, or null for "nothing here" — which draws as a dash, not a zero. */
    function accValue(acc, measure) {
        const agg = measure.field === ROW_COUNT ? 'count' : measure.agg;
        if (agg === 'count')  return acc.n;
        if (agg === 'unique') return acc.set.size;
        if (agg === 'sum')    return acc.n ? acc.sum : null;
        if (agg === 'avg')    return acc.n ? acc.sum / acc.n : null;
        return acc.best === undefined ? null : acc.best;
    }

    /** The configuration every pivot starts from: nothing grouped, rows counted, totals on. */
    function defaults() {
        return {
            rows: [],
            cols: [],
            values: [{ field: ROW_COUNT, agg: 'count' }],
            grandRow: true,
            grandCol: true,
            blanks: true,
            rowOrder: 'key',   // 'key' | 'valueDesc' | 'valueAsc'
            colOrder: 'key'
        };
    }

    /**
     * The whole pivot, in one pass over the rows.
     *
     * <p>Row keys, column keys and every cell are collected together rather than in a pass each.
     * Totals are accumulated alongside rather than summed from the cells afterwards, because a total
     * of averages or of distinct counts is not the average or the distinct count of the whole.
     *
     * @param rows    the rows to group
     * @param cfg     rows, cols, values, grandRow, grandCol, blanks, rowOrder, colOrder
     * @param options {@code read(row, column)} — how a cell is read; defaults to {@link #cellText}
     */
    function compute(rows, cfg, options) {
        const read = (options && options.read) || cellText;
        const rowFields = cfg.rows || [];
        const colFields = cfg.cols || [];
        const measures  = (cfg.values && cfg.values.length) ? cfg.values : [{ field: ROW_COUNT, agg: 'count' }];

        const rowKeys = new Map();   // joined key -> parts
        const colKeys = new Map();
        const cells   = new Map();   // row key -> Map(col key -> accumulators)
        const rowTot  = new Map();
        const colTot  = new Map();
        const grand   = measures.map(m => newAcc(m.agg));

        const accsFor = (map, key) => {
            let accs = map.get(key);
            if (!accs) { accs = measures.map(m => newAcc(m.agg)); map.set(key, accs); }
            return accs;
        };

        for (const row of rows || []) {
            const rowParts = rowFields.map(f => read(row, f));
            const colParts = colFields.map(f => read(row, f));

            // "Keep blank values" off drops the row from the pivot entirely rather than bucketing it
            // under (blank) — the usual reason to turn it off is that the blanks are noise.
            if (cfg.blanks === false && (rowParts.some(p => p === '') || colParts.some(p => p === ''))) continue;

            const rowKey = rowParts.join(SEP);
            const colKey = colParts.join(SEP);
            if (!rowKeys.has(rowKey)) rowKeys.set(rowKey, rowParts);
            if (!colKeys.has(colKey)) colKeys.set(colKey, colParts);

            let byCol = cells.get(rowKey);
            if (!byCol) { byCol = new Map(); cells.set(rowKey, byCol); }

            const cell = accsFor(byCol, colKey);
            const rt   = accsFor(rowTot, rowKey);
            const ct   = accsFor(colTot, colKey);
            for (let i = 0; i < measures.length; i++) {
                accept(cell[i], measures[i], row, read);
                accept(rt[i],   measures[i], row, read);
                accept(ct[i],   measures[i], row, read);
                accept(grand[i], measures[i], row, read);
            }
        }

        const sortKeys = keys => [...keys.values()].sort((a, b) => {
            for (let i = 0; i < a.length; i++) {
                const cmp = compareText(a[i], b[i]);
                if (cmp) return cmp;
            }
            return 0;
        });

        // Ordering by value uses each group's total of the first measure. Keys without a value sink
        // to the end, and the sort is stable, so ties keep their A→Z order.
        const orderKeys = (keys, order, totals) => {
            const sorted = sortKeys(keys);
            if (!order || order === 'key') return sorted;
            const dir = order === 'valueAsc' ? 1 : -1;
            const valueOf = key => {
                const accs = totals.get(key.join(SEP));
                const value = accs ? accValue(accs[0], measures[0]) : null;
                return value === null || value === undefined ? null : value;
            };
            return sorted.map(key => [key, valueOf(key)])
                .sort(([, a], [, b]) => (a === null || b === null) ? (a === null) - (b === null) : compareText(a, b) * dir)
                .map(([key]) => key);
        };

        return {
            rowFields, colFields, measures,
            rowKeys: rowFields.length ? orderKeys(rowKeys, cfg.rowOrder, rowTot) : [[]],
            colKeys: colFields.length ? orderKeys(colKeys, cfg.colOrder, colTot) : [[]],
            cells, rowTot, colTot, grand,
            // A total column repeats the only column when nothing is pivoted across, and a total row
            // repeats the only row when nothing is grouped down — so each is offered only when it
            // says something the table does not already.
            showGrandCol: cfg.grandCol !== false && colFields.length > 0,
            showGrandRow: cfg.grandRow !== false && rowFields.length > 0
        };
    }

    /**
     * The pivot flattened to a header row plus body rows of plain strings. A column header carries
     * its whole path, since "EMEA / Q3 · Sum of amount" only means something in full.
     */
    function matrix(model) {
        if (!model) return null;
        const { rowFields, colFields, measures, rowKeys, colKeys } = model;

        const header = (rowFields.length ? rowFields.slice() : ['Group']);
        colKeys.forEach(colKey => measures.forEach(m => {
            const path = colFields.length ? colKey.map(keyLabel).join(' / ') : '';
            header.push(path ? path + ' · ' + measureLabel(m) : measureLabel(m));
        }));
        if (model.showGrandCol) measures.forEach(m => header.push('Total · ' + measureLabel(m)));

        const plain = (acc, m) => {
            const value = acc ? accValue(acc, m) : null;
            return value === null || value === undefined ? '' : String(value);
        };

        const body = rowKeys.map(key => {
            const line = rowFields.length ? key.map(keyLabel) : ['Total'];
            const byCol = model.cells.get(key.join(SEP));
            colKeys.forEach(colKey => {
                const accs = byCol && byCol.get(colKey.join(SEP));
                measures.forEach((m, i) => line.push(plain(accs && accs[i], m)));
            });
            if (model.showGrandCol) {
                const accs = model.rowTot.get(key.join(SEP));
                measures.forEach((m, i) => line.push(plain(accs && accs[i], m)));
            }
            return line;
        });

        if (model.showGrandRow) {
            const line = ['Total'];
            while (line.length < Math.max(1, rowFields.length)) line.push('');
            colKeys.forEach(colKey => {
                const accs = model.colTot.get(colKey.join(SEP));
                measures.forEach((m, i) => line.push(plain(accs && accs[i], m)));
            });
            if (model.showGrandCol) measures.forEach((m, i) => line.push(plain(model.grand[i], m)));
            body.push(line);
        }

        return { header: header, body: body };
    }

    /**
     * The pivot as a grid holds rows: a column list and one object per line. A header that repeats
     * — a row field that happens to read like a measure's column — is numbered rather than letting
     * the second overwrite the first in every row.
     */
    function table(rows, cfg, options) {
        const flat = matrix(compute(rows, cfg, options));
        const columns = [];
        const taken = new Set();
        flat.header.forEach(name => {
            let unique = name, n = 2;
            while (taken.has(unique)) unique = name + ' (' + (n++) + ')';
            taken.add(unique);
            columns.push(unique);
        });
        return {
            columns: columns,
            rows: flat.body.map(line => {
                const row = {};
                columns.forEach((column, i) => { row[column] = line[i] === undefined ? '' : line[i]; });
                return row;
            })
        };
    }

    /** Columns whose values read as numbers often enough to default to Sum and right-align. */
    function detectNumeric(rows, columns, options) {
        const read = (options && options.read) || cellText;
        const found = new Set();
        const sample = Math.min(rows.length, 200);
        columns.forEach(column => {
            let seen = 0, numeric = 0;
            for (let i = 0; i < sample; i++) {
                const text = read(rows[i], column);
                if (text === '') continue;
                seen++;
                if (toNumber(text) !== null) numeric++;
            }
            if (seen && numeric / seen >= 0.8) found.add(column);
        });
        return found;
    }

    // ── The saved form ───────────────────────────────────────────────────────
    // What a page carries on an action (AppPagePivot): the same fields, with the row count spelled as
    // an aggregation of its own rather than as a control-character field name.

    const ORDERS = ['key', 'valueDesc', 'valueAsc'];

    /** A configuration as it is saved onto an action. */
    function toSpec(cfg) {
        return {
            rows: (cfg.rows || []).slice(),
            cols: (cfg.cols || []).slice(),
            values: (cfg.values || []).map(m => m.field === ROW_COUNT
                ? { field: '', agg: ROW_COUNT_AGG }
                : { field: m.field, agg: m.agg }),
            grandRow: cfg.grandRow !== false,
            grandCol: cfg.grandCol !== false,
            blanks: cfg.blanks !== false,
            rowOrder: ORDERS.includes(cfg.rowOrder) ? cfg.rowOrder : 'key',
            colOrder: ORDERS.includes(cfg.colOrder) ? cfg.colOrder : 'key'
        };
    }

    /** A saved pivot back as a configuration {@link #compute} takes; null for one that groups nothing. */
    function fromSpec(spec) {
        if (!spec) return null;
        const cfg = defaults();
        cfg.rows = (spec.rows || []).filter(Boolean);
        cfg.cols = (spec.cols || []).filter(Boolean);
        cfg.values = (spec.values || [])
            .filter(v => v && (v.agg === ROW_COUNT_AGG || (v.field && AGGS[v.agg])))
            .map(v => v.agg === ROW_COUNT_AGG ? { field: ROW_COUNT, agg: 'count' } : { field: v.field, agg: v.agg });
        if (!cfg.values.length) cfg.values = [{ field: ROW_COUNT, agg: 'count' }];
        cfg.grandRow = spec.grandRow !== false;
        cfg.grandCol = spec.grandCol !== false;
        cfg.blanks = spec.blanks !== false;
        cfg.rowOrder = ORDERS.includes(spec.rowOrder) ? spec.rowOrder : 'key';
        cfg.colOrder = ORDERS.includes(spec.colOrder) ? spec.colOrder : 'key';
        return cfg;
    }

    /** Whether a saved pivot asks for any grouping at all — an empty one leaves rows as they came. */
    function isActive(spec) {
        return !!spec && ((spec.rows || []).some(Boolean) || (spec.cols || []).some(Boolean)
            || (spec.values || []).some(v => v && (v.agg === ROW_COUNT_AGG || v.field)));
    }

    /** One line saying what a saved pivot does — for a card, a status line, a title. */
    function describe(spec) {
        const cfg = fromSpec(spec);
        if (!cfg) return '';
        const what = cfg.values.map(measureLabel).join(', ');
        const by = [cfg.rows.length ? 'by ' + cfg.rows.join(', ') : '', cfg.cols.length ? 'across ' + cfg.cols.join(', ') : '']
            .filter(Boolean).join(' ');
        return what + (by ? ' ' + by : '');
    }

    global.GridPivot = {
        ROW_COUNT, ROW_COUNT_LABEL, ROW_COUNT_AGG, AGGS, SEP, ORDERS,
        cellText, toNumber, compareText, fieldLabel, measureLabel, keyLabel,
        newAcc, accept, accValue, defaults, compute, matrix, table, detectNumeric,
        toSpec, fromSpec, isActive, describe
    };
})(window);
