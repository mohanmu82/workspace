/**
 * Shared helpers for the App Catalog pages (app.html and its four sub-pages).
 *
 * Beyond the usual escaping/fetch plumbing this owns two things the sub-pages agree on:
 * the currently selected app — remembered in localStorage so switching tabs keeps your
 * context — and detection of whether the page is running inside the app.html shell, which
 * suppresses each sub-page's own back-link and trims its padding.
 */
(function (global) {
    'use strict';

    const SELECTED_APP_KEY = 'appCatalog.selectedApp';

    const AC = {

        // ── Escaping / formatting ────────────────────────────────────────────

        esc(value) {
            return String(value === undefined || value === null ? '' : value)
                .replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
        },

        /**
         * Escapes a value for use inside a single-quoted JS string within an HTML attribute,
         * e.g. onclick="edit('<here>')". Backslash/quote escaping first, then HTML escaping, so a
         * name containing an apostrophe survives the round trip instead of breaking the handler.
         */
        attr(value) {
            return AC.esc(String(value === undefined || value === null ? '' : value)
                .replace(/\\/g, '\\\\')
                .replace(/'/g, "\\'"));
        },

        prettyJson(value) {
            if (value === undefined || value === null) return '';
            try { return JSON.stringify(value, null, 2); } catch (e) { return String(value); }
        },

        /** Parses a JSON-object textarea, treating blank as an empty object. */
        parseJsonObject(text, fieldLabel) {
            const raw = (text || '').trim();
            if (!raw) return {};
            let parsed;
            try {
                parsed = JSON.parse(raw);
            } catch (e) {
                throw new Error(fieldLabel + ' is not valid JSON: ' + e.message);
            }
            if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
                throw new Error(fieldLabel + ' must be a JSON object, e.g. {"region": "emea"}');
            }
            return parsed;
        },

        methodClass(method) {
            const m = (method || 'get').toLowerCase();
            return ['get', 'post', 'put', 'delete'].includes(m) ? m : 'other';
        },

        // ── REST ─────────────────────────────────────────────────────────────

        /** Fetches JSON, surfacing the server's {"error": "..."} message as the thrown Error. */
        async api(method, url, body) {
            const options = { method, headers: {} };
            if (body !== undefined) {
                options.headers['Content-Type'] = 'application/json';
                options.body = JSON.stringify(body);
            }
            const response = await fetch(url, options);
            const text = await response.text();
            let data = null;
            if (text) {
                try { data = JSON.parse(text); } catch (e) { data = text; }
            }
            if (!response.ok) {
                const message = data && data.error ? data.error : (typeof data === 'string' && data ? data : response.statusText);
                throw new Error(message);
            }
            return data;
        },

        listApps()                 { return AC.api('GET', '/appcatalog/apps'); },
        listEnvironments(appName)  { return AC.api('GET', '/appcatalog/environments?appName=' + encodeURIComponent(appName)); },
        listUseCases(appName)      { return AC.api('GET', '/appcatalog/usecases?appName=' + encodeURIComponent(appName)); },
        listInstances(appName)     { return AC.api('GET', '/appcatalog/instances' + (appName ? '?appName=' + encodeURIComponent(appName) : '')); },
        listGroups()               { return AC.api('GET', '/appcatalog/groups'); },

        // ── Status bar ───────────────────────────────────────────────────────

        setStatus(elementId, message, type) {
            const bar = document.getElementById(elementId);
            if (!bar) return;
            bar.textContent = message || '';
            bar.className = 'status-bar' + (type ? ' ' + type : '');
        },

        // ── Selects ──────────────────────────────────────────────────────────

        /**
         * Repopulates a select, keeping the current selection when it still exists so a
         * background refresh never yanks the user's choice out from under them.
         */
        fillSelect(select, options, placeholder) {
            const previous = select.value;
            select.innerHTML = '';
            if (placeholder !== undefined) {
                const opt = document.createElement('option');
                opt.value = '';
                opt.textContent = placeholder;
                select.appendChild(opt);
            }
            options.forEach(o => {
                const opt = document.createElement('option');
                opt.value = typeof o === 'string' ? o : o.value;
                opt.textContent = typeof o === 'string' ? o : o.label;
                select.appendChild(opt);
            });
            if ([...select.options].some(o => o.value === previous)) select.value = previous;
        },

        // ── Cross-page app selection ─────────────────────────────────────────

        selectedApp()             { return localStorage.getItem(SELECTED_APP_KEY) || ''; },
        setSelectedApp(appName)   { appName ? localStorage.setItem(SELECTED_APP_KEY, appName) : localStorage.removeItem(SELECTED_APP_KEY); },

        // ── Shell integration ────────────────────────────────────────────────

        /** True when this sub-page is rendered inside the app.html tab shell. */
        isEmbedded() {
            try { return global.self !== global.top; } catch (e) { return true; }
        },

        /** Trims chrome that the shell already provides. Call once on load. */
        applyEmbedding() {
            if (AC.isEmbedded()) document.body.classList.add('embedded');
        },

        /** Confirm helper so every destructive action phrases itself the same way. */
        confirmDelete(what, name) {
            return global.confirm('Delete ' + what + ' "' + name + '"? This cannot be undone.');
        }
    };

    global.AC = AC;

})(window);
