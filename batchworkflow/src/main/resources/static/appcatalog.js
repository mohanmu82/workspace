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

        // ── Document inputs (json / xml) ─────────────────────────────────────
        // A use case input declared json or xml holds a whole document rather than a word, so both
        // the use case editor (its default) and the instance editor (the value for this run) give it
        // a textarea with a pretty printer. The formatting lives here so the two agree on what
        // "formatted" means, and on what a document that will not parse is told.

        /** The input types whose value is a document — edited in a textarea, and pretty-printable. */
        DOCUMENT_TYPES: ['json', 'xml'],

        isDocumentType(type) { return AC.DOCUMENT_TYPES.includes(type || 'string'); },

        /** Pretty-prints a json or xml document, throwing a readable message when it is neither. */
        formatDocument(type, text) {
            if (type === 'xml') return AC.formatXml(text);
            try {
                return JSON.stringify(JSON.parse(text), null, 2);
            } catch (e) {
                throw new Error('Not valid JSON: ' + e.message);
            }
        },

        /**
         * Re-indents XML using the browser's own parser, so what comes back is only ever a document
         * that parsed. Any leading declaration is kept as written — the parser drops it, and a value
         * that names its encoding means to keep naming it.
         */
        formatXml(text) {
            const doc = new DOMParser().parseFromString(text, 'application/xml');
            const error = doc.querySelector('parsererror');
            if (error) throw new Error('Not valid XML: ' + error.textContent.trim().split('\n')[0]);
            const declaration = text.match(/^\s*<\?xml[^>]*\?>/);
            return (declaration ? declaration[0].trim() + '\n' : '') + formatXmlNode(doc.documentElement, 0);
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

    // ── XML formatting internals ─────────────────────────────────────────────

    function formatXmlNode(node, depth) {
        const indent = '  '.repeat(depth);
        const childIndent = '  '.repeat(depth + 1);
        const open = '<' + node.nodeName + xmlAttributes(node) + '>';
        const close = '</' + node.nodeName + '>';

        // Whitespace-only text between elements is the previous indentation and goes; anything else a
        // child node carries is content.
        const children = [...node.childNodes].filter(child =>
            child.nodeType === Node.ELEMENT_NODE
            || child.nodeType === Node.CDATA_SECTION_NODE
            || child.nodeType === Node.COMMENT_NODE
            || (child.nodeType === Node.TEXT_NODE && child.nodeValue.trim()));

        if (!children.length) return indent + '<' + node.nodeName + xmlAttributes(node) + '/>';

        // A lone text child stays on the element's own line — <id>42</id> reads worse over three.
        if (children.length === 1 && children[0].nodeType === Node.TEXT_NODE) {
            return indent + open + escapeXmlText(children[0].nodeValue.trim()) + close;
        }
        // Content mixed in among the elements is written back exactly as it stands: indenting text is
        // editing it, and formatting reformats a document rather than changing what it says.
        if (children.some(child => child.nodeType !== Node.ELEMENT_NODE && child.nodeType !== Node.COMMENT_NODE)) {
            return indent + open + [...node.childNodes].map(serializeXmlInline).join('') + close;
        }

        const inner = children.map(child => child.nodeType === Node.ELEMENT_NODE
            ? formatXmlNode(child, depth + 1)
            : childIndent + '<!--' + child.nodeValue + '-->').join('\n');
        return indent + open + '\n' + inner + '\n' + indent + close;
    }

    /** One node and everything under it on a single line, exactly as the parser read it. */
    function serializeXmlInline(node) {
        if (node.nodeType === Node.CDATA_SECTION_NODE) return '<![CDATA[' + node.nodeValue + ']]>';
        if (node.nodeType === Node.COMMENT_NODE)       return '<!--' + node.nodeValue + '-->';
        if (node.nodeType !== Node.ELEMENT_NODE)       return escapeXmlText(node.nodeValue);
        const children = [...node.childNodes];
        if (!children.length) return '<' + node.nodeName + xmlAttributes(node) + '/>';
        return '<' + node.nodeName + xmlAttributes(node) + '>'
             + children.map(serializeXmlInline).join('') + '</' + node.nodeName + '>';
    }

    function xmlAttributes(node) {
        let attributes = '';
        for (const attribute of node.attributes) {
            attributes += ' ' + attribute.name + '="' + attribute.value.replace(/"/g, '&quot;') + '"';
        }
        return attributes;
    }

    function escapeXmlText(text) {
        return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    global.AC = AC;

})(window);
