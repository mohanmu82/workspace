// Shared WebSocket client for /ws/tasks — lets pages run task searches over a single
// persistent connection instead of one HTTP request per query, and receive live
// create/update/delete notifications so open views stay in sync across browsers.
const TaskWS = (() => {
  const ARRAY_FIELDS = new Set(['status', 'priority', 'tags']);
  const NUMBER_FIELDS = new Set(['page', 'size']);
  const REQUEST_TIMEOUT_MS = 5000;

  let socket = null;
  let connected = false;
  let reconnectDelay = 1000;
  let reqCounter = 0;
  const pending = new Map();
  const changeListeners = new Set();

  function wsUrl() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${proto}//${location.host}/ws/tasks`;
  }

  function connect() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;
    let ws;
    try {
      ws = new WebSocket(wsUrl());
    } catch (e) {
      return;
    }
    socket = ws;
    ws.onopen = () => { connected = true; reconnectDelay = 1000; };
    ws.onclose = () => {
      connected = false;
      pending.forEach(entry => { clearTimeout(entry.timer); entry.reject(new Error('WebSocket closed')); });
      pending.clear();
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 15000);
    };
    ws.onerror = () => {};
    ws.onmessage = evt => {
      let msg;
      try { msg = JSON.parse(evt.data); } catch (e) { return; }
      if (msg.type === 'task-changed') {
        changeListeners.forEach(cb => { try { cb(msg); } catch (e) { console.error(e); } });
        return;
      }
      const entry = msg.requestId && pending.get(msg.requestId);
      if (!entry) return;
      pending.delete(msg.requestId);
      clearTimeout(entry.timer);
      if (msg.type === 'error') entry.reject(new Error(msg.message || 'WebSocket request failed'));
      else entry.resolve(msg.data);
    };
  }

  function send(type, payload) {
    return new Promise((resolve, reject) => {
      if (!connected || !socket || socket.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }
      const requestId = 'r' + (++reqCounter) + '-' + Date.now();
      const timer = setTimeout(() => {
        pending.delete(requestId);
        reject(new Error('WebSocket request timed out'));
      }, REQUEST_TIMEOUT_MS);
      pending.set(requestId, { resolve, reject, timer });
      socket.send(JSON.stringify({ type, requestId, payload }));
    });
  }

  connect();

  return {
    isConnected: () => connected,

    // Converts a URLSearchParams (as built for the REST /api/tasks query string) into the
    // shape TaskSearchRequest expects, so callers can share one params builder for both paths.
    paramsToPayload(usp) {
      const payload = {};
      for (const key of new Set(usp.keys())) {
        const values = usp.getAll(key);
        if (ARRAY_FIELDS.has(key)) payload[key] = values;
        else if (NUMBER_FIELDS.has(key)) payload[key] = Number(values[0]);
        else payload[key] = values[0];
      }
      return payload;
    },

    search(payload) { return send('search', payload); },
    getFilterOptions() { return send('filter-options'); },

    // Registers a callback for {type:'task-changed', changeType, taskId, task} broadcasts.
    // Returns an unsubscribe function.
    onTaskChanged(cb) {
      changeListeners.add(cb);
      return () => changeListeners.delete(cb);
    },
  };
})();
