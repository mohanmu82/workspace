// Browser-SSO login: redirects to the IdP URL from /api/auth/config, captures the JWT
// it sends back, and gates the page / API calls on the resulting permission grant.
// See auth.* properties in application.properties.
(function () {
  const TOKEN_KEY = 'authJwt';
  let authConfig = null;
  let currentUser = null;

  // Attach the bearer token to every same-origin fetch, regardless of when in the
  // page's lifecycle the token becomes available.
  const nativeFetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    init = init || {};
    const token = sessionStorage.getItem(TOKEN_KEY);
    if (token) {
      init.headers = Object.assign({}, init.headers, { Authorization: 'Bearer ' + token });
    }
    return nativeFetch(input, init);
  };

  async function loadConfig() {
    authConfig = await nativeFetch('/api/auth/config').then(r => r.json());
    return authConfig;
  }

  function captureTokenFromUrl() {
    const params = new URLSearchParams(location.search);
    const tokenParam = authConfig.tokenParam || 'token';
    const token = params.get(tokenParam);
    if (!token) return;
    sessionStorage.setItem(TOKEN_KEY, token);
    params.delete(tokenParam);
    const rest = params.toString();
    history.replaceState({}, '', location.pathname + (rest ? '?' + rest : '') + location.hash);
  }

  function redirectToLogin() {
    const back = encodeURIComponent(location.href);
    location.href = authConfig.jwtUrl + '?' + authConfig.redirectParam + '=' + back;
  }

  function renderBadge() {
    const el = document.getElementById('userBadge');
    if (el && currentUser) el.textContent = 'Welcome, ' + currentUser.name + '!';
  }

  function applyPermissionGating() {
    const isAdmin = !!currentUser.isAdmin;
    document.querySelectorAll('.admin-only').forEach(el => { el.style.display = isAdmin ? '' : 'none'; });
    const perm = currentUser.permission || {};
    document.querySelectorAll('.needs-create').forEach(el => { el.disabled = !perm.create; });
    document.querySelectorAll('.needs-update').forEach(el => { el.disabled = !perm.update; });
    document.querySelectorAll('.needs-delete').forEach(el => { el.disabled = !perm.delete; });
  }

  function showNoAccessScreen() {
    document.body.innerHTML =
      '<div class="d-flex flex-column align-items-center justify-content-center text-center" style="height:100vh">' +
      '<h4 class="mb-2"><i class="bi bi-lock"></i> You don\'t have access to this application</h4>' +
      '<p class="text-muted">Signed in as ' + escapeHtml(currentUser.name) + '. Ask an admin to grant you access.</p>' +
      '</div>';
  }

  function escapeHtml(s) {
    const d = document.createElement('div');
    d.textContent = s == null ? '' : String(s);
    return d.innerHTML;
  }

  async function init() {
    await loadConfig();

    if (!authConfig.enabled) {
      currentUser = { userid: 'local-dev', name: 'Local Dev', isAdmin: true,
        permission: { read: true, create: true, update: true, delete: true } };
      renderBadge();
      applyPermissionGating();
      return;
    }

    captureTokenFromUrl();
    if (!sessionStorage.getItem(TOKEN_KEY)) { redirectToLogin(); return; }

    const res = await fetch('/api/auth/me');
    if (res.status === 401) {
      sessionStorage.removeItem(TOKEN_KEY);
      redirectToLogin();
      return;
    }
    currentUser = await res.json();
    renderBadge();

    if (!currentUser.isAdmin && !currentUser.permission) {
      showNoAccessScreen();
      return;
    }
    applyPermissionGating();
  }

  window.AuthJs = { getUser: () => currentUser };
  document.addEventListener('DOMContentLoaded', init);
})();
