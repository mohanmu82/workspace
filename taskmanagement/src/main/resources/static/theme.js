// Shared dark-mode toggle + "My Tasks" identity helpers used across all pages.
(function () {
  function getStoredTheme() {
    return localStorage.getItem('theme') || 'light';
  }

  function applyThemeIcon(theme) {
    const icon = document.getElementById('themeIcon');
    if (icon) icon.className = theme === 'dark' ? 'bi bi-sun' : 'bi bi-moon-stars';
  }

  window.toggleTheme = function () {
    const next = getStoredTheme() === 'dark' ? 'light' : 'dark';
    localStorage.setItem('theme', next);
    document.documentElement.setAttribute('data-bs-theme', next);
    applyThemeIcon(next);
  };

  document.addEventListener('DOMContentLoaded', function () {
    applyThemeIcon(getStoredTheme());
  });
})();

// Shared "my name" identity, stored locally, used for My Tasks filtering
// and to prefill assignee/author fields so people stop retyping their name.
(function () {
  const KEY = 'myName';

  window.getMyName = function () {
    return localStorage.getItem(KEY) || '';
  };

  window.setMyName = function (name) {
    if (name) localStorage.setItem(KEY, name);
    else localStorage.removeItem(KEY);
  };

  window.promptForMyName = function () {
    const current = window.getMyName();
    const name = prompt('Your name (used for "My Tasks" and to prefill assignee/author fields):', current);
    if (name === null) return current;
    window.setMyName(name.trim());
    return window.getMyName();
  };
})();
