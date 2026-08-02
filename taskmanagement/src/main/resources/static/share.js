// Shared "Share this page" menu, injected into the navbar next to the theme toggle
// on every page. Lets a user send the current page's URL via email, Outlook web
// compose, or Microsoft Teams — no server round-trip, just deep links.
(function () {
  function shareLinks() {
    const url = encodeURIComponent(location.href);
    const title = encodeURIComponent(document.title || 'Task Manager');
    return {
      email: `mailto:?subject=${title}&body=${url}`,
      outlook: `https://outlook.office.com/mail/deeplink/compose?subject=${title}&body=${url}`,
      teams: `https://teams.microsoft.com/share?href=${url}&msgText=${title}`,
    };
  }

  window.openShareLink = function (kind) {
    const href = shareLinks()[kind];
    if (href) window.open(href, '_blank', 'noopener');
  };

  function buildMenu() {
    const wrap = document.createElement('div');
    wrap.className = 'dropdown d-inline-block';
    wrap.innerHTML =
      '<button class="btn btn-outline-light btn-sm dropdown-toggle" type="button" id="sharePageBtn" data-bs-toggle="dropdown" aria-expanded="false" title="Share this page">' +
      '<i class="bi bi-share"></i></button>' +
      '<ul class="dropdown-menu dropdown-menu-end" aria-labelledby="sharePageBtn">' +
      '<li><a class="dropdown-item" href="#" onclick="openShareLink(\'email\'); return false;"><i class="bi bi-envelope me-2"></i>Email</a></li>' +
      '<li><a class="dropdown-item" href="#" onclick="openShareLink(\'outlook\'); return false;"><i class="bi bi-microsoft me-2"></i>Outlook</a></li>' +
      '<li><a class="dropdown-item" href="#" onclick="openShareLink(\'teams\'); return false;"><i class="bi bi-microsoft-teams me-2"></i>Teams</a></li>' +
      '</ul>';
    return wrap;
  }

  document.addEventListener('DOMContentLoaded', function () {
    const themeBtn = document.getElementById('themeToggleBtn');
    if (!themeBtn || document.getElementById('sharePageBtn')) return;
    themeBtn.parentNode.insertBefore(buildMenu(), themeBtn);
  });
})();
