(function () {
  const STORAGE_KEY = 'zanbo_admin_token';

  function readToken() {
    try {
      return (
        window.localStorage.getItem(STORAGE_KEY) ||
        window.sessionStorage.getItem(STORAGE_KEY) ||
        ''
      ).trim();
    } catch {
      return '';
    }
  }

  function storeToken(token, persistent = true) {
    const normalized = String(token || '').trim();
    if (!normalized) return '';
    try {
      const target = persistent ? window.localStorage : window.sessionStorage;
      target.setItem(STORAGE_KEY, normalized);
    } catch {
      // ignore storage errors
    }
    return normalized;
  }

  function clearToken() {
    try {
      window.localStorage.removeItem(STORAGE_KEY);
      window.sessionStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore storage errors
    }
  }

  async function zanboAdminFetch(input, init = {}) {
    const headers = new Headers(init.headers || {});
    let token = headers.get('X-Admin-Token') || headers.get('Authorization') || readToken();
    if (!token && typeof window.prompt === 'function') {
      const answer = window.prompt('请输入 BACKEND_ADMIN_TOKEN（将缓存到当前浏览器）', '');
      token = storeToken(answer || '');
    }
    if (token && !headers.get('X-Admin-Token') && !headers.get('Authorization')) {
      headers.set('X-Admin-Token', String(token).trim());
    }
    return window.fetch(input, { ...init, headers });
  }

  window.zanboAdminFetch = zanboAdminFetch;
  window.setZanboAdminToken = storeToken;
  window.clearZanboAdminToken = clearToken;
})();
