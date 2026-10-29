const KEY = 'orchestrator.apiKey';

function apiKey() {
  try { return sessionStorage.getItem(KEY) || ''; } catch { return ''; }
}

export async function api(path, { method = 'GET', body, retry = true } = {}) {
  const headers = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const key = apiKey();
  if (key) headers['X-API-Key'] = key;
  const res = await fetch(path, { method, headers, body: body === undefined ? undefined : body });
  if (res.status === 401 && retry) {
    const k = window.prompt('This deployment requires an API key for write operations:');
    if (k) {
      try { sessionStorage.setItem(KEY, k); } catch { /* ignore */ }
      return api(path, { method, body, retry: false });
    }
  }
  let data = null;
  const text = await res.text();
  if (text) { try { data = JSON.parse(text); } catch { data = { error: text }; } }
  return { ok: res.ok, status: res.status, data };
}
