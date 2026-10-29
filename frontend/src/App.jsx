import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ReactFlow, Background, Controls, Handle, Position, MarkerType } from '@xyflow/react';
import { TEMPLATES } from './templates.js';
import { api } from './api.js';
import { layoutTasks } from './layout.js';

const TERMINAL_RUN = new Set(['succeeded', 'failed', 'cancelled']);
const TYPE_ICON = { delay: '⏱', math: '∑', transform: 'Aa', http: '⇄' };

const fmtMs = (ms) => (ms == null ? '—' : ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(ms < 10000 ? 2 : 1)} s`);
const pretty = (v) => JSON.stringify(v, null, 2);

function useNow(active) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!active) return undefined;
    const id = setInterval(() => setNow(Date.now()), 250);
    return () => clearInterval(id);
  }, [active]);
  return now;
}

/* ---------------- DAG node ---------------- */
function TaskNode({ data }) {
  const { task, status, selected, flagged, now } = data;
  const retryIn = status === 'ready' && task.next_run_at ? Math.max(0, new Date(task.next_run_at).getTime() - now) : 0;
  return (
    <div className={`node node-${status} ${selected ? 'node-selected' : ''} ${flagged ? 'node-flagged' : ''}`}
         data-testid={`node-${task.id}`}>
      <Handle type="target" position={Position.Left} />
      <div className="node-top">
        <span className="node-type"><span className="node-icon">{TYPE_ICON[task.type] || '•'}</span>{task.type}</span>
        <span className={`pill pill-${status}`}>{status}</span>
      </div>
      <div className="node-title">{task.id}</div>
      <div className="node-meta">
        <span>{task.attempts != null ? `attempt ${task.attempts}/${task.max_attempts}` : `max ${task.max_attempts ?? 3} attempts`}</span>
        <span>{retryIn > 0 ? `retry in ${(retryIn / 1000).toFixed(1)}s` : task.duration_ms != null ? fmtMs(task.duration_ms) : ''}</span>
      </div>
      {status === 'running' && <div className="node-progress"><div /></div>}
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
const nodeTypes = { task: TaskNode };

const EDGE_COLOR = { succeeded: '#10b981', failed: '#ef4444', running: '#f59e0b' };

/* ---------------- App ---------------- */
export default function App() {
  const [templateKey, setTemplateKey] = useState('parallel-join');
  const [editorText, setEditorText] = useState(pretty(TEMPLATES[0].definition));
  const [errors, setErrors] = useState(null); // null = not validated, [] = valid
  const [notice, setNotice] = useState(null);
  const [busy, setBusy] = useState(false);
  const [workflows, setWorkflows] = useState([]);
  const [runs, setRuns] = useState([]);
  const [runId, setRunId] = useState(null);
  const [run, setRun] = useState(null);
  const [events, setEvents] = useState([]);
  const [selectedTask, setSelectedTask] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [health, setHealth] = useState('unknown');
  const timelineRef = useRef(null);

  const parsed = useMemo(() => {
    try { return { def: JSON.parse(editorText) }; } catch (e) { return { parseError: e.message }; }
  }, [editorText]);

  /* ----- background polling: health, metrics, runs, workflows ----- */
  const refreshLists = useCallback(async () => {
    const [w, r, m, h] = await Promise.all([
      api('/api/workflows'), api('/api/runs?limit=12'), api('/api/metrics'), api('/health/ready'),
    ]).catch(() => [{}, {}, {}, { ok: false }]);
    if (w.ok) setWorkflows(w.data);
    if (r.ok) setRuns(r.data);
    if (m.ok) setMetrics(m.data);
    setHealth(h.ok ? 'up' : 'down');
  }, []);
  useEffect(() => {
    refreshLists();
    const id = setInterval(refreshLists, 2000);
    return () => clearInterval(id);
  }, [refreshLists]);

  /* ----- select the most recent run on first load ----- */
  const autoSelected = useRef(false);
  useEffect(() => {
    if (!autoSelected.current && runs.length && !runId) {
      autoSelected.current = true;
      const params = new URLSearchParams(window.location.search);
      setRunId(params.get('run') || runs[0].id);
    }
  }, [runs, runId]);

  /* ----- poll the selected run every 1s while anything is still moving ----- */
  useEffect(() => {
    if (!runId) return undefined;
    let stop = false;
    let timer;
    const poll = async () => {
      const [r, e] = await Promise.all([api(`/api/runs/${runId}`), api(`/api/runs/${runId}/events`)]);
      if (stop) return;
      if (r.ok) setRun(r.data);
      if (e.ok) setEvents(e.data);
      const moving = !r.ok ? false
        : !TERMINAL_RUN.has(r.data.status) || r.data.tasks.some((t) => t.status === 'running');
      if (moving) timer = setTimeout(poll, 1000);
    };
    poll();
    return () => { stop = true; clearTimeout(timer); };
  }, [runId]);

  useEffect(() => {
    if (timelineRef.current) timelineRef.current.scrollTop = timelineRef.current.scrollHeight;
  }, [events.length]);

  const live = run && (!TERMINAL_RUN.has(run.status) || run.tasks.some((t) => t.status === 'running'));
  const now = useNow(live);

  /* ----- editor actions ----- */
  const loadTemplate = (t) => {
    setTemplateKey(t.key);
    setEditorText(pretty(t.definition));
    setErrors(null);
    setNotice(null);
    setRunId(null);
    setRun(null);
    setEvents([]);
    setSelectedTask(null);
  };

  const validate = async () => {
    if (parsed.parseError) { setErrors([{ code: 'malformed_json', path: '$', message: parsed.parseError }]); return false; }
    const r = await api('/api/workflows/validate', { method: 'POST', body: editorText });
    if (r.status === 422) { setErrors(r.data.errors); return false; }
    if (!r.ok) { setErrors([{ code: `http_${r.status}`, path: '$', message: r.data?.error || 'request failed' }]); return false; }
    setErrors([]);
    return true;
  };

  /** Save a new immutable version unless the editor matches the latest stored version of that name. */
  const saveDefinition = async () => {
    if (parsed.parseError) { setErrors([{ code: 'malformed_json', path: '$', message: parsed.parseError }]); return null; }
    const latest = workflows.find((w) => w.name === parsed.def.name);
    if (latest) {
      const d = await api(`/api/workflows/${latest.id}`);
      if (d.ok && JSON.stringify(d.data.definition) === JSON.stringify(parsed.def)) {
        setErrors([]);
        return { id: latest.id, version: latest.version, reused: true };
      }
    }
    const r = await api('/api/workflows', { method: 'POST', body: editorText });
    if (r.status === 422) { setErrors(r.data.errors); return null; }
    if (!r.ok) { setErrors([{ code: `http_${r.status}`, path: '$', message: r.data?.error || 'request failed' }]); return null; }
    setErrors([]);
    refreshLists();
    return { id: r.data.id, version: r.data.version, reused: false };
  };

  const onSave = async () => {
    setBusy(true);
    const s = await saveDefinition();
    if (s) setNotice(s.reused ? `Unchanged — ${parsed.def.name} v${s.version} already stored` : `Saved ${parsed.def.name} v${s.version}`);
    setBusy(false);
  };

  const onRun = async () => {
    setBusy(true);
    setNotice(null);
    const s = await saveDefinition();
    if (s) {
      const r = await api(`/api/workflows/${s.id}/runs`, { method: 'POST' });
      if (r.ok) {
        setSelectedTask(null);
        setRun(null);
        setEvents([]);
        setRunId(r.data.run_id);
        setNotice(`Started run of ${parsed.def.name} v${s.version}`);
        refreshLists();
      } else {
        setNotice(r.data?.error || 'failed to start run');
      }
    }
    setBusy(false);
  };

  const onCancel = async () => {
    if (!runId) return;
    const r = await api(`/api/runs/${runId}/cancel`, { method: 'POST' });
    setNotice(r.status === 202 ? 'Cancellation requested' : r.data?.error || `cancel failed (${r.status})`);
    const rr = await api(`/api/runs/${runId}`);
    if (rr.ok) setRun(rr.data);
  };

  const selectRun = (id) => {
    setRunId(id);
    setRun(null);
    setEvents([]);
    setSelectedTask(null);
  };

  /* ----- graph model: live run if selected, otherwise the editor draft ----- */
  const graphTasks = useMemo(() => {
    if (run) {
      return run.tasks.map((t) => ({
        id: t.task_key, type: t.type, depends_on: t.depends_on, status: t.status, attempts: t.attempts,
        max_attempts: t.max_attempts, duration_ms: t.duration_ms, next_run_at: t.next_run_at,
      }));
    }
    if (!parsed.def || !Array.isArray(parsed.def.tasks)) return [];
    return parsed.def.tasks
      .filter((t) => t && typeof t.id === 'string')
      .map((t) => ({ id: t.id, type: t.type, depends_on: Array.isArray(t.depends_on) ? t.depends_on : [],
        status: 'draft', max_attempts: t.max_attempts ?? 3 }));
  }, [run, parsed]);

  const flaggedIds = useMemo(() => {
    const set = new Set();
    (errors || []).forEach((e) => {
      const m = /\$\.tasks\[(\d+)\]/.exec(e.path || '');
      if (m && parsed.def?.tasks?.[Number(m[1])]) set.add(parsed.def.tasks[Number(m[1])].id);
      const c = /among tasks \[(.*)\]/.exec(e.message || '');
      if (c) c[1].split(',').map((s) => s.trim()).forEach((s) => set.add(s));
    });
    return set;
  }, [errors, parsed]);

  const { nodes, edges } = useMemo(() => {
    const pos = layoutTasks(graphTasks);
    const statusOf = Object.fromEntries(graphTasks.map((t) => [t.id, t.status]));
    const ns = graphTasks.map((t) => ({
      id: t.id, type: 'task', position: pos[t.id] || { x: 0, y: 0 }, draggable: false,
      data: { task: t, status: t.status, selected: selectedTask === t.id, flagged: flaggedIds.has(t.id), now },
    }));
    const es = [];
    graphTasks.forEach((t) => (t.depends_on || []).forEach((d) => {
      if (!statusOf[d]) return;
      const cyc = flaggedIds.has(t.id) && flaggedIds.has(d) && !run;
      const color = cyc ? '#ef4444' : EDGE_COLOR[statusOf[d]] || '#475569';
      const active = ['running', 'ready'].includes(t.status) && statusOf[d] === 'succeeded';
      es.push({
        id: `${d}->${t.id}`, source: d, target: t.id, type: 'smoothstep', animated: active || cyc,
        style: { stroke: color, strokeWidth: active ? 2.4 : 1.8, opacity: ['skipped', 'cancelled'].includes(t.status) ? 0.4 : 1 },
        markerEnd: { type: MarkerType.ArrowClosed, color, width: 16, height: 16 },
      });
    }));
    return { nodes: ns, edges: es };
  }, [graphTasks, selectedTask, flaggedIds, now, run]);

  const selected = run?.tasks.find((t) => t.task_key === selectedTask);
  const counts = useMemo(() => {
    const c = {};
    (run?.tasks || []).forEach((t) => { c[t.status] = (c[t.status] || 0) + 1; });
    return c;
  }, [run]);
  const runStart = run ? new Date(run.created_at).getTime() : 0;
  const liveDuration = run?.started_at
    ? (run.finished_at ? new Date(run.finished_at) : new Date(now)) - new Date(run.started_at) : null;
  const tmpl = TEMPLATES.find((t) => t.key === templateKey);

  return (
    <div className="app">
      <header className="header">
        <div className="brand">
          <svg width="30" height="30" viewBox="0 0 32 32" aria-hidden="true">
            <path d="M9 16 L23 7 M9 16 L23 25" stroke="#64748b" strokeWidth="2" />
            <circle cx="7" cy="16" r="4.5" fill="#10b981" /><circle cx="25" cy="7" r="4.5" fill="#f59e0b" />
            <circle cx="25" cy="25" r="4.5" fill="#3b82f6" />
          </svg>
          <div>
            <h1>DAG Orchestrator</h1>
            <div className="sub">Workflow engine · Java 21 · Spring Boot · PostgreSQL</div>
          </div>
          <span className="tag">single-node scheduler</span>
        </div>
        <div className="stats">
          <Stat label="executor" value={metrics ? `${metrics.in_flight_tasks}/${metrics.max_concurrent_tasks}` : '—'} hint="in-flight / max" />
          <Stat label="peak" value={metrics?.peak_concurrency ?? '—'} hint="max parallel tasks" />
          <Stat label="runs ok" value={metrics?.runs_by_status?.succeeded ?? 0} tone="ok" />
          <Stat label="runs failed" value={metrics?.runs_by_status?.failed ?? 0} tone="bad" />
          <div className={`health health-${health}`} data-testid="health"><span />{health === 'up' ? 'healthy' : health}</div>
        </div>
      </header>

      <main className="layout">
        {/* ---------- left: editor + runs ---------- */}
        <section className="panel left">
          <div className="panel-head"><h2>Definition</h2><span className="muted small">versioned · immutable</span></div>
          <div className="chips" role="tablist">
            {TEMPLATES.map((t) => (
              <button key={t.key} className={`chip ${templateKey === t.key ? 'chip-on' : ''} ${t.key === 'cycle' ? 'chip-warn' : ''}`}
                      onClick={() => loadTemplate(t)} data-testid={`template-${t.key}`}>{t.label}</button>
            ))}
          </div>
          {tmpl && <p className="hint">{tmpl.hint}</p>}
          <textarea className="editor" spellCheck={false} value={editorText} data-testid="editor"
                    onChange={(e) => { setEditorText(e.target.value); setErrors(null); }} />
          <div className="actions">
            <button className="btn" onClick={validate} disabled={busy} data-testid="btn-validate">Validate</button>
            <button className="btn" onClick={onSave} disabled={busy} data-testid="btn-save">Save version</button>
            <button className="btn btn-primary" onClick={onRun} disabled={busy} data-testid="btn-run">▶ Run</button>
          </div>
          {errors && errors.length > 0 && (
            <div className="errors" data-testid="validation-errors">
              <div className="errors-head">422 · {errors.length} validation error{errors.length > 1 ? 's' : ''}</div>
              {errors.map((e, i) => (
                <div key={i} className="error-row">
                  <code className="err-code">{e.code}</code>
                  <span className="err-path">{e.path}</span>
                  <div className="err-msg">{e.message}</div>
                </div>
              ))}
            </div>
          )}
          {errors && errors.length === 0 && <div className="ok-box" data-testid="validation-ok">✓ Valid DAG — no cycles, all dependencies resolved</div>}
          {notice && <div className="notice">{notice}</div>}

          <div className="panel-head runs-head"><h2>Recent runs</h2></div>
          <div className="run-list" data-testid="run-list">
            {runs.length === 0 && <div className="muted small">No runs yet.</div>}
            {runs.map((r) => (
              <button key={r.id} className={`run-row ${r.id === runId ? 'run-row-on' : ''}`} onClick={() => selectRun(r.id)}>
                <span className={`dot dot-${r.status}`} />
                <span className="run-name">{r.workflow_name}<span className="muted"> v{r.workflow_version}</span></span>
                <span className="run-dur">{fmtMs(r.duration_ms)}</span>
                <span className={`pill pill-${r.status}`}>{r.status}</span>
              </button>
            ))}
          </div>
        </section>

        {/* ---------- center: DAG canvas ---------- */}
        <section className="canvas-wrap">
          <div className="canvas-head">
            {run ? (
              <>
                <span className={`pill pill-lg pill-${run.status}`} data-testid="run-status">{run.status}</span>
                <span className="canvas-title">{run.workflow_name} <span className="muted">v{run.workflow_version}</span></span>
                <span className="muted mono small">run {run.id.slice(0, 8)}</span>
                <span className="canvas-dur">{fmtMs(liveDuration)}</span>
              </>
            ) : (
              <>
                <span className="pill pill-lg pill-draft">draft</span>
                <span className="canvas-title">{parsed.def?.name || 'untitled'}</span>
                <span className="muted small">preview of the editor definition — press Run to execute</span>
              </>
            )}
            <div className="legend">
              {['pending', 'ready', 'running', 'succeeded', 'failed', 'skipped', 'cancelled'].map((s) => (
                <span key={s}><i className={`dot dot-${s}`} />{s}</span>
              ))}
            </div>
          </div>
          {run && (
            <div className="progress" title="task status breakdown">
              {['succeeded', 'running', 'ready', 'pending', 'failed', 'skipped', 'cancelled'].map((s) => counts[s] ? (
                <div key={s} className={`seg seg-${s}`} style={{ flex: counts[s] }} />
              ) : null)}
            </div>
          )}
          <div className="canvas" data-testid="dag-canvas">
            <ReactFlow key={run ? run.id : `draft-${templateKey}-${graphTasks.length}`} nodes={nodes} edges={edges}
                       nodeTypes={nodeTypes} fitView fitViewOptions={{ padding: 0.25, maxZoom: 1.15 }}
                       nodesConnectable={false} elementsSelectable={false} proOptions={{ hideAttribution: true }}
                       onNodeClick={(_, n) => setSelectedTask(n.id)} minZoom={0.3}>
              <Background color="#1e293b" gap={22} size={1.4} />
              <Controls showInteractive={false} />
            </ReactFlow>
          </div>
        </section>

        {/* ---------- right: run details + timeline ---------- */}
        <section className="panel right">
          <div className="panel-head">
            <h2>Run details</h2>
            {run && !TERMINAL_RUN.has(run.status) && (
              <button className="btn btn-danger btn-sm" onClick={onCancel} data-testid="btn-cancel">Cancel run</button>
            )}
          </div>
          {!run && <div className="empty">Select a run, or press <b>Run</b> to execute the current definition.</div>}
          {run && (
            <div className="summary">
              <KV k="status" v={<span className={`pill pill-${run.status}`}>{run.status}</span>} />
              <KV k="duration" v={fmtMs(liveDuration)} />
              <KV k="tasks" v={`${counts.succeeded || 0}/${run.tasks.length} succeeded`} />
              <KV k="retries" v={events.filter((e) => e.type === 'task_retry_scheduled').length} />
            </div>
          )}
          {run && (
            <div className="task-card" data-testid="task-details">
              {!selected && <div className="muted small">Click a node to inspect attempts, timing, errors and result.</div>}
              {selected && (
                <>
                  <div className="task-card-head">
                    <span className="mono strong">{selected.task_key}</span>
                    <span className={`pill pill-${selected.status}`}>{selected.status}</span>
                  </div>
                  <div className="kv-grid">
                    <KV k="type" v={selected.type} />
                    <KV k="attempts" v={`${selected.attempts} / ${selected.max_attempts}`} />
                    <KV k="duration" v={fmtMs(selected.duration_ms)} />
                    <KV k="depends on" v={selected.depends_on.join(', ') || '—'} />
                  </div>
                  {selected.last_error && <div className="task-error"><b>last error</b>{selected.last_error}</div>}
                  {selected.result && <pre className="json">{pretty(selected.result)}</pre>}
                  <details><summary className="muted small">config</summary><pre className="json">{pretty(selected.config)}</pre></details>
                </>
              )}
            </div>
          )}
          {run && <div className="panel-head"><h2>Event timeline</h2><span className="muted small">{events.length} events</span></div>}
          {run && (
            <div className="timeline" ref={timelineRef} data-testid="timeline">
              {events.map((e) => (
                <div key={e.id} className={`ev ev-${e.type}`} onClick={() => e.task_key && setSelectedTask(e.task_key)}>
                  <span className="ev-t mono">+{((new Date(e.created_at).getTime() - runStart) / 1000).toFixed(2)}s</span>
                  <span className="ev-dot" />
                  <div className="ev-body">
                    <div><span className="ev-type">{e.type.replaceAll('_', ' ')}</span>{e.task_key && <span className="mono ev-task"> {e.task_key}</span>}</div>
                    <EventDetail e={e} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
      </main>
    </div>
  );
}

function EventDetail({ e }) {
  const d = e.detail || {};
  const parts = [];
  if (d.attempt != null) parts.push(`attempt ${d.attempt}${d.max_attempts ? `/${d.max_attempts}` : ''}`);
  if (d.duration_ms != null) parts.push(fmtMs(d.duration_ms));
  if (d.backoff_ms != null) parts.push(`retry in ${fmtMs(d.backoff_ms)}`);
  if (d.action) parts.push(`action: ${d.action}`);
  if (d.reason) parts.push(d.reason);
  if (d.error) parts.push(d.error);
  if (d.outcome) parts.push(`late ${d.outcome} refused (CAS)`);
  if (!parts.length) return null;
  return <div className="ev-detail">{parts.join(' · ')}</div>;
}

function Stat({ label, value, hint, tone }) {
  return (
    <div className={`stat ${tone ? `stat-${tone}` : ''}`} title={hint}>
      <div className="stat-v">{value}</div>
      <div className="stat-l">{label}</div>
    </div>
  );
}

function KV({ k, v }) {
  return <div className="kv"><span className="kv-k">{k}</span><span className="kv-v">{v}</span></div>;
}
