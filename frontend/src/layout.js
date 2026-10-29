// Column = longest path from a root (topological depth). Bounded iterations so cyclic drafts still render.
export function layoutTasks(tasks) {
  const ids = new Set(tasks.map((t) => t.id));
  const depth = Object.fromEntries(tasks.map((t) => [t.id, 0]));
  for (let i = 0; i < tasks.length; i++) {
    let changed = false;
    for (const t of tasks) {
      for (const d of t.depends_on || []) {
        if (!ids.has(d) || d === t.id) continue;
        const nd = Math.min(depth[d] + 1, tasks.length);
        if (nd > depth[t.id]) { depth[t.id] = nd; changed = true; }
      }
    }
    if (!changed) break;
  }
  const columns = {};
  tasks.forEach((t) => { (columns[depth[t.id]] ||= []).push(t.id); });
  const maxRows = Math.max(1, ...Object.values(columns).map((c) => c.length));
  const COL = 270, ROW = 130;
  const pos = {};
  Object.entries(columns).forEach(([col, list]) => {
    const offset = ((maxRows - list.length) * ROW) / 2;
    list.forEach((id, i) => { pos[id] = { x: Number(col) * COL, y: offset + i * ROW }; });
  });
  return pos;
}
