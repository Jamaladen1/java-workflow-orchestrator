// Demo templates. The first three are also seeded server-side (src/main/resources/seed).
export const TEMPLATES = [
  {
    key: 'parallel-join',
    label: 'parallel-join',
    hint: 'Two branches run at the same time; join waits for both.',
    definition: {
      name: 'parallel-join',
      tasks: [
        { id: 'extract', type: 'delay', config: { duration_ms: 800 }, depends_on: [] },
        { id: 'enrich_users', type: 'delay', config: { duration_ms: 2500 }, depends_on: ['extract'] },
        { id: 'enrich_orders', type: 'http', config: { status: 200, delay_ms: 2000 }, depends_on: ['extract'] },
        { id: 'join', type: 'math', config: { operation: 'sum', values: [2, 3] }, depends_on: ['enrich_users', 'enrich_orders'] },
        { id: 'publish', type: 'transform', config: { operation: 'upper', input: 'report ready' }, depends_on: ['join'] },
      ],
    },
  },
  {
    key: 'retry-demo',
    label: 'retry-demo',
    hint: 'flaky_charge fails twice, retries with 2s → 4s backoff, then succeeds.',
    definition: {
      name: 'retry-demo',
      tasks: [
        { id: 'validate', type: 'delay', config: { duration_ms: 500 }, depends_on: [] },
        { id: 'flaky_charge', type: 'delay', config: { duration_ms: 700, fail_until_attempt: 3 }, max_attempts: 3, depends_on: ['validate'] },
        { id: 'audit_log', type: 'delay', config: { duration_ms: 1500 }, depends_on: ['validate'] },
        { id: 'notify', type: 'transform', config: { operation: 'upper', input: 'payment captured' }, depends_on: ['flaky_charge', 'audit_log'] },
      ],
    },
  },
  {
    key: 'fail-fast',
    label: 'fail-fast',
    hint: 'always_fails exhausts its attempts; downstream tasks are skipped, the in-flight branch finishes.',
    definition: {
      name: 'fail-fast',
      tasks: [
        { id: 'start', type: 'delay', config: { duration_ms: 300 }, depends_on: [] },
        { id: 'always_fails', type: 'delay', config: { duration_ms: 600, fail_until_attempt: 10 }, max_attempts: 2, depends_on: ['start'] },
        { id: 'slow_branch', type: 'delay', config: { duration_ms: 5000 }, depends_on: ['start'] },
        { id: 'join', type: 'math', config: { operation: 'sum', values: [1, 2] }, depends_on: ['always_fails', 'slow_branch'] },
        { id: 'report', type: 'transform', config: { operation: 'upper', input: 'never runs' }, depends_on: ['join'] },
      ],
    },
  },
  {
    key: 'cycle',
    label: 'cycle (invalid)',
    hint: 'a → b → c → a is rejected with a structured 422.',
    definition: {
      name: 'cycle-invalid',
      tasks: [
        { id: 'ingest', type: 'delay', config: { duration_ms: 200 }, depends_on: [] },
        { id: 'a', type: 'delay', config: { duration_ms: 200 }, depends_on: ['ingest', 'c'] },
        { id: 'b', type: 'math', config: { operation: 'sum', values: [1, 2] }, depends_on: ['a'] },
        { id: 'c', type: 'transform', config: { operation: 'upper', input: 'x' }, depends_on: ['b'] },
      ],
    },
  },
];
