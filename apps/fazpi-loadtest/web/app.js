const form = document.querySelector('#config-form');
const numberFields = new Set([
  'virtual_duration_minutes','test_duration_seconds','publisher_concurrency','payload_bytes',
  'worker_replicas','worker_concurrency','processing_millis','processing_jitter_millis',
  'queue_wait_sla_ms',
  'max_attempts','retry_initial_millis','retry_maximum_millis',
  'completed_retention_minutes','failed_retention_minutes'
]);
const decimalFields = new Set(['publish_rate','transient_failure_percent','permanent_failure_percent','duplicate_publish_percent']);
let defaults = null;
let state = null;

const fazpiPeakPreset = [
  { account: 'Casur', agent: 'Kata en línea', people_per_hour: 100000, messages_per_person: 1 },
  { account: 'Casur', agent: 'Agente 2 (completar tasa)', people_per_hour: 0, messages_per_person: 1 },
  { account: 'Pascual', agent: 'Agente principal', people_per_hour: 500, messages_per_person: 1 },
  { account: 'Cuenta 3', agent: 'Agente principal', people_per_hour: 100, messages_per_person: 1 },
  { account: 'Cuenta 4', agent: 'Agente principal', people_per_hour: 100, messages_per_person: 1 }
];
const curvePresets = {
  payment: [0.35, 0.45, 0.65, 1, 1.55, 2.5, 2, 1.35, 0.9, 0.6, 0.4, 0.25],
  uniform: Array(12).fill(1),
  double: [0.4, 0.65, 1.2, 2.3, 1.5, 0.7, 0.45, 0.8, 1.9, 1.35, 0.7, 0.35]
};

const $ = id => document.getElementById(id);
const fmt = value => new Intl.NumberFormat('es-CO', { maximumFractionDigits: 1 }).format(Number(value || 0));
const duration = seconds => seconds < 60 ? `${fmt(seconds)}s` : `${Math.floor(seconds/60)}m ${Math.floor(seconds%60)}s`;

async function api(path, options = {}) {
  const response = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...options });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
  return body;
}

function setForm(config) {
  Object.entries(config).forEach(([name, value]) => {
    if (name !== 'traffic_profiles' && name !== 'arrival_weights' && form.elements[name]) form.elements[name].value = value;
  });
  setProfiles(config.traffic_profiles || []);
  setArrivalWeights(config.arrival_weights || curvePresets.payment);
  updatePreview();
}

function readForm() {
  const result = {};
  new FormData(form).forEach((value, key) => {
    result[key] = numberFields.has(key) ? Number.parseInt(value, 10) : decimalFields.has(key) ? Number.parseFloat(value) : value;
  });
  result.traffic_profiles = readProfiles();
  result.arrival_weights = readArrivalWeights();
  return result;
}

function setProfiles(profiles) {
  $('traffic-profiles').innerHTML = '';
  profiles.forEach(addProfileRow);
  if (!profiles.length) addProfileRow({ account: '', agent: '', people_per_hour: 0, messages_per_person: 1 });
}

function addProfileRow(profile = { account: '', agent: '', people_per_hour: 0, messages_per_person: 1 }) {
  const row = document.createElement('div');
  row.className = 'traffic-profile';
  row.innerHTML = `
    <label>Cuenta<input class="profile-account" type="text" value="${escapeHTML(profile.account)}" required></label>
    <label>Agente<input class="profile-agent" type="text" value="${escapeHTML(profile.agent)}" required></label>
    <label>Personas/h<input class="profile-rate" type="number" min="0" max="100000000" value="${Number(profile.people_per_hour || 0)}" required></label>
    <label>Msg/persona<input class="profile-messages" type="number" min="1" max="10000" value="${Number(profile.messages_per_person || 1)}" required></label>
    <button type="button" class="remove-profile" title="Quitar agente">×</button>`;
  row.querySelector('.remove-profile').addEventListener('click', () => {
    row.remove();
    if (!$('traffic-profiles').children.length) addProfileRow();
    updatePreview();
  });
  row.querySelectorAll('input').forEach(input => input.addEventListener('input', updatePreview));
  $('traffic-profiles').appendChild(row);
}

function readProfiles() {
  return [...document.querySelectorAll('.traffic-profile')].map(row => ({
    account: row.querySelector('.profile-account').value.trim(),
    agent: row.querySelector('.profile-agent').value.trim(),
    people_per_hour: Number.parseInt(row.querySelector('.profile-rate').value, 10) || 0,
    messages_per_person: Number.parseInt(row.querySelector('.profile-messages').value, 10) || 1
  }));
}

function setArrivalWeights(weights) {
  $('arrival-weights').innerHTML = weights.map((weight, index) => {
    const start = String(index * 5).padStart(2, '0');
    const end = String((index + 1) * 5).padStart(2, '0');
    return `<label>${start}-${end}<input class="arrival-weight" type="number" min="0" max="100" step="0.05" value="${Number(weight)}"></label>`;
  }).join('');
  document.querySelectorAll('.arrival-weight').forEach(input => input.addEventListener('input', () => { renderCurve(); updatePreview(); }));
  renderCurve();
}

function readArrivalWeights() {
  return [...document.querySelectorAll('.arrival-weight')].map(input => Number.parseFloat(input.value) || 0);
}

function renderCurve() {
  const weights = readArrivalWeights();
  const maximum = Math.max(1, ...weights);
  $('curve-chart').innerHTML = weights.map((weight, index) => `<div class="curve-column" title="Min ${index*5}-${(index+1)*5}: factor ${weight}"><i style="height:${Math.max(3, weight/maximum*100)}%"></i><small>${index*5}</small></div>`).join('');
}

function updatePreview() {
  const c = readForm();
  const peoplePerHour = c.traffic_profiles.reduce((total, profile) => total + profile.people_per_hour, 0);
  const expected = c.traffic_profiles.reduce((total, profile) => total + Math.ceil(profile.people_per_hour * (c.virtual_duration_minutes || 0) / 60) * profile.messages_per_person, 0);
  $('people-preview').textContent = fmt(peoplePerHour);
  $('expected-preview').textContent = fmt(expected);
  $('slots-preview').textContent = fmt((c.worker_replicas || 0) * (c.worker_concurrency || 0));
}

form.addEventListener('input', updatePreview);
$('reset-config').addEventListener('click', () => setForm({ ...defaults, queue: `fazpi-sim-${new Date().toTimeString().slice(0,8).replaceAll(':','')}` }));
$('fazpi-preset').addEventListener('click', () => { setProfiles(fazpiPeakPreset); updatePreview(); });
$('add-profile').addEventListener('click', () => { addProfileRow(); updatePreview(); });
document.querySelectorAll('[data-curve]').forEach(button => button.addEventListener('click', () => { setArrivalWeights(curvePresets[button.dataset.curve]); updatePreview(); }));
form.addEventListener('submit', async event => {
  event.preventDefault();
  $('form-error').hidden = true;
  try {
    state = await api('/api/start', { method: 'POST', body: JSON.stringify(readForm()) });
    render(state);
  } catch (error) {
    $('form-error').textContent = error.message;
    $('form-error').hidden = false;
  }
});
$('stop').addEventListener('click', () => control('/api/stop'));
$('pause').addEventListener('click', () => control('/api/pause'));
$('resume').addEventListener('click', () => control('/api/resume'));

async function control(path) {
  try { await api(path, { method: 'POST' }); await refresh(); }
  catch (error) { $('form-error').textContent = error.message; $('form-error').hidden = false; }
}

function render(snapshot) {
  const active = ['starting','running','stopping'].includes(snapshot.status);
  const c = snapshot.counters || {};
  const q = snapshot.queue_stats || {};
  const totals = snapshot.queue_totals_for_run || {};
  const terminal = Number(c.completed || 0) + Number(c.permanent_failures || 0);
  const expected = Number(snapshot.expected_unique || 0);

  $('status').textContent = snapshot.status.toUpperCase();
  $('status').className = `status ${snapshot.status}`;
  $('run-title').textContent = snapshot.run_id ? `${snapshot.config.queue} · ${snapshot.run_id}` : 'Esperando un escenario';
  const profiles = snapshot.profiles || [];
  const accounts = new Set(profiles.map(profile => profile.account)).size;
  const conversations = profiles.reduce((total, profile) => total + Number(profile.expected_conversations || 0), 0);
  $('run-subtitle').textContent = snapshot.error || (snapshot.run_id ? `${fmt(accounts)} cuentas · ${fmt(profiles.length)} agentes · ${fmt(conversations)} conversaciones · ${fmt(expected)} mensajes` : 'Configura la simulación y presiona “Iniciar escenario”.');

  $('start').disabled = active;
  $('stop').disabled = !active;
  $('pause').disabled = !active || q.paused;
  $('resume').disabled = !active || !q.paused;
  [...form.elements].forEach(element => { if (element.name) element.disabled = active; });

  $('m-published').textContent = fmt(c.published_unique);
  $('m-published-sub').textContent = `de ${fmt(expected)} · ${fmt(c.duplicates_confirmed)} duplicados bloqueados`;
  $('m-terminal').textContent = fmt(terminal);
  $('m-terminal-sub').textContent = `${fmt(c.completed)} completados · ${fmt(c.permanent_failures)} permanentes`;
  $('m-waiting').textContent = fmt(q.waiting);
  $('m-active').textContent = fmt(q.active);
  $('m-active-sub').textContent = `${fmt(q.worker_concurrency)} slots informados`;
  $('m-pub-rate').textContent = `${fmt(q.rates_per_second?.published)}/s`;
  $('m-complete-rate').textContent = `${fmt(q.rates_per_second?.completed)}/s`;
  $('m-wait').textContent = `${fmt(q.average_queue_wait_ms)} ms`;
  $('m-elapsed').textContent = duration(snapshot.elapsed_seconds || 0);
  $('m-progress').textContent = `${expected ? fmt(terminal / expected * 100) : 0}% procesado`;

  const capacity = snapshot.capacity || {};
  $('c-target').textContent = `${fmt(capacity.target_messages_per_second)} msg/s`;
  $('c-capacity').textContent = `${fmt(capacity.estimated_worker_capacity_per_second)} msg/s`;
  $('c-required').textContent = fmt(capacity.estimated_required_slots);
  $('c-configured').textContent = `${fmt(capacity.configured_slots)} configurados`;
  $('c-margin').textContent = `${fmt(capacity.estimated_capacity_margin)}×`;
  $('capacity-verdict').className = `capacity-card verdict ${Number(capacity.estimated_capacity_margin || 0) >= 1 ? 'healthy' : 'risk'}`;

  $('t-reserved').textContent = fmt(totals.reserved);
  $('t-completed').textContent = fmt(totals.completed);
  $('t-failed').textContent = fmt(totals.failed);
  $('t-retried').textContent = fmt(totals.retried);
  $('t-recovered').textContent = fmt(totals.recovered);
  $('t-stalled').textContent = fmt(totals.stalled);

  renderValidations(snapshot.validations || []);
  renderProfiles(profiles);
  renderWorkers(q.workers || []);
  renderEvents(snapshot.events || []);
  renderChart($('pressure-chart'), snapshot.samples || [], [
    { key: 'waiting', color: '#4aa3ff' }, { key: 'active', color: '#f2c94c' }
  ]);
  renderChart($('rate-chart'), snapshot.samples || [], [
    { key: 'published_rate', color: '#36d9c4' }, { key: 'completed_rate', color: '#55d98b' }
  ]);
}

function renderProfiles(profiles) {
  const sla = Number(state?.config?.queue_wait_sla_ms || 0);
  $('profile-results').innerHTML = profiles.length ? profiles.map(profile => {
    const terminal = Number(profile.completed || 0) + Number(profile.permanent_failures || 0);
    const violatesSLA = sla > 0 && Number(profile.maximum_queue_wait_ms || 0) > sla;
    return `<tr>
      <td>${escapeHTML(profile.account)}</td><td><strong>${escapeHTML(profile.agent)}</strong></td>
      <td>${fmt(profile.people_per_hour)}</td><td>${fmt(profile.messages_per_person)}</td><td>${fmt(profile.published)} / ${fmt(profile.expected_messages)}</td>
      <td>${fmt(terminal)}</td><td class="${profile.current_backlog > 0 ? 'warn-value' : ''}">${fmt(profile.current_backlog)}</td>
      <td>${fmt(profile.peak_backlog)}</td><td>${fmt(profile.average_queue_wait_ms)} ms</td><td class="${violatesSLA ? 'danger-value' : ''}">${fmt(profile.maximum_queue_wait_ms)} ms</td>
    </tr>`;
  }).join('') : '<tr><td colspan="10" class="empty">Sin perfiles de tráfico.</td></tr>';
}

function renderValidations(items) {
  $('validations').innerHTML = items.length ? items.map(item => {
    const icon = item.status === 'passed' ? '✓' : item.status === 'failed' ? '!' : '…';
    return `<div class="validation ${item.status}"><span class="check">${icon}</span><div><strong>${escapeHTML(item.name)}</strong><small>${escapeHTML(item.explanation)}</small></div></div>`;
  }).join('') : '<p class="empty">Las validaciones aparecerán al iniciar.</p>';
}

function renderWorkers(workers) {
  $('worker-summary').textContent = `${workers.length} réplicas`;
  $('workers').innerHTML = workers.length ? workers.map(worker => `<tr><td>${escapeHTML(worker.instance)}</td><td>${fmt(worker.concurrency)}</td><td>${new Date(worker.last_heartbeat_at).toLocaleTimeString()}</td></tr>`).join('') : '<tr><td colspan="3" class="empty">Sin workers registrados</td></tr>';
}

function renderEvents(events) {
  $('events').innerHTML = events.length ? events.map(event => `<div class="event"><time>${new Date(event.at).toLocaleTimeString()}</time><span class="type">${escapeHTML(event.type)}</span><span class="job" title="${escapeHTML(event.job_id)}">${escapeHTML(event.group || event.job_id)}</span></div>`).join('') : '<p class="empty">Sin eventos</p>';
}

function renderChart(svg, samples, series) {
  const width = 720, height = 210, left = 42, right = 8, top = 10, bottom = 25;
  const data = samples.slice(-90);
  const max = Math.max(1, ...data.flatMap(point => series.map(line => Number(point[line.key] || 0))));
  let markup = '';
  for (let i = 0; i <= 4; i++) {
    const y = top + (height - top - bottom) * i / 4;
    markup += `<line class="chart-grid-line" x1="${left}" y1="${y}" x2="${width-right}" y2="${y}"/><text class="chart-label" x="0" y="${y+4}">${fmt(max * (4-i)/4)}</text>`;
  }
  if (data.length > 1) {
    series.forEach(line => {
      const points = data.map((point, index) => {
        const x = left + index * (width-left-right) / (data.length-1);
        const y = top + (height-top-bottom) * (1 - Number(point[line.key] || 0) / max);
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      }).join(' ');
      markup += `<polyline class="chart-line" stroke="${line.color}" points="${points}"/>`;
    });
  } else {
    markup += `<text class="chart-label" x="${width/2-70}" y="${height/2}">Esperando muestras...</text>`;
  }
  svg.innerHTML = markup;
}

function escapeHTML(value) {
  return String(value ?? '').replace(/[&<>'"]/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));
}

async function refresh() {
  try {
    state = await api('/api/state');
    $('connection').className = 'connection online';
    $('connection').innerHTML = '<i></i> Backend conectado';
    render(state);
  } catch (error) {
    $('connection').className = 'connection offline';
    $('connection').innerHTML = '<i></i> Sin conexión';
  }
}

async function initialize() {
  try {
    defaults = await api('/api/defaults');
    setForm(defaults);
  } catch (error) {
    $('form-error').textContent = error.message;
    $('form-error').hidden = false;
  }
  await refresh();
  setInterval(refresh, 1000);
}

initialize();
