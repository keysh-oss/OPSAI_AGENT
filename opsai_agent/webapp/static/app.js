const run = document.getElementById('run');
const clear = document.getElementById('clear');
const queryEl = document.getElementById('query');
const cypherEl = document.getElementById('cypher');
const recordsEl = document.getElementById('records');

let cy = cytoscape({ container: document.getElementById('cy'), elements: [], style: [
  { selector: 'node', style: { 'label': 'data(label)', 'background-color': '#1976d2', 'color':'#fff', 'text-valign':'center', 'text-halign':'center' } },
  { selector: 'edge', style: { 'label': 'data(label)', 'curve-style': 'bezier', 'target-arrow-shape': 'triangle' } }
]});

run.addEventListener('click', async () => {
  const q = queryEl.value.trim();
  if (!q) return;
  cypherEl.textContent = 'Translating...';
  recordsEl.textContent = '';
  try {
    const res = await fetch('/api/query', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ query: q }) });
    const j = await res.json();
    if (j.error) {
      cypherEl.textContent = 'Error: ' + j.error;
      return;
    }
    cypherEl.textContent = j.cypher || '(no cypher returned)';
    recordsEl.textContent = JSON.stringify(j.results.records, null, 2);

    // build graph elements from records if possible
    const nodes = {};
    const edges = [];
    for (const r of (j.results.records || [])) {
      for (const [k, v] of Object.entries(r)) {
        if (Array.isArray(v)) {
          for (const item of v) {
            if (item && item.type === 'node') {
              nodes[item.id] = { data: { id: String(item.id), label: item.properties.id || item.labels?.join(',') || ('node:'+String(item.id)) } };
            }
            if (item && item.type === 'relationship') {
              edges.push({ data: { id: 'e'+edges.length, source: String(item.start_node_id), target: String(item.end_node_id), label: item.type_name } });
            }
          }
        } else if (v && v.type === 'node') {
          nodes[v.id] = { data: { id: String(v.id), label: v.properties.id || v.labels?.join(',') || ('node:'+String(v.id)) } };
        } else if (v && v.type === 'relationship') {
          edges.push({ data: { id: 'e'+edges.length, source: String(v.start_node_id), target: String(v.end_node_id), label: v.type_name } });
        }
      }
    }

    cy.elements().remove();
    cy.add(Object.values(nodes));
    cy.add(edges);
    cy.layout({ name: 'cose' }).run();

  } catch (err) {
    cypherEl.textContent = 'Error: ' + err.message;
  }
});

clear.addEventListener('click', () => {
  queryEl.value = '';
  cy.elements().remove();
  cypherEl.textContent = '(results will appear here)';
  recordsEl.textContent = '(records)';
});
