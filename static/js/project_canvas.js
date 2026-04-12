(function () {
  const host = document.getElementById('canvas-host');
  const nodeLayer = document.getElementById('canvas-node-layer');
  const edgeLayer = document.getElementById('canvas-edge-layer');
  const edgeSvg = edgeLayer?.parentElement;
  const warningEl = document.getElementById('canvas-warning');
  const resetBtn = document.getElementById('canvas-reset-view');
  if (!host || !nodeLayer || !edgeLayer || !edgeSvg) return;

  const projectName = host.dataset.projectName;
  const canvasFileName = host.dataset.canvasFileName;
  let scale = 1;
  let offsetX = 0;
  let offsetY = 0;
  let isDragging = false;
  let dragStart = { x: 0, y: 0 };
  const sideVectors = {
    top: { x: 0, y: -1 },
    right: { x: 1, y: 0 },
    bottom: { x: 0, y: 1 },
    left: { x: -1, y: 0 }
  };

  const colorPalette = {
    '1': { border: '#ff6b6b', background: 'rgba(255, 107, 107, 0.12)' },
    '2': { border: '#f4a261', background: 'rgba(244, 162, 97, 0.12)' },
    '3': { border: '#ffd166', background: 'rgba(255, 209, 102, 0.12)' },
    '4': { border: '#7bd389', background: 'rgba(123, 211, 137, 0.12)' },
    '5': { border: '#47c6d6', background: 'rgba(71, 198, 214, 0.14)' },
    '6': { border: '#6ea8fe', background: 'rgba(110, 168, 254, 0.14)' }
  };

  const applyTransform = () => {
    const transform = `translate(${offsetX}px, ${offsetY}px) scale(${scale})`;
    nodeLayer.style.transform = transform;
    edgeLayer.parentElement.style.transform = transform;
  };

  const anchor = (node, side) => {
    const x = node.x;
    const y = node.y;
    const w = node.width;
    const h = node.height;
    if (side === 'top') return { x: x + w / 2, y };
    if (side === 'bottom') return { x: x + w / 2, y: y + h };
    if (side === 'left') return { x, y: y + h / 2 };
    return { x: x + w, y: y + h / 2 };
  };

  const buildEdgePath = (fromPoint, toPoint, fromSide, toSide) => {
    const fromVector = sideVectors[fromSide] || sideVectors.right;
    const toVector = sideVectors[toSide] || sideVectors.left;
    const dx = toPoint.x - fromPoint.x;
    const dy = toPoint.y - fromPoint.y;
    const distance = Math.hypot(dx, dy);
    const handle = Math.max(42, Math.min(140, distance * 0.45));
    const c1x = fromPoint.x + fromVector.x * handle;
    const c1y = fromPoint.y + fromVector.y * handle;
    const c2x = toPoint.x + toVector.x * handle;
    const c2y = toPoint.y + toVector.y * handle;
    return `M ${fromPoint.x} ${fromPoint.y} C ${c1x} ${c1y}, ${c2x} ${c2y}, ${toPoint.x} ${toPoint.y}`;
  };

  const fitToScreen = (bounds) => {
    const padding = 80;
    const width = Math.max(1, bounds.width + padding);
    const height = Math.max(1, bounds.height + padding);
    const hostRect = host.getBoundingClientRect();
    const sx = hostRect.width / width;
    const sy = hostRect.height / height;
    scale = Math.min(1, sx, sy);
    offsetX = (hostRect.width - bounds.width * scale) / 2;
    offsetY = (hostRect.height - bounds.height * scale) / 2;
    applyTransform();
  };

  const render = async () => {
    const response = await fetch(
      `/api/projects/${encodeURIComponent(projectName)}/canvas/${encodeURIComponent(canvasFileName || '')}`,
      { headers: { Accept: 'application/json' } }
    );
    const payload = await response.json();
    if (!response.ok) {
      warningEl.textContent = payload.error || 'Failed to load canvas.';
      return;
    }

    const nodes = payload.nodes || [];
    const edges = payload.edges || [];
    const bounds = payload.bounds || { min_x: 0, min_y: 0, max_x: 1000, max_y: 800, width: 1000, height: 800 };

    nodeLayer.innerHTML = '';
    edgeLayer.innerHTML = '';

    nodeLayer.style.width = `${bounds.width}px`;
    nodeLayer.style.height = `${bounds.height}px`;
    edgeSvg.style.width = `${bounds.width}px`;
    edgeSvg.style.height = `${bounds.height}px`;
    edgeSvg.setAttribute('viewBox', `0 0 ${bounds.width} ${bounds.height}`);

    const byId = {};
    nodes.forEach((node) => {
      byId[node.id] = node;
      const el = document.createElement('div');
      const palette = colorPalette[String(node.raw?.color || '').trim()] || null;
      el.className = 'km-canvas-node';
      el.style.left = `${node.x - bounds.min_x}px`;
      el.style.top = `${node.y - bounds.min_y}px`;
      el.style.width = `${node.width}px`;
      el.style.height = `${node.height}px`;
      if (palette) {
        el.style.borderColor = palette.border;
        el.style.backgroundColor = palette.background;
        el.style.boxShadow = `0 0 0 1px ${palette.border} inset`;
      }
      el.innerHTML = `<div class="km-canvas-node-content km-markdown-preview">${node.html || ''}</div>`;
      nodeLayer.appendChild(el);
    });

    edges.forEach((edge) => {
      const from = byId[edge.fromNode];
      const to = byId[edge.toNode];
      if (!from || !to) return;
      const p1 = anchor({ ...from, x: from.x - bounds.min_x, y: from.y - bounds.min_y }, edge.fromSide || 'right');
      const p2 = anchor({ ...to, x: to.x - bounds.min_x, y: to.y - bounds.min_y }, edge.toSide || 'left');
      const d = buildEdgePath(p1, p2, edge.fromSide || 'right', edge.toSide || 'left');
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('d', d);
      path.setAttribute('class', 'km-canvas-edge');
      path.setAttribute('marker-end', 'url(#km-arrowhead)');
      edgeLayer.appendChild(path);
    });

    fitToScreen(bounds);
    warningEl.textContent = (payload.warnings || []).join(' ');
  };

  host.addEventListener('wheel', (event) => {
    event.preventDefault();
    const delta = event.deltaY > 0 ? 0.9 : 1.1;
    scale = Math.max(0.2, Math.min(2.2, scale * delta));
    applyTransform();
  }, { passive: false });

  host.addEventListener('mousedown', (event) => {
    isDragging = true;
    dragStart = { x: event.clientX - offsetX, y: event.clientY - offsetY };
  });
  window.addEventListener('mousemove', (event) => {
    if (!isDragging) return;
    offsetX = event.clientX - dragStart.x;
    offsetY = event.clientY - dragStart.y;
    applyTransform();
  });
  window.addEventListener('mouseup', () => {
    isDragging = false;
  });

  resetBtn?.addEventListener('click', () => window.location.reload());

  render();
})();
