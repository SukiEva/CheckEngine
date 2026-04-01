export function createCanvasRenderer(options) {
  const {
    dropZone,
    fixedTypes,
    state,
    laneDefs,
    renderMaterialIcons,
    escapeHtml,
    formatOutputsText,
    getOutputFields,
    formatVariableWhenSummary,
    normalizeValueToInput,
    getSortedSteps,
    addNodeByType,
    selectNode,
    deleteNodeById,
    moveNodeInLane,
  } = options;

  function buildNodeActionButtons(node) {
    const canMove = node.type === 'step';
    if (!canMove) {
      return '';
    }
    return `
      <button class="node-action-btn" type="button" data-node-action="move-up" title="上移节点" aria-label="上移节点"><span class="ep-icon">arrow_upward</span></button>
      <button class="node-action-btn" type="button" data-node-action="move-down" title="下移节点" aria-label="下移节点"><span class="ep-icon">arrow_downward</span></button>
    `;
  }

  class ExecNodeCard extends HTMLElement {
    set nodeData(payload) {
      this.payload = payload;
      this.render();
    }

    connectedCallback() {
      this.addEventListener('click', this.onClick);
    }

    disconnectedCallback() {
      this.removeEventListener('click', this.onClick);
    }

    onClick = (event) => {
      if (!this.payload || !this.payload.node) {
        return;
      }
      const actionButton = event.target.closest('[data-node-action]');
      if (!actionButton) {
        this.dispatchEvent(new CustomEvent('node-select', {
          detail: { nodeId: this.payload.node.id },
          bubbles: true,
        }));
        return;
      }
      this.dispatchEvent(new CustomEvent('node-action', {
        detail: {
          nodeId: this.payload.node.id,
          action: actionButton.dataset.nodeAction || '',
        },
        bubbles: true,
      }));
    };

    render() {
      if (!this.payload || !this.payload.node) {
        return;
      }
      const { node, index, selectedId } = this.payload;
      const nodeRank = node.type === 'step' ? `S${node.stepOrder || index + 1}` : `#${index + 1}`;
      const nodeTitle = node.type === 'on_fail' ? node.type : (node.title || '(未命名节点)');
      const stepExtraInfo = node.type === 'step'
        ? `
          <div class="muted">ds: ${escapeHtml(node.datasource || '-')} / mode: ${escapeHtml(node.resultMode || '-')}</div>
          <div class="muted">outputs: ${escapeHtml(formatOutputsText(getOutputFields(node)) || '无')}</div>
        `
        : '';
      const onFailExtraInfo = node.type === 'on_fail'
        ? `<div class="muted">decision: ${escapeHtml(node.decision || '无')}</div>`
        : '';
      const variableExtraInfo = node.type === 'variable'
        ? `
          <div class="muted">when: ${escapeHtml(formatVariableWhenSummary(node.variableWhenRows))}</div>
          <div class="muted">default: ${escapeHtml(normalizeValueToInput(node.variableDefault) || '(空)')}</div>
        `
        : '';
      const showDeleteAction = node.type !== 'on_fail';

      this.className = `flow-node${selectedId === node.id ? ' selected' : ''}`;
      this.dataset.nodeId = node.id;
      this.innerHTML = `
        <div class="node-head">
          <span>${node.type}</span>
          <span class="node-head-actions">
            <span class="node-rank-chip">${nodeRank}</span>
            <span class="node-action-group">
              ${buildNodeActionButtons(node)}
              ${showDeleteAction
                ? '<button class="node-action-btn node-action-danger" type="button" data-node-action="delete" title="删除节点" aria-label="删除节点"><span class="ep-icon">delete_outline</span></button>'
                : ''}
            </span>
          </span>
        </div>
        <div class="node-body">
          <strong>${escapeHtml(nodeTitle)}</strong>
          ${node.type === 'step' && node.description
            ? `<div class="muted">description: ${escapeHtml(node.description)}</div>`
            : ''}
          ${stepExtraInfo}
          ${onFailExtraInfo}
          ${variableExtraInfo}
        </div>
      `;
      renderMaterialIcons(this);
    }
  }

  class ExecFlowLane extends HTMLElement {
    set laneData(payload) {
      this.payload = payload;
      this.render();
    }

    connectedCallback() {
      this.addEventListener('click', this.handleClick);
    }

    disconnectedCallback() {
      this.removeEventListener('click', this.handleClick);
    }

    handleClick = (event) => {
      const addButton = event.target.closest('[data-add-type]');
      if (!addButton) {
        return;
      }
      this.dispatchEvent(new CustomEvent('lane-add-node', {
        detail: { laneType: addButton.dataset.addType || '' },
        bubbles: true,
      }));
    };

    render() {
      if (!this.payload || !this.payload.lane) {
        return;
      }
      const { lane, laneNodes, selectedId } = this.payload;
      const laneHasFixedNode = fixedTypes.has(lane.type) && laneNodes.length > 0;
      const addButtonHtml = laneHasFixedNode
        ? ''
        : `<button class="el-button el-button--primary is-plain is-circle el-button--small" data-add-type="${lane.type}" type="button" title="新增 ${lane.type} 节点" aria-label="新增 ${lane.type} 节点"><span class="ep-icon">add</span></button>`;

      this.className = 'flow-lane';
      this.innerHTML = `
        <div class="flow-lane-head">
          <strong>${lane.label}</strong>
          ${addButtonHtml}
        </div>
        <div class="flow-lane-body" data-lane-body></div>
      `;

      const laneBody = this.querySelector('[data-lane-body]');
      if (!laneBody) {
        return;
      }
      if (!laneNodes.length) {
        laneBody.innerHTML = '<div class="lane-empty">暂无节点，点击“+ 添加”创建。</div>';
        renderMaterialIcons(this);
        return;
      }

      laneNodes.forEach((node, index) => {
        const nodeCard = document.createElement('exec-node-card');
        nodeCard.nodeData = { node, index, selectedId };
        laneBody.appendChild(nodeCard);
      });
      renderMaterialIcons(this);
    }
  }

  if (!customElements.get('exec-node-card')) {
    customElements.define('exec-node-card', ExecNodeCard);
  }
  if (!customElements.get('exec-flow-lane')) {
    customElements.define('exec-flow-lane', ExecFlowLane);
  }

  function resolveLaneNodes() {
    return {
      variable: state.nodes.filter((item) => item.type === 'variable'),
      step: getSortedSteps(),
      on_fail: state.nodes.filter((item) => item.type === 'on_fail'),
    };
  }

  function handleCanvasEvent(event) {
    const eventType = event.type;
    if (eventType === 'lane-add-node') {
      addNodeByType(event.detail?.laneType || '');
      return;
    }
    if (eventType === 'node-select') {
      selectNode(event.detail?.nodeId || '');
      return;
    }
    if (eventType !== 'node-action') {
      return;
    }
    const action = event.detail?.action || '';
    const nodeId = event.detail?.nodeId || '';
    if (action === 'delete') {
      deleteNodeById(nodeId);
      return;
    }
    if (action === 'move-up') {
      moveNodeInLane(nodeId, -1);
      return;
    }
    if (action === 'move-down') {
      moveNodeInLane(nodeId, 1);
    }
  }

  dropZone.addEventListener('lane-add-node', handleCanvasEvent);
  dropZone.addEventListener('node-select', handleCanvasEvent);
  dropZone.addEventListener('node-action', handleCanvasEvent);

  return function renderCanvas() {
    dropZone.innerHTML = '';
    const laneNodeMap = resolveLaneNodes();
    laneDefs.forEach((lane) => {
      const laneEl = document.createElement('exec-flow-lane');
      laneEl.laneData = {
        lane,
        laneNodes: laneNodeMap[lane.type] || [],
        selectedId: state.selectedId,
      };
      dropZone.appendChild(laneEl);
    });
    renderMaterialIcons(dropZone);
  };
}

export function createCanvasNodeController(options) {
  const {
    state,
    nodeDefMap,
    fixedTypes,
    makeNode,
    getNextStepOrder,
    applyStepVerticalLayout,
    renderCanvas,
    renderEditor,
    saveLocal,
    statusText,
    escapeHtml,
  } = options;

  function addNodeByType(nodeType) {
    const def = nodeDefMap[nodeType] || null;
    if (!def) return;
    if (fixedTypes.has(nodeType) && state.nodes.some((item) => item.type === nodeType)) {
      statusText.classList.add('status-warn');
      statusText.innerHTML = `<strong>${escapeHtml(nodeType)}</strong> 顶层仅允许 1 个节点。`;
      return;
    }
    const node = makeNode(def, 0, 0);
    state.nodes.push(node);
    if (node.type === 'step') {
      applyStepVerticalLayout();
    }
    state.selectedId = node.id;
    renderCanvas();
    renderEditor();
    saveLocal();
    statusText.classList.remove('status-warn');
    statusText.innerHTML = `<strong>已添加节点：</strong>${escapeHtml(def.label)}`;
  }

  function selectNode(nodeId) {
    state.selectedId = nodeId;
    renderCanvas();
    renderEditor();
  }

  function deleteNodeById(nodeId) {
    const targetNode = state.nodes.find((item) => item.id === nodeId);
    state.nodes = state.nodes.filter((item) => item.id !== nodeId);
    if (!state.nodes.find((item) => item.id === state.selectedId)) {
      state.selectedId = state.nodes.length ? state.nodes[0].id : null;
    }
    applyStepVerticalLayout();
    renderCanvas();
    renderEditor();
    saveLocal();
    const label = targetNode && targetNode.title ? targetNode.title : nodeId;
    statusText.innerHTML = `<strong>已删除节点：</strong>${escapeHtml(label)}`;
  }

  function getSortedSteps() {
    return state.nodes
      .filter((item) => item.type === 'step')
      .sort((left, right) => (left.stepOrder || 0) - (right.stepOrder || 0));
  }

  function moveStepOrder(nodeId, offset) {
    const steps = getSortedSteps();
    const currentIndex = steps.findIndex((item) => item.id === nodeId);
    if (currentIndex < 0) return;
    const targetIndex = currentIndex + offset;
    if (targetIndex < 0 || targetIndex >= steps.length) return;
    const currentStep = steps[currentIndex];
    const targetStep = steps[targetIndex];
    const temp = currentStep.stepOrder;
    currentStep.stepOrder = targetStep.stepOrder;
    targetStep.stepOrder = temp;
    applyStepVerticalLayout();
    renderCanvas();
    renderEditor();
    saveLocal();
    statusText.classList.remove('status-warn');
    statusText.innerHTML = `<strong>已调整 Step 顺序：</strong>${escapeHtml(currentStep.title || currentStep.id)}`;
  }

  function moveNodeInLane(nodeId, offset) {
    const node = state.nodes.find((item) => item.id === nodeId);
    if (!node) return;
    if (node.type === 'step') {
      moveStepOrder(nodeId, offset);
    }
  }

  return {
    addNodeByType,
    selectNode,
    deleteNodeById,
    moveNodeInLane,
    getSortedSteps,
    getNextStepOrder,
  };
}
