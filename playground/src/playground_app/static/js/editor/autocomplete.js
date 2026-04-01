export function createAutocompleteBinder(options) {
  const {
    state,
    requiredInputRows,
    readRuntimeInputPayload,
    getOutputFields,
  } = options;

  let runtimeAutocompleteContext = null;
  let runtimeFallbackPanel = null;
  let scrollListenerBound = false;

  function getReferenceRange(target) {
    const cursor = target.selectionStart || 0;
    const text = target.value || '';
    const before = text.slice(0, cursor);
    const matched = before.match(/\$[a-zA-Z0-9_$.]*$/);
    if (!matched) return null;
    return {
      start: cursor - matched[0].length,
      end: cursor,
      keyword: matched[0],
    };
  }

  function hideRuntimeAutocomplete() {
    if (!runtimeAutocompleteContext) return;
    runtimeAutocompleteContext.panel.innerHTML = '';
    runtimeAutocompleteContext.panel.style.display = 'none';
    runtimeAutocompleteContext.items = [];
    runtimeAutocompleteContext.activeIndex = -1;
  }

  function renderRuntimeAutocomplete() {
    if (!runtimeAutocompleteContext) return;
    const { panel, items, activeIndex } = runtimeAutocompleteContext;
    if (!items.length) {
      hideRuntimeAutocomplete();
      return;
    }
    panel.innerHTML = items
      .map((item, index) => `<div class="runtime-ac-item${index === activeIndex ? ' active' : ''}" data-runtime-ac-index="${index}">${item}</div>`)
      .join('');
    panel.style.display = 'block';
    panel.querySelectorAll('[data-runtime-ac-index]').forEach((item) => {
      item.addEventListener('mousedown', (event) => {
        event.preventDefault();
        const targetIndex = Number(item.getAttribute('data-runtime-ac-index'));
        applyRuntimeAutocomplete(targetIndex);
      });
    });
  }

  function applyRuntimeAutocomplete(index) {
    if (!runtimeAutocompleteContext) return;
    const { target, items, range } = runtimeAutocompleteContext;
    if (index < 0 || index >= items.length || !range) return;
    const selected = items[index];
    const original = target.value || '';
    target.value = `${original.slice(0, range.start)}${selected}${original.slice(range.end)}`;
    const nextCursor = range.start + selected.length;
    target.selectionStart = nextCursor;
    target.selectionEnd = nextCursor;
    target.focus();
    hideRuntimeAutocomplete();
  }

  function positionRuntimeAutocomplete(target) {
    if (!runtimeAutocompleteContext) return;
    const targetRect = target.getBoundingClientRect();
    const panel = runtimeAutocompleteContext.panel;
    panel.style.left = `${targetRect.left + window.scrollX}px`;
    panel.style.top = `${targetRect.bottom + window.scrollY + 4}px`;
    panel.style.width = `${Math.max(targetRect.width, 260)}px`;
  }

  function getStepIdSuggestions(currentNodeId) {
    return state.nodes
      .filter((item) => item.type === 'step' && item.id !== currentNodeId)
      .map((item, index) => {
        const rawKey = (item.title || `${item.type}_${index + 1}`).trim();
        return rawKey
          .replaceAll(/\s+/g, '_')
          .replaceAll(/[^a-zA-Z0-9_]/g, '_');
      });
  }

  function getRuntimePathSuggestions(currentNode) {
    const basics = [
      '$input',
      '$variables',
      '$steps',
    ];
    const inputPayload = readRuntimeInputPayload();
    Object.keys(inputPayload).forEach((key) => {
      basics.push(`$input.${key}`);
    });
    requiredInputRows.forEach((requiredRow) => {
      if (!Object.prototype.hasOwnProperty.call(inputPayload, requiredRow.key)) {
        basics.push(`$input.${requiredRow.key}`);
      }
    });
    const runtimePaths = [];
    const currentNodeOutputFields = getOutputFields(currentNode);
    const currentNodeLocalPaths = currentNodeOutputFields.map((field) => `$.${field}`);

    state.nodes.forEach((item, index) => {
      const rawKey = (item.title || `${item.type}_${index + 1}`).trim();
      const nodeKey = rawKey
        .replaceAll(/\s+/g, '_')
        .replaceAll(/[^a-zA-Z0-9_]/g, '_');
      const outputFields = getOutputFields(item);
      if (item.type === 'variable') {
        runtimePaths.push(`$variables.${nodeKey}`);
      }
      if (item.type === 'step') {
        runtimePaths.push(`$steps.${nodeKey}`);
        outputFields.forEach((field) => runtimePaths.push(`$steps.${nodeKey}.${field}`));
      }
    });

    if (currentNode.type !== 'step') {
      basics.push('$steps.<step_id>.<output>');
    }
    return [...new Set([...basics, ...currentNodeLocalPaths, ...runtimePaths])];
  }

  function bindRuntimeAutocomplete(currentNode) {
    const targets = Array.from(document.querySelectorAll('[data-ref-autocomplete="true"]'));
    const consumeStepOptions = document.getElementById('consumeStepOptions');
    const stepIdSuggestions = getStepIdSuggestions(currentNode.id);
    if (consumeStepOptions) {
      consumeStepOptions.innerHTML = stepIdSuggestions
        .map((stepId) => `<option value="${stepId}"></option>`)
        .join('');
    }

    targets.forEach((target) => {
      if (target.dataset.runtimeAcBound === 'true') {
        return;
      }
      target.dataset.runtimeAcBound = 'true';
      target.addEventListener('input', () => {
        const range = getReferenceRange(target);
        if (!range || !range.keyword.startsWith('$')) {
          hideRuntimeAutocomplete();
          return;
        }
        const allSuggestions = getRuntimePathSuggestions(currentNode);
        const matched = allSuggestions.filter((item) => item.toLowerCase().includes(range.keyword.toLowerCase()));
        const panel = ensureRuntimePanel(target);
        runtimeAutocompleteContext = {
          target,
          panel,
          items: matched,
          activeIndex: matched.length ? 0 : -1,
          range,
        };
        positionRuntimeAutocomplete(target);
        renderRuntimeAutocomplete();
      });
      target.addEventListener('keydown', (event) => {
        if (!runtimeAutocompleteContext || runtimeAutocompleteContext.target !== target) return;
        if (!runtimeAutocompleteContext.items.length) return;
        if (event.key === 'ArrowDown') {
          event.preventDefault();
          runtimeAutocompleteContext.activeIndex = (runtimeAutocompleteContext.activeIndex + 1) % runtimeAutocompleteContext.items.length;
          renderRuntimeAutocomplete();
        } else if (event.key === 'ArrowUp') {
          event.preventDefault();
          runtimeAutocompleteContext.activeIndex = (runtimeAutocompleteContext.activeIndex - 1 + runtimeAutocompleteContext.items.length) % runtimeAutocompleteContext.items.length;
          renderRuntimeAutocomplete();
        } else if (event.key === 'Enter') {
          event.preventDefault();
          applyRuntimeAutocomplete(runtimeAutocompleteContext.activeIndex);
        } else if (event.key === 'Escape') {
          hideRuntimeAutocomplete();
        }
      });
      target.addEventListener('blur', () => {
        setTimeout(() => {
          hideRuntimeAutocomplete();
        }, 120);
      });
    });

    if (!scrollListenerBound) {
      document.addEventListener('scroll', () => {
        if (runtimeAutocompleteContext) {
          positionRuntimeAutocomplete(runtimeAutocompleteContext.target);
        }
      }, { passive: true });
      scrollListenerBound = true;
    }
  }

  function ensureRuntimePanel(target) {
    const inlinePanel = document.getElementById(target.id === 'f_sql' ? 'ac_sql' : 'ac_runtime_inline');
    if (inlinePanel) {
      return inlinePanel;
    }
    const sqlPanel = document.getElementById('ac_sql');
    if (sqlPanel) {
      return sqlPanel;
    }
    if (runtimeFallbackPanel) {
      return runtimeFallbackPanel;
    }
    const panel = document.createElement('div');
    panel.id = 'ac_runtime_fallback';
    panel.className = 'autocomplete-panel';
    panel.style.position = 'fixed';
    panel.style.zIndex = '60';
    panel.style.display = 'none';
    document.body.appendChild(panel);
    runtimeFallbackPanel = panel;
    return panel;
  }

  function createAutocomplete(config) {
    const { inputEl, panelEl, getKeyword, buildOptions, applySuggestion } = config;
    if (!inputEl || !panelEl) return;

    let options = [];
    let selectedIndex = -1;

    function closePanel() {
      panelEl.style.display = 'none';
      panelEl.innerHTML = '';
      selectedIndex = -1;
      options = [];
    }

    function openPanel() {
      if (!options.length) {
        closePanel();
        return;
      }
      panelEl.innerHTML = options
        .map((item, index) => `<div class="runtime-ac-item${index === selectedIndex ? ' active' : ''}" data-index="${index}">${item}</div>`)
        .join('');
      panelEl.style.display = 'block';
      panelEl.querySelectorAll('[data-index]').forEach((item) => {
        item.addEventListener('mousedown', (event) => {
          event.preventDefault();
          const index = Number(item.getAttribute('data-index'));
          const selected = options[index];
          if (!selected) return;
          applySuggestion(selected);
          closePanel();
        });
      });
    }

    inputEl.addEventListener('input', () => {
      const keyword = getKeyword();
      options = buildOptions(keyword);
      selectedIndex = options.length ? 0 : -1;
      openPanel();
    });

    inputEl.addEventListener('keydown', (event) => {
      if (!options.length) return;
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        selectedIndex = (selectedIndex + 1) % options.length;
        openPanel();
      } else if (event.key === 'ArrowUp') {
        event.preventDefault();
        selectedIndex = (selectedIndex - 1 + options.length) % options.length;
        openPanel();
      } else if (event.key === 'Enter') {
        if (selectedIndex >= 0 && selectedIndex < options.length) {
          event.preventDefault();
          applySuggestion(options[selectedIndex]);
          closePanel();
        }
      } else if (event.key === 'Escape') {
        closePanel();
      }
    });

    inputEl.addEventListener('blur', () => {
      setTimeout(closePanel, 120);
    });
  }

  function bindAutocompletes(node) {
    const sqlTextarea = document.getElementById('f_sql');
    const sqlPanel = document.getElementById('ac_sql');

    if (sqlTextarea && sqlPanel && sqlTextarea.dataset.sqlAcBound !== 'true') {
      sqlTextarea.dataset.sqlAcBound = 'true';
      createAutocomplete({
        inputEl: sqlTextarea,
        panelEl: sqlPanel,
        getKeyword: () => {
          const cursor = sqlTextarea.selectionStart || 0;
          const beforeCursor = sqlTextarea.value.slice(0, cursor);
          const matched = beforeCursor.match(/[$][a-zA-Z0-9_.]*$/);
          return matched ? matched[0] : '';
        },
        buildOptions: (keyword) => {
          const suggestionPool = getRuntimePathSuggestions(node);
          if (!keyword) return [];
          return suggestionPool.filter((item) => item.toLowerCase().includes(keyword.toLowerCase()));
        },
        applySuggestion: (value) => {
          const cursor = sqlTextarea.selectionStart || 0;
          const beforeCursor = sqlTextarea.value.slice(0, cursor);
          const afterCursor = sqlTextarea.value.slice(cursor);
          const matched = beforeCursor.match(/[$][a-zA-Z0-9_.]*$/);
          if (!matched) return;
          const start = cursor - matched[0].length;
          sqlTextarea.value = `${beforeCursor.slice(0, start)}${value}${afterCursor}`;
          const nextCursor = start + value.length;
          sqlTextarea.selectionStart = nextCursor;
          sqlTextarea.selectionEnd = nextCursor;
          sqlTextarea.focus();
        },
      });
    }

    bindRuntimeAutocomplete(node);
  }

  return {
    bindAutocompletes,
    hideRuntimeAutocomplete,
  };
}
