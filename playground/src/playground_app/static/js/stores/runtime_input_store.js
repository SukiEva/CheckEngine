export function createRuntimeInputStore(options) {
  const {
    runtimeInputRowsEl,
    storageKey,
    requiredRows,
    defaultRows,
    parseInputValue,
    renderMaterialIcons,
  } = options;

  let runtimeInputRows = [...defaultRows];

  function normalizeInputRows(rows) {
    const normalizedMap = new Map();
    if (Array.isArray(rows)) {
      rows.forEach((row) => {
        if (!row || typeof row !== 'object') return;
        const key = typeof row.key === 'string' ? row.key.trim() : '';
        if (!key) return;
        const value = Object.prototype.hasOwnProperty.call(row, 'value')
          ? String(row.value ?? '')
          : '';
        normalizedMap.set(key, { key, value });
      });
    }
    requiredRows.forEach((requiredRow) => {
      const normalizedRequiredKey = String(requiredRow.key || '').trim();
      if (!normalizedRequiredKey) return;
      if (!normalizedMap.has(normalizedRequiredKey)) {
        normalizedMap.set(normalizedRequiredKey, {
          key: normalizedRequiredKey,
          value: String(requiredRow.value ?? ''),
        });
      }
    });
    return Array.from(normalizedMap.values());
  }

  function renderRuntimeInputRows() {
    if (!runtimeInputRowsEl) return;
    runtimeInputRowsEl.innerHTML = runtimeInputRows.map((row, index) => {
      const safeKey = row && typeof row.key === 'string' ? row.key : '';
      const safeValue = row && Object.prototype.hasOwnProperty.call(row, 'value') ? String(row.value ?? '') : '';
      return `
      <div class="field-row field-row--double runtime-input-row" data-input-row="${index}">
        <input data-input-key="${index}" placeholder="参数名，例如 source_object_id" value="${safeKey}" />
        <input data-input-value="${index}" placeholder="参数值，例如 DEMO_1" value="${safeValue}" />
        <button class="el-button el-button--danger is-plain is-circle el-button--small field-row-action" type="button" data-remove-input="${index}" aria-label="删除参数" title="删除参数"><span class="ep-icon">delete_outline</span></button>
      </div>
      `;
    }).join('');
    if (typeof renderMaterialIcons === 'function') {
      renderMaterialIcons(runtimeInputRowsEl);
    }

    runtimeInputRowsEl.querySelectorAll('[data-remove-input]').forEach((button) => {
      button.addEventListener('click', () => {
        const index = Number(button.getAttribute('data-remove-input'));
        const row = runtimeInputRows[index];
        if (row && requiredRows.some((item) => item.key === row.key)) {
          return;
        }
        runtimeInputRows = runtimeInputRows.filter((_item, itemIndex) => itemIndex !== index);
        runtimeInputRows = normalizeInputRows(runtimeInputRows);
        saveInputConfigLocal();
        renderRuntimeInputRows();
      });
    });

    runtimeInputRowsEl.querySelectorAll('[data-input-key], [data-input-value]').forEach((input) => {
      input.addEventListener('change', () => {
        runtimeInputRows = readRuntimeInputRows();
        saveInputConfigLocal();
      });
    });
  }

  function readRuntimeInputRows() {
    if (!runtimeInputRowsEl) return normalizeInputRows(runtimeInputRows);
    const keyInputs = Array.from(runtimeInputRowsEl.querySelectorAll('[data-input-key]'));
    const valueInputs = Array.from(runtimeInputRowsEl.querySelectorAll('[data-input-value]'));
    const rows = keyInputs.map((keyInput, index) => ({
      key: keyInput.value.trim(),
      value: valueInputs[index] ? valueInputs[index].value : '',
    }));
    return normalizeInputRows(rows);
  }

  function readRuntimeInputPayload() {
    const rows = readRuntimeInputRows();
    const payload = {};
    rows.forEach((row) => {
      if (!row.key) return;
      payload[row.key] = parseInputValue(row.value);
    });
    return payload;
  }

  function saveInputConfigLocal() {
    const currentRows = readRuntimeInputRows();
    runtimeInputRows = currentRows;
    localStorage.setItem(storageKey, JSON.stringify(currentRows));
  }

  function loadInputConfigLocal() {
    const raw = localStorage.getItem(storageKey);
    if (!raw) {
      runtimeInputRows = normalizeInputRows(defaultRows);
      localStorage.setItem(storageKey, JSON.stringify(runtimeInputRows));
      return;
    }
    try {
      const parsed = JSON.parse(raw);
      const normalized = normalizeInputRows(parsed);
      runtimeInputRows = normalized;
      if (!Array.isArray(parsed) || parsed.length !== normalized.length) {
        localStorage.setItem(storageKey, JSON.stringify(normalized));
      }
    } catch (_error) {
      runtimeInputRows = normalizeInputRows(defaultRows);
      localStorage.setItem(storageKey, JSON.stringify(runtimeInputRows));
    }
  }

  function appendInputRow() {
    runtimeInputRows = readRuntimeInputRows();
    runtimeInputRows.push({ key: '', value: '' });
    renderRuntimeInputRows();
    saveInputConfigLocal();
  }

  return {
    renderRuntimeInputRows,
    readRuntimeInputRows,
    readRuntimeInputPayload,
    saveInputConfigLocal,
    loadInputConfigLocal,
    appendInputRow,
  };
}
