export function initRuntimeDslOperations(options) {
  const {
    runtimeDialog,
    validateDialog,
    runtimeStatus,
    runtimeResultCode,
    validateStatus,
    validateResultCode,
    statusText,
    validateButton,
    runButton,
    renderMaterialIcons,
    escapeHtml,
    toDslObject,
    readDatasourceConfigRows,
    loadDatasourceConfigLocal,
    readRuntimeInputPayload,
  } = options;

  function closeRuntimeDialog() {
    if (typeof runtimeDialog.close === 'function' && runtimeDialog.hasAttribute('open')) {
      runtimeDialog.close();
    } else {
      runtimeDialog.removeAttribute('open');
    }
  }

  function openRuntimeDialog() {
    if (typeof runtimeDialog.showModal === 'function') {
      runtimeDialog.showModal();
    } else {
      runtimeDialog.setAttribute('open', 'open');
    }
  }

  function closeValidateDialog() {
    if (typeof validateDialog.close === 'function' && validateDialog.hasAttribute('open')) {
      validateDialog.close();
    } else {
      validateDialog.removeAttribute('open');
    }
  }

  function openValidateDialog() {
    if (typeof validateDialog.showModal === 'function') {
      validateDialog.showModal();
    } else {
      validateDialog.setAttribute('open', 'open');
    }
  }

  function renderValidateResult(payload) {
    const resultJson = JSON.stringify(payload || {}, null, 2);
    validateResultCode.textContent = resultJson;
    if (window.Prism && typeof window.Prism.highlightElement === 'function') {
      window.Prism.highlightElement(validateResultCode);
    }
  }

  function renderRuntimeResult(payload) {
    runtimeResultCode.textContent = JSON.stringify(payload, null, 2);
    if (window.Prism && typeof window.Prism.highlightElement === 'function') {
      window.Prism.highlightElement(runtimeResultCode);
    }
  }

  function setRunButtonLoading(isLoading) {
    if (isLoading) {
      runButton.classList.add('btn-loading', 'is-loading');
      runButton.disabled = true;
      runButton.innerHTML = '<span class="ep-icon">hourglass_top</span>运行中';
      renderMaterialIcons(runButton);
      return;
    }
    runButton.classList.remove('btn-loading', 'is-loading');
    runButton.disabled = false;
    runButton.innerHTML = '<span class="ep-icon">play_circle</span>运行 DSL';
    renderMaterialIcons(runButton);
  }

  function setValidateButtonLoading(isLoading) {
    if (isLoading) {
      validateButton.classList.add('btn-loading', 'is-loading');
      validateButton.disabled = true;
      validateButton.innerHTML = '<span class="ep-icon">hourglass_top</span>校验中';
      renderMaterialIcons(validateButton);
      return;
    }
    validateButton.classList.remove('btn-loading', 'is-loading');
    validateButton.disabled = false;
    validateButton.innerHTML = '<span class="ep-icon">fact_check</span>校验 DSL';
    renderMaterialIcons(validateButton);
  }

  async function parseApiPayload(response) {
    const responseText = await response.text();
    if (!responseText.trim()) {
      return {};
    }
    try {
      return JSON.parse(responseText);
    } catch (_error) {
      return { error: responseText };
    }
  }

  async function validateDslNow() {
    const dslPayload = toDslObject();
    setValidateButtonLoading(true);
    openValidateDialog();
    validateStatus.textContent = '校验中...';
    renderValidateResult({});
    try {
      const response = await fetch('/api/validate-dsl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dsl_text: JSON.stringify(dslPayload, null, 2),
        }),
      });
      const payload = await parseApiPayload(response);
      if (!response.ok) {
        statusText.classList.add('status-warn');
        statusText.innerHTML = `<strong>校验失败：</strong>${escapeHtml(payload.detail || payload.error || '未知错误')}`;
        validateStatus.textContent = `校验失败: ${payload.detail || payload.error || '未知错误'}`;
        renderValidateResult(payload);
        return;
      }
      statusText.classList.remove('status-warn');
      statusText.textContent = 'DSL 校验通过。';
      validateStatus.textContent = 'DSL 校验通过。';
      renderValidateResult(payload);
    } catch (error) {
      statusText.classList.add('status-warn');
      statusText.innerHTML = `<strong>校验失败：</strong>${escapeHtml(error.message || '网络错误')}`;
      validateStatus.textContent = `校验失败: ${error.message || '网络错误'}`;
      renderValidateResult({
        error: error.message || '网络错误',
      });
    } finally {
      setValidateButtonLoading(false);
    }
  }

  async function runDslNow() {
    const dslPayload = toDslObject();
    let datasourcePayload = readDatasourceConfigRows();
    if (!datasourcePayload.length) {
      loadDatasourceConfigLocal();
      datasourcePayload = readDatasourceConfigRows();
    }
    if (!datasourcePayload.length) {
      statusText.classList.add('status-warn');
      statusText.textContent = '运行失败：请先在“数据源配置”页签中维护至少一个数据源。';
      return;
    }
    const inputPayload = readRuntimeInputPayload();
    setRunButtonLoading(true);
    openRuntimeDialog();
    runtimeStatus.textContent = '执行中...';
    renderRuntimeResult({});
    try {
      const response = await fetch('/api/run-dsl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dsl_text: JSON.stringify(dslPayload, null, 2),
          input_data: inputPayload,
          datasources: datasourcePayload,
        }),
      });
      const payload = await parseApiPayload(response);
      if (!response.ok) {
        runtimeStatus.textContent = `执行失败: ${payload.detail || payload.error || '未知错误'}`;
        renderRuntimeResult(payload);
        return;
      }
      runtimeStatus.textContent = '执行成功。';
      renderRuntimeResult(payload.result);
      statusText.classList.remove('status-warn');
      statusText.textContent = '运行完成。';
    } catch (error) {
      runtimeStatus.textContent = `请求失败: ${error.message || '网络错误'}`;
    } finally {
      setRunButtonLoading(false);
    }
  }

  return {
    closeRuntimeDialog,
    closeValidateDialog,
    validateDslNow,
    runDslNow,
    setRunButtonLoading,
  };
}
