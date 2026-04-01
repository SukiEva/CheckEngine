export function createDatasourceStore(options) {
  const {
    datasourceList,
    datasourceStatus,
    storageKey,
    defaultConfigs,
    renderEditor,
    renderMaterialIcons,
  } = options;

  let datasourceNameOptions = [];

  function updateDatasourceOptions(datasourceConfigs) {
    if (!Array.isArray(datasourceConfigs)) {
      datasourceNameOptions = [];
      return;
    }
    datasourceNameOptions = datasourceConfigs
      .map((item) => (item && typeof item.name === 'string' ? item.name.trim() : ''))
      .filter((name) => name.length > 0);
  }

  function renderDatasourceOptionsHtml(escapeAttr) {
    if (!datasourceNameOptions.length) {
      return '';
    }
    return datasourceNameOptions
      .map((name) => `<option value="${escapeAttr(name)}"></option>`)
      .join('');
  }

  function normalizeDatasourceConfigs(rawDatasourceConfigs) {
    if (!Array.isArray(rawDatasourceConfigs)) {
      return [];
    }
    return rawDatasourceConfigs
      .map((item) => {
        if (!item || typeof item !== 'object' || Array.isArray(item)) {
          return null;
        }
        const name = typeof item.name === 'string' ? item.name.trim() : '';
        const dbUrl = typeof item.db_url === 'string' ? item.db_url.trim() : '';
        if (!name && !dbUrl) {
          return null;
        }
        return { name, db_url: dbUrl };
      })
      .filter(Boolean);
  }

  function readDatasourceConfigRows() {
    return Array.from(datasourceList.children)
      .map((card) => ({
        name: card.querySelector('[data-role="name"]').value.trim(),
        db_url: card.querySelector('[data-role="db_url"]').value.trim(),
      }))
      .filter((item) => item.name || item.db_url);
  }

  function saveDatasourceConfigLocal() {
    const datasourcePayload = normalizeDatasourceConfigs(readDatasourceConfigRows());
    localStorage.setItem(storageKey, JSON.stringify(datasourcePayload));
    datasourceStatus.textContent = datasourcePayload.length
      ? `已保存 ${datasourcePayload.length} 个数据源到本地浏览器。`
      : '已清空本地数据源配置。';
    updateDatasourceOptions(datasourcePayload);
    renderEditor();
  }

  function createDatasourceCard(data) {
    const card = document.createElement('div');
    card.className = 'datasource-card';
    card.innerHTML = `
      <div class="kv-row">
        <input data-role="name" placeholder="datasource 名称，例如 saas_db" />
        <input data-role="db_url" placeholder="postgresql+psycopg2://user:pass@host:5432/dbname" />
        <button class="el-button el-button--danger is-plain is-circle el-button--small" type="button" data-role="remove" title="删除数据源" aria-label="删除数据源"><span class="ep-icon">delete_outline</span></button>
      </div>
    `;
    if (typeof renderMaterialIcons === 'function') {
      renderMaterialIcons(card);
    }
    card.querySelector('[data-role="name"]').value = data && data.name ? data.name : '';
    card.querySelector('[data-role="db_url"]').value = data && data.db_url ? data.db_url : '';
    card.querySelector('[data-role="remove"]').addEventListener('click', () => {
      card.remove();
      saveDatasourceConfigLocal();
    });
    card.querySelectorAll('[data-role="name"], [data-role="db_url"]').forEach((input) => {
      input.addEventListener('change', () => {
        saveDatasourceConfigLocal();
        renderEditor();
      });
    });
    return card;
  }

  function renderDatasourceCards(datasourceItems) {
    datasourceList.innerHTML = '';
    datasourceItems.forEach((item) => datasourceList.appendChild(createDatasourceCard(item)));
    if (typeof renderMaterialIcons === 'function') {
      renderMaterialIcons(datasourceList);
    }
    updateDatasourceOptions(datasourceItems);
  }

  function loadDatasourceConfigLocal() {
    datasourceStatus.textContent = '正在载入本地数据源配置...';
    try {
      const rawLocalValue = localStorage.getItem(storageKey);
      if (!rawLocalValue) {
        renderDatasourceCards(defaultConfigs);
        localStorage.setItem(storageKey, JSON.stringify(defaultConfigs));
        datasourceStatus.textContent = `未找到本地配置，已初始化默认 ${defaultConfigs.length} 个数据源。`;
        renderEditor();
        return;
      }
      const parsed = JSON.parse(rawLocalValue);
      const datasourceConfigs = normalizeDatasourceConfigs(parsed);
      if (!datasourceConfigs.length) {
        renderDatasourceCards(defaultConfigs);
        localStorage.setItem(storageKey, JSON.stringify(defaultConfigs));
        datasourceStatus.textContent = `本地配置为空，已恢复默认 ${defaultConfigs.length} 个数据源。`;
        renderEditor();
        return;
      }
      renderDatasourceCards(datasourceConfigs);
      datasourceStatus.textContent = `已从本地载入 ${datasourceConfigs.length} 个数据源。`;
      renderEditor();
    } catch (error) {
      renderDatasourceCards(defaultConfigs);
      localStorage.setItem(storageKey, JSON.stringify(defaultConfigs));
      datasourceStatus.textContent = `本地配置读取失败，已加载默认数据源。错误: ${error.message || '未知错误'}`;
      renderEditor();
    }
  }

  function appendDatasourceCard() {
    datasourceList.appendChild(createDatasourceCard({ name: '', db_url: '' }));
    saveDatasourceConfigLocal();
    renderEditor();
  }

  return {
    renderDatasourceOptionsHtml,
    readDatasourceConfigRows,
    loadDatasourceConfigLocal,
    saveDatasourceConfigLocal,
    appendDatasourceCard,
  };
}
