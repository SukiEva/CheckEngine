import { createCanvasNodeController, createCanvasRenderer } from "./components/canvas_components.js";
import { initRuntimeDslOperations } from "./runtime_dsl_operations.js";
import { consumesTextToRows as codecConsumesTextToRows, createDslCodec, formatConsumesTextFromRows as codecFormatConsumesTextFromRows, formatOutputsText as codecFormatOutputsText, formatVariableWhenSummary as codecFormatVariableWhenSummary, getOutputFields as codecGetOutputFields, isValidDslIdentifier as codecIsValidDslIdentifier, normalizeOutputRows as codecNormalizeOutputRows, normalizeStepPolicyType as codecNormalizeStepPolicyType, normalizeValueToInput as codecNormalizeValueToInput, parseInputValue as codecParseInputValue, parseJsonObjectOrEmpty as codecParseJsonObjectOrEmpty } from "./dsl/dsl_codec.js";
import { createAutocompleteBinder } from "./editor/autocomplete.js";
import { createDatasourceStore } from "./stores/datasource_store.js";
import { createRuntimeInputStore } from "./stores/runtime_input_store.js";
import { closeDialog as uiCloseDialog, escapeAttr as uiEscapeAttr, escapeHtml as uiEscapeHtml, highlightCode as uiHighlightCode, openDialog as uiOpenDialog, renderMaterialIcons as uiRenderMaterialIcons } from "./utils/ui_utils.js";

    const renderMaterialIcons = uiRenderMaterialIcons;

    const NODE_DEF_MAP = {
      variable: { type: 'variable', label: 'Variable 节点', defaultTitle: '计算变量' },
      step: { type: 'step', label: 'Step 节点', defaultTitle: '执行步骤' },
      on_fail: { type: 'on_fail', label: 'On Fail 节点', defaultTitle: '最终失败判定' },
    };

    const dropZone = document.getElementById('dropZone');
    const editorPanel = document.getElementById('editorPanel');
    const dslPreviewCode = document.getElementById('dslPreviewCode');
    const jsonPreviewDialog = document.getElementById('jsonPreviewDialog');
    const runtimeDialog = document.getElementById('runtimeDialog');
    const validateDialog = document.getElementById('validateDialog');
    const runtimeInputRowsEl = document.getElementById('runtimeInputRows');
    const addInputParamButton = document.getElementById('btnAddInputParam');
    const runtimeStatus = document.getElementById('runtimeStatus');
    const runtimeResultCode = document.getElementById('runtimeResultCode');
    const validateStatus = document.getElementById('validateStatus');
    const validateResultCode = document.getElementById('validateResultCode');
    const statusText = document.getElementById('statusText');
    const validateButton = document.getElementById('btnValidate');
    const runButton = document.getElementById('btnRun');
    const datasourceList = document.getElementById('datasourceList');
    const datasourceStatus = document.getElementById('datasourceStatus');
    const DATASOURCE_CONFIG_STORAGE_KEY = 'execdsl_flow_designer_datasource_v1';
    const INPUT_CONFIG_STORAGE_KEY = 'execdsl_flow_designer_input_v1';
    const DEFAULT_DATASOURCE_CONFIGS = [
      {
        name: 'saas_db',
        db_url: 'postgresql+psycopg2://user:password@127.0.0.1:5432/saas_db',
      },
      {
        name: 'data_db',
        db_url: 'postgresql+psycopg2://user:password@127.0.0.1:5432/data_db',
      },
    ];
    const REQUIRED_INPUT_ROWS = [
      { key: 'source_object_id', value: 'DEMO_1' },
      { key: 'renter_id', value: 'DEMO_RENTER_1' },
      { key: 'accounting_period', value: '2026-01' },
    ];
    const DEFAULT_INPUT_ROWS = [...REQUIRED_INPUT_ROWS];

    const state = {
      nodes: [],
      selectedId: null,
    };

    const FIXED_TYPES = new Set(['on_fail']);
    const SQL_NODE_TYPES = new Set(['step']);
    const DECISION_NODE_TYPES = new Set(['step', 'on_fail']);
    const OUTPUT_NODE_TYPES = new Set(['step']);
    const LEGACY_TOP_LEVEL_FIELDS = new Set(['context', 'prechecks']);

    function isSqlNode(nodeType) {
      return SQL_NODE_TYPES.has(nodeType);
    }

    function isDecisionNode(nodeType) {
      return DECISION_NODE_TYPES.has(nodeType);
    }

    function shouldShowDecisionField(node) {
      if (!node) {
        return false;
      }
      if (node.type === 'on_fail') {
        return true;
      }
      if (node.type !== 'step') {
        return false;
      }
      const stepPolicyType = getNodeStepPolicyType(node);
      return stepPolicyType === 'on_fail' || stepPolicyType === 'on_pass';
    }

    function hasOutputs(nodeType) {
      return OUTPUT_NODE_TYPES.has(nodeType);
    }

    function nodeTypeToEditorKind(nodeType) {
      if (nodeType === 'step') {
        return 'sql';
      }
      return 'variable';
    }

    function getDefaultDecisionByNodeType(nodeType) {
      if (nodeType === 'on_fail') {
        return 'exists($steps.some_step.some_output)';
      }
      return '';
    }

    function normalizeStepPolicyType(stepPolicyType, legacyStepPolicyType) {
      return codecNormalizeStepPolicyType(stepPolicyType, legacyStepPolicyType);
    }

    function getNodeStepPolicyType(node) {
      if (!node || typeof node !== 'object') {
        return 'none';
      }
      return normalizeStepPolicyType(node.stepPolicyType, node.precheckPolicyType);
    }

    function isValidDslIdentifier(value) {
      return codecIsValidDslIdentifier(value);
    }

    function isTopLevelVariableNode(node) {
      return !!node && node.type === 'variable' && !Number.isFinite(node.stepOrder);
    }

    function canEditNodeKind(node) {
      return !!node && node.type !== 'on_fail' && !isTopLevelVariableNode(node);
    }

    function makeNode(def, x, y) {
      return {
        id: makeNodeId(),
        type: def.type,
        title: def.defaultTitle,
        x,
        y,
        sql: 'SELECT 1 AS ok;',
        sqlParamsText: '{}',
        sqlParams: [{ key: '', value: '' }],
        datasource: 'saas_db',
        resultMode: 'records',
        decision: getDefaultDecisionByNodeType(def.type),
        variableWhenRows: [{ condition: '', value: '' }],
        variableValue: '',
        variableDefault: '',
        failMode: 'single',
        stepPolicyType: 'none',
        messageCn: '',
        messageEn: '',
        divider: '',
        dividerCn: '',
        dividerEn: '',
        outputRows: [{ field: 'ok' }],
        consumes: '',
        consumeRows: [{ stepName: '', alias: '' }],
        description: '',
        stepOrder: def.type === 'step' ? getNextStepOrder() : null
      };
    }

    function makeNodeId() {
      return `node_${Date.now()}_${Math.random().toString(16).slice(2, 7)}`;
    }

    const CANVAS_LANE_DEFS = [
      { type: 'variable', label: '1) Variables' },
      { type: 'step', label: '2) Steps' },
      { type: 'on_fail', label: '3) On Fail' },
    ];

    const runtimeInputStore = createRuntimeInputStore({
      runtimeInputRowsEl,
      storageKey: INPUT_CONFIG_STORAGE_KEY,
      requiredRows: REQUIRED_INPUT_ROWS,
      defaultRows: DEFAULT_INPUT_ROWS,
      parseInputValue: codecParseInputValue,
      renderMaterialIcons,
    });

    let canvasNodeController = null;
    let datasourceStore = null;
    let autocompleteBinder = null;
    let dslCodec = null;
    const renderCanvas = createCanvasRenderer({
      dropZone,
      fixedTypes: FIXED_TYPES,
      state,
      laneDefs: CANVAS_LANE_DEFS,
      renderMaterialIcons,
      escapeHtml,
      formatOutputsText,
      getOutputFields,
      formatVariableWhenSummary,
      normalizeValueToInput,
      getSortedSteps: () => canvasNodeController.getSortedSteps(),
      addNodeByType: (nodeType) => canvasNodeController.addNodeByType(nodeType),
      selectNode: (nodeId) => canvasNodeController.selectNode(nodeId),
      deleteNodeById: (nodeId) => canvasNodeController.deleteNodeById(nodeId),
      moveNodeInLane: (nodeId, offset) => canvasNodeController.moveNodeInLane(nodeId, offset),
    });

    function renderEditor() {
      const node = state.nodes.find((item) => item.id === state.selectedId);
      if (!node) {
        editorPanel.innerHTML = '<div class="muted">请先在画布中选择一个节点。</div>';
        return;
      }

      const showNodeTitleField = node.type !== 'on_fail';
      const editableNodeKind = canEditNodeKind(node);
      const resolvedStepPolicyType = node.type === 'step'
        ? getNodeStepPolicyType(node)
        : 'none';
      const shouldShowDecision = shouldShowDecisionField({
        ...node,
        stepPolicyType: resolvedStepPolicyType,
      });
      const isStepOnFailPolicy = node.type === 'step' && resolvedStepPolicyType === 'on_fail';
      const showFailPolicyFields = node.type === 'on_fail' || isStepOnFailPolicy;
      const showDividerConfig = showFailPolicyFields && (node.failMode === 'sub_repeat' || node.failMode === 'full_repeat');

      editorPanel.innerHTML = `
        ${showNodeTitleField ? `
        <div class="field">
          <label>节点标题</label>
          <input id="f_title" value="${escapeAttr(node.title)}" />
        </div>
        ` : ''}
        ${node.type === 'step' ? `
        <div class="field">
          <label>Description</label>
          <input id="f_description" value="${escapeAttr(node.description || '')}" placeholder="可选：用于 step 的说明" />
        </div>
        ` : ''}
        ${editableNodeKind ? `
        <div class="field">
          <label>类型</label>
          <select id="f_node_kind">
            <option value="sql" ${nodeTypeToEditorKind(node.type) === 'sql' ? 'selected' : ''}>sql</option>
            <option value="variable" ${nodeTypeToEditorKind(node.type) === 'variable' ? 'selected' : ''}>variable</option>
          </select>
        </div>
        ` : `
        <div class="field">
          <label>类型（只读）</label>
          <input class="editor-readonly-input" value="${node.type}" disabled />
        </div>
        `}
        ${node.type === 'step' ? `
        <div class="field field-with-actions">
          <div class="field-header">
            <label>Consumes（step_name + alias）</label>
            <button class="el-button el-button--primary is-plain is-circle el-button--small" id="btnAddConsumeRow" type="button" title="新增 consume" aria-label="新增 consume"><span class="ep-icon">add</span></button>
          </div>
          <div id="f_consumes_rows" class="field-row-list"></div>
          <div class="field-note">step_name 支持选择已有 step 名，也支持输入 <code>$</code> 触发运行时变量联想。</div>
          <datalist id="consumeStepOptions"></datalist>
        </div>
        ` : ''}
        ${isSqlNode(node.type) ? `
        <div class="field">
          <label>Datasource</label>
          <input
            id="f_datasource"
            list="datasourceOptions"
            value="${escapeAttr(node.datasource || '')}"
            placeholder="输入或选择已配置的数据源"
          />
          <datalist id="datasourceOptions">${renderDatasourceOptionsHtml()}</datalist>
        </div>
        <div class="field">
          <label>Result Mode</label>
          <select id="f_result_mode">
            <option value="record" ${(node.resultMode || '') === 'record' ? 'selected' : ''}>record</option>
            <option value="records" ${(node.resultMode || '') === 'records' ? 'selected' : ''}>records</option>
          </select>
        </div>
        <div class="field field-autocomplete">
          <label>SQL</label>
          <textarea id="f_sql" data-ref-autocomplete="true">${escapeHtml(node.sql)}</textarea>
          <div class="field-note">输入 <code>$</code> 可联想运行时变量路径（例如 <code>$steps.xx.yy</code>）。</div>
          <div class="autocomplete-panel" id="ac_sql"></div>
        </div>
        <div class="field">
          <label>SQL 变量（自动提取 :param）</label>
          <div id="f_sql_params_rows"></div>
          <div class="field-note">变量名从 SQL 中自动提取并只读，仅需输入变量值。</div>
        </div>
        ${hasOutputs(node.type) ? `
        <div class="field field-with-actions">
          <div class="field-header">
            <label>Outputs（与 Consumes 一样按行维护）</label>
            <button class="el-button el-button--primary is-plain is-circle el-button--small" id="btnAddOutputRow" type="button" title="新增 output" aria-label="新增 output"><span class="ep-icon">add</span></button>
          </div>
          <div id="f_outputs_rows" class="field-row-list"></div>
          <div class="field-note">每行一个输出字段，必须是合法 SQL 标识符（字母或下划线开头，仅包含字母、数字、下划线）。</div>
        </div>
        ` : ''}
        ` : ''}
        ${node.type === 'step' ? `
        <div class="field">
          <label>Step 短路策略（可选）</label>
          <select id="f_step_policy_type">
            <option value="none" ${resolvedStepPolicyType === 'none' ? 'selected' : ''}>none（不短路）</option>
            <option value="on_fail" ${resolvedStepPolicyType === 'on_fail' ? 'selected' : ''}>on_fail（失败时短路）</option>
            <option value="on_pass" ${resolvedStepPolicyType === 'on_pass' ? 'selected' : ''}>on_pass（成功时短路）</option>
          </select>
        </div>
        ` : ''}
        ${shouldShowDecision ? `
        <div class="field">
          <label>Decision（用于 step 短路策略 / 顶层 on_fail）</label>
          <input id="f_decision" data-ref-autocomplete="true" value="${escapeAttr(node.decision || '')}" />
        </div>
        ` : ''}
        ${node.type === 'variable' ? `
        <div class="field field-with-actions">
          <div class="field-header">
            <label>When 条件与赋值（condition/value）</label>
            <button class="el-button el-button--primary is-plain is-circle el-button--small" id="btnAddVariableWhen" type="button" title="新增 when 条件" aria-label="新增 when 条件"><span class="ep-icon">add</span></button>
          </div>
          <div id="f_variable_when_rows" class="field-row-list"></div>
        </div>
        <div class="field">
          <label>默认值（default）</label>
          <input id="f_variable_default" data-ref-autocomplete="true" value="${escapeAttr(node.variableDefault || '')}" placeholder="例如 500" />
        </div>
        ` : ''}
        ${showFailPolicyFields ? `
        <div class="field">
          <label>Fail Mode</label>
          <select id="f_fail_mode">
            <option value="single" ${(node.failMode || '') === 'single' ? 'selected' : ''}>single</option>
            <option value="sub_repeat" ${(node.failMode || '') === 'sub_repeat' ? 'selected' : ''}>sub_repeat</option>
            <option value="full_repeat" ${(node.failMode || '') === 'full_repeat' ? 'selected' : ''}>full_repeat</option>
          </select>
        </div>
        <div class="field">
          <label>Message CN</label>
          <textarea id="f_message_cn" data-ref-autocomplete="true">${escapeHtml(node.messageCn || '')}</textarea>
        </div>
        <div class="field">
          <label>Message EN</label>
          <textarea id="f_message_en" data-ref-autocomplete="true">${escapeHtml(node.messageEn || '')}</textarea>
        </div>
        ${showDividerConfig ? `
        <div class="field">
          <label>Divider（通用）</label>
          <input id="f_divider" value="${escapeAttr(node.divider || '')}" />
        </div>
        <div class="field">
          <label>Divider CN（中文优先）</label>
          <input id="f_divider_cn" value="${escapeAttr(node.dividerCn || '')}" />
        </div>
        <div class="field">
          <label>Divider EN（英文优先）</label>
          <input id="f_divider_en" value="${escapeAttr(node.dividerEn || '')}" />
        </div>
        ` : ''}
        ` : ''}
        `;
      renderMaterialIcons(editorPanel);

      const persistEditorNode = () => {
        if (canEditNodeKind(node)) {
          const nodeKindSelect = document.getElementById('f_node_kind');
          if (nodeKindSelect) {
            const nextType = nodeKindSelect.value === 'sql' ? 'step' : 'variable';
            if (nextType !== node.type) {
              node.type = nextType;
              if (nextType === 'step' && !Number.isFinite(node.stepOrder)) {
                node.stepOrder = getNextStepOrder();
              }
              if (nextType !== 'step') {
                node.stepOrder = null;
              }
              applyStepVerticalLayout();
              renderCanvas();
              renderEditor();
              saveLocal();
              return;
            }
          }
        }
        if (showNodeTitleField) {
          node.title = document.getElementById('f_title').value.trim();
        } else {
          node.title = node.type;
        }
        if (shouldShowDecisionField(node)) {
          node.decision = document.getElementById('f_decision').value.trim();
        } else {
          node.decision = '';
        }

        if (node.type === 'variable') {
          node.variableWhenRows = readVariableWhenRows();
          node.variableDefault = document.getElementById('f_variable_default').value.trim();
        }
        if (isSqlNode(node.type)) {
          const previousSqlParamKeys = Array.isArray(node.sqlParams)
            ? node.sqlParams.map((row) => row.key)
            : [];
          node.sql = document.getElementById('f_sql').value;
          node.sqlParams = readSqlParamRows();
          node.sqlParamsText = JSON.stringify(sqlParamsRowsToObject(node.sqlParams));
          node.datasource = document.getElementById('f_datasource').value.trim();
          node.resultMode = document.getElementById('f_result_mode').value;
          const latestSqlParamKeys = node.sqlParams.map((row) => row.key);
          if (JSON.stringify(previousSqlParamKeys) !== JSON.stringify(latestSqlParamKeys)) {
            renderSqlParamRows(node, persistEditorNode);
          }
        }
        if (node.type === 'step') {
          const previousPolicyType = getNodeStepPolicyType(node);
          node.stepPolicyType = document.getElementById('f_step_policy_type').value;
          if (previousPolicyType !== node.stepPolicyType) {
            if (node.stepPolicyType === 'on_pass' && !node.decision) {
              node.decision = 'not exists($.ok)';
              node.failMode = 'single';
              node.messageCn = '';
              node.messageEn = '';
              node.divider = '';
              node.dividerCn = '';
              node.dividerEn = '';
            } else if (node.stepPolicyType === 'none') {
              node.decision = '';
              node.failMode = 'single';
              node.messageCn = '';
              node.messageEn = '';
              node.divider = '';
              node.dividerCn = '';
              node.dividerEn = '';
            } else if (node.stepPolicyType === 'on_fail' && !node.decision) {
              node.decision = 'exists($.ok)';
            }
            renderEditor();
            return;
          }
        }
        if (node.type === 'on_fail' || (node.type === 'step' && getNodeStepPolicyType(node) === 'on_fail')) {
          const previousFailMode = node.failMode;
          node.failMode = document.getElementById('f_fail_mode').value;
          node.messageCn = document.getElementById('f_message_cn').value;
          node.messageEn = document.getElementById('f_message_en').value;
          if (node.failMode === 'sub_repeat' || node.failMode === 'full_repeat') {
            node.divider = (document.getElementById('f_divider') || { value: '' }).value;
            node.dividerCn = (document.getElementById('f_divider_cn') || { value: '' }).value;
            node.dividerEn = (document.getElementById('f_divider_en') || { value: '' }).value;
          } else {
            node.divider = '';
            node.dividerCn = '';
            node.dividerEn = '';
          }
          if (previousFailMode !== node.failMode) {
            renderEditor();
            return;
          }
        }
        if (hasOutputs(node.type)) {
          node.outputRows = readOutputRows();
        } else {
          node.outputRows = [{ field: '' }];
        }
        if (node.type === 'step') {
          node.consumeRows = readConsumeRows();
          node.consumes = formatConsumesTextFromRows(node.consumeRows);
        } else {
          node.consumeRows = [{ stepName: '', alias: '' }];
          node.consumes = '';
        }
        if (node.type === 'step') {
          node.description = document.getElementById('f_description').value.trim();
        } else {
          node.description = '';
        }
        renderCanvas();
        saveLocal();
        statusText.classList.remove('status-warn');
        statusText.innerHTML = `<strong>已自动保存：</strong>${escapeHtml(node.title || node.id)}`;
      };

      if (isSqlNode(node.type)) {
        renderSqlParamRows(node, persistEditorNode);
      }
      if (hasOutputs(node.type)) {
        renderOutputRows(node, persistEditorNode);
      }
      if (node.type === 'step') {
        renderConsumeRows(node, persistEditorNode);
      }
      if (node.type === 'variable') {
        renderVariableWhenRows(node, persistEditorNode);
      }
      [
        'f_title',
        'f_node_kind',
        'f_decision',
        'f_variable_default',
        'f_sql',
        'f_datasource',
        'f_result_mode',
        'f_step_policy_type',
        'f_fail_mode',
        'f_message_cn',
        'f_message_en',
        'f_divider',
        'f_divider_cn',
        'f_divider_en',
        'f_description',
      ].forEach((elementId) => {
        const input = document.getElementById(elementId);
        if (!input) return;
        if (elementId === 'f_step_policy_type') {
          input.addEventListener('input', persistEditorNode);
        }
        input.addEventListener('change', persistEditorNode);
      });

      bindAutocompletes(node);
    }

    datasourceStore = createDatasourceStore({
      datasourceList,
      datasourceStatus,
      storageKey: DATASOURCE_CONFIG_STORAGE_KEY,
      defaultConfigs: DEFAULT_DATASOURCE_CONFIGS,
      renderEditor,
      renderMaterialIcons,
    });

    function renderDatasourceOptionsHtml() {
      return datasourceStore.renderDatasourceOptionsHtml(escapeAttr);
    }

    function getNextStepOrder() {
      const stepOrders = state.nodes
        .filter((item) => item.type === 'step' && Number.isFinite(item.stepOrder))
        .map((item) => item.stepOrder);
      if (!stepOrders.length) return 1;
      return Math.max(...stepOrders) + 1;
    }

    function renderSqlParamRows(node, onChanged) {
      const rowsContainer = document.getElementById('f_sql_params_rows');
      if (!rowsContainer) return;
      const rows = getSqlParamsFromTemplate(node.sql || '', node.sqlParams);
      const drawRows = () => {
        if (!rows.length) {
          rowsContainer.innerHTML = '<div class="muted">未检测到 SQL 命名变量（例如 <code>:user_id</code>）。</div>';
          bindAutocompletes(node);
          return;
        }
        rowsContainer.innerHTML = rows.map((row, index) => `
          <div style="display:grid; grid-template-columns:1fr 1fr auto; gap:6px; margin-bottom:6px; align-items:center;">
            <input data-sql-key="${index}" value="${escapeAttr(row.key || '')}" placeholder="变量名，如 user_id" disabled class="editor-readonly-input" />
            <input data-sql-value="${index}" data-ref-autocomplete="true" value="${escapeAttr(row.value || '')}" placeholder="值，如 $input.user_id 或 100" />
            <span class="muted" style="font-size:0.78rem; text-align:center;">自动</span>
          </div>
        `).join('');
        renderMaterialIcons(rowsContainer);
        rowsContainer.querySelectorAll('[data-sql-value]').forEach((input) => {
          input.addEventListener('change', () => {
            if (typeof onChanged === 'function') onChanged();
          });
        });
        bindAutocompletes(node);
      };
      drawRows();
    }

    function extractSqlParamKeys(sqlTemplate) {
      if (typeof sqlTemplate !== 'string' || !sqlTemplate.trim()) return [];
      const withoutSingleQuote = sqlTemplate.replace(/'(?:''|[^'])*'/g, ' ');
      const withoutDoubleQuote = withoutSingleQuote.replace(/"(?:[^"\\]|\\.)*"/g, ' ');
      const withoutLineComment = withoutDoubleQuote.replace(/--.*$/gm, ' ');
      const cleanedSql = withoutLineComment.replace(/\/\*[\s\S]*?\*\//g, ' ');
      const regex = /:([a-zA-Z_][a-zA-Z0-9_]*)/g;
      const keys = [];
      const seen = new Set();
      let matched = regex.exec(cleanedSql);
      while (matched) {
        const tokenStart = matched.index;
        const previousChar = tokenStart > 0 ? cleanedSql[tokenStart - 1] : '';
        if (previousChar === ':') {
          matched = regex.exec(cleanedSql);
          continue;
        }
        const key = matched[1];
        if (!seen.has(key)) {
          seen.add(key);
          keys.push(key);
        }
        matched = regex.exec(cleanedSql);
      }
      return keys;
    }

    function getSqlParamsFromTemplate(sqlTemplate, existingRows) {
      const extractedKeys = extractSqlParamKeys(sqlTemplate);
      const existingValueMap = {};
      if (Array.isArray(existingRows)) {
        existingRows.forEach((row) => {
          if (!row || typeof row.key !== 'string') return;
          existingValueMap[row.key] = typeof row.value === 'string'
            ? row.value
            : normalizeValueToInput(row.value);
        });
      }
      return extractedKeys.map((key) => ({
        key,
        value: Object.prototype.hasOwnProperty.call(existingValueMap, key)
          ? existingValueMap[key]
          : '',
      }));
    }

    function renderVariableWhenRows(node, onChanged) {
      const rowsContainer = document.getElementById('f_variable_when_rows');
      const addButton = document.getElementById('btnAddVariableWhen');
      if (!rowsContainer || !addButton) return;
      const rows = Array.isArray(node.variableWhenRows) && node.variableWhenRows.length
        ? node.variableWhenRows
        : [{ condition: '', value: '' }];
      const drawRows = () => {
        rowsContainer.innerHTML = rows.map((row, index) => `
          <div class="field-row field-row--double">
            <input data-variable-condition="${index}" data-ref-autocomplete="true" value="${escapeAttr(row.condition || '')}" placeholder="条件，如 $input.amount > 1000" />
            <input data-variable-value="${index}" data-ref-autocomplete="true" value="${escapeAttr(normalizeValueToInput(Object.prototype.hasOwnProperty.call(row, 'value') ? row.value : ''))}" placeholder="赋值，如 1000 或 \"PASS\"" />
            <button class="el-button el-button--danger is-plain is-circle el-button--small field-row-action" type="button" data-variable-remove="${index}" title="删除 when 条件" aria-label="删除 when 条件"><span class="ep-icon">delete_outline</span></button>
          </div>
        `).join('');
        renderMaterialIcons(rowsContainer);
        rowsContainer.querySelectorAll('[data-variable-remove]').forEach((button) => {
          button.addEventListener('click', () => {
            const latestRows = collectVariableWhenDraftRows(rowsContainer);
            rows.splice(0, rows.length, ...latestRows);
            const removeIndex = Number(button.getAttribute('data-variable-remove'));
            rows.splice(removeIndex, 1);
            if (!rows.length) rows.push({ condition: '', value: '' });
            drawRows();
            if (typeof onChanged === 'function') onChanged();
          });
        });
        rowsContainer.querySelectorAll('[data-variable-condition], [data-variable-value]').forEach((input) => {
          input.addEventListener('change', () => {
            if (typeof onChanged === 'function') onChanged();
          });
        });
        bindAutocompletes(node);
      };
      drawRows();
      addButton.addEventListener('click', () => {
        const latestRows = collectVariableWhenDraftRows(rowsContainer);
        rows.splice(0, rows.length, ...latestRows);
        rows.push({ condition: '', value: '' });
        drawRows();
        if (typeof onChanged === 'function') onChanged();
      });
    }

    function renderOutputRows(node, onChanged) {
      const rowsContainer = document.getElementById('f_outputs_rows');
      const addButton = document.getElementById('btnAddOutputRow');
      if (!rowsContainer || !addButton) return;
      const rows = normalizeOutputRows(node.outputRows);
      const drawRows = () => {
        rowsContainer.innerHTML = rows.map((row, index) => `
          <div class="field-row field-row--single">
            <input data-output-field="${index}" value="${escapeAttr(row.field || '')}" placeholder="输出字段名，如 final_amount（字母/数字/下划线）" />
            <button class="el-button el-button--danger is-plain is-circle el-button--small field-row-action" type="button" data-output-remove="${index}" title="删除 output" aria-label="删除 output"><span class="ep-icon">delete_outline</span></button>
          </div>
        `).join('');
        renderMaterialIcons(rowsContainer);
        rowsContainer.querySelectorAll('[data-output-remove]').forEach((button) => {
          button.addEventListener('click', () => {
            const latestRows = collectOutputDraftRows(rowsContainer);
            rows.splice(0, rows.length, ...latestRows);
            const removeIndex = Number(button.getAttribute('data-output-remove'));
            rows.splice(removeIndex, 1);
            if (!rows.length) rows.push({ field: '' });
            drawRows();
            if (typeof onChanged === 'function') onChanged();
          });
        });
        rowsContainer.querySelectorAll('[data-output-field]').forEach((input) => {
          input.addEventListener('change', () => {
            if (typeof onChanged === 'function') onChanged();
          });
        });
      };
      drawRows();
      addButton.addEventListener('click', () => {
        const latestRows = collectOutputDraftRows(rowsContainer);
        rows.splice(0, rows.length, ...latestRows);
        rows.push({ field: '' });
        drawRows();
        if (typeof onChanged === 'function') onChanged();
      });
    }

    function renderConsumeRows(node, onChanged) {
      const rowsContainer = document.getElementById('f_consumes_rows');
      const addButton = document.getElementById('btnAddConsumeRow');
      const datalist = document.getElementById('consumeStepOptions');
      if (!rowsContainer || !addButton || !datalist) return;
      const rows = Array.isArray(node.consumeRows) && node.consumeRows.length
        ? node.consumeRows
        : codecConsumesTextToRows(node.consumes || '');
      if (!rows.length) {
        rows.push({ stepName: '', alias: '' });
      }
      const renderStepOptions = () => {
        const stepIds = getStepIdSuggestions(node.id);
        datalist.innerHTML = stepIds
          .map((stepId) => `<option value="${escapeAttr(stepId)}"></option>`)
          .join('');
      };
      const drawRows = () => {
        renderStepOptions();
        rowsContainer.innerHTML = rows.map((row, index) => `
          <div class="field-row field-row--double">
            <input data-consume-step="${index}" data-ref-autocomplete="true" list="consumeStepOptions" value="${escapeAttr(row.stepName || '')}" placeholder="step_name 或 $steps.xxx" />
            <input data-consume-alias="${index}" value="${escapeAttr(row.alias || '')}" placeholder="alias（必填）" />
            <button class="el-button el-button--danger is-plain is-circle el-button--small field-row-action" type="button" data-consume-remove="${index}" title="删除 consume" aria-label="删除 consume"><span class="ep-icon">delete_outline</span></button>
          </div>
        `).join('');
        renderMaterialIcons(rowsContainer);
        rowsContainer.querySelectorAll('[data-consume-remove]').forEach((button) => {
          button.addEventListener('click', () => {
            const latestRows = collectConsumeDraftRows(rowsContainer);
            rows.splice(0, rows.length, ...latestRows);
            const removeIndex = Number(button.getAttribute('data-consume-remove'));
            rows.splice(removeIndex, 1);
            if (!rows.length) rows.push({ stepName: '', alias: '' });
            drawRows();
            if (typeof onChanged === 'function') onChanged();
          });
        });
        rowsContainer.querySelectorAll('[data-consume-step], [data-consume-alias]').forEach((input) => {
          input.addEventListener('change', () => {
            if (typeof onChanged === 'function') onChanged();
          });
        });
        bindAutocompletes(node);
      };
      drawRows();
      addButton.addEventListener('click', () => {
        const latestRows = collectConsumeDraftRows(rowsContainer);
        rows.splice(0, rows.length, ...latestRows);
        rows.push({ stepName: '', alias: '' });
        drawRows();
        if (typeof onChanged === 'function') onChanged();
      });
    }

    function collectVariableWhenDraftRows(rowsContainer) {
      const conditions = Array.from(rowsContainer.querySelectorAll('[data-variable-condition]'));
      const draftRows = conditions.map((conditionInput) => {
        const rowIndex = conditionInput.getAttribute('data-variable-condition');
        const valueInput = rowIndex === null
          ? null
          : rowsContainer.querySelector(`[data-variable-value="${rowIndex}"]`);
        return {
          condition: conditionInput.value.trim(),
          value: valueInput ? valueInput.value : '',
        };
      });
      return draftRows.length ? draftRows : [{ condition: '', value: '' }];
    }

    function collectConsumeDraftRows(rowsContainer) {
      const stepInputs = Array.from(rowsContainer.querySelectorAll('[data-consume-step]'));
      const aliasInputs = Array.from(rowsContainer.querySelectorAll('[data-consume-alias]'));
      const draftRows = stepInputs.map((input, index) => ({
        stepName: input.value.trim(),
        alias: aliasInputs[index] ? aliasInputs[index].value.trim() : '',
      }));
      return draftRows.length ? draftRows : [{ stepName: '', alias: '' }];
    }

    function collectOutputDraftRows(rowsContainer) {
      const outputInputs = Array.from(rowsContainer.querySelectorAll('[data-output-field]'));
      const draftRows = outputInputs.map((input) => ({
        field: input.value.trim(),
      }));
      return draftRows.length ? draftRows : [{ field: '' }];
    }

    function readSqlParamRows() {
      const sqlInput = document.getElementById('f_sql');
      const sqlTemplate = sqlInput ? sqlInput.value : '';
      const extractedKeys = extractSqlParamKeys(sqlTemplate);
      if (!extractedKeys.length) return [];
      const valueInputs = Array.from(document.querySelectorAll('[data-sql-value]'));
      return extractedKeys.map((key, index) => ({
        key,
        value: valueInputs[index] ? valueInputs[index].value.trim() : '',
      }));
    }

    function readVariableWhenRows() {
      const rowsContainer = document.getElementById('f_variable_when_rows');
      if (!rowsContainer) return [];
      const conditions = Array.from(rowsContainer.querySelectorAll('[data-variable-condition]'));
      return conditions
        .map((conditionInput) => {
          const rowIndex = conditionInput.getAttribute('data-variable-condition');
          if (rowIndex === null) return null;
          const valueInput = rowsContainer.querySelector(`[data-variable-value="${rowIndex}"]`);
          if (!valueInput) return null;
          const condition = conditionInput.value.trim();
          const valueText = valueInput.value;
          if (!condition) return null;
          return {
            condition,
            value: parseInputValue(valueText),
          };
        })
        .filter((row) => row !== null);
    }

    function readConsumeRows() {
      const stepInputs = Array.from(document.querySelectorAll('[data-consume-step]'));
      const aliasInputs = Array.from(document.querySelectorAll('[data-consume-alias]'));
      return stepInputs
        .map((input, index) => ({
          stepName: input.value.trim(),
          alias: aliasInputs[index] ? aliasInputs[index].value.trim() : '',
        }))
        .filter((row) => row.stepName && row.alias);
    }

    function readOutputRows() {
      const outputInputs = Array.from(document.querySelectorAll('[data-output-field]'));
      return outputInputs
        .map((input) => ({
          field: input.value.trim(),
        }))
        .filter((row) => row.field);
    }

    function sqlParamsRowsToObject(rows) {
      const result = {};
      rows.forEach((row) => {
        if (!row.key) return;
        result[row.key] = parseInputValue(row.value);
      });
      return result;
    }

    function objectToSqlParamsRows(rawObject) {
      if (!rawObject || typeof rawObject !== 'object' || Array.isArray(rawObject)) {
        return [];
      }
      const entries = Object.entries(rawObject).map(([key, value]) => ({
        key,
        value: normalizeValueToInput(value),
      }));
      return entries;
    }

    function renderRuntimeInputRows() {
      runtimeInputStore.renderRuntimeInputRows();
    }

    function readRuntimeInputPayload() {
      return runtimeInputStore.readRuntimeInputPayload();
    }

    function loadInputConfigLocal() {
      runtimeInputStore.loadInputConfigLocal();
    }

    function applyStepVerticalLayout() {
      const steps = canvasNodeController
        ? canvasNodeController.getSortedSteps()
        : state.nodes
          .filter((item) => item.type === 'step')
          .sort((left, right) => (left.stepOrder || 0) - (right.stepOrder || 0));
      steps.forEach((stepNode, index) => {
        stepNode.stepOrder = index + 1;
      });
    }

    autocompleteBinder = createAutocompleteBinder({
      state,
      requiredInputRows: REQUIRED_INPUT_ROWS,
      readRuntimeInputPayload,
      getOutputFields,
    });

    function bindAutocompletes(node) {
      autocompleteBinder.bindAutocompletes(node);
    }

    function buildDividerPayload(node) {
      if (!node || (node.failMode !== 'sub_repeat' && node.failMode !== 'full_repeat')) {
        return {};
      }
      const payload = {};
      if (typeof node.divider === 'string' && node.divider !== '') {
        payload.divider = node.divider;
        return payload;
      }
      if (typeof node.dividerCn === 'string' && node.dividerCn !== '') {
        payload.divider_cn = node.dividerCn;
      }
      if (typeof node.dividerEn === 'string' && node.dividerEn !== '') {
        payload.divider_en = node.dividerEn;
      }
      return payload;
    }

    function showElMessage(message, type = 'success') {
      const maybeElementPlusMessage = window.ElementPlus && typeof window.ElementPlus.ElMessage === 'function'
        ? window.ElementPlus.ElMessage
        : (typeof window.ElMessage === 'function' ? window.ElMessage : null);
      if (maybeElementPlusMessage) {
        maybeElementPlusMessage({ message, type });
        return;
      }
      renderFallbackToast(message, type);
    }

    function renderFallbackToast(message, type = 'success') {
      const toastId = 'epFallbackToast';
      const existing = document.getElementById(toastId);
      if (existing) {
        existing.remove();
      }
      const toastEl = document.createElement('div');
      toastEl.id = toastId;
      const tone = type === 'error' ? '#f56c6c' : '#67c23a';
      toastEl.style.cssText = [
        'position:fixed',
        'top:18px',
        'left:50%',
        'transform:translateX(-50%)',
        'z-index:9999',
        'padding:8px 12px',
        'border-radius:6px',
        `background:${tone}`,
        'color:#fff',
        'font-size:13px',
        'box-shadow:0 2px 8px rgba(0,0,0,0.16)',
      ].join(';');
      toastEl.textContent = message;
      document.body.appendChild(toastEl);
      window.setTimeout(() => {
        toastEl.remove();
      }, 1800);
    }

    function toDslObject() {
      return dslCodec.toDslObject();
    }

    function renderDslPreview() {
      const payload = toDslObject();
      const jsonText = JSON.stringify(payload, null, 2);
      if (!dslPreviewCode) return;
      dslPreviewCode.textContent = jsonText;
      uiHighlightCode(dslPreviewCode);
    }

    function openDslPreview() {
      renderDslPreview();
      uiOpenDialog(jsonPreviewDialog);
    }

    function closeDslPreview() {
      uiCloseDialog(jsonPreviewDialog);
    }

    function fromDslObject(payload) {
      return dslCodec.fromDslObject(payload);
    }

    function readDatasourceConfigRows() {
      return datasourceStore.readDatasourceConfigRows();
    }

    function loadDatasourceConfigLocal() {
      datasourceStore.loadDatasourceConfigLocal();
    }

    function saveLocal() {
      localStorage.setItem('execdsl_flow_designer_state_v1', JSON.stringify(state.nodes));
    }

    function normalizeStateNodes() {
      let stepCursor = 1;
      state.nodes = state.nodes.map((node) => {
        const { precheckPolicyType: legacyStepPolicyType, ...rawNode } = node;
        const safeStepOrder = node.type === 'step'
          ? (Number.isFinite(node.stepOrder) ? node.stepOrder : stepCursor)
          : null;
        const normalized = {
          ...rawNode,
          sqlParamsText: typeof node.sqlParamsText === 'string' ? node.sqlParamsText : '{}',
          sqlParams: Array.isArray(node.sqlParams) ? node.sqlParams : objectToSqlParamsRows(parseJsonObjectOrEmpty(typeof node.sqlParamsText === 'string' ? node.sqlParamsText : '{}')),
          datasource: typeof node.datasource === 'string' ? node.datasource : 'saas_db',
          resultMode: typeof node.resultMode === 'string' ? node.resultMode : 'records',
          failMode: typeof node.failMode === 'string' ? node.failMode : 'single',
          stepPolicyType: normalizeStepPolicyType(node.stepPolicyType, legacyStepPolicyType),
          messageCn: typeof node.messageCn === 'string' ? node.messageCn : '',
          messageEn: typeof node.messageEn === 'string' ? node.messageEn : '',
          divider: typeof node.divider === 'string' ? node.divider : '',
          dividerCn: typeof node.dividerCn === 'string' ? node.dividerCn : '',
          dividerEn: typeof node.dividerEn === 'string' ? node.dividerEn : '',
          description: typeof node.description === 'string' ? node.description : '',
          outputRows: normalizeOutputRows(node.outputRows),
          variableValue: typeof node.variableValue === 'string' ? node.variableValue : '',
          variableWhenRows: Array.isArray(node.variableWhenRows)
            ? node.variableWhenRows
            : (typeof node.decision === 'string' && node.decision
              ? [{ condition: node.decision, value: typeof node.variableValue === 'string' ? node.variableValue : '' }]
              : [{ condition: '', value: '' }]
            ),
          variableDefault: typeof node.variableDefault === 'string' ? node.variableDefault : '',
          consumes: typeof node.consumes === 'string' ? node.consumes : '',
          consumeRows: Array.isArray(node.consumeRows) ? node.consumeRows : codecConsumesTextToRows(typeof node.consumes === 'string' ? node.consumes : ''),
          stepOrder: safeStepOrder,
        };
        if (node.type === 'step') {
          stepCursor += 1;
        }
        return normalized;
      });
      const sortedSteps = canvasNodeController
        ? canvasNodeController.getSortedSteps()
        : state.nodes
          .filter((item) => item.type === 'step')
          .sort((left, right) => (left.stepOrder || 0) - (right.stepOrder || 0));
      sortedSteps.forEach((item, index) => {
        item.stepOrder = index + 1;
      });
      applyStepVerticalLayout();
    }

    function parseJsonObjectOrEmpty(rawText) {
      return codecParseJsonObjectOrEmpty(rawText);
    }

    function normalizeOutputRows(outputRows) {
      return codecNormalizeOutputRows(outputRows);
    }

    function formatOutputsText(rowsOrFields) {
      return codecFormatOutputsText(rowsOrFields);
    }

    function getOutputFields(node) {
      return codecGetOutputFields(node);
    }

    function formatVariableWhenSummary(variableWhenRows) {
      return codecFormatVariableWhenSummary(variableWhenRows);
    }

    function formatConsumesTextFromRows(rows) {
      return codecFormatConsumesTextFromRows(rows);
    }

    function normalizeValueToInput(value) {
      return codecNormalizeValueToInput(value);
    }

    function parseInputValue(rawText) {
      return codecParseInputValue(rawText);
    }

    function validateDslIdentifier(value, path) {
      if (!isValidDslIdentifier(value)) {
        throw new Error(`${path} 必须是合法标识符：以字母或下划线开头，且仅包含字母、数字、下划线。`);
      }
    }

    function validateOutputs(outputs, path) {
      if (!Array.isArray(outputs)) {
        return;
      }
      const seenOutputs = new Set();
      outputs.forEach((output, index) => {
        if (typeof output !== 'string' || !output.trim()) {
          throw new Error(`${path}[${index}] 不能为空。`);
        }
        const normalizedOutput = output.trim();
        validateDslIdentifier(normalizedOutput, `${path}[${index}]`);
        if (seenOutputs.has(normalizedOutput)) {
          throw new Error(`${path}[${index}] 重复：${normalizedOutput}`);
        }
        seenOutputs.add(normalizedOutput);
      });
    }

    function validateConsumes(consumes, path) {
      if (!Array.isArray(consumes)) {
        return;
      }
      const seenAliases = new Set();
      consumes.forEach((consume, index) => {
        if (!consume || typeof consume !== 'object' || Array.isArray(consume)) {
          throw new Error(`${path}[${index}] 必须是对象。`);
        }
        if (typeof consume.alias !== 'string' || !consume.alias.trim()) {
          throw new Error(`${path}[${index}].alias 不能为空。`);
        }
        const normalizedAlias = consume.alias.trim();
        validateDslIdentifier(normalizedAlias, `${path}[${index}].alias`);
        if (seenAliases.has(normalizedAlias)) {
          throw new Error(`${path}[${index}].alias 重复：${normalizedAlias}`);
        }
        seenAliases.add(normalizedAlias);
      });
    }

    function validateStepItems(stepItems) {
      stepItems.forEach((stepItem, index) => {
        if (!stepItem || typeof stepItem !== 'object' || Array.isArray(stepItem)) {
          throw new Error(`steps[${index}] 必须是对象。`);
        }
        if (typeof stepItem.name !== 'string' || !stepItem.name.trim()) {
          throw new Error(`steps[${index}].name 不能为空。`);
        }
        validateDslIdentifier(stepItem.name, `steps[${index}].name`);
        validateOutputs(stepItem.outputs, `steps[${index}].outputs`);
        validateConsumes(stepItem.consumes, `steps[${index}].consumes`);
      });
    }

    function validateDslPayload(payload) {
      const allowedTopFields = ['variables', 'steps', 'on_fail'];
      const unknownTopFields = Object.keys(payload).filter((key) => !allowedTopFields.includes(key));
      if (unknownTopFields.length) {
        const legacyFields = unknownTopFields.filter((key) => LEGACY_TOP_LEVEL_FIELDS.has(key));
        if (legacyFields.length) {
          throw new Error(
            `当前 playground 仅支持顶层 variables / steps / on_fail；请先将 ${legacyFields.join(' / ')} 收敛到 step 短路策略或顶层 on_fail。`
          );
        }
        throw new Error(`存在未知顶层字段: ${unknownTopFields.join(', ')}`);
      }
      if (!Array.isArray(payload.steps)) {
        throw new Error('steps 必须是数组。');
      }
      if (!payload.on_fail || typeof payload.on_fail !== 'object' || Array.isArray(payload.on_fail)) {
        throw new Error('on_fail 必须是对象。');
      }
      Object.keys(payload.variables || {}).forEach((variableName) => {
        validateDslIdentifier(variableName, `variables.${variableName}`);
      });
      validateStepItems(payload.steps);
    }

    function loadLocal() {
      const raw = localStorage.getItem('execdsl_flow_designer_state_v1');
      if (!raw) return;
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          state.nodes = parsed;
          normalizeStateNodes();
        }
      } catch (_error) {
        console.warn('加载本地缓存失败，已忽略。');
      }
    }

    function escapeHtml(value) {
      return uiEscapeHtml(value);
    }

    function escapeAttr(value) {
      return uiEscapeAttr(value);
    }

    dslCodec = createDslCodec({
      state,
      validateDslPayload,
      makeNodeId,
      getDefaultDecisionByNodeType,
      sqlParamsRowsToObject,
      objectToSqlParamsRows,
    });

    canvasNodeController = createCanvasNodeController({
      state,
      nodeDefMap: NODE_DEF_MAP,
      fixedTypes: FIXED_TYPES,
      makeNode,
      getNextStepOrder,
      applyStepVerticalLayout,
      renderCanvas,
      renderEditor,
      saveLocal,
      statusText,
      escapeHtml,
    });

    const {
      closeRuntimeDialog,
      closeValidateDialog,
      validateDslNow,
      runDslNow,
      setRunButtonLoading,
    } = initRuntimeDslOperations({
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
    });

    document.getElementById('btnReset').addEventListener('click', () => {
      state.nodes = [];
      state.selectedId = null;
      renderCanvas();
      renderEditor();
      saveLocal();
      statusText.textContent = '画布已清空。';
    });

    document.getElementById('btnImport').addEventListener('click', () => {
      document.getElementById('fileImportJson').click();
    });

    document.getElementById('fileImportJson').addEventListener('change', async (event) => {
      const target = event.target;
      const selectedFile = target.files && target.files[0] ? target.files[0] : null;
      target.value = '';
      if (!selectedFile) return;
      try {
        const rawText = await selectedFile.text();
        const payload = JSON.parse(rawText);
        state.nodes = fromDslObject(payload);
        state.selectedId = state.nodes.length ? state.nodes[0].id : null;
        renderCanvas();
        renderEditor();
        saveLocal();
        statusText.innerHTML = `<strong>导入成功：</strong>${escapeHtml(selectedFile.name)}`;
      } catch (error) {
        statusText.classList.add('status-warn');
        statusText.innerHTML = `<strong>导入失败：</strong>${escapeHtml(error.message || 'JSON 格式不正确或结构无法解析。')}`;
      }
    });

    document.getElementById('btnPreview').addEventListener('click', () => {
      try {
        validateDslPayload(toDslObject());
        openDslPreview();
        statusText.classList.remove('status-warn');
        statusText.textContent = '已打开 JSON 预览（通过基础结构校验）。';
      } catch (error) {
        statusText.classList.add('status-warn');
        statusText.innerHTML = `<strong>校验失败：</strong>${escapeHtml(error.message || '结构不合法')}`;
      }
    });

    document.getElementById('btnClosePreview').addEventListener('click', () => {
      closeDslPreview();
    });

    document.getElementById('btnCopyPreview').addEventListener('click', async () => {
      const json = JSON.stringify(toDslObject(), null, 2);
      try {
        await copyPreviewJson(json);
        statusText.classList.remove('status-warn');
        statusText.textContent = '已复制 DSL JSON。';
        showElMessage('复制成功', 'success');
      } catch (error) {
        statusText.classList.add('status-warn');
        statusText.innerHTML = `<strong>复制失败：</strong>${escapeHtml(error.message || '请手动复制预览内容')}`;
        showElMessage('复制失败', 'error');
      }
    });

    document.getElementById('btnDownloadPreview').addEventListener('click', () => {
      const json = JSON.stringify(toDslObject(), null, 2);
      downloadDslJson(json);
      statusText.classList.remove('status-warn');
      statusText.textContent = '已下载 DSL JSON。';
    });

    document.getElementById('btnValidate').addEventListener('click', () => {
      try {
        validateDslPayload(toDslObject());
        validateDslNow();
      } catch (error) {
        statusText.classList.add('status-warn');
        statusText.innerHTML = `<strong>校验失败：</strong>${escapeHtml(error.message || '结构不合法')}`;
      }
    });

    document.getElementById('btnRun').addEventListener('click', () => {
      try {
        validateDslPayload(toDslObject());
        runDslNow();
      } catch (error) {
        statusText.classList.add('status-warn');
        statusText.innerHTML = `<strong>校验失败：</strong>${escapeHtml(error.message || '结构不合法')}`;
      }
    });

    document.getElementById('btnCloseRuntime').addEventListener('click', () => {
      closeRuntimeDialog();
    });

    document.getElementById('btnCloseValidate').addEventListener('click', () => {
      closeValidateDialog();
    });

    document.querySelectorAll('[data-tab-target]').forEach((tabButton) => {
      tabButton.addEventListener('click', () => {
        const target = tabButton.getAttribute('data-tab-target');
        document.querySelectorAll('[data-tab-target]').forEach((item) => {
          const isActiveTab = item === tabButton;
          item.classList.toggle('active', isActiveTab);
          item.classList.toggle('is-active', isActiveTab);
        });
        document.querySelectorAll('[data-tab-panel]').forEach((panel) => {
          const isActive = panel.getAttribute('data-tab-panel') === target;
          panel.classList.toggle('active', isActive);
        });
      });
    });

    document.getElementById('btnDsAdd').addEventListener('click', () => {
      datasourceStore.appendDatasourceCard();
    });

    if (addInputParamButton) {
      addInputParamButton.addEventListener('click', () => {
        runtimeInputStore.appendInputRow();
      });
    }

    function downloadDslJson(jsonText) {
      const blob = new Blob([jsonText], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const downloadLink = document.createElement('a');
      downloadLink.href = url;
      downloadLink.download = 'execdsl_flow_design.json';
      downloadLink.click();
      URL.revokeObjectURL(url);
    }

    function fallbackCopyText(text) {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.setAttribute('readonly', 'readonly');
      textarea.style.position = 'fixed';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();
      const copySuccess = document.execCommand('copy');
      document.body.removeChild(textarea);
      if (!copySuccess) {
        throw new Error('浏览器不支持自动复制，请手动复制。');
      }
    }

    async function copyPreviewJson(text) {
      if (navigator.clipboard && typeof navigator.clipboard.write === 'function' && window.ClipboardItem) {
        const clipboardItem = new window.ClipboardItem({
          'text/plain': new Blob([text], { type: 'text/plain' }),
        });
        await navigator.clipboard.write([clipboardItem]);
        return;
      }
      if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
        await navigator.clipboard.writeText(text);
        return;
      }
      fallbackCopyText(text);
    }

    loadLocal();
    loadInputConfigLocal();
    renderRuntimeInputRows();
    renderCanvas();
    renderEditor();
    loadDatasourceConfigLocal();
    closeDslPreview();
    setRunButtonLoading(false);
    renderMaterialIcons();
  
