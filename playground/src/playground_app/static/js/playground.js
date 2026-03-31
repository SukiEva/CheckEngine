import { createCanvasNodeController, createCanvasRenderer } from "./components/canvas_components.js";
import { initRuntimeDslOperations } from "./runtime_dsl_operations.js";
import { createDslCodec, formatConsumesTextFromRows as codecFormatConsumesTextFromRows, formatOutputsText as codecFormatOutputsText, formatVariableWhenSummary as codecFormatVariableWhenSummary, getOutputFields as codecGetOutputFields, normalizeOutputRows as codecNormalizeOutputRows, normalizeValueToInput as codecNormalizeValueToInput, parseInputValue as codecParseInputValue, parseJsonObjectOrEmpty as codecParseJsonObjectOrEmpty } from "./dsl/dsl_codec.js";
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
      return node.precheckPolicyType === 'on_fail' || node.precheckPolicyType === 'on_pass';
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
        precheckPolicyType: 'none',
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
      const resolvedPrecheckPolicyType = node.type === 'step' && node.precheckPolicyType === 'on_pass'
        ? 'on_pass'
        : (node.precheckPolicyType === 'none' ? 'none' : 'on_fail');
      const shouldShowDecision = shouldShowDecisionField({
        ...node,
        precheckPolicyType: resolvedPrecheckPolicyType,
      });
      const isPrecheckOnFailPolicy = node.type === 'step' && resolvedPrecheckPolicyType === 'on_fail';
      const showFailPolicyFields = node.type === 'on_fail' || isPrecheckOnFailPolicy;
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
        ${node.type !== 'on_fail' ? `
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
        <div class="field">
          <label>Consumes（step_name + alias）</label>
          <div id="f_consumes_rows"></div>
          <button class="el-button el-button--primary is-plain is-circle el-button--small" id="btnAddConsumeRow" type="button" title="新增 consume" aria-label="新增 consume"><span class="ep-icon">add</span></button>
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
        <div class="field">
          <label>Outputs（与 Consumes 一样按行维护）</label>
          <div id="f_outputs_rows"></div>
          <button class="el-button el-button--primary is-plain is-circle el-button--small" id="btnAddOutputRow" type="button" title="新增 output" aria-label="新增 output"><span class="ep-icon">add</span></button>
          <div class="field-note">每行一个输出字段，不使用逗号分隔。</div>
        </div>
        ` : ''}
        ` : ''}
        ${node.type === 'step' ? `
        <div class="field">
          <label>Step Policy（可选）</label>
          <select id="f_precheck_policy_type">
            <option value="none" ${resolvedPrecheckPolicyType === 'none' ? 'selected' : ''}>none（不短路）</option>
            <option value="on_fail" ${resolvedPrecheckPolicyType === 'on_fail' ? 'selected' : ''}>on_fail（失败短路）</option>
            <option value="on_pass" ${resolvedPrecheckPolicyType === 'on_pass' ? 'selected' : ''}>on_pass（成功短路）</option>
          </select>
        </div>
        ` : ''}
        ${shouldShowDecision ? `
        <div class="field">
          <label>Decision（用于 step/on_fail）</label>
          <input id="f_decision" data-ref-autocomplete="true" value="${escapeAttr(node.decision || '')}" />
        </div>
        ` : ''}
        ${node.type === 'variable' ? `
        <div class="field">
          <label>When 条件与赋值（condition/value）</label>
          <div id="f_variable_when_rows"></div>
          <button class="el-button el-button--primary is-plain is-circle el-button--small" id="btnAddVariableWhen" type="button" title="新增 when 条件" aria-label="新增 when 条件"><span class="ep-icon">add</span></button>
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
        if (node.type !== 'on_fail') {
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
        const currentStepOrder = Number.isFinite(node.stepOrder) ? Number(node.stepOrder) : index + 1;

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
          const previousPolicyType = node.precheckPolicyType === 'on_pass'
            ? 'on_pass'
            : (node.precheckPolicyType === 'none' ? 'none' : 'on_fail');
          node.precheckPolicyType = document.getElementById('f_precheck_policy_type').value;
          if (previousPolicyType !== node.precheckPolicyType) {
            if (node.precheckPolicyType === 'on_pass' && !node.decision) {
              node.decision = 'not exists($.ok)';
              node.failMode = 'single';
              node.messageCn = '';
              node.messageEn = '';
              node.divider = '';
              node.dividerCn = '';
              node.dividerEn = '';
            } else if (node.precheckPolicyType === 'none') {
              node.decision = '';
              node.failMode = 'single';
              node.messageCn = '';
              node.messageEn = '';
              node.divider = '';
              node.dividerCn = '';
              node.dividerEn = '';
            } else if (node.precheckPolicyType === 'on_fail' && !node.decision) {
              node.decision = 'exists($.ok)';
            }
            renderEditor();
            return;
          }
        }
        if (node.type === 'on_fail' || (node.type === 'step' && node.precheckPolicyType === 'on_fail')) {
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
        'f_precheck_policy_type',
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
        if (elementId === 'f_precheck_policy_type') {
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
          bindRuntimeAutocomplete(node);
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
        bindRuntimeAutocomplete(node);
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
          <div style="display:grid; grid-template-columns:1fr 1fr auto; gap:6px; margin-bottom:6px;">
            <input data-variable-condition="${index}" data-ref-autocomplete="true" value="${escapeAttr(row.condition || '')}" placeholder="条件，如 $input.amount > 1000" />
            <input data-variable-value="${index}" data-ref-autocomplete="true" value="${escapeAttr(normalizeValueToInput(Object.prototype.hasOwnProperty.call(row, 'value') ? row.value : ''))}" placeholder="赋值，如 1000 或 \"PASS\"" />
            <button class="el-button el-button--danger is-plain is-circle el-button--small" type="button" data-variable-remove="${index}" title="删除 when 条件" aria-label="删除 when 条件"><span class="ep-icon">delete</span></button>
          </div>
        `).join('');
        renderMaterialIcons(rowsContainer);
        rowsContainer.querySelectorAll('[data-variable-remove]').forEach((button) => {
          button.addEventListener('click', () => {
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
        bindRuntimeAutocomplete(node);
      };
      drawRows();
      addButton.addEventListener('click', () => {
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
          <div style="display:grid; grid-template-columns:minmax(0,1fr) auto; gap:6px; margin-bottom:6px;">
            <input data-output-field="${index}" value="${escapeAttr(row.field || '')}" placeholder="输出字段名，如 final_amount" />
            <button class="el-button el-button--danger is-plain is-circle el-button--small" type="button" data-output-remove="${index}" title="删除 output" aria-label="删除 output"><span class="ep-icon">delete</span></button>
          </div>
        `).join('');
        renderMaterialIcons(rowsContainer);
        rowsContainer.querySelectorAll('[data-output-remove]').forEach((button) => {
          button.addEventListener('click', () => {
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
        : consumesTextToRows(node.consumes || '');
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
          <div style="display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr) auto; gap:6px; margin-bottom:6px;">
            <input data-consume-step="${index}" data-ref-autocomplete="true" list="consumeStepOptions" value="${escapeAttr(row.stepName || '')}" placeholder="step_name 或 $steps.xxx" />
            <input data-consume-alias="${index}" value="${escapeAttr(row.alias || '')}" placeholder="alias（必填）" />
            <button class="el-button el-button--danger is-plain is-circle el-button--small" type="button" data-consume-remove="${index}" title="删除 consume" aria-label="删除 consume"><span class="ep-icon">delete</span></button>
          </div>
        `).join('');
        renderMaterialIcons(rowsContainer);
        rowsContainer.querySelectorAll('[data-consume-remove]').forEach((button) => {
          button.addEventListener('click', () => {
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
        bindRuntimeAutocomplete(node);
      };
      drawRows();
      addButton.addEventListener('click', () => {
        rows.push({ stepName: '', alias: '' });
        drawRows();
        if (typeof onChanged === 'function') onChanged();
      });
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

    function normalizeInputRows(rows) {
      return rows;
    }

    function renderRuntimeInputRows() {
      runtimeInputStore.renderRuntimeInputRows();
    }

    function readRuntimeInputRows() {
      return runtimeInputStore.readRuntimeInputRows();
    }

    function readRuntimeInputPayload() {
      return runtimeInputStore.readRuntimeInputPayload();
    }

    function saveInputConfigLocal() {
      runtimeInputStore.saveInputConfigLocal();
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
      normalizeValueToInput,
    });

    function bindAutocompletes(node) {
      autocompleteBinder.bindAutocompletes(node);
    }

    const runtimeAutocompleteEl = document.createElement('div');
    runtimeAutocompleteEl.className = 'autocomplete-panel';
    runtimeAutocompleteEl.style.position = 'fixed';
    runtimeAutocompleteEl.style.left = '0';
    runtimeAutocompleteEl.style.top = '0';
    runtimeAutocompleteEl.style.width = 'min(420px, 80vw)';
    runtimeAutocompleteEl.style.display = 'none';
    runtimeAutocompleteEl.style.zIndex = '60';
    document.body.appendChild(runtimeAutocompleteEl);
    const runtimeAutocompleteState = {
      target: null,
      matches: [],
      activeIndex: 0,
      range: null,
    };

    function getReferenceRange(target) {
      const start = target.selectionStart || 0;
      const end = target.selectionEnd || 0;
      if (start !== end) return null;
      const value = target.value || '';
      let tokenStart = start;
      while (tokenStart > 0 && /[\$.\w]/.test(value[tokenStart - 1])) {
        tokenStart -= 1;
      }
      let tokenEnd = start;
      while (tokenEnd < value.length && /[\$.\w]/.test(value[tokenEnd])) {
        tokenEnd += 1;
      }
      const token = value.slice(tokenStart, tokenEnd);
      if (!token.startsWith('$')) return null;
      return { start: tokenStart, end: tokenEnd, token };
    }

    function hideRuntimeAutocomplete() {
      runtimeAutocompleteState.target = null;
      runtimeAutocompleteState.matches = [];
      runtimeAutocompleteState.activeIndex = 0;
      runtimeAutocompleteState.range = null;
      runtimeAutocompleteEl.style.display = 'none';
      runtimeAutocompleteEl.innerHTML = '';
    }

    function renderRuntimeAutocomplete() {
      runtimeAutocompleteEl.innerHTML = runtimeAutocompleteState.matches
        .map((item, index) => `
          <button type="button" class="autocomplete-item${index === runtimeAutocompleteState.activeIndex ? ' active' : ''}" data-ac-index="${index}">
            ${escapeHtml(item)}
          </button>
        `)
        .join('');
      runtimeAutocompleteEl.style.display = runtimeAutocompleteState.matches.length ? 'grid' : 'none';
      runtimeAutocompleteEl.querySelectorAll('[data-ac-index]').forEach((button) => {
        button.addEventListener('mousedown', (event) => {
          event.preventDefault();
          const index = Number(button.getAttribute('data-ac-index'));
          applyRuntimeAutocomplete(index);
        });
      });
    }

    function applyRuntimeAutocomplete(index) {
      const match = runtimeAutocompleteState.matches[index];
      const target = runtimeAutocompleteState.target;
      const range = runtimeAutocompleteState.range;
      if (!match || !target || !range) {
        hideRuntimeAutocomplete();
        return;
      }
      const value = target.value || '';
      target.value = `${value.slice(0, range.start)}${match}${value.slice(range.end)}`;
      const nextCaret = range.start + match.length;
      target.focus();
      target.setSelectionRange(nextCaret, nextCaret);
      hideRuntimeAutocomplete();
    }

    function positionRuntimeAutocomplete(target) {
      const rect = target.getBoundingClientRect();
      runtimeAutocompleteEl.style.left = `${Math.max(12, rect.left)}px`;
      runtimeAutocompleteEl.style.top = `${Math.min(window.innerHeight - 20, rect.bottom + 8)}px`;
    }

    function bindRuntimeAutocomplete(currentNode) {
      const updateAutocomplete = (target) => {
        if (!(target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement)) {
          hideRuntimeAutocomplete();
          return;
        }
        if (target.dataset.refAutocomplete !== 'true') {
          hideRuntimeAutocomplete();
          return;
        }
        const range = getReferenceRange(target);
        if (!range) {
          hideRuntimeAutocomplete();
          return;
        }
        const token = range.token.toLowerCase();
        const matches = getRuntimePathSuggestions(currentNode)
          .filter((item) => item.toLowerCase().startsWith(token))
          .slice(0, 12);
        if (!matches.length) {
          hideRuntimeAutocomplete();
          return;
        }
        runtimeAutocompleteState.target = target;
        runtimeAutocompleteState.matches = matches;
        runtimeAutocompleteState.activeIndex = 0;
        runtimeAutocompleteState.range = range;
        positionRuntimeAutocomplete(target);
        renderRuntimeAutocomplete();
      };

      editorPanel.querySelectorAll('[data-ref-autocomplete="true"]').forEach((inputEl) => {
        if (inputEl.dataset.runtimeAcBound === 'true') {
          return;
        }
        inputEl.dataset.runtimeAcBound = 'true';
        inputEl.addEventListener('focus', () => updateAutocomplete(inputEl));
        inputEl.addEventListener('input', () => updateAutocomplete(inputEl));
        inputEl.addEventListener('click', () => updateAutocomplete(inputEl));
        inputEl.addEventListener('blur', () => {
          window.setTimeout(hideRuntimeAutocomplete, 120);
        });
        inputEl.addEventListener('keydown', (event) => {
          if (!runtimeAutocompleteState.matches.length || runtimeAutocompleteState.target !== inputEl) return;
          if (event.key === 'ArrowDown') {
            event.preventDefault();
            runtimeAutocompleteState.activeIndex = (runtimeAutocompleteState.activeIndex + 1) % runtimeAutocompleteState.matches.length;
            renderRuntimeAutocomplete();
            return;
          }
          if (event.key === 'ArrowUp') {
            event.preventDefault();
            runtimeAutocompleteState.activeIndex = (runtimeAutocompleteState.activeIndex - 1 + runtimeAutocompleteState.matches.length) % runtimeAutocompleteState.matches.length;
            renderRuntimeAutocomplete();
            return;
          }
          if (event.key === 'Enter') {
            event.preventDefault();
            applyRuntimeAutocomplete(runtimeAutocompleteState.activeIndex);
            return;
          }
          if (event.key === 'Escape') {
            hideRuntimeAutocomplete();
          }
        });
      });
    }

    function createAutocomplete(config) {
      const {
        inputEl,
        panelEl,
        getKeyword,
        buildOptions,
        applySuggestion,
      } = config;
      let activeIndex = -1;
      let currentOptions = [];
      let waitTimer = 0;

      const hidePanel = () => {
        panelEl.classList.remove('show');
        panelEl.innerHTML = '';
        activeIndex = -1;
        currentOptions = [];
      };

      const renderWaiting = () => {
        panelEl.classList.add('show');
        panelEl.innerHTML = '<div class="autocomplete-waiting">联想中...</div>';
      };

      const renderOptions = (options) => {
        currentOptions = options;
        activeIndex = options.length ? 0 : -1;
        if (!options.length) {
          panelEl.classList.remove('show');
          panelEl.innerHTML = '';
          return;
        }
        panelEl.classList.add('show');
        panelEl.innerHTML = options
          .map((item, index) => `
            <button type="button" class="autocomplete-item${index === activeIndex ? ' active' : ''}" data-value="${escapeAttr(item)}">${escapeHtml(item)}</button>
          `)
          .join('');
        panelEl.querySelectorAll('.autocomplete-item').forEach((button, index) => {
          button.addEventListener('mousedown', (event) => {
            event.preventDefault();
            applySuggestion(options[index]);
            hidePanel();
          });
        });
      };

      const updateActiveVisual = () => {
        panelEl.querySelectorAll('.autocomplete-item').forEach((button, index) => {
          button.classList.toggle('active', index === activeIndex);
        });
      };

      const requestSuggestions = () => {
        const keyword = getKeyword();
        if (!keyword) {
          hidePanel();
          return;
        }
        renderWaiting();
        clearTimeout(waitTimer);
        waitTimer = window.setTimeout(() => {
          const options = buildOptions(keyword).slice(0, 10);
          renderOptions(options);
        }, 220);
      };

      inputEl.addEventListener('focus', requestSuggestions);
      inputEl.addEventListener('input', requestSuggestions);
      inputEl.addEventListener('blur', () => {
        window.setTimeout(hidePanel, 120);
      });
      inputEl.addEventListener('keydown', (event) => {
        if (!panelEl.classList.contains('show') || !currentOptions.length) return;
        if (event.key === 'ArrowDown') {
          event.preventDefault();
          activeIndex = (activeIndex + 1) % currentOptions.length;
          updateActiveVisual();
          return;
        }
        if (event.key === 'ArrowUp') {
          event.preventDefault();
          activeIndex = (activeIndex - 1 + currentOptions.length) % currentOptions.length;
          updateActiveVisual();
          return;
        }
        if (event.key === 'Enter') {
          event.preventDefault();
          if (activeIndex >= 0 && activeIndex < currentOptions.length) {
            applySuggestion(currentOptions[activeIndex]);
            hidePanel();
          }
          return;
        }
        if (event.key === 'Escape') {
          hidePanel();
        }
      });
    }

    function getStepIdSuggestions(currentNodeId) {
      return state.nodes
        .filter((item) => item.type === 'step' && item.id !== currentNodeId)
        .map((item, index) => normalizeNodeKey(item, index + 1));
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
      REQUIRED_INPUT_ROWS.forEach((requiredRow) => {
        if (!Object.prototype.hasOwnProperty.call(inputPayload, requiredRow.key)) {
          basics.push(`$input.${requiredRow.key}`);
        }
      });
      const runtimePaths = [];
      const currentNodeOutputFields = getOutputFields(currentNode);
      const currentNodeLocalPaths = currentNodeOutputFields.map((field) => `$.${field}`);

      state.nodes.forEach((item, index) => {
        const nodeKey = normalizeNodeKey(item, index + 1);
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

    function normalizeNodeKey(node, fallbackIndex) {
      const rawKey = (node.title || `${node.type}_${fallbackIndex}`).trim();
      return rawKey
        .replaceAll(/\s+/g, '_')
        .replaceAll(/[^a-zA-Z0-9_]/g, '_');
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

    function updateDatasourceOptions(datasourceConfigs) {
      return datasourceConfigs;
    }

    function createDatasourceCard(data) {
      return data;
    }

    function readDatasourceConfigRows() {
      return datasourceStore.readDatasourceConfigRows();
    }

    function renderDatasourceCards(datasourceItems) {
      return datasourceItems;
    }

    function normalizeDatasourceConfigs(rawDatasourceConfigs) {
      return rawDatasourceConfigs;
    }

    function loadDatasourceConfigLocal() {
      datasourceStore.loadDatasourceConfigLocal();
    }

    function saveDatasourceConfigLocal() {
      datasourceStore.saveDatasourceConfigLocal();
    }

    function saveLocal() {
      localStorage.setItem('execdsl_flow_designer_state_v1', JSON.stringify(state.nodes));
    }

    function normalizeStateNodes() {
      let stepCursor = 1;
      state.nodes = state.nodes.map((node) => {
        const safeStepOrder = node.type === 'step'
          ? (Number.isFinite(node.stepOrder) ? node.stepOrder : stepCursor)
          : null;
        const normalized = {
          ...node,
          sqlParamsText: typeof node.sqlParamsText === 'string' ? node.sqlParamsText : '{}',
          sqlParams: Array.isArray(node.sqlParams) ? node.sqlParams : objectToSqlParamsRows(parseJsonObjectOrEmpty(typeof node.sqlParamsText === 'string' ? node.sqlParamsText : '{}')),
          datasource: typeof node.datasource === 'string' ? node.datasource : 'saas_db',
          resultMode: typeof node.resultMode === 'string' ? node.resultMode : 'records',
          failMode: typeof node.failMode === 'string' ? node.failMode : 'single',
          precheckPolicyType: typeof node.precheckPolicyType === 'string' ? node.precheckPolicyType : 'none',
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
          consumeRows: Array.isArray(node.consumeRows) ? node.consumeRows : consumesTextToRows(typeof node.consumes === 'string' ? node.consumes : ''),
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

    function parseConsumesRows(rows) {
      if (!Array.isArray(rows) || !rows.length) return [];
      return rows
        .filter((row) => row && typeof row === 'object')
        .map((row) => {
          const stepName = typeof row.stepName === 'string' ? row.stepName.trim() : '';
          const alias = typeof row.alias === 'string' ? row.alias.trim() : '';
          if (!stepName || !alias) return null;
          const fromPath = stepName.startsWith('$') ? stepName : `$steps.${stepName}`;
          return {
            from: fromPath,
            alias,
          };
        })
        .filter(Boolean);
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

    function formatConsumesText(consumes) {
      if (!Array.isArray(consumes)) return '';
      return consumes
        .map((consume) => {
          const fromPath = consume && typeof consume.from === 'string' ? consume.from : '';
          const alias = consume && typeof consume.alias === 'string' ? consume.alias : '';
          const compactFrom = fromPath.replace(/^\$steps\./, '');
          if (!compactFrom) return '';
          if (alias && alias !== compactFrom) {
            return `${compactFrom}:${alias}`;
          }
          return compactFrom;
        })
        .filter(Boolean)
        .join(', ');
    }

    function consumesTextToRows(rawText) {
      if (!rawText || !rawText.trim()) return [{ stepName: '', alias: '' }];
      return rawText
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
        .map((token) => {
          const [rawName, rawAlias] = token.split(':').map((part) => part.trim());
          return {
            stepName: rawName || '',
            alias: rawAlias || '',
          };
        });
    }

    function formatConsumesTextFromRows(rows) {
      return codecFormatConsumesTextFromRows(rows);
    }

    function normalizeDslPayload(payload) {
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
        throw new Error('DSL 顶层必须是对象。');
      }
      const normalized = { ...payload };
      normalized.variables = normalizeVariables(normalized.variables || {});
      normalized.steps = normalizeSteps(normalized.steps || []);
      normalized.on_fail = normalizeFailPolicy(normalized.on_fail || {});
      return normalized;
    }

    function normalizeVariables(rawVariables) {
      const normalized = {};
      Object.entries(rawVariables || {}).forEach(([name, value]) => {
        if (!value || typeof value !== 'object') return;
        if (Array.isArray(value.when) || Object.prototype.hasOwnProperty.call(value, 'default')) {
          normalized[name] = value;
          return;
        }
        const assign = value.assign_by_condition || {};
        normalized[name] = {
          when: assign.decision ? [{ condition: assign.decision, value: assign.value }] : [],
          default: Object.prototype.hasOwnProperty.call(assign, 'value') ? assign.value : null,
        };
      });
      return normalized;
    }

    function normalizeFailPolicy(rawPolicy) {
      const safePolicy = rawPolicy && typeof rawPolicy === 'object' && !Array.isArray(rawPolicy) ? rawPolicy : {};
      const message = safePolicy.message && typeof safePolicy.message === 'object' ? safePolicy.message : {};
      const normalized = {
        ...safePolicy,
        mode: safePolicy.mode || 'single',
        message_cn: safePolicy.message_cn || message.zh || '',
        message_en: safePolicy.message_en || message.en || '',
        divider: typeof safePolicy.divider === 'string' ? safePolicy.divider : '',
        divider_cn: typeof safePolicy.divider_cn === 'string' ? safePolicy.divider_cn : '',
        divider_en: typeof safePolicy.divider_en === 'string' ? safePolicy.divider_en : '',
      };
      delete normalized.message;
      return normalized;
    }

    function normalizeSteps(rawSteps) {
      if (!Array.isArray(rawSteps)) return [];
      return rawSteps.map((item) => ({
        ...item,
        name: item.name || item.id || '',
        sql_template: item.sql_template || item.sql || '',
        sql_params: item.sql_params || {},
        consumes: normalizeConsumes(item.consumes),
        ...(item.on_fail && typeof item.on_fail === 'object' && !Array.isArray(item.on_fail)
          ? { on_fail: normalizeFailPolicy(item.on_fail) }
          : {}),
        ...(item.on_pass && typeof item.on_pass === 'object' && !Array.isArray(item.on_pass)
          ? { on_pass: { decision: item.on_pass.decision || '' } }
          : {}),
      }));
    }

    function normalizeConsumes(rawConsumes) {
      if (!Array.isArray(rawConsumes)) return [];
      return rawConsumes.map((consume) => {
        if (consume && typeof consume === 'object' && !Array.isArray(consume)) {
          return {
            from: consume.from || '',
            alias: consume.alias || 'cte_alias',
          };
        }
        if (typeof consume === 'string') {
          const compact = consume.replace(/^\$steps\./, '');
          return {
            from: consume.startsWith('$') ? consume : `$steps.${compact}`,
            alias: compact.replace(/[^a-zA-Z0-9_]/g, '_') || 'cte_alias',
          };
        }
        return { from: '', alias: 'cte_alias' };
      });
    }

    function normalizeValueToInput(value) {
      return codecNormalizeValueToInput(value);
    }

    function parseInputValue(rawText) {
      return codecParseInputValue(rawText);
    }

    function validateDslPayload(payload) {
      const allowedTopFields = ['variables', 'steps', 'on_fail'];
      const unknownTopFields = Object.keys(payload).filter((key) => !allowedTopFields.includes(key));
      if (unknownTopFields.length) {
        throw new Error(`存在未知顶层字段: ${unknownTopFields.join(', ')}`);
      }
      if (!Array.isArray(payload.steps)) {
        throw new Error('steps 必须是数组。');
      }
      if (!payload.on_fail || typeof payload.on_fail !== 'object' || Array.isArray(payload.on_fail)) {
        throw new Error('on_fail 必须是对象。');
      }
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
  
