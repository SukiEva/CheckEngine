export function normalizeValueToInput(value) {
  if (value === null || typeof value === 'undefined') {
    return '';
  }
  if (typeof value === 'string') {
    return value;
  }
  return JSON.stringify(value);
}

export function parseInputValue(rawText) {
  if (!rawText || !rawText.trim()) {
    return '';
  }
  const trimmed = rawText.trim();
  const integerLiteralMatch = /^-?\d+$/.test(trimmed);
  if (integerLiteralMatch) {
    try {
      const parsedInteger = BigInt(trimmed);
      const maxSafeInteger = BigInt(Number.MAX_SAFE_INTEGER);
      const minSafeInteger = BigInt(Number.MIN_SAFE_INTEGER);
      if (parsedInteger > maxSafeInteger || parsedInteger < minSafeInteger) {
        return trimmed;
      }
    } catch (_error) {
      return trimmed;
    }
  }
  try {
    return JSON.parse(trimmed);
  } catch (_error) {
    return trimmed;
  }
}

export function parseJsonObjectOrEmpty(rawText) {
  if (!rawText || !rawText.trim()) return {};
  const parsed = JSON.parse(rawText);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('sql_params 必须是 JSON 对象。');
  }
  return parsed;
}

export function normalizeOutputRows(outputRows) {
  if (Array.isArray(outputRows)) {
    const normalizedFromRows = outputRows
      .map((row) => {
        if (typeof row === 'string') {
          return row.trim();
        }
        return row && typeof row.field === 'string' ? row.field.trim() : '';
      })
      .filter(Boolean)
      .map((field) => ({ field }));
    return normalizedFromRows.length ? normalizedFromRows : [{ field: '' }];
  }
  return [{ field: '' }];
}

export function formatOutputsText(rowsOrFields) {
  if (!Array.isArray(rowsOrFields) || !rowsOrFields.length) return '';
  if (typeof rowsOrFields[0] === 'string') {
    return rowsOrFields
      .map((field) => field.trim())
      .filter(Boolean)
      .join(', ');
  }
  return rowsOrFields
    .map((row) => (row && typeof row.field === 'string' ? row.field.trim() : ''))
    .filter(Boolean)
    .join(', ');
}

export function getOutputFields(node) {
  if (!node || typeof node !== 'object') return [];
  const rows = normalizeOutputRows(node.outputRows);
  return rows
    .map((row) => row.field)
    .filter(Boolean);
}

export function formatVariableWhenSummary(variableWhenRows) {
  if (!Array.isArray(variableWhenRows) || !variableWhenRows.length) {
    return '无';
  }
  const normalizedRows = variableWhenRows
    .filter((row) => row && typeof row === 'object')
    .map((row) => ({
      condition: typeof row.condition === 'string' ? row.condition.trim() : '',
      value: normalizeValueToInput(Object.prototype.hasOwnProperty.call(row, 'value') ? row.value : ''),
    }))
    .filter((row) => row.condition);
  if (!normalizedRows.length) {
    return '无';
  }
  const [firstRow] = normalizedRows;
  if (normalizedRows.length === 1) {
    return `${firstRow.condition} => ${firstRow.value || '(空)'}`;
  }
  return `${firstRow.condition} => ${firstRow.value || '(空)'} 等 ${normalizedRows.length} 条`;
}

export function formatConsumesText(consumes) {
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

export function consumesTextToRows(rawText) {
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

export function formatConsumesTextFromRows(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  return rows
    .map((row) => {
      if (!row || typeof row !== 'object') return '';
      const stepName = typeof row.stepName === 'string' ? row.stepName.trim() : '';
      const alias = typeof row.alias === 'string' ? row.alias.trim() : '';
      if (!stepName) return '';
      return alias ? `${stepName}:${alias}` : stepName;
    })
    .filter(Boolean)
    .join(', ');
}

export function normalizeConsumes(rawConsumes) {
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

function buildDividerPayload(node) {
  if (!node || (node.failMode !== 'sub_repeat' && node.failMode !== 'full_repeat')) {
    return {};
  }
  const payload = {};
  if (node.divider) {
    payload.divider = node.divider;
  }
  if (node.dividerCn) {
    payload.divider_cn = node.dividerCn;
  }
  if (node.dividerEn) {
    payload.divider_en = node.dividerEn;
  }
  return payload;
}

export function createDslCodec(options) {
  const {
    state,
    validateDslPayload,
    makeNodeId,
    getDefaultDecisionByNodeType,
    sqlParamsRowsToObject,
    objectToSqlParamsRows,
  } = options;

  function normalizeNodeKey(node, fallbackIndex) {
    const rawKey = (node.title || `${node.type}_${fallbackIndex}`).trim();
    return rawKey
      .replaceAll(/\s+/g, '_')
      .replaceAll(/[^a-zA-Z0-9_]/g, '_');
  }

  function toDslObject() {
    const variables = {};
    const steps = [];
    let onFail = null;

    state.nodes.forEach((node, index) => {
      const key = normalizeNodeKey(node, index + 1);
      const outputs = getOutputFields(node);
      const sqlParams = Array.isArray(node.sqlParams)
        ? sqlParamsRowsToObject(node.sqlParams)
        : parseJsonObjectOrEmpty(node.sqlParamsText || '{}');
      const currentStepOrder = Number.isFinite(node.stepOrder) ? Number(node.stepOrder) : index + 1;

      if (node.type === 'variable') {
        const variableDefault = parseInputValue(node.variableDefault);
        const variableWhenRows = Array.isArray(node.variableWhenRows)
          ? node.variableWhenRows
            .map((whenRow) => {
              const safeWhenRow = whenRow && typeof whenRow === 'object' ? whenRow : {};
              const safeCondition = typeof safeWhenRow.condition === 'string' ? safeWhenRow.condition.trim() : '';
              const safeValue = Object.prototype.hasOwnProperty.call(safeWhenRow, 'value') ? safeWhenRow.value : '';
              return {
                condition: safeCondition,
                value: parseInputValue(typeof safeValue === 'string' ? safeValue : normalizeValueToInput(safeValue)),
              };
            })
            .filter((whenRow) => whenRow.condition)
          : [];
        if (Number.isFinite(node.stepOrder)) {
          steps.push({
            name: key,
            ...(node.description ? { description: node.description } : {}),
            type: 'variable',
            when: variableWhenRows,
            default: variableDefault,
            _step_order: currentStepOrder,
          });
        } else {
          variables[key] = {
            when: variableWhenRows,
            default: variableDefault,
          };
        }
      }

      if (node.type === 'step') {
        const stepPayload = {
          name: key,
          ...(node.description ? { description: node.description } : {}),
          type: 'sql',
          datasource: node.datasource || 'saas_db',
          result_mode: node.resultMode || 'records',
          sql_template: node.sql,
          sql_params: sqlParams,
          consumes: parseConsumesRows(node.consumeRows),
          outputs,
          _step_order: currentStepOrder,
        };
        if (node.precheckPolicyType === 'on_pass') {
          stepPayload.on_pass = {
            decision: node.decision || 'not exists($.ok)',
          };
        } else if (node.precheckPolicyType === 'on_fail') {
          stepPayload.on_fail = {
            decision: node.decision || 'exists($.ok)',
            mode: node.failMode || 'single',
            message_cn: node.messageCn || `${key} 未通过`,
            message_en: node.messageEn || `${key} failed`,
            ...buildDividerPayload(node),
          };
        }
        steps.push(stepPayload);
      }

      if (node.type === 'on_fail') {
        onFail = {
          decision: node.decision || 'exists($steps.some_step.some_output)',
          mode: node.failMode || 'single',
          message_cn: node.messageCn || `${key} 命中失败条件`,
          message_en: node.messageEn || `${key} failure condition matched`,
          ...buildDividerPayload(node),
        };
      }
    });

    const sortedSteps = steps
      .sort((left, right) => left._step_order - right._step_order)
      .map((item) => {
        const { _step_order, ...stepPayload } = item;
        return stepPayload;
      });

    const result = {};
    result.variables = variables;
    result.steps = sortedSteps;
    result.on_fail = onFail || {
      decision: 'exists($steps.some_step.some_output)',
      mode: 'single',
      message_cn: '默认失败',
      message_en: 'default failure',
    };
    return result;
  }

  function fromDslObject(payload) {
    const normalizedPayload = normalizeDslPayload(payload);
    validateDslPayload(normalizedPayload);
    const nextNodes = [];
    const pushNode = (nodeType, title, extra) => {
      nextNodes.push({
        id: makeNodeId(),
        type: nodeType,
        title: title || `${nodeType}_${nextNodes.length + 1}`,
        x: 0,
        y: 0,
        sql: extra.sql || 'SELECT 1 AS ok;',
        sqlParamsText: extra.sqlParamsText || '{}',
        sqlParams: Array.isArray(extra.sqlParams) ? extra.sqlParams : objectToSqlParamsRows(parseJsonObjectOrEmpty(extra.sqlParamsText || '{}')),
        datasource: extra.datasource || 'saas_db',
        resultMode: extra.resultMode || 'records',
        decision: typeof extra.decision === 'string' ? extra.decision : getDefaultDecisionByNodeType(nodeType),
        variableWhenRows: Array.isArray(extra.variableWhenRows) ? extra.variableWhenRows : [],
        variableValue: extra.variableValue || '',
        variableDefault: extra.variableDefault || '',
        failMode: extra.failMode || 'single',
        messageCn: extra.messageCn || '',
        messageEn: extra.messageEn || '',
        divider: extra.divider || '',
        dividerCn: extra.dividerCn || '',
        dividerEn: extra.dividerEn || '',
        outputRows: normalizeOutputRows(extra.outputRows),
        consumes: extra.consumes || '',
        consumeRows: Array.isArray(extra.consumeRows) ? extra.consumeRows : consumesTextToRows(extra.consumes || ''),
        description: extra.description || '',
        precheckPolicyType: extra.precheckPolicyType || 'none',
        stepOrder: Object.prototype.hasOwnProperty.call(extra, 'stepOrder')
          ? extra.stepOrder
          : (nodeType === 'step' ? nextNodes.filter((node) => node.type === 'step').length + 1 : null),
      });
    };

    const variableMap = normalizedPayload.variables || {};
    Object.entries(variableMap).forEach(([key, value]) => {
      const whenItems = Array.isArray(value.when) ? value.when : [];
      const variableDefault = normalizeValueToInput(value.default);
      pushNode('variable', key, {
        variableWhenRows: whenItems.length
          ? whenItems.map((whenItem) => ({
            condition: normalizeValueToInput(whenItem.condition),
            value: normalizeValueToInput(whenItem.value),
          }))
          : [],
        variableDefault,
      });
    });

    (normalizedPayload.steps || []).forEach((item) => {
      if (item.type === 'variable') {
        const variableWhenItems = Array.isArray(item.when) ? item.when : [];
        pushNode('variable', item.name || '', {
          variableWhenRows: variableWhenItems.length
            ? variableWhenItems.map((whenItem) => ({
              condition: normalizeValueToInput(whenItem.condition),
              value: normalizeValueToInput(whenItem.value),
            }))
            : [],
          variableDefault: normalizeValueToInput(item.default),
          consumes: formatConsumesText(item.consumes),
          stepOrder: nextNodes.filter((node) => node.type === 'step' || node.type === 'variable').length + 1,
          consumeRows: normalizeConsumes(item.consumes).map((consume) => ({
            stepName: (consume.from || '').replace(/^\$steps\./, ''),
            alias: consume.alias || '',
          })),
          description: item.description || '',
        });
        return;
      }

      const hasStepPolicy = !!(item.on_fail || item.on_pass);
      const hasOnFail = !!(item.on_fail && typeof item.on_fail === 'object');
      pushNode('step', item.name || '', {
        sql: item.sql_template || '',
        precheckPolicyType: hasStepPolicy ? (hasOnFail ? 'on_fail' : 'on_pass') : 'none',
        decision: hasStepPolicy
          ? (hasOnFail
            ? (item.on_fail && item.on_fail.decision ? item.on_fail.decision : 'exists($.ok)')
            : (item.on_pass && item.on_pass.decision ? item.on_pass.decision : 'not exists($.ok)'))
          : '',
        failMode: item.on_fail && item.on_fail.mode ? item.on_fail.mode : 'single',
        messageCn: item.on_fail && item.on_fail.message_cn ? item.on_fail.message_cn : '',
        messageEn: item.on_fail && item.on_fail.message_en ? item.on_fail.message_en : '',
        divider: item.on_fail && item.on_fail.divider ? item.on_fail.divider : '',
        dividerCn: item.on_fail && item.on_fail.divider_cn ? item.on_fail.divider_cn : '',
        dividerEn: item.on_fail && item.on_fail.divider_en ? item.on_fail.divider_en : '',
        outputRows: normalizeOutputRows(Array.isArray(item.outputs) ? item.outputs : []),
        consumes: formatConsumesText(item.consumes),
        datasource: item.datasource || 'saas_db',
        resultMode: item.result_mode || 'records',
        sqlParamsText: JSON.stringify(item.sql_params || {}, null, 2),
        sqlParams: objectToSqlParamsRows(item.sql_params || {}),
        stepOrder: nextNodes.filter((node) => node.type === 'step' || node.type === 'variable').length + 1,
        consumeRows: normalizeConsumes(item.consumes).map((consume) => ({
          stepName: (consume.from || '').replace(/^\$steps\./, ''),
          alias: consume.alias || '',
        })),
        description: item.description || '',
      });
    });

    if (normalizedPayload.on_fail) {
      pushNode('on_fail', 'on_fail', {
        decision: normalizedPayload.on_fail.decision || 'exists($steps.some_step.some_output)',
        failMode: normalizedPayload.on_fail.mode || 'single',
        messageCn: normalizedPayload.on_fail.message_cn || '',
        messageEn: normalizedPayload.on_fail.message_en || '',
        divider: normalizedPayload.on_fail.divider || '',
        dividerCn: normalizedPayload.on_fail.divider_cn || '',
        dividerEn: normalizedPayload.on_fail.divider_en || '',
      });
    }

    const stepNodes = nextNodes
      .filter((item) => item.type === 'step')
      .sort((left, right) => (left.stepOrder || 0) - (right.stepOrder || 0));
    stepNodes.forEach((stepNode, index) => {
      stepNode.stepOrder = index + 1;
    });
    return nextNodes;
  }

  return {
    toDslObject,
    fromDslObject,
    normalizeNodeKey,
  };
}
