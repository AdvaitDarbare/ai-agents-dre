import React, { useState, useEffect } from "react";
import { Loader2, Table, CheckCircle2, XCircle, AlertTriangle, ChevronDown, ChevronRight, AlertCircle } from "lucide-react";
import { getDatasetProfile, getContract } from "../../api";

const SchemaValidationTable = ({ datasetName }) => {
  const [contract, setContract] = useState(null);
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(true);
  const [expandedRows, setExpandedRows] = useState(new Set());

  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        const [cRes, pRes] = await Promise.all([
          getContract(datasetName),
          getDatasetProfile(datasetName).catch(() => null),
        ]);
        setContract(cRes.data);
        if (pRes) setProfile(pRes.data);
      } catch (e) {
        console.error("Failed to load schema data", e);
      } finally {
        setLoading(false);
      }
    };
    fetch();
  }, [datasetName]);

  const toggleRow = (columnName) => {
    const newExpanded = new Set(expandedRows);
    if (newExpanded.has(columnName)) {
      newExpanded.delete(columnName);
    } else {
      newExpanded.add(columnName);
    }
    setExpandedRows(newExpanded);
  };

  if (loading)
    return (
      <div className="flex justify-center p-8">
        <Loader2 className="animate-spin text-slate-300" size={20} />
      </div>
    );

  // Parse the YAML contract to extract column definitions
  let columns = [];
  try {
    const yaml = contract?.yaml_content || contract?.content || "";

    // Find the columns section and split by "- name:"
    const columnsMatch = yaml.match(/columns:\s*\n([\s\S]+)/);
    if (columnsMatch) {
      const columnsSection = columnsMatch[1];
      const columnBlocks = columnsSection.split(/\n- name:\s*/);

      columnBlocks.forEach((colBlock, idx) => {
        // Skip empty entries
        if (!colBlock.trim()) return;

        // Extract column name from the first line
        const lines = colBlock.split('\n');
        let name = lines[0].trim();

        // Handle first column which may still have "- name:" prefix
        if (name.startsWith('- name:')) {
          name = name.replace(/^- name:\s*/, '').trim();
        }

        if (!name) return;

        const typeMatch = colBlock.match(/data_type:\s*(\S+)/);
        const nullableMatch = colBlock.match(/nullable:\s*(\S+)/);
        const patternMatch = colBlock.match(/pattern:\s*'([^']+)'|pattern:\s*"([^"]+)"/);
        const pkMatch = colBlock.match(/isPrimaryKey:\s*(\S+)/);
        const minMatch = colBlock.match(/min_value:\s*(\S+)/);
        const maxMatch = colBlock.match(/max_value:\s*(\S+)/);
        const allowedMatch = colBlock.match(/allowed_values:\s*\[([^\]]+)\]/);

        const constraints = [];
        if (pkMatch && pkMatch[1] === "true") constraints.push({ type: "PK", value: "Primary Key" });
        if (patternMatch) {
          const pattern = patternMatch[1] || patternMatch[2];
          constraints.push({ type: "pattern", value: pattern });
        }
        if (minMatch) constraints.push({ type: "min", value: minMatch[1] });
        if (maxMatch) constraints.push({ type: "max", value: maxMatch[1] });
        if (allowedMatch) constraints.push({ type: "allowed", value: allowedMatch[1].trim() });

        columns.push({
          name,
          expectedType: typeMatch ? typeMatch[1] : "?",
          nullable: nullableMatch ? nullableMatch[1] === "true" : true,
          constraints,
        });
      });
    }
  } catch (e) {
    console.error("Failed to parse contract YAML", e);
  }

  if (!columns.length)
    return (
      <div className="text-center p-6 text-slate-400 text-sm italic">
        No schema contract found for {datasetName}.
      </div>
    );

  // Cross-reference with profile data
  const profileCols = profile?.column_profiles || {};

  // Calculate summary stats
  const totalViolations = columns.reduce((sum, col) => {
    const profCol = profileCols[col.name];
    return sum + (profCol?.violations?.length || 0);
  }, 0);
  const failedColumns = columns.filter(col => {
    const profCol = profileCols[col.name];
    return profCol?.violations?.length > 0;
  }).length;

  return (
    <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
      <div className="flex items-center gap-4 p-8 pb-6">
        <div className="p-2 bg-indigo-50 text-indigo-500 rounded-lg">
          <Table size={16} />
        </div>
        <h4 className="text-base font-black uppercase text-slate-600 tracking-wider">
          Schema Validation Detail
        </h4>
        <div className="ml-auto flex items-center gap-2">
          {totalViolations > 0 && (
            <span className="text-xs font-bold text-rose-600 bg-rose-50 px-3 py-1 rounded-full border border-rose-200">
              {totalViolations} violations
            </span>
          )}
          {failedColumns > 0 && (
            <span className="text-xs font-bold text-amber-600 bg-amber-50 px-3 py-1 rounded-full border border-amber-200">
              {failedColumns}/{columns.length} columns failing
            </span>
          )}
          {totalViolations === 0 && (
            <span className="text-xs font-bold text-emerald-600 bg-emerald-50 px-3 py-1 rounded-full border border-emerald-200">
              All {columns.length} columns valid
            </span>
          )}
        </div>
      </div>
      <div className="overflow-x-auto px-8 pb-8">
        <table className="w-full text-base">
          <thead className="bg-slate-50 text-left text-sm font-black uppercase text-slate-500 sticky top-0">
            <tr>
              <th className="p-4 pl-4 rounded-l-lg w-4"></th>
              <th className="p-4">Column</th>
              <th className="p-4">Expected Type</th>
              <th className="p-4">Nullable</th>
              <th className="p-4">Constraints</th>
              <th className="p-4">Actual Stats</th>
              <th className="p-4 rounded-r-lg text-center">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {columns.map((col) => {
              const profCol = profileCols[col.name];
              const violations = profCol?.violations || [];
              const hasViolations = violations.length > 0;
              const isExpanded = expandedRows.has(col.name);

              return (
                <React.Fragment key={col.name}>
                  <tr
                    className={`hover:bg-slate-50/50 transition-colors cursor-pointer ${hasViolations ? 'bg-rose-50/20' : ''}`}
                    onClick={() => hasViolations && toggleRow(col.name)}
                  >
                    <td className="p-4 pl-4">
                      {hasViolations ? (
                        isExpanded ? (
                          <ChevronDown size={16} className="text-slate-400" />
                        ) : (
                          <ChevronRight size={16} className="text-slate-400" />
                        )
                      ) : null}
                    </td>
                    <td className="p-4 font-bold text-slate-700 text-base">
                      {col.name}
                      {hasViolations && (
                        <span className="ml-2 text-xs bg-rose-100 text-rose-600 px-2 py-0.5 rounded font-black">
                          {violations.length} issue{violations.length > 1 ? 's' : ''}
                        </span>
                      )}
                    </td>
                    <td className="p-4">
                      <code className="text-sm bg-slate-100 px-2 py-1 rounded text-slate-600">
                        {col.expectedType}
                      </code>
                      {profCol?.type && profCol.type !== col.expectedType && (
                        <span className="ml-1 text-xs text-amber-600">
                          (actual: {profCol.type})
                        </span>
                      )}
                    </td>
                    <td className="p-4 text-base text-slate-500">
                      {col.nullable ? (
                        <span className="text-emerald-600">✓ Yes</span>
                      ) : (
                        <span className="text-slate-700 font-semibold">✗ No</span>
                      )}
                    </td>
                    <td className="p-4">
                      <div className="flex flex-wrap gap-1">
                        {col.constraints.length > 0 ? (
                          col.constraints.map((c, i) => (
                            <span
                              key={i}
                              className="text-xs bg-orange-50 text-orange-600 px-2 py-1 rounded font-bold border border-orange-200"
                            >
                              {c.type === "PK" ? c.value : `${c.type}: ${c.value}`}
                            </span>
                          ))
                        ) : (
                          <span className="text-sm text-slate-300">-</span>
                        )}
                      </div>
                    </td>
                    <td className="p-4">
                      {profCol ? (
                        <div className="text-sm text-slate-500 space-y-0.5">
                          <div>
                            Rows: <span className="font-bold text-slate-700">{profCol.total_rows?.toLocaleString()}</span>
                          </div>
                          <div>
                            Nulls: <span className={`font-bold ${profCol.null_rate > 0 ? 'text-amber-600' : 'text-emerald-600'}`}>
                              {profCol.null_count?.toLocaleString()} ({(profCol.null_rate * 100).toFixed(1)}%)
                            </span>
                          </div>
                          <div>
                            Unique: <span className="font-bold text-slate-700">{profCol.unique_count?.toLocaleString()}</span>
                          </div>
                        </div>
                      ) : (
                        <span className="text-sm text-slate-300">No data</span>
                      )}
                    </td>
                    <td className="p-4 text-center">
                      {hasViolations ? (
                        <XCircle size={18} className="text-rose-500 inline" />
                      ) : (
                        <CheckCircle2 size={18} className="text-emerald-500 inline" />
                      )}
                    </td>
                  </tr>
                  {isExpanded && hasViolations && (
                    <tr>
                      <td colSpan="7" className="p-0">
                        <div className="bg-rose-50/50 border-l-4 border-rose-400 p-4 ml-4 mr-4 mb-2">
                          <div className="flex items-start gap-2 mb-3">
                            <AlertCircle size={16} className="text-rose-600 mt-0.5" />
                            <div className="flex-1">
                              <h5 className="text-sm font-black text-rose-700 uppercase tracking-wide mb-2">
                                Validation Failures for "{col.name}"
                              </h5>
                              <div className="space-y-3">
                                {profCol.violation_examples?.map((violationData, idx) => {
                                  // Map violation types to UI styling
                                  const typeMap = {
                                    "NULL": { label: "Null Constraint", icon: "🚫", color: "rose" },
                                    "PATTERN": { label: "Pattern Validation", icon: "📝", color: "amber" },
                                    "RANGE_MIN": { label: "Range Check (Min)", icon: "📊", color: "orange" },
                                    "RANGE_MAX": { label: "Range Check (Max)", icon: "📊", color: "orange" },
                                    "DUPLICATE": { label: "Uniqueness", icon: "🔄", color: "purple" },
                                    "ALLOWED_VALUES": { label: "Allowed Values", icon: "📋", color: "blue" }
                                  };

                                  const vType = typeMap[violationData.type] || { label: "Unknown", icon: "⚠️", color: "slate" };
                                  const examples = violationData.examples || [];

                                  return (
                                    <div key={idx} className="bg-white rounded-lg border border-rose-200 shadow-sm overflow-hidden">
                                      <div className="flex items-start gap-2 p-4 bg-rose-50/50">
                                        <span className="text-lg">{vType.icon}</span>
                                        <div className="flex-1">
                                          <div className="flex items-center gap-2 mb-1">
                                            <span className={`text-xs font-black uppercase px-3 py-1 rounded bg-${vType.color}-100 text-${vType.color}-700 border border-${vType.color}-300`}>
                                              {vType.label}
                                            </span>
                                            <span className="text-sm text-slate-600">
                                              {violationData.count?.toLocaleString()} rows affected
                                            </span>
                                          </div>
                                          <p className="text-sm text-slate-700 font-medium">
                                            {violations[idx]}
                                          </p>
                                        </div>
                                      </div>

                                      {examples.length > 0 && (
                                        <div className="p-4 bg-slate-50">
                                          <div className="text-sm font-bold text-slate-500 uppercase mb-2">
                                            Sample Violating Rows ({examples.length})
                                          </div>
                                          <div className="space-y-2">
                                            {examples.slice(0, 5).map((row, rowIdx) => (
                                              <div key={rowIdx} className="bg-white rounded border border-slate-200 p-2 text-sm font-mono">
                                                <div className="grid grid-cols-2 gap-x-3 gap-y-1">
                                                  {Object.entries(row).map(([key, value]) => (
                                                    <div key={key} className="flex">
                                                      <span className="text-slate-500 font-bold w-32 truncate">{key}:</span>
                                                      <span className={`flex-1 ${value === null ? 'text-rose-600 font-bold' : key === col.name && violationData.type === 'PATTERN' ? 'text-amber-600 font-bold' : 'text-slate-700'}`}>
                                                        {value === null ? 'NULL' : value}
                                                      </span>
                                                    </div>
                                                  ))}
                                                </div>
                                              </div>
                                            ))}
                                          </div>
                                          {examples.length > 5 && (
                                            <div className="text-xs text-slate-400 mt-2 text-center">
                                              + {examples.length - 5} more rows...
                                            </div>
                                          )}
                                        </div>
                                      )}
                                    </div>
                                  );
                                })}
                              </div>
                            </div>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default SchemaValidationTable;
