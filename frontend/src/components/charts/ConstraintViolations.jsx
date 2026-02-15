import React, { useState, useEffect } from "react";
import { Loader2, AlertTriangle, XOctagon, AlertCircle } from "lucide-react";
import { getDatasetProfile } from "../../api";

const ConstraintViolations = ({ datasetName }) => {
  const [violations, setViolations] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetch = async () => {
      setLoading(true);
      try {
        const res = await getDatasetProfile(datasetName);
        setViolations(res.data?.constraint_violations || []);
      } catch (e) {
        console.error("Failed to load constraint violations", e);
      } finally {
        setLoading(false);
      }
    };
    fetch();
  }, [datasetName]);

  if (loading)
    return (
      <div className="flex justify-center p-8">
        <Loader2 className="animate-spin text-slate-300" size={20} />
      </div>
    );

  if (violations.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 p-8">
        <div className="flex items-center gap-4 mb-6">
          <div className="p-2 bg-emerald-50 text-emerald-500 rounded-lg">
            <AlertCircle size={16} />
          </div>
          <h4 className="text-base font-black uppercase text-slate-600 tracking-wider">
            Schema Constraint Violations
          </h4>
        </div>
        <div className="text-center py-8 text-emerald-600 text-base">
          <div className="mb-2 text-2xl">✓</div>
          <div className="font-semibold">All schema constraints satisfied</div>
          <div className="text-sm text-slate-500 mt-1">
            No violations detected in current dataset
          </div>
        </div>
      </div>
    );
  }

  const severityConfig = {
    error: {
      icon: XOctagon,
      bgColor: "bg-rose-50",
      borderColor: "border-rose-200",
      textColor: "text-rose-700",
      badgeBg: "bg-rose-100",
      badgeText: "text-rose-700",
      iconColor: "text-rose-500",
    },
    warning: {
      icon: AlertTriangle,
      bgColor: "bg-amber-50",
      borderColor: "border-amber-200",
      textColor: "text-amber-700",
      badgeBg: "bg-amber-100",
      badgeText: "text-amber-700",
      iconColor: "text-amber-500",
    },
  };

  return (
    <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
      <div className="flex items-center gap-4 p-8 pb-6">
        <div className="p-2 bg-rose-50 text-rose-500 rounded-lg">
          <AlertTriangle size={16} />
        </div>
        <h4 className="text-base font-black uppercase text-slate-600 tracking-wider">
          Schema Constraint Violations
        </h4>
        <span className="ml-auto text-xs font-bold text-rose-600 bg-rose-50 px-3 py-1 rounded-full border border-rose-200">
          {violations.length} violation{violations.length > 1 ? 's' : ''}
        </span>
      </div>

      <div className="px-8 pb-8 space-y-3">
        {violations.map((violation, idx) => {
          const severity = violation.severity || "error";
          const config = severityConfig[severity] || severityConfig.error;
          const Icon = config.icon;

          return (
            <div
              key={idx}
              className={`${config.bgColor} ${config.borderColor} border rounded-lg p-4`}
            >
              <div className="flex items-start gap-3">
                <div className={`p-2 bg-white rounded-lg ${config.borderColor} border`}>
                  <Icon size={18} className={config.iconColor} />
                </div>

                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-2">
                    <span className={`text-xs font-black uppercase px-3 py-1 rounded ${config.badgeBg} ${config.badgeText} border ${config.borderColor}`}>
                      {violation.type?.replace(/_/g, ' ')}
                    </span>
                    <span className={`text-xs font-black uppercase px-3 py-1 rounded ${config.badgeBg} ${config.badgeText}`}>
                      {severity.toUpperCase()}
                    </span>
                  </div>

                  <p className={`text-base font-semibold ${config.textColor} mb-2`}>
                    {violation.message}
                  </p>

                  {(violation.expected !== undefined || violation.actual !== undefined) && (
                    <div className="grid grid-cols-2 gap-3 mt-3">
                      {violation.expected !== undefined && (
                        <div className="bg-white rounded-lg p-2 border border-slate-200">
                          <div className="text-xs font-bold text-slate-500 uppercase mb-1">
                            Expected
                          </div>
                          <div className="text-base font-bold text-slate-700">
                            {typeof violation.expected === 'number'
                              ? violation.expected.toLocaleString()
                              : violation.expected}
                          </div>
                        </div>
                      )}
                      {violation.actual !== undefined && (
                        <div className="bg-white rounded-lg p-2 border border-slate-200">
                          <div className="text-xs font-bold text-slate-500 uppercase mb-1">
                            Actual
                          </div>
                          <div className={`text-base font-bold ${config.textColor}`}>
                            {typeof violation.actual === 'number'
                              ? violation.actual.toLocaleString()
                              : violation.actual}
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  {violation.column && (
                    <div className="mt-2">
                      <span className="text-xs text-slate-500 font-bold">Column: </span>
                      <code className="text-sm bg-white px-1.5 py-0.5 rounded text-slate-700 font-mono border border-slate-200">
                        {violation.column}
                      </code>
                    </div>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      <div className="px-8 pb-8">
        <div className="bg-primary/5 border border-primary/10 rounded-lg p-4">
          <div className="flex items-start gap-2">
            <AlertCircle size={16} className="text-primary mt-0.5" />
            <div className="flex-1">
              <div className="text-sm font-bold text-slate-700 mb-1">
                Schema Constraints vs Statistical Anomalies
              </div>
              <div className="text-sm text-slate-600 leading-relaxed font-medium">
                <strong>Schema Constraints</strong> are hard limits defined in your YAML contract (e.g., max_rows: 900).
                These violations indicate your data doesn't meet your contract specifications.<br />
                <strong>Statistical Anomalies</strong> (shown in Volume chart) detect unusual patterns compared to historical baselines using Z-scores.
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ConstraintViolations;
