import axios from 'axios';

export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';
const REQUEST_TIMEOUT_MS = Number(import.meta.env.VITE_API_TIMEOUT_MS || 15000);

const api = axios.create({
    baseURL: API_BASE_URL,
    timeout: REQUEST_TIMEOUT_MS,
});

export const getPulse = () => api.get('/pulse');
export const getDatasets = () => api.get('/datasets');
export const deleteDataset = async (name, options = {}) => {
  const query = new URLSearchParams();
  if (options.policyApproved) query.set('policy_approved', 'true');
  if (options.policyReason && String(options.policyReason).trim()) {
    query.set('policy_reason', String(options.policyReason).trim());
  }
  const suffix = query.toString() ? `?${query.toString()}` : '';
  const response = await api.delete(`/datasets/${encodeURIComponent(name)}${suffix}`);
  return response.data;
};
export const evaluateDataset = (name) => api.post(`/evaluate/${name}`);
export const enqueueEvaluateDataset = (name) =>
  api.post(`/jobs/evaluate/${encodeURIComponent(name)}`);
export const enqueueBulkEvaluateDatasets = (datasetNames) =>
  api.post('/jobs/evaluate-bulk', { dataset_names: datasetNames });
export const getJobStatus = (jobId) => api.get(`/jobs/${encodeURIComponent(jobId)}`);
export const getHistory = (name) => api.get(`/history/${name}`);
export const chatWithCopilot = (query) => api.post('/chat', { query });

// --- New Client Functions ---
export const getDatasetData = (name, limit = 100) => api.get(`/datasets/${name}/data?limit=${limit}`);
export const getLineage = (dataset) => api.get(`/lineage${dataset ? `?dataset=${dataset}` : ''}`);
export const getGlobalStats = () => api.get('/stats/global');
export const getDatasetMetrics = (name) => api.get(`/metrics/${name}`);
export const getSystemHealth = () => api.get('/health/system');
export const getDatasetProfile = (name) => api.get(`/profile/${name}`);
export const getRemediationPlan = (name) => api.get(`/remediation/${name}`);
export const applyRemediation = (data) => api.post('/remediation/apply', data);
// --- Governance/History ---
export const getGovernanceHistory = (dataset) => api.get(`/governance/${dataset}/history`);
export const getHistoricalFile = (filename) => api.get(`/governance/file/${filename}`);
export const rollbackSchema = (data) => api.post('/governance/rollback', data);

export const getRecentRuns = () => api.get('/runs');

// --- Contract Generation ---
export const getContract = (datasetName) => api.get(`/contracts/${datasetName}`);
export const proposeContract = (datasetName) => api.post('/contracts/propose', { dataset_name: datasetName });
export const saveContract = (data) => api.post('/contracts/save', data);

// --- HITL Contract Approval Workflow ---
export const getPendingContracts = async () => {
  const response = await api.get('/contracts/pending');
  return response.data;
};

export const approveContract = async (datasetName, approvedYaml) => {
  const response = await api.post('/contracts/approve', {
    dataset_name: datasetName,
    approved_yaml: approvedYaml
  });
  return response.data;
};

export const rejectContract = async (datasetName) => {
  const response = await api.delete(`/contracts/pending/${datasetName}`);
  return response.data;
};

// --- AI Contract Modification ---
export const aiModifyContract = async (datasetName, instruction, currentYaml) => {
  const response = await api.post(`/contract/${datasetName}/ai-modify`, {
    instruction,
    current_yaml: currentYaml,
  });
  return response.data;
};

// --- AI Assistant Chat ---
export const chatWithAssistant = async (message, context = {}) => {
  const response = await api.post('/chat', {
    query: message,
    context
  });
  return response.data;
};

// --- Visualization Data ---
export const getIncidents = (limit = 50) => api.get(`/incidents?limit=${limit}`);
export const getMetricTimeseries = (name, metric = 'row_count', limit = 30) =>
    api.get(`/metrics/${name}/timeseries?metric=${metric}&limit=${limit}`);
export const getBaselines = (name) => api.get(`/baselines/${name}`);
export const getSloHistory = (name, limit = 100) => api.get(`/slos/${name}?limit=${limit}`);
export const getSloSummary = (name, window = 200) => api.get(`/slos/${name}/summary?window=${window}`);

export default api;
