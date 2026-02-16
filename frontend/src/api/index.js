import axios from 'axios';

const api = axios.create({
    baseURL: 'http://localhost:8000',
});

export const getPulse = () => api.get('/pulse');
export const getDatasets = () => api.get('/datasets');
export const deleteDataset = async (name) => {
  const response = await api.delete(`/datasets/${encodeURIComponent(name)}`);
  return response.data;
};
export const evaluateDataset = (name) => api.post(`/evaluate/${name}`);
export const getHistory = (name) => api.get(`/history/${name}`);
export const chatWithCopilot = (query) => api.post(`/chat?query=${encodeURIComponent(query)}`);

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
export const aiModifyContract = async (datasetName, instruction) => {
  const response = await api.post(`/contract/${datasetName}/ai-modify`, {
    instruction
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
