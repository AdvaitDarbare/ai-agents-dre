import React, { useState, useEffect } from "react";
import { FileText, Clock, Edit3, MessageSquare, Upload, Save, X, Check, Loader2, AlertCircle } from "lucide-react";
import axios from "axios";
import { API_BASE_URL } from "../api";
import { Light as SyntaxHighlighter } from "react-syntax-highlighter";
import yaml from "react-syntax-highlighter/dist/esm/languages/hljs/yaml";
import { atomOneLight } from "react-syntax-highlighter/dist/esm/styles/hljs";

SyntaxHighlighter.registerLanguage("yaml", yaml);

const ContractGovernance = ({ datasetName }) => {
  const [versions, setVersions] = useState([]);
  const [currentContract, setCurrentContract] = useState("");
  const [selectedVersion, setSelectedVersion] = useState(null);
  const [isEditing, setIsEditing] = useState(false);
  const [editedContract, setEditedContract] = useState("");
  const [showChat, setShowChat] = useState(false);
  const [chatMessage, setChatMessage] = useState("");
  const [chatHistory, setChatHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [isAiGenerated, setIsAiGenerated] = useState(false);
  const apiBase = API_BASE_URL || "http://localhost:8000";

  useEffect(() => {
    fetchVersionHistory();
    fetchCurrentContract();
  }, [datasetName]);

  const fetchVersionHistory = async () => {
    try {
      const response = await axios.get(`${apiBase}/contract-history/${encodeURIComponent(datasetName)}`);
      const items = response.data || [];
      setVersions(items);
      if (!selectedVersion && items.length > 0) {
        setSelectedVersion(items[0]);
      }
      return items;
    } catch (err) {
      console.error("Failed to load version history", err);
      setVersions([]);
      return [];
    }
  };

  const fetchCurrentContract = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${apiBase}/contract/${encodeURIComponent(datasetName)}`);
      setCurrentContract(response.data.yaml_content || "");
      setEditedContract(response.data.yaml_content || "");
    } catch (err) {
      console.error("Failed to load current contract", err);
      setCurrentContract("");
      setEditedContract("");
    } finally {
      setLoading(false);
    }
  };

  const handleSaveContract = async () => {
    setSaving(true);
    try {
      const response = await axios.post(`${apiBase}/contract/${encodeURIComponent(datasetName)}`, {
        yaml_content: editedContract,
        change_type: isAiGenerated ? "ai_generated" : "manual_edit",
        changed_by: isAiGenerated ? "AI Assistant" : "user"
      });
      setCurrentContract(editedContract);
      setIsEditing(false);
      setIsAiGenerated(false); // Reset flag after saving
      const latest = await fetchVersionHistory();
      if (latest.length > 0) {
        setSelectedVersion(latest[0]);
      }
      if (response?.data?.scan?.enqueued) {
        alert(`Contract version saved. Auto-scan queued (${response.data.scan.job_id}).`);
      } else if (response?.data?.scan?.error) {
        alert(`Contract version saved. Auto-scan could not start: ${response.data.scan.error}`);
      } else {
        alert("Contract version saved.");
      }
    } catch (err) {
      console.error("Failed to save contract", err);
      alert("Failed to save contract: " + (err.response?.data?.detail || err.message));
    } finally {
      setSaving(false);
    }
  };

  const handleLoadVersion = async (version) => {
    try {
      const response = await axios.get(
        `${apiBase}/contract/${encodeURIComponent(datasetName)}/version/${encodeURIComponent(version.version_id)}`,
      );
      setEditedContract(response.data.yaml_content);
      setSelectedVersion(version);
      setIsEditing(true);
      setChatHistory((prev) => [
        ...prev,
        {
          role: "system",
          content: `Loaded version ${version.version_id}. Review and click Save Contract to restore as active.`,
        },
      ]);
    } catch (err) {
      console.error("Failed to load version", err);
      alert("Failed to load selected version: " + (err.response?.data?.detail || err.message));
    }
  };

  const handleChatSubmit = async () => {
    if (!chatMessage.trim()) return;

    const userMessage = { role: "user", content: chatMessage };
    const updatedHistory = [...chatHistory, userMessage];
    setChatHistory(updatedHistory);
    setChatMessage("");

    try {
      // Call AI modification endpoint
      const response = await axios.post(`${apiBase}/contract/${encodeURIComponent(datasetName)}/ai-modify`, {
        instruction: chatMessage,
        current_yaml: editedContract || currentContract
      });

      const aiMessage = {
        role: "assistant",
        content: response.data.explanation
      };
      setChatHistory([...updatedHistory, aiMessage]);

      // Update ONLY the edited contract (not current - that's the saved version)
      setEditedContract(response.data.modified_yaml);
      setIsAiGenerated(true); // Mark this edit as AI-generated

      // Show success message
      const successMessage = {
        role: "system",
        content: "✅ Contract updated in editor! Review the changes and click 'Save Contract' when ready, or Cancel to discard."
      };
      setChatHistory([...updatedHistory, aiMessage, successMessage]);

      // Auto-open edit mode to show changes
      setIsEditing(true);
    } catch (err) {
      console.error("Chat error", err);
      const errorMessage = {
        role: "assistant",
        content: `❌ Error: ${err.response?.data?.detail || err.message}`
      };
      setChatHistory([...updatedHistory, errorMessage]);
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center p-12">
        <Loader2 className="animate-spin text-slate-400" size={32} />
      </div>
    );
  }

  return (
    <div className="grid grid-cols-4 gap-6">
      {/* Left Sidebar: Version History */}
      <div className="col-span-1 bg-white rounded-xl border border-slate-200 p-5 shadow-sm">
        <div className="flex items-center gap-2 mb-4">
          <Clock size={16} className="text-slate-500" />
          <h3 className="text-xs font-black uppercase text-slate-600 tracking-wider">
            Version History
          </h3>
        </div>

        <div className="space-y-2 max-h-[600px] overflow-y-auto">
          {versions.length === 0 ? (
            <div className="text-xs text-slate-400 italic text-center py-4">
              No version history available
            </div>
          ) : (
            versions.map((version, idx) => (
              <div
                key={version.version_id}
                onClick={() => handleLoadVersion(version)}
                className={`p-3 rounded-lg border cursor-pointer transition-all ${selectedVersion?.version_id === version.version_id
                    ? "border-primary/50 bg-primary/10"
                    : "border-slate-200 hover:border-slate-300 bg-white"
                  }`}
              >
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs font-bold text-slate-700">
                    {new Date(version.timestamp).toLocaleString()}
                  </span>
                  {idx === 0 && (
                    <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-emerald-50 text-emerald-600">
                      LATEST
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-1.5 mb-1">
                  {version.change_type === "ai_generated" ? (
                    <>
                      <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-purple-50 text-purple-600 border border-purple-200">
                        🤖 AI Generated
                      </span>
                    </>
                  ) : (
                    <span className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-primary/10 text-primary border border-primary/20">
                      ✏️ Manual Edit
                    </span>
                  )}
                </div>
                <div className="text-[10px] text-slate-400">
                  by {version.changed_by || "system"}
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Main Content: Contract Viewer/Editor */}
      <div className="col-span-3 bg-white rounded-xl border border-slate-200 shadow-sm">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-slate-200">
          <div className="flex items-center gap-3">
            <FileText size={20} className="text-primary" />
            <div>
              <h3 className="text-sm font-black uppercase text-slate-700 tracking-wider">
                Data Contract
              </h3>
              <p className="text-xs text-slate-500 mt-0.5">
                {datasetName}.yaml
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {!isEditing ? (
              <>
                <button
                  onClick={() => setShowChat(!showChat)}
                  className={`flex items-center gap-2 px-3 py-2 rounded-lg text-xs font-bold transition-colors ${showChat
                      ? "bg-primary/20 text-primary"
                      : "bg-slate-100 text-slate-600 hover:bg-slate-200"
                    }`}
                >
                  <MessageSquare size={14} />
                  AI Assistant
                </button>
                <button
                  onClick={() => setIsEditing(true)}
                  className="flex items-center gap-2 px-3 py-2 bg-primary text-white rounded-lg text-xs font-bold hover:bg-primary/90 transition-colors"
                >
                  <Edit3 size={14} />
                  Edit Contract
                </button>
              </>
            ) : (
              <>
                <button
                  onClick={() => {
                    setIsEditing(false);
                    setEditedContract(currentContract);
                  }}
                  className="flex items-center gap-2 px-3 py-2 bg-slate-100 text-slate-600 rounded-lg text-xs font-bold hover:bg-slate-200 transition-colors"
                >
                  <X size={14} />
                  Cancel
                </button>
                <button
                  onClick={handleSaveContract}
                  disabled={saving}
                  className="flex items-center gap-2 px-3 py-2 bg-emerald-500 text-white rounded-lg text-xs font-bold hover:bg-emerald-600 transition-colors disabled:opacity-50"
                >
                  {saving ? (
                    <Loader2 size={14} className="animate-spin" />
                  ) : (
                    <Save size={14} />
                  )}
                  {saving ? "Saving..." : "Save Contract"}
                </button>
              </>
            )}
          </div>
        </div>

        {/* Contract Content */}
        <div className="p-5">
          {isEditing ? (
            <div>
              <textarea
                value={editedContract}
                onChange={(e) => setEditedContract(e.target.value)}
                className="w-full h-[500px] p-4 font-mono text-xs border-2 border-slate-300 rounded-lg focus:outline-none focus:border-indigo-400 bg-slate-50"
                spellCheck={false}
              />
              <div className="mt-3 flex items-center gap-2 text-xs text-slate-500">
                <AlertCircle size={14} />
                <span>Changes will create a new version in the history</span>
              </div>
            </div>
          ) : (
            <div className="border-2 border-slate-200 rounded-lg overflow-hidden">
              <SyntaxHighlighter
                language="yaml"
                style={atomOneLight}
                customStyle={{
                  margin: 0,
                  padding: "1.5rem",
                  fontSize: "12px",
                  lineHeight: "1.6",
                  maxHeight: "500px",
                  overflow: "auto"
                }}
              >
                {currentContract}
              </SyntaxHighlighter>
            </div>
          )}
        </div>

        {/* AI Chat Panel */}
        {showChat && (
          <div className="border-t-2 border-slate-200 bg-slate-50 p-5">
            <div className="mb-3">
              <h4 className="text-xs font-black uppercase text-slate-600 tracking-wider mb-2">
                AI Contract Assistant
              </h4>
              <p className="text-xs text-slate-500">
                Ask the AI to modify your contract (e.g., "Add a unique constraint to Patient ID" or "Set Age nullable to false")
              </p>
            </div>

            {/* Chat History */}
            <div className="bg-white rounded-lg border border-slate-200 p-4 mb-3 max-h-[200px] overflow-y-auto">
              {chatHistory.length === 0 ? (
                <div className="text-xs text-slate-400 italic text-center py-2">
                  Start a conversation to modify your contract...
                </div>
              ) : (
                chatHistory.map((msg, idx) => (
                  <div
                    key={idx}
                    className={`mb-3 last:mb-0 ${msg.role === "user" ? "text-right" : msg.role === "system" ? "text-center" : "text-left"
                      }`}
                  >
                    <div
                      className={`inline-block px-3 py-2 rounded-lg text-xs ${msg.role === "user"
                          ? "bg-primary text-white"
                          : msg.role === "system"
                            ? "bg-emerald-50 text-emerald-700 border border-emerald-200 font-semibold"
                            : "bg-slate-100 text-slate-700"
                        }`}
                    >
                      {msg.content}
                    </div>
                  </div>
                ))
              )}
            </div>

            {/* Chat Input */}
            <div className="flex gap-2">
              <input
                type="text"
                value={chatMessage}
                onChange={(e) => setChatMessage(e.target.value)}
                onKeyPress={(e) => e.key === "Enter" && handleChatSubmit()}
                placeholder="Type your request..."
                className="flex-1 px-3 py-2 border-2 border-slate-300 rounded-lg text-xs focus:outline-none focus:border-indigo-400"
              />
              <button
                onClick={handleChatSubmit}
                className="px-4 py-2 bg-primary text-white rounded-lg text-xs font-bold hover:bg-primary/90 transition-colors"
              >
                Send
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ContractGovernance;
