import { useState, useRef, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Send, Bot, User, Sparkles, X } from 'lucide-react';
import { chatWithAssistant, proposeContract, aiModifyContract } from '../api';

export default function ContractAssistant({ dataset, onClose }) {
  const [workingYaml, setWorkingYaml] = useState('');
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: `Hi! I'm your AI Contract Assistant for **${dataset}**. I can help you:\n\n• Generate a new contract from data\n• Modify existing contracts with natural language\n• Explain contract rules and quality metrics\n• Answer questions about data quality\n\nHow can I assist you today?`
    }
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(scrollToBottom, [messages]);

  const handleSend = async () => {
    if (!input.trim() || isLoading) return;

    const userMessage = input.trim();
    setInput('');
    setMessages(prev => [...prev, { role: 'user', content: userMessage }]);
    setIsLoading(true);

    try {
      // Check for contract generation intent
      if (userMessage.toLowerCase().includes('generate contract') ||
          userMessage.toLowerCase().includes('create contract')) {
        const result = await proposeContract(dataset);
        const proposedYaml = result?.proposed_yaml || '';
        setWorkingYaml(proposedYaml);
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: `I've generated a contract proposal for **${dataset}**:\n\n\`\`\`yaml\n${proposedYaml}\n\`\`\`\n\nWould you like me to modify anything?`
        }]);
      }
      // Check for modification intent
      else if (userMessage.toLowerCase().includes('modify') ||
               userMessage.toLowerCase().includes('add') ||
               userMessage.toLowerCase().includes('change')) {
        if (!workingYaml) {
          setMessages(prev => [...prev, {
            role: 'assistant',
            content: 'Generate or load a contract first so I have YAML to modify.'
          }]);
          return;
        }
        const result = await aiModifyContract(dataset, userMessage, workingYaml);
        const modifiedYaml = result?.modified_yaml || workingYaml;
        setWorkingYaml(modifiedYaml);
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: `I've updated the contract based on your request:\n\n\`\`\`yaml\n${modifiedYaml}\n\`\`\`\n\nThe changes have been applied. Anything else?`
        }]);
      }
      // General chat
      else {
        const result = await chatWithAssistant(userMessage, { dataset });
        setMessages(prev => [...prev, {
          role: 'assistant',
          content: result.response || result.message
        }]);
      }
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: `Sorry, I encountered an error: ${err.message}`
      }]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, x: 300 }}
      animate={{ opacity: 1, x: 0 }}
      exit={{ opacity: 0, x: 300 }}
      className="fixed right-0 top-0 h-full w-[480px] bg-card border-l border-border shadow-2xl flex flex-col z-40"
    >
      {/* Header */}
      <div className="px-6 py-4 border-b border-border flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Sparkles className="text-orange-500" size={24} />
          <div>
            <h2 className="font-bold">AI Contract Assistant</h2>
            <p className="text-xs text-muted-foreground">{dataset}</p>
          </div>
        </div>
        <button onClick={onClose}>
          <X className="text-muted-foreground hover:text-foreground" />
        </button>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.map((msg, idx) => (
          <div key={idx} className={`flex gap-3 ${msg.role === 'user' ? 'justify-end' : ''}`}>
            {msg.role === 'assistant' && (
              <div className="w-8 h-8 rounded-full bg-orange-500/20 flex items-center justify-center flex-shrink-0">
                <Bot size={16} className="text-orange-500" />
              </div>
            )}
            <div className={`max-w-[80%] px-4 py-2 rounded-xl ${
              msg.role === 'user'
                ? 'bg-orange-500 text-white'
                : 'bg-muted/30'
            }`}>
              <div className="text-sm whitespace-pre-wrap">
                {msg.content}
              </div>
            </div>
            {msg.role === 'user' && (
              <div className="w-8 h-8 rounded-full bg-blue-500/20 flex items-center justify-center flex-shrink-0">
                <User size={16} className="text-blue-500" />
              </div>
            )}
          </div>
        ))}
        {isLoading && (
          <div className="flex gap-3">
            <div className="w-8 h-8 rounded-full bg-orange-500/20 flex items-center justify-center">
              <Bot size={16} className="text-orange-500 animate-pulse" />
            </div>
            <div className="bg-muted/30 px-4 py-2 rounded-xl">
              <div className="flex gap-1">
                <span className="w-2 h-2 bg-orange-500 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                <span className="w-2 h-2 bg-orange-500 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                <span className="w-2 h-2 bg-orange-500 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="p-4 border-t border-border">
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSend()}
            placeholder="Ask about contracts, generate, modify..."
            className="flex-1 px-4 py-2 bg-muted/30 rounded-lg border border-border focus:outline-none focus:ring-2 focus:ring-orange-500"
          />
          <button
            onClick={handleSend}
            disabled={isLoading || !input.trim()}
            className="px-4 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 disabled:opacity-50"
          >
            <Send size={18} />
          </button>
        </div>
      </div>
    </motion.div>
  );
}
