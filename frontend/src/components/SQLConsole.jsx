import { API_BASE } from '../config'
import React, { useState, useEffect } from 'react'
import axios from 'axios'
import { Editor } from '@monaco-editor/react'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism'
import {
  Play, Save, History, BookOpen, Database, Download,
  Clock, CheckCircle, AlertCircle, BarChart3, Table
} from 'lucide-react'


const SQLConsole = () => {
  const [query, setQuery] = useState('SELECT user_id, age, sign_up_date, is_active FROM users WHERE is_active = true AND age > 30 LIMIT 10;')
  const [results, setResults] = useState(null)
  const [loading, setLoading] = useState(false)
  const [templates, setTemplates] = useState([])
  const [queryHistory, setQueryHistory] = useState([])
  const [selectedTemplate, setSelectedTemplate] = useState('')
  const [showResults, setShowResults] = useState(false)
  const [schema, setSchema] = useState(null)

  useEffect(() => {
    fetchTemplates()
    fetchQueryHistory()
    fetchSchema()
  }, [])

  const fetchTemplates = async () => {
    try {
      const response = await axios.get(`${API_BASE}/api/sql/templates`)
      setTemplates(response.data.templates || [])
    } catch (error) {
      console.error('Failed to fetch templates:', error)
    }
  }

  const fetchQueryHistory = async () => {
    try {
      const response = await axios.get(`${API_BASE}/api/sql/history`)
      setQueryHistory(response.data.history || [])
    } catch (error) {
      console.error('Failed to fetch query history:', error)
    }
  }

  const fetchSchema = async () => {
    try {
      const response = await axios.get(`${API_BASE}/api/sql/schema`)
      setSchema(response.data)
    } catch (error) {
      console.error('Failed to fetch schema:', error)
    }
  }

  const executeQuery = async () => {
    if (!query.trim()) return

    setLoading(true)
    setResults(null)

    try {
      const response = await axios.post(`${API_BASE}/api/sql/execute`, {
        sql_query: query.trim(),
        use_cache: true,
        max_rows: 1000
      })

      setResults(response.data)
      setShowResults(true)
      
      // Refresh history after successful query
      setTimeout(fetchQueryHistory, 500)
    } catch (error) {
      const errorData = error.response?.data || { error: 'Unknown error' }
      setResults({
        error_message: errorData.error || errorData.detail || 'Query execution failed',
        execution_time_seconds: 0,
        row_count: 0
      })
      setShowResults(true)
    } finally {
      setLoading(false)
    }
  }

  const loadTemplate = async (templateName) => {
    setSelectedTemplate(templateName)
    
    try {
      const response = await axios.post(`${API_BASE}/api/sql/execute-template`, {
        template_name: templateName,
        parameters: {}
      })
      
      // Find the template to get its SQL
      const template = templates.find(t => t.name === templateName)
      if (template) {
        // Format the template SQL with default parameters
        let formattedSQL = template.sql_template
        template.parameters.forEach(param => {
          const placeholder = `{${param.name}}`
          formattedSQL = formattedSQL.replace(new RegExp(placeholder, 'g'), param.default)
        })
        setQuery(formattedSQL.trim())
      }
    } catch (error) {
      console.error('Failed to load template:', error)
    }
  }

  const loadHistoryQuery = (historyQuery) => {
    setQuery(historyQuery.sql_query)
    setSelectedTemplate('')
  }

  const exportResults = () => {
    if (!results || !results.result_data) return

    const csvContent = [
      results.result_data.columns.join(','),
      ...results.result_data.data.map(row => 
        results.result_data.columns.map(col => JSON.stringify(row[col] || '')).join(',')
      )
    ].join('\n')

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    link.href = URL.createObjectURL(blob)
    link.download = `query_results_${Date.now()}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
  }

  const formatExecutionTime = (seconds) => {
    if (seconds < 1) return `${Math.round(seconds * 1000)}ms`
    return `${seconds.toFixed(2)}s`
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white mb-2">SQL Console</h2>
          <p className="text-slate-400">Interactive query interface for data analysis</p>
        </div>
        <div className="flex items-center space-x-4">
          {results && (
            <div className="flex items-center space-x-4 text-sm">
              <div className="flex items-center space-x-1">
                <Clock className="w-4 h-4 text-blue-400" />
                <span className="text-slate-300">
                  {formatExecutionTime(results.execution_time_seconds)}
                </span>
              </div>
              {results.row_count > 0 && (
                <div className="flex items-center space-x-1">
                  <Table className="w-4 h-4 text-green-400" />
                  <span className="text-slate-300">{results.row_count} rows</span>
                </div>
              )}
              {results.cached && (
                <span className="px-2 py-1 bg-blue-600/20 border border-blue-500/30 rounded text-xs text-blue-300">
                  Cached
                </span>
              )}
            </div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Left Sidebar */}
        <div className="lg:col-span-1 space-y-6">
          {/* Query Templates */}
          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-4">
            <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
              <BookOpen className="w-5 h-5 mr-2 text-blue-400" />
              Query Templates
            </h3>
            <div className="space-y-2">
              {templates.map((template) => (
                <button
                  key={template.name}
                  onClick={() => loadTemplate(template.name)}
                  className={`w-full text-left p-3 rounded-lg border transition-all ${
                    selectedTemplate === template.name
                      ? 'bg-blue-600/20 border-blue-500/50 text-blue-300'
                      : 'bg-slate-700/50 border-slate-600/50 text-slate-300 hover:bg-slate-700/70'
                  }`}
                >
                  <div className="font-medium text-sm">{template.description}</div>
                  <div className="text-xs text-slate-400 mt-1">{template.category}</div>
                </button>
              ))}
            </div>
          </div>

          {/* Database Schema */}
          {schema && (
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-4">
              <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
                <Database className="w-5 h-5 mr-2 text-green-400" />
                Schema
              </h3>
              <div className="space-y-3">
                {Object.entries(schema.tables).map(([tableName, tableInfo]) => (
                  <div key={tableName} className="border border-slate-600/50 rounded-lg p-3">
                    <h4 className="text-white font-medium mb-2">{tableName}</h4>
                    <div className="space-y-1">
                      {tableInfo.columns.map((column) => (
                        <div key={column.name} className="flex justify-between text-xs">
                          <span className="text-slate-300">{column.name}</span>
                          <span className="text-slate-400">{column.type}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Query History */}
          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-4">
            <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
              <History className="w-5 h-5 mr-2 text-yellow-400" />
              Recent Queries
            </h3>
            <div className="space-y-2 max-h-64 overflow-y-auto">
              {queryHistory.slice(0, 10).map((historyItem, index) => (
                <button
                  key={index}
                  onClick={() => loadHistoryQuery(historyItem)}
                  className="w-full text-left p-2 rounded border border-slate-600/50 hover:bg-slate-700/50 transition-all"
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs text-slate-400">
                      {new Date(historyItem.timestamp).toLocaleString()}
                    </span>
                    {historyItem.error_message ? (
                      <AlertCircle className="w-3 h-3 text-red-400" />
                    ) : (
                      <CheckCircle className="w-3 h-3 text-green-400" />
                    )}
                  </div>
                  <div className="text-xs text-slate-300 truncate">
                    {historyItem.sql_query}
                  </div>
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Main Content */}
        <div className="lg:col-span-3 space-y-6">
          {/* Query Editor */}
          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold text-white">Query Editor</h3>
              <div className="flex items-center space-x-2">
                <button
                  onClick={executeQuery}
                  disabled={loading}
                  className="flex items-center space-x-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-600/50 px-4 py-2 rounded-lg text-white transition-all"
                >
                  {loading ? (
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                  ) : (
                    <Play className="w-4 h-4" />
                  )}
                  <span>Execute</span>
                </button>
              </div>
            </div>
            
            <div className="border border-slate-600 rounded-lg overflow-hidden">
              <Editor
                height="200px"
                defaultLanguage="sql"
                value={query}
                onChange={(value) => setQuery(value || '')}
                theme="vs-dark"
                options={{
                  minimap: { enabled: false },
                  fontSize: 14,
                  lineNumbers: 'on',
                  wordWrap: 'on',
                  automaticLayout: true
                }}
              />
            </div>
          </div>

          {/* Results */}
          {showResults && results && (
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-white flex items-center">
                  {results.error_message ? (
                    <>
                      <AlertCircle className="w-5 h-5 mr-2 text-red-400" />
                      Query Error
                    </>
                  ) : (
                    <>
                      <BarChart3 className="w-5 h-5 mr-2 text-green-400" />
                      Query Results
                    </>
                  )}
                </h3>
                {results.result_data && (
                  <button
                    onClick={exportResults}
                    className="flex items-center space-x-2 bg-green-600 hover:bg-green-700 px-3 py-2 rounded-lg text-white transition-all"
                  >
                    <Download className="w-4 h-4" />
                    <span>Export CSV</span>
                  </button>
                )}
              </div>

              {results.error_message ? (
                <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4">
                  <p className="text-red-400 font-medium mb-2">Error:</p>
                  <p className="text-red-300 text-sm">{results.error_message}</p>
                </div>
              ) : results.result_data && results.result_data.data.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="min-w-full">
                    <thead>
                      <tr className="border-b border-slate-600">
                        {results.result_data.columns.map((column, index) => (
                          <th key={index} className="text-left py-3 px-4 text-slate-300 font-medium">
                            {column}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {results.result_data.data.map((row, rowIndex) => (
                        <tr key={rowIndex} className="border-b border-slate-700/50 hover:bg-slate-700/30">
                          {results.result_data.columns.map((column, colIndex) => (
                            <td key={colIndex} className="py-3 px-4 text-sm text-slate-300">
                              {row[column] !== null && row[column] !== undefined 
                                ? String(row[column]) 
                                : <span className="text-slate-500 italic">null</span>
                              }
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  
                  {results.row_count > results.result_data.data.length && (
                    <div className="mt-4 p-3 bg-yellow-500/10 border border-yellow-500/30 rounded-lg">
                      <p className="text-yellow-300 text-sm">
                        Showing {results.result_data.data.length} of {results.row_count} rows. 
                        Results may be limited for performance.
                      </p>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-center py-8">
                  <Table className="w-12 h-12 text-slate-600 mx-auto mb-4" />
                  <p className="text-slate-400">Query executed successfully, but returned no data</p>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default SQLConsole