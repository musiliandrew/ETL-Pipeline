import React, { useState, useEffect } from 'react'
import axios from 'axios'
import { 
  LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, 
  Tooltip, Legend, ResponsiveContainer, BarChart, Bar 
} from 'recharts'
import {
  Play, Square, RefreshCw, Zap, Database, Activity,
  CheckCircle, AlertCircle, Clock, TrendingUp, Settings,
  FileText, Filter, Calendar, Download
} from 'lucide-react'

const API_BASE = 'http://localhost:8000'

const PipelineMonitor = ({ websocket }) => {
  const [pipelines, setPipelines] = useState([])
  const [selectedPipeline, setSelectedPipeline] = useState(null)
  const [pipelineConfig, setPipelineConfig] = useState({
    pipeline_type: 'production',
    processing_mode: 'batch',
    data_quality_enabled: true,
    schema_evolution_enabled: true,
    benchmark_mode: false
  })
  const [loading, setLoading] = useState(false)
  const [filter, setFilter] = useState('all')
  
  useEffect(() => {
    fetchPipelines()
    
    // Refresh pipelines every 5 seconds
    const interval = setInterval(fetchPipelines, 5000)
    return () => clearInterval(interval)
  }, [])

  useEffect(() => {
    // Listen for WebSocket messages
    if (websocket) {
      const handleMessage = (event) => {
        try {
          const message = JSON.parse(event.data)
          if (message.type.startsWith('pipeline_')) {
            fetchPipelines() // Refresh pipeline list
            if (selectedPipeline && message.pipeline_id === selectedPipeline.id) {
              fetchPipelineDetails(selectedPipeline.id) // Refresh selected pipeline
            }
          }
        } catch (error) {
          console.error('Error parsing WebSocket message:', error)
        }
      }

      websocket.addEventListener('message', handleMessage)
      return () => websocket.removeEventListener('message', handleMessage)
    }
  }, [websocket, selectedPipeline])

  const fetchPipelines = async () => {
    try {
      const response = await axios.get(`${API_BASE}/api/pipeline`)
      setPipelines(response.data.pipelines || [])
    } catch (error) {
      console.error('Failed to fetch pipelines:', error)
    }
  }

  const fetchPipelineDetails = async (pipelineId) => {
    try {
      const response = await axios.get(`${API_BASE}/api/pipeline/${pipelineId}`)
      setSelectedPipeline(response.data)
    } catch (error) {
      console.error('Failed to fetch pipeline details:', error)
    }
  }

  const startPipeline = async () => {
    setLoading(true)
    try {
      const response = await axios.post(`${API_BASE}/api/pipeline/start`, pipelineConfig)
      fetchPipelines()
      
      // Auto-select the new pipeline
      setTimeout(() => {
        fetchPipelineDetails(response.data.pipeline_id)
      }, 1000)
    } catch (error) {
      console.error('Failed to start pipeline:', error)
    } finally {
      setLoading(false)
    }
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'completed': return <CheckCircle className="w-5 h-5 text-green-400" />
      case 'running': case 'initializing': return <Activity className="w-5 h-5 text-blue-400 animate-pulse" />
      case 'failed': return <AlertCircle className="w-5 h-5 text-red-400" />
      default: return <Clock className="w-5 h-5 text-gray-400" />
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'completed': return 'text-green-400'
      case 'running': case 'initializing': return 'text-blue-400'
      case 'failed': return 'text-red-400'
      default: return 'text-gray-400'
    }
  }

  const filteredPipelines = pipelines.filter(pipeline => {
    if (filter === 'all') return true
    return pipeline.status === filter
  })

  // Generate sample metrics data for visualization
  const generateMetricsData = (pipeline) => {
    if (!pipeline || !pipeline.current_metrics) return []
    
    // Create sample time series data
    const now = new Date()
    return Array.from({ length: 10 }, (_, i) => ({
      time: new Date(now.getTime() - (9 - i) * 10000).toLocaleTimeString(),
      throughput: Math.random() * 100 + 50,
      memory: Math.random() * 40 + 30,
      cpu: Math.random() * 60 + 20
    }))
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white mb-2">Pipeline Monitor</h2>
          <p className="text-slate-400">Real-time ETL pipeline execution monitoring</p>
        </div>
        <button
          onClick={startPipeline}
          disabled={loading}
          className="flex items-center space-x-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-600/50 px-6 py-3 rounded-lg text-white transition-all"
        >
          {loading ? (
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
          ) : (
            <Play className="w-5 h-5" />
          )}
          <span>Start New Pipeline</span>
        </button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Pipeline List */}
        <div className="lg:col-span-1">
          {/* Configuration Panel */}
          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6 mb-6">
            <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
              <Settings className="w-5 h-5 mr-2 text-blue-400" />
              Pipeline Configuration
            </h3>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-2">
                  Pipeline Type
                </label>
                <select
                  value={pipelineConfig.pipeline_type}
                  onChange={(e) => setPipelineConfig({...pipelineConfig, pipeline_type: e.target.value})}
                  className="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-white"
                >
                  <option value="basic">Basic</option>
                  <option value="production">Production</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-slate-300 mb-2">
                  Processing Mode
                </label>
                <select
                  value={pipelineConfig.processing_mode}
                  onChange={(e) => setPipelineConfig({...pipelineConfig, processing_mode: e.target.value})}
                  className="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-white"
                >
                  <option value="batch">Batch</option>
                  <option value="streaming">Streaming</option>
                  <option value="micro_batch">Micro-batch</option>
                </select>
              </div>

              <div className="space-y-2">
                <label className="flex items-center">
                  <input
                    type="checkbox"
                    checked={pipelineConfig.data_quality_enabled}
                    onChange={(e) => setPipelineConfig({...pipelineConfig, data_quality_enabled: e.target.checked})}
                    className="mr-2"
                  />
                  <span className="text-sm text-slate-300">Data Quality Assessment</span>
                </label>
                
                <label className="flex items-center">
                  <input
                    type="checkbox"
                    checked={pipelineConfig.schema_evolution_enabled}
                    onChange={(e) => setPipelineConfig({...pipelineConfig, schema_evolution_enabled: e.target.checked})}
                    className="mr-2"
                  />
                  <span className="text-sm text-slate-300">Schema Evolution</span>
                </label>
                
                <label className="flex items-center">
                  <input
                    type="checkbox"
                    checked={pipelineConfig.benchmark_mode}
                    onChange={(e) => setPipelineConfig({...pipelineConfig, benchmark_mode: e.target.checked})}
                    className="mr-2"
                  />
                  <span className="text-sm text-slate-300">Benchmark Mode</span>
                </label>
              </div>
            </div>
          </div>

          {/* Pipeline List */}
          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold text-white flex items-center">
                <Database className="w-5 h-5 mr-2 text-green-400" />
                Active Pipelines ({filteredPipelines.length})
              </h3>
              
              <select
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                className="bg-slate-700 border border-slate-600 rounded-lg px-3 py-1 text-sm text-white"
              >
                <option value="all">All</option>
                <option value="running">Running</option>
                <option value="completed">Completed</option>
                <option value="failed">Failed</option>
              </select>
            </div>

            <div className="space-y-2 max-h-96 overflow-y-auto">
              {filteredPipelines.map((pipeline) => (
                <div
                  key={pipeline.id}
                  onClick={() => fetchPipelineDetails(pipeline.id)}
                  className={`p-4 rounded-lg border cursor-pointer transition-all ${
                    selectedPipeline?.id === pipeline.id
                      ? 'bg-blue-600/20 border-blue-500/50'
                      : 'bg-slate-700/30 border-slate-600/50 hover:bg-slate-700/50'
                  }`}
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-mono text-blue-400">
                      {pipeline.id.substring(0, 12)}...
                    </span>
                    {getStatusIcon(pipeline.status)}
                  </div>
                  
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-sm text-slate-300 capitalize">{pipeline.type}</span>
                    <span className={`text-sm ${getStatusColor(pipeline.status)}`}>
                      {pipeline.status}
                    </span>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <div className="bg-slate-700 rounded-full h-2 flex-1">
                      <div 
                        className={`h-2 rounded-full transition-all duration-300 ${
                          pipeline.progress === 100 ? 'bg-green-400' : 'bg-blue-400'
                        }`}
                        style={{ width: `${pipeline.progress || 0}%` }}
                      ></div>
                    </div>
                    <span className="text-xs text-slate-400">{pipeline.progress || 0}%</span>
                  </div>
                  
                  {pipeline.start_time && (
                    <div className="text-xs text-slate-500 mt-2">
                      {new Date(pipeline.start_time).toLocaleString()}
                    </div>
                  )}
                </div>
              ))}
              
              {filteredPipelines.length === 0 && (
                <div className="text-center py-8">
                  <Database className="w-12 h-12 text-slate-600 mx-auto mb-4" />
                  <p className="text-slate-400">No pipelines match the current filter</p>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Pipeline Details */}
        <div className="lg:col-span-2 space-y-6">
          {selectedPipeline ? (
            <>
              {/* Pipeline Status */}
              <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
                <div className="flex items-center justify-between mb-6">
                  <h3 className="text-lg font-semibold text-white">Pipeline Details</h3>
                  <div className="flex items-center space-x-3">
                    {getStatusIcon(selectedPipeline.status)}
                    <span className={`text-lg font-medium ${getStatusColor(selectedPipeline.status)}`}>
                      {selectedPipeline.status?.toUpperCase()}
                    </span>
                  </div>
                </div>

                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                  <div className="bg-slate-700/30 rounded-lg p-3">
                    <div className="text-slate-400 text-sm">Pipeline ID</div>
                    <div className="text-white font-mono text-sm">{selectedPipeline.id}</div>
                  </div>
                  <div className="bg-slate-700/30 rounded-lg p-3">
                    <div className="text-slate-400 text-sm">Type</div>
                    <div className="text-white capitalize">{selectedPipeline.type}</div>
                  </div>
                  <div className="bg-slate-700/30 rounded-lg p-3">
                    <div className="text-slate-400 text-sm">Mode</div>
                    <div className="text-white capitalize">{selectedPipeline.processing_mode}</div>
                  </div>
                  <div className="bg-slate-700/30 rounded-lg p-3">
                    <div className="text-slate-400 text-sm">Progress</div>
                    <div className="text-white">{selectedPipeline.progress || 0}%</div>
                  </div>
                </div>

                {selectedPipeline.stage && (
                  <div className="mb-4">
                    <div className="flex justify-between items-center mb-2">
                      <span className="text-slate-300">Current Stage: {selectedPipeline.stage}</span>
                      <span className="text-slate-400">{selectedPipeline.progress || 0}%</span>
                    </div>
                    <div className="bg-slate-700 rounded-full h-3">
                      <div 
                        className="bg-gradient-to-r from-blue-500 to-blue-400 h-3 rounded-full transition-all duration-500"
                        style={{ width: `${selectedPipeline.progress || 0}%` }}
                      ></div>
                    </div>
                  </div>
                )}

                {(selectedPipeline.start_time || selectedPipeline.end_time) && (
                  <div className="flex justify-between text-sm text-slate-400">
                    {selectedPipeline.start_time && (
                      <span>Started: {new Date(selectedPipeline.start_time).toLocaleString()}</span>
                    )}
                    {selectedPipeline.end_time && (
                      <span>Ended: {new Date(selectedPipeline.end_time).toLocaleString()}</span>
                    )}
                  </div>
                )}
              </div>

              {/* Metrics Chart */}
              {selectedPipeline.current_metrics && (
                <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
                  <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
                    <TrendingUp className="w-5 h-5 mr-2 text-blue-400" />
                    Performance Metrics
                  </h3>
                  
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={generateMetricsData(selectedPipeline)}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                      <XAxis dataKey="time" stroke="#9CA3AF" />
                      <YAxis stroke="#9CA3AF" />
                      <Tooltip 
                        contentStyle={{
                          backgroundColor: '#1F2937',
                          border: '1px solid #374151',
                          borderRadius: '8px'
                        }}
                      />
                      <Legend />
                      <Line 
                        type="monotone" 
                        dataKey="throughput" 
                        stroke="#3B82F6" 
                        name="Throughput (rps)"
                        strokeWidth={2}
                        dot={false}
                      />
                      <Line 
                        type="monotone" 
                        dataKey="memory" 
                        stroke="#10B981" 
                        name="Memory %"
                        strokeWidth={2}
                        dot={false}
                      />
                      <Line 
                        type="monotone" 
                        dataKey="cpu" 
                        stroke="#F59E0B" 
                        name="CPU %"
                        strokeWidth={2}
                        dot={false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              )}

              {/* Data Quality Report */}
              {selectedPipeline.data_quality_report && (
                <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
                  <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
                    <CheckCircle className="w-5 h-5 mr-2 text-green-400" />
                    Data Quality Report
                  </h3>
                  
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div className="bg-slate-700/30 rounded-lg p-4 text-center">
                      <div className="text-2xl font-bold text-green-400">
                        {Math.round(selectedPipeline.data_quality_report.overall_quality_score * 100)}%
                      </div>
                      <div className="text-slate-400 text-sm">Overall Quality</div>
                    </div>
                    <div className="bg-slate-700/30 rounded-lg p-4 text-center">
                      <div className="text-2xl font-bold text-blue-400">
                        {selectedPipeline.data_quality_report.total_records || 0}
                      </div>
                      <div className="text-slate-400 text-sm">Total Records</div>
                    </div>
                    <div className="bg-slate-700/30 rounded-lg p-4 text-center">
                      <div className="text-2xl font-bold text-yellow-400">
                        {selectedPipeline.data_quality_report.validation_issues?.length || 0}
                      </div>
                      <div className="text-slate-400 text-sm">Quality Issues</div>
                    </div>
                    <div className="bg-slate-700/30 rounded-lg p-4 text-center">
                      <div className="text-2xl font-bold text-purple-400">
                        {selectedPipeline.data_quality_report.data_types_detected || 0}
                      </div>
                      <div className="text-slate-400 text-sm">Data Types</div>
                    </div>
                  </div>
                </div>
              )}

              {/* Schema Changes */}
              {selectedPipeline.schema_changes && selectedPipeline.schema_changes.length > 0 && (
                <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
                  <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
                    <FileText className="w-5 h-5 mr-2 text-yellow-400" />
                    Schema Evolution Changes
                  </h3>
                  
                  <div className="space-y-3">
                    {selectedPipeline.schema_changes.map((change, index) => (
                      <div key={index} className="bg-slate-700/30 rounded-lg p-4">
                        <div className="flex justify-between items-center mb-2">
                          <span className="text-white font-medium">{change.change_type}</span>
                          <span className="text-slate-400 text-sm">{change.timestamp}</span>
                        </div>
                        <p className="text-slate-300 text-sm">{change.description}</p>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Error Display */}
              {selectedPipeline.error && (
                <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-red-500/50 p-6">
                  <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
                    <AlertCircle className="w-5 h-5 mr-2 text-red-400" />
                    Pipeline Error
                  </h3>
                  <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4">
                    <p className="text-red-300">{selectedPipeline.error}</p>
                  </div>
                </div>
              )}
            </>
          ) : (
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-12">
              <div className="text-center">
                <Zap className="w-16 h-16 text-slate-600 mx-auto mb-4" />
                <h3 className="text-lg font-semibold text-white mb-2">No Pipeline Selected</h3>
                <p className="text-slate-400 mb-6">
                  Select a pipeline from the list to view detailed monitoring information
                </p>
                <button
                  onClick={startPipeline}
                  disabled={loading}
                  className="flex items-center space-x-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-600/50 px-6 py-3 rounded-lg text-white mx-auto transition-all"
                >
                  {loading ? (
                    <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                  ) : (
                    <Play className="w-5 h-5" />
                  )}
                  <span>Start Your First Pipeline</span>
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default PipelineMonitor