import React, { useState, useEffect } from 'react'
import axios from 'axios'
import { 
  BarChart, Bar, LineChart, Line, AreaChart, Area, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer 
} from 'recharts'
import {
  Database, Activity, Zap, CheckCircle, AlertTriangle, 
  TrendingUp, Users, FileText, Clock, Gauge, Server
} from 'lucide-react'
import { API_BASE } from '../config'

const Dashboard = ({ health, systemMetrics, websocket }) => {
  const [metrics, setMetrics] = useState(null)
  const [recentPipelines, setRecentPipelines] = useState([])
  const [loading, setLoading] = useState(true)
  
  useEffect(() => {
    fetchDashboardData()
    
    // Refresh data every 30 seconds
    const interval = setInterval(fetchDashboardData, 30000)
    return () => clearInterval(interval)
  }, [])

  const fetchDashboardData = async () => {
    try {
      // Fetch pipeline metrics
      const pipelinesResponse = await axios.get(`${API_BASE}/api/pipeline`)
      setRecentPipelines(pipelinesResponse.data.pipelines || [])
      
      setLoading(false)
    } catch (error) {
      console.error('Failed to fetch dashboard data:', error)
      setLoading(false)
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'healthy': case 'completed': case 'connected': return 'text-green-400'
      case 'running': case 'in_progress': return 'text-blue-400' 
      case 'degraded': case 'warning': return 'text-yellow-400'
      case 'error': case 'failed': case 'disconnected': return 'text-red-400'
      default: return 'text-gray-400'
    }
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'healthy': case 'completed': return <CheckCircle className="w-5 h-5 text-green-400" />
      case 'running': case 'in_progress': return <Activity className="w-5 h-5 text-blue-400 animate-pulse" />
      case 'degraded': case 'warning': return <AlertTriangle className="w-5 h-5 text-yellow-400" />
      case 'error': case 'failed': return <AlertTriangle className="w-5 h-5 text-red-400" />
      default: return <Clock className="w-5 h-5 text-gray-400" />
    }
  }

  // Sample data for charts
  const performanceData = [
    { name: 'Mon', pipelines: 12, success: 11, failed: 1 },
    { name: 'Tue', pipelines: 8, success: 7, failed: 1 },
    { name: 'Wed', pipelines: 15, success: 14, failed: 1 },
    { name: 'Thu', pipelines: 10, success: 9, failed: 1 },
    { name: 'Fri', pipelines: 18, success: 16, failed: 2 },
    { name: 'Sat', pipelines: 5, success: 5, failed: 0 },
    { name: 'Sun', pipelines: 3, success: 3, failed: 0 }
  ]

  const processingModeData = [
    { name: 'Batch', value: 65, color: '#3B82F6' },
    { name: 'Streaming', value: 25, color: '#10B981' },
    { name: 'Micro-batch', value: 10, color: '#F59E0B' }
  ]

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-400"></div>
      </div>
    )
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-white mb-2">System Dashboard</h2>
        <p className="text-slate-400">Real-time overview of your ETL pipeline infrastructure</p>
      </div>

      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        {/* System Health */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-400 text-sm">System Health</p>
              <p className={`text-2xl font-bold ${getStatusColor(health?.status)}`}>
                {health?.status?.toUpperCase() || 'UNKNOWN'}
              </p>
            </div>
            <Server className="w-8 h-8 text-blue-400" />
          </div>
          {health?.components && (
            <div className="mt-4 space-y-2">
              {Object.entries(health.components).map(([component, status]) => (
                <div key={component} className="flex justify-between text-sm">
                  <span className="text-slate-300 capitalize">{component}</span>
                  <span className={getStatusColor(status)}>{status}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Active Pipelines */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-400 text-sm">Active Pipelines</p>
              <p className="text-2xl font-bold text-blue-400">
                {recentPipelines.filter(p => p.status === 'running').length}
              </p>
            </div>
            <Zap className="w-8 h-8 text-yellow-400" />
          </div>
          <div className="mt-4">
            <div className="flex justify-between text-sm">
              <span className="text-slate-300">Total Pipelines</span>
              <span className="text-white">{recentPipelines.length}</span>
            </div>
          </div>
        </div>

        {/* CPU Usage */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-400 text-sm">CPU Usage</p>
              <p className="text-2xl font-bold text-green-400">
                {systemMetrics.cpu_percent ? `${Math.round(systemMetrics.cpu_percent)}%` : 'N/A'}
              </p>
            </div>
            <Gauge className="w-8 h-8 text-green-400" />
          </div>
          {systemMetrics.cpu_percent && (
            <div className="mt-4">
              <div className="bg-slate-700 rounded-full h-2">
                <div 
                  className="bg-green-400 h-2 rounded-full transition-all duration-300"
                  style={{ width: `${systemMetrics.cpu_percent}%` }}
                ></div>
              </div>
            </div>
          )}
        </div>

        {/* Memory Usage */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-400 text-sm">Memory Usage</p>
              <p className="text-2xl font-bold text-purple-400">
                {systemMetrics.memory_percent ? `${Math.round(systemMetrics.memory_percent)}%` : 'N/A'}
              </p>
            </div>
            <Activity className="w-8 h-8 text-purple-400" />
          </div>
          {systemMetrics.memory_percent && (
            <div className="mt-4">
              <div className="bg-slate-700 rounded-full h-2">
                <div 
                  className="bg-purple-400 h-2 rounded-full transition-all duration-300"
                  style={{ width: `${systemMetrics.memory_percent}%` }}
                ></div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Pipeline Performance Chart */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
            <TrendingUp className="w-5 h-5 mr-2 text-blue-400" />
            Pipeline Performance (Last 7 Days)
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={performanceData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="name" stroke="#9CA3AF" />
              <YAxis stroke="#9CA3AF" />
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#1F2937',
                  border: '1px solid #374151',
                  borderRadius: '8px'
                }}
              />
              <Legend />
              <Area 
                type="monotone" 
                dataKey="success" 
                stackId="1"
                stroke="#10B981" 
                fill="#10B981" 
                fillOpacity={0.3}
                name="Successful"
              />
              <Area 
                type="monotone" 
                dataKey="failed" 
                stackId="1"
                stroke="#EF4444" 
                fill="#EF4444" 
                fillOpacity={0.3}
                name="Failed"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Processing Mode Distribution */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
            <Database className="w-5 h-5 mr-2 text-green-400" />
            Processing Mode Distribution
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={processingModeData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={100}
                paddingAngle={5}
                dataKey="value"
              >
                {processingModeData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#1F2937',
                  border: '1px solid #374151',
                  borderRadius: '8px'
                }}
              />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Recent Pipelines Table */}
      <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
        <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
          <FileText className="w-5 h-5 mr-2 text-yellow-400" />
          Recent Pipeline Executions
        </h3>
        
        {recentPipelines.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="border-b border-slate-600">
                  <th className="text-left py-3 px-4 text-slate-300">Pipeline ID</th>
                  <th className="text-left py-3 px-4 text-slate-300">Type</th>
                  <th className="text-left py-3 px-4 text-slate-300">Status</th>
                  <th className="text-left py-3 px-4 text-slate-300">Progress</th>
                  <th className="text-left py-3 px-4 text-slate-300">Start Time</th>
                  <th className="text-left py-3 px-4 text-slate-300">File</th>
                </tr>
              </thead>
              <tbody>
                {recentPipelines.slice(0, 10).map((pipeline) => (
                  <tr key={pipeline.id} className="border-b border-slate-700/50 hover:bg-slate-700/30">
                    <td className="py-3 px-4 text-sm font-mono text-blue-400">
                      {pipeline.id.substring(0, 12)}...
                    </td>
                    <td className="py-3 px-4 text-sm text-slate-300 capitalize">
                      {pipeline.type}
                    </td>
                    <td className="py-3 px-4">
                      <div className="flex items-center space-x-2">
                        {getStatusIcon(pipeline.status)}
                        <span className={`text-sm ${getStatusColor(pipeline.status)}`}>
                          {pipeline.status}
                        </span>
                      </div>
                    </td>
                    <td className="py-3 px-4">
                      <div className="flex items-center space-x-2">
                        <div className="bg-slate-700 rounded-full h-2 w-16">
                          <div 
                            className={`h-2 rounded-full transition-all duration-300 ${
                              pipeline.progress === 100 ? 'bg-green-400' : 'bg-blue-400'
                            }`}
                            style={{ width: `${pipeline.progress || 0}%` }}
                          ></div>
                        </div>
                        <span className="text-sm text-slate-300">{pipeline.progress || 0}%</span>
                      </div>
                    </td>
                    <td className="py-3 px-4 text-sm text-slate-400">
                      {pipeline.start_time ? new Date(pipeline.start_time).toLocaleString() : 'N/A'}
                    </td>
                    <td className="py-3 px-4 text-sm text-slate-400 truncate max-w-xs">
                      {pipeline.file_path?.split('/').pop() || 'N/A'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-center py-8">
            <Database className="w-12 h-12 text-slate-600 mx-auto mb-4" />
            <p className="text-slate-400">No pipelines have been executed yet</p>
            <p className="text-sm text-slate-500">Start your first pipeline to see activity here</p>
          </div>
        )}
      </div>

      {/* Quick Actions */}
      <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
        <h3 className="text-lg font-semibold text-white mb-4">Quick Actions</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <button className="bg-blue-600/20 hover:bg-blue-600/30 border border-blue-500/30 rounded-lg p-4 text-left transition-all">
            <Zap className="w-6 h-6 text-blue-400 mb-2" />
            <h4 className="text-white font-medium">Start New Pipeline</h4>
            <p className="text-sm text-slate-400">Launch a new ETL pipeline</p>
          </button>
          
          <button className="bg-green-600/20 hover:bg-green-600/30 border border-green-500/30 rounded-lg p-4 text-left transition-all">
            <Users className="w-6 h-6 text-green-400 mb-2" />
            <h4 className="text-white font-medium">Query Data</h4>
            <p className="text-sm text-slate-400">Run SQL queries on processed data</p>
          </button>
          
          <button className="bg-yellow-600/20 hover:bg-yellow-600/30 border border-yellow-500/30 rounded-lg p-4 text-left transition-all">
            <Activity className="w-6 h-6 text-yellow-400 mb-2" />
            <h4 className="text-white font-medium">Monitor Streams</h4>
            <p className="text-sm text-slate-400">View real-time data streams</p>
          </button>
        </div>
      </div>
    </div>
  )
}

export default Dashboard