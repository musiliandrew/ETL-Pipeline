import React, { useState, useEffect } from 'react'
import axios from 'axios'
import { 
  Database, 
  Play, 
  BarChart3, 
  Code, 
  Settings, 
  Activity,
  CheckCircle,
  AlertCircle,
  Clock,
  Zap,
  TrendingUp,
  Users,
  FileText,
  Gauge,
  Terminal,
  GitBranch,
  Shield
} from 'lucide-react'

// Import components
import Dashboard from './components/Dashboard'
import SQLConsole from './components/SQLConsole'
import PipelineMonitor from './components/PipelineMonitor'
import DataQuality from './components/DataQuality'
import SchemaEvolution from './components/SchemaEvolution'
import StreamingBenchmark from './components/StreamingBenchmark'

const API_BASE = 'http://localhost:8000'

function App() {
  const [activeTab, setActiveTab] = useState('dashboard')
  const [health, setHealth] = useState(null)
  const [websocket, setWebsocket] = useState(null)
  const [systemMetrics, setSystemMetrics] = useState({})
  const [notifications, setNotifications] = useState([])

  useEffect(() => {
    // Initialize health check
    checkHealth()
    
    // Setup WebSocket connection
    setupWebSocket()
    
    // Cleanup on unmount
    return () => {
      if (websocket) {
        websocket.close()
      }
    }
  }, [])

  const checkHealth = async () => {
    try {
      const response = await axios.get(`${API_BASE}/health`)
      setHealth(response.data)
      setSystemMetrics(response.data.system_metrics || {})
    } catch (error) {
      console.error('Health check failed:', error)
      setHealth({ status: 'error', components: { api: 'disconnected' } })
    }
  }

  const setupWebSocket = () => {
    try {
      const ws = new WebSocket(`ws://localhost:8000/ws`)
      
      ws.onopen = () => {
        console.log('WebSocket connected')
        setWebsocket(ws)
        
        // Send periodic pings to keep connection alive
        const pingInterval = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ping' }))
          }
        }, 30000)
        
        ws.pingInterval = pingInterval
      }
      
      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data)
          handleWebSocketMessage(message)
        } catch (error) {
          console.error('Failed to parse WebSocket message:', error)
        }
      }
      
      ws.onclose = () => {
        console.log('WebSocket disconnected')
        if (ws.pingInterval) {
          clearInterval(ws.pingInterval)
        }
        
        // Attempt reconnection after 5 seconds
        setTimeout(() => {
          if (!websocket || websocket.readyState === WebSocket.CLOSED) {
            setupWebSocket()
          }
        }, 5000)
      }
      
      ws.onerror = (error) => {
        console.error('WebSocket error:', error)
      }
      
    } catch (error) {
      console.error('Failed to setup WebSocket:', error)
    }
  }

  const handleWebSocketMessage = (message) => {
    switch (message.type) {
      case 'pipeline_started':
        addNotification(`Pipeline ${message.pipeline_id} started`, 'info')
        break
      case 'pipeline_completed':
        addNotification(`Pipeline ${message.pipeline_id} completed successfully`, 'success')
        break
      case 'pipeline_error':
        addNotification(`Pipeline ${message.pipeline_id} failed: ${message.error}`, 'error')
        break
      case 'pipeline_update':
        // Handle real-time pipeline updates
        break
      case 'pong':
        // Handle ping response
        break
      default:
        console.log('Unknown WebSocket message type:', message.type)
    }
  }

  const addNotification = (message, type) => {
    const notification = {
      id: Date.now(),
      message,
      type,
      timestamp: new Date()
    }
    
    setNotifications(prev => [notification, ...prev.slice(0, 9)]) // Keep only 10 notifications
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
      setNotifications(prev => prev.filter(n => n.id !== notification.id))
    }, 5000)
  }

  const tabs = [
    {
      id: 'dashboard',
      name: 'Dashboard',
      icon: BarChart3,
      description: 'System overview and metrics'
    },
    {
      id: 'pipelines',
      name: 'Pipelines',
      icon: Zap,
      description: 'ETL pipeline management'
    },
    {
      id: 'sql',
      name: 'SQL Console',
      icon: Terminal,
      description: 'Interactive data queries'
    },
    {
      id: 'quality',
      name: 'Data Quality',
      icon: Shield,
      description: 'Data quality monitoring'
    },
    {
      id: 'schema',
      name: 'Schema Evolution',
      icon: GitBranch,
      description: 'Schema version management'
    },
    {
      id: 'streaming',
      name: 'Streaming',
      icon: Activity,
      description: 'Real-time processing'
    }
  ]

  const getConnectionStatus = () => {
    if (!websocket) return { status: 'disconnected', color: 'red' }
    
    switch (websocket.readyState) {
      case WebSocket.CONNECTING:
        return { status: 'connecting', color: 'yellow' }
      case WebSocket.OPEN:
        return { status: 'connected', color: 'green' }
      case WebSocket.CLOSING:
        return { status: 'closing', color: 'yellow' }
      case WebSocket.CLOSED:
        return { status: 'disconnected', color: 'red' }
      default:
        return { status: 'unknown', color: 'gray' }
    }
  }

  const connectionStatus = getConnectionStatus()

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-slate-900">
      {/* Background Pattern */}
      <div className="absolute inset-0 opacity-5">
        <div className="absolute inset-0" 
             style={{
               backgroundImage: 'radial-gradient(circle at 1px 1px, white 1px, transparent 1px)',
               backgroundSize: '40px 40px'
             }}>
        </div>
      </div>

      {/* Notifications */}
      <div className="fixed top-4 right-4 z-50 space-y-2">
        {notifications.map(notification => (
          <div
            key={notification.id}
            className={`
              px-4 py-3 rounded-lg border backdrop-blur-sm animate-in slide-in-from-right
              ${notification.type === 'success' 
                ? 'bg-green-500/10 border-green-500/20 text-green-400' 
                : notification.type === 'error'
                ? 'bg-red-500/10 border-red-500/20 text-red-400'
                : 'bg-blue-500/10 border-blue-500/20 text-blue-400'
              }
            `}
          >
            <div className="flex items-center space-x-2">
              {notification.type === 'success' && <CheckCircle className="w-4 h-4" />}
              {notification.type === 'error' && <AlertCircle className="w-4 h-4" />}
              {notification.type === 'info' && <Clock className="w-4 h-4" />}
              <span className="text-sm font-medium">{notification.message}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Header */}
      <header className="relative border-b border-slate-800/50 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between py-6">
            <div>
              <h1 className="text-3xl font-bold text-white flex items-center space-x-3">
                <Database className="w-8 h-8 text-blue-400" />
                <span>ETL Showcase</span>
              </h1>
              <p className="text-slate-300 mt-1">
                Professional data engineering platform
              </p>
            </div>
            
            {/* Status indicators */}
            <div className="flex items-center space-x-6">
              {/* Health Status */}
              <div className="flex items-center space-x-2">
                <div className={`w-3 h-3 rounded-full ${
                  health?.status === 'healthy' ? 'bg-green-400' : 'bg-red-400'
                }`}></div>
                <span className="text-white text-sm">
                  API {health?.status || 'unknown'}
                </span>
              </div>
              
              {/* WebSocket Status */}
              <div className="flex items-center space-x-2">
                <div className={`w-3 h-3 rounded-full bg-${connectionStatus.color}-400`}></div>
                <span className="text-white text-sm">
                  WS {connectionStatus.status}
                </span>
              </div>
              
              {/* System Metrics */}
              {systemMetrics.cpu_percent !== undefined && (
                <div className="hidden md:flex items-center space-x-4 text-sm">
                  <div className="flex items-center space-x-1">
                    <Gauge className="w-4 h-4 text-blue-400" />
                    <span className="text-slate-300">
                      CPU: {Math.round(systemMetrics.cpu_percent)}%
                    </span>
                  </div>
                  <div className="flex items-center space-x-1">
                    <Activity className="w-4 h-4 text-green-400" />
                    <span className="text-slate-300">
                      RAM: {Math.round(systemMetrics.memory_percent)}%
                    </span>
                  </div>
                </div>
              )}
            </div>
          </div>
          
          {/* Navigation Tabs */}
          <div className="border-t border-slate-800/50">
            <nav className="flex space-x-8" aria-label="Tabs">
              {tabs.map((tab) => {
                const Icon = tab.icon
                const isActive = activeTab === tab.id
                
                return (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={`
                      flex items-center space-x-2 py-4 px-1 border-b-2 font-medium text-sm
                      ${isActive
                        ? 'border-blue-500 text-blue-400'
                        : 'border-transparent text-slate-400 hover:text-slate-300 hover:border-slate-600'
                      }
                    `}
                    title={tab.description}
                  >
                    <Icon className="w-5 h-5" />
                    <span>{tab.name}</span>
                  </button>
                )
              })}
            </nav>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Render active tab content */}
        {activeTab === 'dashboard' && (
          <Dashboard 
            health={health} 
            systemMetrics={systemMetrics}
            websocket={websocket}
          />
        )}
        
        {activeTab === 'pipelines' && (
          <PipelineMonitor websocket={websocket} />
        )}
        
        {activeTab === 'sql' && (
          <SQLConsole />
        )}
        
        {activeTab === 'quality' && (
          <DataQuality />
        )}
        
        {activeTab === 'schema' && (
          <SchemaEvolution />
        )}
        
        {activeTab === 'streaming' && (
          <StreamingBenchmark />
        )}
      </main>
    </div>
  )
}

export default App