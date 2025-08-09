import { API_BASE } from '../config'
import React, { useState, useEffect } from 'react'
import axios from 'axios'
import JSONPretty from 'react-json-pretty'
import { 
  LineChart, Line, AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer 
} from 'recharts'
import {
  GitBranch, FileText, Plus, Minus, Edit3, AlertTriangle,
  Clock, CheckCircle, Database, Code, History, RefreshCw
} from 'lucide-react'


const SchemaEvolution = () => {
  const [schemaHistory, setSchemaHistory] = useState([])
  const [currentSchema, setCurrentSchema] = useState(null)
  const [evolutionStats, setEvolutionStats] = useState(null)
  const [selectedVersion, setSelectedVersion] = useState(null)
  const [loading, setLoading] = useState(false)
  const [showDiff, setShowDiff] = useState(false)

  useEffect(() => {
    fetchSchemaData()
  }, [])

  const fetchSchemaData = async () => {
    setLoading(true)
    try {
      // Try to fetch real data
      const response = await axios.get(`${API_BASE}/api/schema/history`)
      setSchemaHistory(response.data.versions || [])
      setCurrentSchema(response.data.current_schema)
      setEvolutionStats(response.data.stats)
    } catch (error) {
      console.error('Failed to fetch schema data:', error)
      // Use mock data
      setSchemaHistory(generateMockSchemaHistory())
      setCurrentSchema(generateMockCurrentSchema())
      setEvolutionStats(generateMockStats())
    } finally {
      setLoading(false)
    }
  }

  const generateMockSchemaHistory = () => {
    const changes = [
      {
        version: 'v1.0.0',
        timestamp: '2024-01-15T10:30:00Z',
        change_type: 'initial_schema',
        description: 'Initial schema creation with basic user fields',
        changes: [
          { type: 'add_table', table: 'users', details: 'Created users table with id, name, email' }
        ],
        schema_snapshot: {
          users: {
            user_id: { type: 'UUID', nullable: false, primary_key: true },
            name: { type: 'VARCHAR(255)', nullable: false },
            email: { type: 'VARCHAR(255)', nullable: false }
          }
        },
        records_affected: 0,
        processing_time_ms: 45,
        compatibility: 'forward_compatible'
      },
      {
        version: 'v1.1.0',
        timestamp: '2024-01-22T14:20:00Z',
        change_type: 'add_column',
        description: 'Added age column to support demographic analysis',
        changes: [
          { type: 'add_column', table: 'users', column: 'age', details: 'Added age INTEGER column' }
        ],
        schema_snapshot: {
          users: {
            user_id: { type: 'UUID', nullable: false, primary_key: true },
            name: { type: 'VARCHAR(255)', nullable: false },
            email: { type: 'VARCHAR(255)', nullable: false },
            age: { type: 'INTEGER', nullable: true }
          }
        },
        records_affected: 1205,
        processing_time_ms: 120,
        compatibility: 'backward_compatible'
      },
      {
        version: 'v1.2.0',
        timestamp: '2024-02-03T09:15:00Z',
        change_type: 'add_column',
        description: 'Added signup tracking fields for user analytics',
        changes: [
          { type: 'add_column', table: 'users', column: 'sign_up_date', details: 'Added sign_up_date DATE column' },
          { type: 'add_column', table: 'users', column: 'is_active', details: 'Added is_active BOOLEAN column' }
        ],
        schema_snapshot: {
          users: {
            user_id: { type: 'UUID', nullable: false, primary_key: true },
            name: { type: 'VARCHAR(255)', nullable: false },
            email: { type: 'VARCHAR(255)', nullable: false },
            age: { type: 'INTEGER', nullable: true },
            sign_up_date: { type: 'DATE', nullable: true },
            is_active: { type: 'BOOLEAN', nullable: true, default: true }
          }
        },
        records_affected: 2840,
        processing_time_ms: 230,
        compatibility: 'backward_compatible'
      },
      {
        version: 'v1.3.0',
        timestamp: '2024-02-18T16:45:00Z',
        change_type: 'modify_column',
        description: 'Extended email field length for longer email addresses',
        changes: [
          { type: 'modify_column', table: 'users', column: 'email', details: 'Changed email from VARCHAR(255) to VARCHAR(500)' }
        ],
        schema_snapshot: {
          users: {
            user_id: { type: 'UUID', nullable: false, primary_key: true },
            name: { type: 'VARCHAR(255)', nullable: false },
            email: { type: 'VARCHAR(500)', nullable: false },
            age: { type: 'INTEGER', nullable: true },
            sign_up_date: { type: 'DATE', nullable: true },
            is_active: { type: 'BOOLEAN', nullable: true, default: true }
          }
        },
        records_affected: 4520,
        processing_time_ms: 890,
        compatibility: 'backward_compatible'
      },
      {
        version: 'v2.0.0',
        timestamp: '2024-03-01T11:30:00Z',
        change_type: 'add_table',
        description: 'Added user preferences table for personalization features',
        changes: [
          { type: 'add_table', table: 'user_preferences', details: 'Created user_preferences table' }
        ],
        schema_snapshot: {
          users: {
            user_id: { type: 'UUID', nullable: false, primary_key: true },
            name: { type: 'VARCHAR(255)', nullable: false },
            email: { type: 'VARCHAR(500)', nullable: false },
            age: { type: 'INTEGER', nullable: true },
            sign_up_date: { type: 'DATE', nullable: true },
            is_active: { type: 'BOOLEAN', nullable: true, default: true }
          },
          user_preferences: {
            id: { type: 'UUID', nullable: false, primary_key: true },
            user_id: { type: 'UUID', nullable: false, foreign_key: 'users.user_id' },
            preferences: { type: 'JSONB', nullable: true },
            created_at: { type: 'TIMESTAMP', nullable: false, default: 'now()' }
          }
        },
        records_affected: 0,
        processing_time_ms: 150,
        compatibility: 'minor_breaking'
      }
    ]

    return changes.reverse() // Most recent first
  }

  const generateMockCurrentSchema = () => {
    return {
      users: {
        user_id: { type: 'UUID', nullable: false, primary_key: true },
        name: { type: 'VARCHAR(255)', nullable: false },
        email: { type: 'VARCHAR(500)', nullable: false },
        age: { type: 'INTEGER', nullable: true },
        sign_up_date: { type: 'DATE', nullable: true },
        is_active: { type: 'BOOLEAN', nullable: true, default: true }
      },
      user_preferences: {
        id: { type: 'UUID', nullable: false, primary_key: true },
        user_id: { type: 'UUID', nullable: false, foreign_key: 'users.user_id' },
        preferences: { type: 'JSONB', nullable: true },
        created_at: { type: 'TIMESTAMP', nullable: false, default: 'now()' }
      }
    }
  }

  const generateMockStats = () => {
    return {
      total_versions: 5,
      total_changes: 8,
      avg_processing_time_ms: 287,
      compatibility_distribution: {
        backward_compatible: 3,
        forward_compatible: 1,
        minor_breaking: 1,
        major_breaking: 0
      },
      change_frequency: [
        { date: '2024-01', changes: 2 },
        { date: '2024-02', changes: 2 },
        { date: '2024-03', changes: 1 }
      ]
    }
  }

  const getChangeIcon = (changeType) => {
    switch (changeType) {
      case 'add_column':
      case 'add_table':
        return <Plus className="w-4 h-4 text-green-400" />
      case 'remove_column':
      case 'remove_table':
        return <Minus className="w-4 h-4 text-red-400" />
      case 'modify_column':
        return <Edit3 className="w-4 h-4 text-yellow-400" />
      case 'initial_schema':
        return <Database className="w-4 h-4 text-blue-400" />
      default:
        return <FileText className="w-4 h-4 text-gray-400" />
    }
  }

  const getCompatibilityColor = (compatibility) => {
    switch (compatibility) {
      case 'backward_compatible': return 'text-green-400'
      case 'forward_compatible': return 'text-blue-400'
      case 'minor_breaking': return 'text-yellow-400'
      case 'major_breaking': return 'text-red-400'
      default: return 'text-gray-400'
    }
  }

  const getCompatibilityBadge = (compatibility) => {
    const colors = {
      backward_compatible: 'bg-green-500/20 border-green-500/30 text-green-300',
      forward_compatible: 'bg-blue-500/20 border-blue-500/30 text-blue-300',
      minor_breaking: 'bg-yellow-500/20 border-yellow-500/30 text-yellow-300',
      major_breaking: 'bg-red-500/20 border-red-500/30 text-red-300'
    }

    return (
      <span className={`px-2 py-1 rounded-full text-xs border ${colors[compatibility] || colors.backward_compatible}`}>
        {compatibility?.replace('_', ' ') || 'Compatible'}
      </span>
    )
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-400"></div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white mb-2">Schema Evolution</h2>
          <p className="text-slate-400">Track and manage database schema changes over time</p>
        </div>
        <button
          onClick={fetchSchemaData}
          className="flex items-center space-x-2 bg-blue-600 hover:bg-blue-700 px-4 py-2 rounded-lg text-white transition-all"
        >
          <RefreshCw className="w-4 h-4" />
          <span>Refresh</span>
        </button>
      </div>

      {/* Stats Overview */}
      {evolutionStats && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
            <div className="flex items-center justify-between mb-2">
              <GitBranch className="w-6 h-6 text-blue-400" />
              <span className="text-2xl font-bold text-blue-400">{evolutionStats.total_versions}</span>
            </div>
            <h3 className="text-white font-semibold">Schema Versions</h3>
            <p className="text-slate-400 text-sm">Total versions tracked</p>
          </div>

          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
            <div className="flex items-center justify-between mb-2">
              <Edit3 className="w-6 h-6 text-green-400" />
              <span className="text-2xl font-bold text-green-400">{evolutionStats.total_changes}</span>
            </div>
            <h3 className="text-white font-semibold">Total Changes</h3>
            <p className="text-slate-400 text-sm">Schema modifications</p>
          </div>

          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
            <div className="flex items-center justify-between mb-2">
              <Clock className="w-6 h-6 text-yellow-400" />
              <span className="text-2xl font-bold text-yellow-400">{evolutionStats.avg_processing_time_ms}ms</span>
            </div>
            <h3 className="text-white font-semibold">Avg Processing</h3>
            <p className="text-slate-400 text-sm">Time per change</p>
          </div>

          <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
            <div className="flex items-center justify-between mb-2">
              <CheckCircle className="w-6 h-6 text-purple-400" />
              <span className="text-2xl font-bold text-purple-400">
                {evolutionStats.compatibility_distribution.backward_compatible}
              </span>
            </div>
            <h3 className="text-white font-semibold">Compatible</h3>
            <p className="text-slate-400 text-sm">Backward compatible</p>
          </div>
        </div>
      )}

      {/* Change Frequency Chart */}
      {evolutionStats && (
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
            <GitBranch className="w-5 h-5 mr-2 text-blue-400" />
            Schema Change Activity
          </h3>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={evolutionStats.change_frequency}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="date" stroke="#9CA3AF" />
              <YAxis stroke="#9CA3AF" />
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#1F2937',
                  border: '1px solid #374151',
                  borderRadius: '8px'
                }}
              />
              <Area 
                type="monotone" 
                dataKey="changes" 
                stroke="#3B82F6" 
                fill="#3B82F6" 
                fillOpacity={0.3}
                name="Schema Changes"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Schema History Timeline */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
            <History className="w-5 h-5 mr-2 text-green-400" />
            Evolution Timeline
          </h3>
          
          <div className="space-y-4 max-h-96 overflow-y-auto">
            {schemaHistory.map((change, index) => (
              <div
                key={change.version}
                onClick={() => setSelectedVersion(selectedVersion?.version === change.version ? null : change)}
                className={`p-4 rounded-lg border cursor-pointer transition-all ${
                  selectedVersion?.version === change.version
                    ? 'bg-blue-600/20 border-blue-500/50'
                    : 'bg-slate-700/30 border-slate-600/50 hover:bg-slate-700/50'
                }`}
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center space-x-2">
                    {getChangeIcon(change.change_type)}
                    <span className="text-white font-medium">{change.version}</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    {getCompatibilityBadge(change.compatibility)}
                    <span className="text-xs text-slate-400">
                      {new Date(change.timestamp).toLocaleDateString()}
                    </span>
                  </div>
                </div>
                
                <p className="text-slate-300 text-sm mb-2">{change.description}</p>
                
                <div className="flex justify-between items-center text-xs text-slate-400">
                  <span>{change.records_affected.toLocaleString()} records affected</span>
                  <span>{change.processing_time_ms}ms processing time</span>
                </div>
                
                {change.changes.map((changeDetail, idx) => (
                  <div key={idx} className="mt-2 p-2 bg-slate-600/30 rounded text-xs">
                    <span className="text-slate-300">{changeDetail.details}</span>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>

        {/* Schema Details */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-white flex items-center">
              <Code className="w-5 h-5 mr-2 text-yellow-400" />
              {selectedVersion ? `Schema ${selectedVersion.version}` : 'Current Schema'}
            </h3>
            {selectedVersion && (
              <button
                onClick={() => setShowDiff(!showDiff)}
                className="text-sm bg-slate-700 hover:bg-slate-600 px-3 py-1 rounded text-white transition-all"
              >
                {showDiff ? 'Hide' : 'Show'} Diff
              </button>
            )}
          </div>

          <div className="bg-slate-900/50 rounded-lg p-4 overflow-auto max-h-96">
            <JSONPretty
              data={selectedVersion ? selectedVersion.schema_snapshot : currentSchema}
              theme={{
                main: 'line-height:1.3;color:#e2e8f0;background:transparent;overflow:auto;',
                error: 'line-height:1.3;color:#e74c3c;background:transparent;',
                key: 'color:#3b82f6;',
                string: 'color:#10b981;',
                value: 'color:#f59e0b;',
                boolean: 'color:#8b5cf6;'
              }}
            />
          </div>
          
          {selectedVersion && (
            <div className="mt-4 p-3 bg-slate-700/30 rounded-lg">
              <h4 className="text-white font-medium mb-2">Change Details</h4>
              <div className="space-y-1 text-sm">
                <div className="flex justify-between">
                  <span className="text-slate-400">Version:</span>
                  <span className="text-white">{selectedVersion.version}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-400">Type:</span>
                  <span className="text-white capitalize">{selectedVersion.change_type.replace('_', ' ')}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-400">Compatibility:</span>
                  <span className={getCompatibilityColor(selectedVersion.compatibility)}>
                    {selectedVersion.compatibility.replace('_', ' ')}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-400">Records Affected:</span>
                  <span className="text-white">{selectedVersion.records_affected.toLocaleString()}</span>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Compatibility Distribution */}
      {evolutionStats && (
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
            <CheckCircle className="w-5 h-5 mr-2 text-green-400" />
            Compatibility Impact Analysis
          </h3>
          
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {Object.entries(evolutionStats.compatibility_distribution).map(([type, count]) => (
              <div key={type} className="bg-slate-700/30 rounded-lg p-4 text-center">
                <div className={`text-2xl font-bold mb-1 ${getCompatibilityColor(type)}`}>
                  {count}
                </div>
                <div className="text-slate-300 text-sm capitalize">
                  {type.replace('_', ' ')}
                </div>
              </div>
            ))}
          </div>
          
          <div className="mt-6 p-4 bg-blue-500/10 border border-blue-500/30 rounded-lg">
            <div className="flex items-start space-x-3">
              <AlertTriangle className="w-5 h-5 text-blue-400 mt-0.5" />
              <div>
                <h4 className="text-blue-300 font-medium mb-1">Schema Evolution Best Practices</h4>
                <ul className="text-blue-200 text-sm space-y-1 list-disc list-inside">
                  <li>Always maintain backward compatibility when possible</li>
                  <li>Use nullable columns for new fields to avoid breaking changes</li>
                  <li>Version your schemas and maintain migration scripts</li>
                  <li>Test schema changes in staging environments first</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default SchemaEvolution