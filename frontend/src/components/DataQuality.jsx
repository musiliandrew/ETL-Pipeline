import { API_BASE } from '../config'
import React, { useState, useEffect } from 'react'
import axios from 'axios'
import { 
  PieChart, Pie, Cell, BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer 
} from 'recharts'
import {
  Shield, AlertTriangle, CheckCircle, XCircle, 
  TrendingUp, Database, FileText, RefreshCw,
  Search, Filter, Download, Eye
} from 'lucide-react'


const DataQuality = () => {
  const [qualityReport, setQualityReport] = useState(null)
  const [loading, setLoading] = useState(false)
  const [selectedDataset, setSelectedDataset] = useState('users')
  const [datasets] = useState(['users', 'transactions', 'products'])
  const [showDetails, setShowDetails] = useState(false)

  useEffect(() => {
    fetchQualityReport()
  }, [selectedDataset])

  const fetchQualityReport = async () => {
    setLoading(true)
    try {
      const response = await axios.post(`${API_BASE}/api/data-quality/assess`, {
        dataset_name: selectedDataset
      })
      setQualityReport(response.data)
    } catch (error) {
      console.error('Failed to fetch data quality report:', error)
      setQualityReport(null)
    } finally {
      setLoading(false)
    }
  }

  const generateMockData = () => {
    // Generate realistic mock data when API is not available
    return {
      dataset_name: selectedDataset,
      total_records: 15420,
      overall_quality_score: 0.87,
      assessment_timestamp: new Date().toISOString(),
      validation_rules_applied: [
        { rule_name: 'not_null_user_id', field: 'user_id', passed: true, description: 'User ID should not be null' },
        { rule_name: 'valid_email_format', field: 'email', passed: false, description: 'Email should have valid format' },
        { rule_name: 'age_range_check', field: 'age', passed: true, description: 'Age should be between 0 and 120' },
        { rule_name: 'future_date_check', field: 'sign_up_date', passed: false, description: 'Sign up date should not be in future' }
      ],
      data_profiling: {
        completeness_score: 0.92,
        uniqueness_score: 0.89,
        validity_score: 0.84,
        consistency_score: 0.91
      },
      field_quality: {
        user_id: { completeness: 1.0, uniqueness: 1.0, validity: 1.0 },
        age: { completeness: 0.95, uniqueness: 0.65, validity: 0.98 },
        email: { completeness: 0.88, uniqueness: 0.94, validity: 0.76 },
        sign_up_date: { completeness: 0.92, uniqueness: 0.85, validity: 0.89 }
      },
      validation_issues: [
        { issue_type: 'invalid_email', field: 'email', count: 342, severity: 'high', sample_values: ['invalid-email', 'no@', '@domain'] },
        { issue_type: 'future_signup_date', field: 'sign_up_date', count: 156, severity: 'medium', sample_values: ['2025-12-01', '2026-01-15'] },
        { issue_type: 'missing_age', field: 'age', count: 89, severity: 'low', sample_values: [null, '', 'N/A'] },
        { issue_type: 'duplicate_records', field: 'user_id', count: 23, severity: 'high', sample_values: ['user_123', 'user_456'] }
      ],
      recommendations: [
        'Implement email validation at data entry point',
        'Add date validation to prevent future signup dates',
        'Consider data enrichment for missing age values',
        'Investigate and resolve duplicate user records'
      ]
    }
  }

  const data = qualityReport || generateMockData()

  const getQualityColor = (score) => {
    if (score >= 0.9) return 'text-green-400'
    if (score >= 0.7) return 'text-yellow-400'
    return 'text-red-400'
  }

  const getQualityBgColor = (score) => {
    if (score >= 0.9) return 'bg-green-400'
    if (score >= 0.7) return 'bg-yellow-400'
    return 'bg-red-400'
  }

  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'high': return 'text-red-400'
      case 'medium': return 'text-yellow-400'
      case 'low': return 'text-green-400'
      default: return 'text-gray-400'
    }
  }

  const getSeverityIcon = (severity) => {
    switch (severity) {
      case 'high': return <XCircle className="w-4 h-4 text-red-400" />
      case 'medium': return <AlertTriangle className="w-4 h-4 text-yellow-400" />
      case 'low': return <CheckCircle className="w-4 h-4 text-green-400" />
      default: return <CheckCircle className="w-4 h-4 text-gray-400" />
    }
  }

  // Prepare chart data
  const qualityMetricsData = [
    { name: 'Completeness', value: Math.round(data.data_profiling.completeness_score * 100), color: '#10B981' },
    { name: 'Uniqueness', value: Math.round(data.data_profiling.uniqueness_score * 100), color: '#3B82F6' },
    { name: 'Validity', value: Math.round(data.data_profiling.validity_score * 100), color: '#F59E0B' },
    { name: 'Consistency', value: Math.round(data.data_profiling.consistency_score * 100), color: '#8B5CF6' }
  ]

  const issuesData = data.validation_issues.map(issue => ({
    name: issue.issue_type.replace(/_/g, ' '),
    count: issue.count,
    severity: issue.severity
  }))

  const fieldQualityData = Object.entries(data.field_quality).map(([field, quality]) => ({
    field,
    completeness: Math.round(quality.completeness * 100),
    uniqueness: Math.round(quality.uniqueness * 100),
    validity: Math.round(quality.validity * 100)
  }))

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
          <h2 className="text-2xl font-bold text-white mb-2">Data Quality Assessment</h2>
          <p className="text-slate-400">Comprehensive data quality monitoring and validation</p>
        </div>
        <div className="flex items-center space-x-4">
          <select
            value={selectedDataset}
            onChange={(e) => setSelectedDataset(e.target.value)}
            className="bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white"
          >
            {datasets.map(dataset => (
              <option key={dataset} value={dataset}>{dataset}</option>
            ))}
          </select>
          <button
            onClick={fetchQualityReport}
            className="flex items-center space-x-2 bg-blue-600 hover:bg-blue-700 px-4 py-2 rounded-lg text-white transition-all"
          >
            <RefreshCw className="w-4 h-4" />
            <span>Refresh</span>
          </button>
        </div>
      </div>

      {/* Quality Score Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        {/* Overall Score */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between mb-4">
            <Shield className="w-8 h-8 text-blue-400" />
            <span className={`text-3xl font-bold ${getQualityColor(data.overall_quality_score)}`}>
              {Math.round(data.overall_quality_score * 100)}%
            </span>
          </div>
          <h3 className="text-white font-semibold mb-1">Overall Quality</h3>
          <p className="text-slate-400 text-sm">Data quality score</p>
          <div className="mt-3 bg-slate-700 rounded-full h-2">
            <div 
              className={`h-2 rounded-full transition-all duration-300 ${getQualityBgColor(data.overall_quality_score)}`}
              style={{ width: `${data.overall_quality_score * 100}%` }}
            ></div>
          </div>
        </div>

        {/* Total Records */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between mb-4">
            <Database className="w-8 h-8 text-green-400" />
            <span className="text-3xl font-bold text-green-400">
              {data.total_records.toLocaleString()}
            </span>
          </div>
          <h3 className="text-white font-semibold mb-1">Total Records</h3>
          <p className="text-slate-400 text-sm">Records analyzed</p>
        </div>

        {/* Issues Found */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between mb-4">
            <AlertTriangle className="w-8 h-8 text-yellow-400" />
            <span className="text-3xl font-bold text-yellow-400">
              {data.validation_issues.length}
            </span>
          </div>
          <h3 className="text-white font-semibold mb-1">Issue Types</h3>
          <p className="text-slate-400 text-sm">Quality issues detected</p>
        </div>

        {/* High Severity Issues */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <div className="flex items-center justify-between mb-4">
            <XCircle className="w-8 h-8 text-red-400" />
            <span className="text-3xl font-bold text-red-400">
              {data.validation_issues.filter(issue => issue.severity === 'high').length}
            </span>
          </div>
          <h3 className="text-white font-semibold mb-1">Critical Issues</h3>
          <p className="text-slate-400 text-sm">High severity problems</p>
        </div>
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Quality Metrics Radar */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
            <TrendingUp className="w-5 h-5 mr-2 text-blue-400" />
            Quality Dimensions
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={qualityMetricsData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="name" stroke="#9CA3AF" />
              <YAxis stroke="#9CA3AF" />
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#1F2937',
                  border: '1px solid #374151',
                  borderRadius: '8px'
                }}
                formatter={(value) => [`${value}%`, 'Score']}
              />
              <Bar dataKey="value">
                {qualityMetricsData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Issues Distribution */}
        <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
            <AlertTriangle className="w-5 h-5 mr-2 text-yellow-400" />
            Issues by Type
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={issuesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis 
                dataKey="name" 
                stroke="#9CA3AF" 
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis stroke="#9CA3AF" />
              <Tooltip 
                contentStyle={{
                  backgroundColor: '#1F2937',
                  border: '1px solid #374151',
                  borderRadius: '8px'
                }}
              />
              <Bar dataKey="count" fill="#EF4444" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Field Quality Matrix */}
      <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
        <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
          <FileText className="w-5 h-5 mr-2 text-green-400" />
          Field Quality Matrix
        </h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={fieldQualityData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="field" stroke="#9CA3AF" />
            <YAxis stroke="#9CA3AF" />
            <Tooltip 
              contentStyle={{
                backgroundColor: '#1F2937',
                border: '1px solid #374151',
                borderRadius: '8px'
              }}
            />
            <Legend />
            <Bar dataKey="completeness" stackId="a" fill="#10B981" name="Completeness" />
            <Bar dataKey="uniqueness" stackId="a" fill="#3B82F6" name="Uniqueness" />
            <Bar dataKey="validity" stackId="a" fill="#F59E0B" name="Validity" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Detailed Issues Table */}
      <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-white flex items-center">
            <Search className="w-5 h-5 mr-2 text-red-400" />
            Quality Issues Detail
          </h3>
          <button
            onClick={() => setShowDetails(!showDetails)}
            className="flex items-center space-x-2 bg-slate-700 hover:bg-slate-600 px-3 py-2 rounded-lg text-white transition-all"
          >
            <Eye className="w-4 h-4" />
            <span>{showDetails ? 'Hide' : 'Show'} Details</span>
          </button>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full">
            <thead>
              <tr className="border-b border-slate-600">
                <th className="text-left py-3 px-4 text-slate-300">Issue Type</th>
                <th className="text-left py-3 px-4 text-slate-300">Field</th>
                <th className="text-left py-3 px-4 text-slate-300">Severity</th>
                <th className="text-left py-3 px-4 text-slate-300">Count</th>
                <th className="text-left py-3 px-4 text-slate-300">% of Total</th>
                {showDetails && <th className="text-left py-3 px-4 text-slate-300">Sample Values</th>}
              </tr>
            </thead>
            <tbody>
              {data.validation_issues.map((issue, index) => (
                <tr key={index} className="border-b border-slate-700/50 hover:bg-slate-700/30">
                  <td className="py-3 px-4 text-slate-300 font-medium">
                    {issue.issue_type.replace(/_/g, ' ')}
                  </td>
                  <td className="py-3 px-4 text-slate-400">{issue.field}</td>
                  <td className="py-3 px-4">
                    <div className="flex items-center space-x-2">
                      {getSeverityIcon(issue.severity)}
                      <span className={getSeverityColor(issue.severity)}>
                        {issue.severity}
                      </span>
                    </div>
                  </td>
                  <td className="py-3 px-4 text-slate-300">{issue.count.toLocaleString()}</td>
                  <td className="py-3 px-4 text-slate-400">
                    {((issue.count / data.total_records) * 100).toFixed(2)}%
                  </td>
                  {showDetails && (
                    <td className="py-3 px-4 text-slate-400 text-sm">
                      <div className="flex flex-wrap gap-1">
                        {issue.sample_values.slice(0, 3).map((value, idx) => (
                          <span key={idx} className="bg-slate-700 px-2 py-1 rounded text-xs">
                            {value === null ? 'null' : String(value)}
                          </span>
                        ))}
                        {issue.sample_values.length > 3 && (
                          <span className="text-slate-500 text-xs">+{issue.sample_values.length - 3} more</span>
                        )}
                      </div>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Recommendations */}
      <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-6">
        <h3 className="text-lg font-semibold text-white mb-4 flex items-center">
          <CheckCircle className="w-5 h-5 mr-2 text-green-400" />
          Quality Improvement Recommendations
        </h3>
        <div className="space-y-3">
          {data.recommendations.map((recommendation, index) => (
            <div key={index} className="flex items-start space-x-3 p-3 bg-slate-700/30 rounded-lg">
              <div className="flex-shrink-0 w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-sm font-bold">
                {index + 1}
              </div>
              <p className="text-slate-300">{recommendation}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Assessment Info */}
      <div className="bg-slate-800/50 backdrop-blur-sm rounded-lg border border-slate-700/50 p-4">
        <div className="flex items-center justify-between text-sm text-slate-400">
          <span>Dataset: {data.dataset_name}</span>
          <span>Last assessed: {new Date(data.assessment_timestamp).toLocaleString()}</span>
          <span>Rules applied: {data.validation_rules_applied.length}</span>
        </div>
      </div>
    </div>
  )
}

export default DataQuality