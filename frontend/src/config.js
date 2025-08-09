/**
 * Application Configuration
 * =======================
 * Handles environment-based API endpoints
 */

// Get API base URL from environment or default to localhost
export const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

// Get WebSocket URL from environment or derive from API base
export const WS_URL = import.meta.env.VITE_WS_URL || API_BASE.replace('http', 'ws') + '/ws'

// Application settings
export const APP_NAME = import.meta.env.VITE_APP_NAME || 'ETL Showcase'
export const APP_VERSION = import.meta.env.VITE_APP_VERSION || '2.0.0'
export const ENVIRONMENT = import.meta.env.VITE_ENVIRONMENT || 'development'

// Debug configuration
export const DEBUG = {
  ENABLED: ENVIRONMENT === 'development',
  LOG_API_CALLS: ENVIRONMENT === 'development',
  LOG_WEBSOCKET: ENVIRONMENT === 'development'
}

console.log('🔧 Frontend Configuration:')
console.log('  API Base:', API_BASE)
console.log('  WebSocket URL:', WS_URL) 
console.log('  Environment:', ENVIRONMENT)
console.log('  App Name:', APP_NAME)