# ETL Showcase 🚀

A professional **ETL (Extract, Transform, Load)** pipeline demonstration showcasing modern data engineering patterns with a beautiful web interface.

## 🎯 Features

- **Professional ETL Pipeline**: CSV → PostgreSQL with production-grade error handling
- **Real-time Web Dashboard**: Live pipeline monitoring and control
- **WebSocket Updates**: Real-time progress tracking
- **Production Components**: Dead letter queues, monitoring, schema evolution
- **Clean Architecture**: Modular, maintainable codebase

## 🏗️ Architecture

```
├── backend/                 # FastAPI Server + ETL Pipeline
│   ├── etl/                # Core ETL modules
│   │   ├── extractor.py    # Data extraction from CSV/JSON
│   │   ├── transform.py    # Data cleaning & standardization  
│   │   ├── load.py         # PostgreSQL loading
│   │   ├── pipeline.py     # Basic orchestration
│   │   └── production_pipeline.py # Enterprise features
│   ├── data/               # Sample data & processing areas
│   │   ├── input/          # Source CSV files
│   │   ├── processed/      # Clean data output
│   │   └── deadletter/     # Failed records
│   └── main.py             # FastAPI web server
└── frontend/               # React Dashboard
    └── src/
        └── App.jsx         # Pipeline control interface
```

## 🚀 Quick Start

### 1. Backend Setup
```bash
cd backend/
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

pip install -r requirements.txt
python main.py
```
**Backend runs on:** http://localhost:8000

### 2. Frontend Setup
```bash
cd frontend/
npm install
npm run dev
```
**Frontend runs on:** http://localhost:5173

### 3. Database Setup (PostgreSQL)
```bash
# Create database
createdb etl_showcase

# Set environment variables (create .env in backend/)
DB_HOST=localhost
DB_PORT=5432  
DB_NAME=etl_showcase
DB_USER=your_username
DB_PASSWORD=your_password
```

## 🎮 How to Use

1. **Start both servers** (backend + frontend)
2. **Open browser** to http://localhost:5173
3. **Select sample file** from dropdown
4. **Click "Start ETL Pipeline"**
5. **Watch real-time progress** as data flows through Extract → Transform → Load

## 🔧 ETL Pipeline Stages

### 1. **Extract** 📥
- Reads CSV/JSON files using pandas
- Comprehensive error handling for malformed data
- Supports multiple file formats

### 2. **Transform** 🔄
- Data cleaning and standardization
- Type conversion (strings → integers, dates)  
- Data validation and quality checks
- Handles missing/invalid values gracefully

### 3. **Load** 📤
- Loads clean data into PostgreSQL
- Batch processing for performance
- Connection pooling and retry logic
- Schema validation

## 🎯 Production Features

- **Dead Letter Queue**: Failed records are isolated for manual review
- **Monitoring**: Pipeline metrics and performance tracking
- **Schema Evolution**: Handles changing data formats over time
- **Retry Logic**: Automatic retry for transient failures
- **Health Checks**: System status monitoring
- **WebSocket Updates**: Real-time pipeline status

## 🛠️ Technology Stack

**Backend:**
- FastAPI (REST API + WebSockets)
- pandas (data processing)
- SQLAlchemy (database ORM)
- PostgreSQL (data warehouse)
- uvicorn (ASGI server)

**Frontend:**
- React 18 (UI framework)
- Vite (build tool)
- Axios (HTTP client)
- WebSocket (real-time updates)

## 📊 Sample Data

The project includes sample CSV files with realistic user data:
- `users_messy.csv`: Demonstrates data cleaning capabilities
- `demo_users.csv`: Clean sample dataset
- `products.csv`: Product catalog data

## 🔗 API Endpoints

- `GET /health` - System health check
- `POST /api/pipeline/start` - Start ETL pipeline
- `GET /api/pipeline/{id}` - Get pipeline status
- `GET /api/pipeline` - List all pipelines  
- `GET /api/data/sample-files` - Available sample files
- `WebSocket /ws` - Real-time updates

## 🎨 Screenshots

The web interface provides:
- **System Status Dashboard** - Health checks and database connectivity
- <img width="1314" height="927" alt="image" src="https://github.com/user-attachments/assets/1f6b103c-edb1-46bc-a40b-bb17bc7ed11e" />

- **Pipeline Control Panel** - File selection and pipeline triggering
- <img width="1322" height="929" alt="image" src="https://github.com/user-attachments/assets/5da19f4a-859a-400c-8e38-9a9b8eb920b6" />

- **Real-time Progress** - Live updates with progress bars
- <img width="1416" height="867" alt="image" src="https://github.com/user-attachments/assets/1f8ceddb-e2cc-4089-9618-925fc9dec58c" />

- **Results Display** - Processing results and error handling
- <img width="1329" height="919" alt="image" src="https://github.com/user-attachments/assets/041ba1c3-8dd3-4364-801b-e231b25e948e" />


## 🚀 Deployment Ready

This showcase is designed to demonstrate production-ready patterns:
- Environment-based configuration
- Proper error handling and logging
- Scalable architecture
- Database connection pooling
- WebSocket support for real-time features

## 📝 License

This project is for educational and portfolio demonstration purposes.

---

**Ready to showcase professional ETL pipeline development!** 🎉
