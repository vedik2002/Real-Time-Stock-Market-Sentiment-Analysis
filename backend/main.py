"""
FastAPI Backend - REST API and WebSocket Server
Serves sentiment data and real-time updates
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Optional
from dotenv import load_dotenv

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from database import Database
from models import (
    SentimentResponse, 
    TrendingStock, 
    PriceCorrelation,
    HealthResponse,
    StatsResponse
)
from websocket import ConnectionManager

# Load environment variables
load_dotenv('../config/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Stock Sentiment Analysis API",
    description="Real-time sentiment analysis for stock market discussions",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


db = Database()
ws_manager = ConnectionManager()


@app.on_event("startup")
async def startup_event():
    """Initialize resources on startup"""
    logger.info("Starting API server...")
    await db.connect()


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on shutdown"""
    logger.info("Shutting down API server...")
    await db.disconnect()


@app.get("/", response_model=dict)
async def root():
    """Root endpoint"""
    return {
        "service": "Stock Sentiment Analysis API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "sentiment": "/api/sentiment",
            "trending": "/api/trending",
            "correlation": "/api/correlation",
            "websocket": "/ws"
        }
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
      
        db_healthy = await db.check_health()
        
        return HealthResponse(
            status="healthy" if db_healthy else "degraded",
            timestamp=datetime.utcnow().isoformat(),
            database="connected" if db_healthy else "disconnected",
            active_connections=ws_manager.get_connection_count()
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.utcnow().isoformat(),
            database="error",
            active_connections=0
        )


@app.get("/api/sentiment", response_model=List[SentimentResponse])
async def get_sentiment(
    ticker: str = Query(..., description="Stock ticker symbol"),
    hours: int = Query(24, ge=1, le=168, description="Hours of historical data"),
    source: Optional[str] = Query(None, description="Filter by source (reddit/twitter)")
):
    """
    Get sentiment data for a ticker
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL')
        hours: Hours of historical data to retrieve (1-168)
        source: Optional source filter
        
    Returns:
        List of sentiment records
    """
    try:
        data = await db.get_sentiment_data(ticker, hours, source)
        return [SentimentResponse(**record) for record in data]
    except Exception as e:
        logger.error(f"Error fetching sentiment data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentiment/aggregated")
async def get_aggregated_sentiment(
    ticker: str = Query(..., description="Stock ticker symbol"),
    interval: str = Query("5min", description="Aggregation interval (5min/1hour/1day)")
):
    """
    Get aggregated sentiment data
    
    Args:
        ticker: Stock ticker symbol
        interval: Time interval for aggregation
        
    Returns:
        Aggregated sentiment statistics
    """
    try:
        data = await db.get_aggregated_sentiment(ticker, interval)
        return {"ticker": ticker, "interval": interval, "data": data}
    except Exception as e:
        logger.error(f"Error fetching aggregated sentiment: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trending", response_model=List[TrendingStock])
async def get_trending_stocks(
    limit: int = Query(10, ge=1, le=50, description="Number of stocks to return")
):
    """
    Get trending stocks based on mention volume
    
    Args:
        limit: Maximum number of stocks to return
        
    Returns:
        List of trending stocks with sentiment
    """
    try:
        data = await db.get_trending_stocks(limit)
        return [TrendingStock(**record) for record in data]
    except Exception as e:
        logger.error(f"Error fetching trending stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/correlation", response_model=List[PriceCorrelation])
async def get_price_correlation(
    ticker: str = Query(..., description="Stock ticker symbol"),
    hours: int = Query(24, ge=1, le=168, description="Hours of historical data")
):
    """
    Get sentiment-price correlation data
    
    Args:
        ticker: Stock ticker symbol
        hours: Hours of historical data
        
    Returns:
        Correlation data points
    """
    try:
        data = await db.get_price_correlation(ticker, hours)
        return [PriceCorrelation(**record) for record in data]
    except Exception as e:
        logger.error(f"Error fetching correlation data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats", response_model=StatsResponse)
async def get_stats():
    """
    Get overall system statistics
    
    Returns:
        System statistics
    """
    try:
        stats = await db.get_system_stats()
        return StatsResponse(**stats)
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tickers")
async def get_available_tickers():
    """
    Get list of available tickers with data
    
    Returns:
        List of ticker symbols
    """
    try:
        tickers = await db.get_available_tickers()
        return {"tickers": tickers}
    except Exception as e:
        logger.error(f"Error fetching tickers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates
    
    Sends real-time sentiment updates to connected clients
    """
    await ws_manager.connect(websocket)
    
    try:
        while True:
            
            data = await websocket.receive_text()
            message = json.loads(data)
            
           
            if message.get("action") == "subscribe":
                ticker = message.get("ticker")
                logger.info(f"Client subscribed to {ticker}")
                
                await ws_manager.subscribe(websocket, ticker)
            
            elif message.get("action") == "unsubscribe":
                ticker = message.get("ticker")
                logger.info(f"Client unsubscribed from {ticker}")
                await ws_manager.unsubscribe(websocket, ticker)
            

            await websocket.send_json({
                "type": "ack",
                "message": f"Action {message.get('action')} processed"
            })
            
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        ws_manager.disconnect(websocket)


@app.get("/api/metrics")
async def get_metrics():
    """
    Prometheus-style metrics endpoint
    
    Returns:
        System metrics in Prometheus format
    """
    try:
        stats = await db.get_system_stats()
        
        metrics = f"""
# HELP sentiment_messages_total Total number of sentiment messages processed
# TYPE sentiment_messages_total counter
sentiment_messages_total {stats.get('total_messages', 0)}

# HELP sentiment_messages_rate Current messages per second
# TYPE sentiment_messages_rate gauge
sentiment_messages_rate {stats.get('messages_per_second', 0)}

# HELP websocket_connections Active WebSocket connections
# TYPE websocket_connections gauge
websocket_connections {ws_manager.get_connection_count()}

# HELP database_query_time_seconds Average database query time
# TYPE database_query_time_seconds gauge
database_query_time_seconds {stats.get('avg_query_time', 0)}
"""
        
        return JSONResponse(content=metrics, media_type="text/plain")
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

import asyncio

async def broadcast_updates():
    """Background task to broadcast real-time updates to WebSocket clients"""
    while True:
        try:
     
            latest_data = await db.get_latest_updates()
            
            if latest_data:
             
                for record in latest_data:
                    await ws_manager.broadcast({
                        "type": "sentiment_update",
                        "data": record
                    })
            
           
            await asyncio.sleep(5)  
            
        except Exception as e:
            logger.error(f"Error in broadcast task: {e}")
            await asyncio.sleep(5)


@app.on_event("startup")
async def start_background_tasks():
   
    asyncio.create_task(broadcast_updates())


def main():
  
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', '8000'))
    workers = int(os.getenv('API_WORKERS', '4'))
    debug = os.getenv('API_DEBUG', 'true').lower() == 'true'
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        workers=1 if debug else workers, 
        reload=debug,
        log_level="info"
    )


if __name__ == '__main__':
    main()