"""
Pydantic Models for API Request/Response Validation
"""

from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class SentimentResponse(BaseModel):
    """Response model for sentiment data"""
    id: int
    timestamp: datetime
    ticker: str
    source: str
    text: str
    sentiment: str
    sentiment_score: float = Field(..., ge=-1.0, le=1.0)
    confidence: float = Field(..., ge=0.0, le=1.0)
    author: Optional[str] = None
    url: Optional[str] = None
    upvotes: int = 0
    comments_count: int = 0
    
    class Config:
        from_attributes = True


class TrendingStock(BaseModel):
    """Response model for trending stocks"""
    ticker: str
    total_mentions: int
    positive_mentions: int
    negative_mentions: int
    neutral_mentions: int = 0
    avg_sentiment_score: float = Field(..., ge=-1.0, le=1.0)
    sentiment_trend: str  # 'bullish', 'bearish', 'neutral'
    last_mention: datetime
    
    class Config:
        from_attributes = True


class PriceCorrelation(BaseModel):
    """Response model for price-sentiment correlation"""
    timestamp: datetime
    ticker: str
    avg_sentiment: float
    mention_count: int
    avg_price: Optional[float] = None
    price_change_pct: Optional[float] = None
    
    class Config:
        from_attributes = True


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str  # 'healthy', 'degraded', 'unhealthy'
    timestamp: str
    database: str
    active_connections: int


class StatsResponse(BaseModel):
    """Response model for system statistics"""
    total_messages: int
    messages_24h: int
    unique_tickers: int
    avg_sentiment_score: float
    messages_per_second: float
    database_size_mb: float
    uptime_seconds: float
    
    class Config:
        from_attributes = True


class WebSocketMessage(BaseModel):
    """WebSocket message model"""
    type: str  # 'sentiment_update', 'price_update', 'error'
    data: Dict[str, Any]
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class SubscriptionRequest(BaseModel):
    """WebSocket subscription request"""
    action: str  # 'subscribe', 'unsubscribe'
    ticker: Optional[str] = None
    tickers: Optional[list] = None