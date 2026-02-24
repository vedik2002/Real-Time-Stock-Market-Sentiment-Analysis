"""
Database Module - PostgreSQL/TimescaleDB Connection and Queries
"""

import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import asyncpg
from dotenv import load_dotenv

load_dotenv('../config/.env')

logger = logging.getLogger(__name__)


class Database:
    """Database connection and query handler"""
    
    def __init__(self):
        self.pool = None
        self.host = os.getenv('TIMESCALEDB_HOST', 'localhost')
        self.port = int(os.getenv('TIMESCALEDB_PORT', '5432'))
        self.database = os.getenv('TIMESCALEDB_DB', 'sentiment_db')
        self.user = os.getenv('TIMESCALEDB_USER', 'sentiment_user')
        self.password = os.getenv('TIMESCALEDB_PASSWORD', 'sentiment_pass')
    
    async def connect(self):
        """Create database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Database pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    async def check_health(self) -> bool:
        """Check database health"""
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    async def get_sentiment_data(
        self, 
        ticker: str, 
        hours: int = 24,
        source: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get sentiment data for a ticker
        
        Args:
            ticker: Stock ticker symbol
            hours: Hours of historical data
            source: Optional source filter
            
        Returns:
            List of sentiment records
        """
        query = """
            SELECT 
                id, timestamp, ticker, source, text, sentiment,
                sentiment_score, confidence, author, url,
                upvotes, comments_count
            FROM sentiment_data
            WHERE ticker = $1
                AND timestamp > NOW() - INTERVAL '{} hours'
                {}
            ORDER BY timestamp DESC
            LIMIT 1000
        """.format(hours, "AND source = $2" if source else "")
        
        try:
            async with self.pool.acquire() as conn:
                if source:
                    rows = await conn.fetch(query, ticker, source)
                else:
                    rows = await conn.fetch(query, ticker)
                
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching sentiment data: {e}")
            return []
    
    async def get_aggregated_sentiment(
        self,
        ticker: str,
        interval: str = '5min'
    ) -> List[Dict[str, Any]]:
        """Get aggregated sentiment statistics"""
        
        # Map interval to SQL
        interval_map = {
            '5min': '5 minutes',
            '1hour': '1 hour',
            '1day': '1 day'
        }
        
        sql_interval = interval_map.get(interval, '5 minutes')
        
        query = f"""
            SELECT 
                time_bucket($1::interval, timestamp) AS bucket,
                COUNT(*) AS mention_count,
                AVG(sentiment_score) AS avg_score,
                SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
                SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count,
                SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
                AVG(confidence) AS avg_confidence
            FROM sentiment_data
            WHERE ticker = $2
                AND timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY bucket
            ORDER BY bucket DESC
        """
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, sql_interval, ticker)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching aggregated sentiment: {e}")
            return []
    
    async def get_trending_stocks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending stocks based on mention volume"""
        query = """
            SELECT 
                ticker,
                COUNT(*) AS total_mentions,
                SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_mentions,
                SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_mentions,
                SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_mentions,
                AVG(sentiment_score) AS avg_sentiment_score,
                CASE 
                    WHEN AVG(sentiment_score) > 0.3 THEN 'bullish'
                    WHEN AVG(sentiment_score) < -0.3 THEN 'bearish'
                    ELSE 'neutral'
                END AS sentiment_trend,
                MAX(timestamp) AS last_mention
            FROM sentiment_data
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY ticker
            ORDER BY total_mentions DESC
            LIMIT $1
        """
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching trending stocks: {e}")
            return []
    
    async def get_price_correlation(
        self,
        ticker: str,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get sentiment-price correlation data"""
        query = """
            SELECT 
                time_bucket('1 hour', s.timestamp) AS timestamp,
                s.ticker,
                AVG(s.sentiment_score) AS avg_sentiment,
                COUNT(*) AS mention_count,
                AVG(p.close) AS avg_price,
                (MAX(p.close) - MIN(p.close)) / NULLIF(MIN(p.close), 0) * 100 AS price_change_pct
            FROM sentiment_data s
            LEFT JOIN stock_prices p 
                ON s.ticker = p.ticker 
                AND time_bucket('1 hour', s.timestamp) = time_bucket('1 hour', p.timestamp)
            WHERE s.ticker = $1
                AND s.timestamp > NOW() - INTERVAL '{} hours'
            GROUP BY time_bucket('1 hour', s.timestamp), s.ticker
            ORDER BY timestamp DESC
        """.format(hours)
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, ticker)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching correlation data: {e}")
            return []
    
    async def get_system_stats(self) -> Dict[str, Any]:
        """Get overall system statistics"""
        queries = {
            'total': "SELECT COUNT(*) FROM sentiment_data",
            'total_24h': "SELECT COUNT(*) FROM sentiment_data WHERE timestamp > NOW() - INTERVAL '24 hours'",
            'unique_tickers': "SELECT COUNT(DISTINCT ticker) FROM sentiment_data WHERE timestamp > NOW() - INTERVAL '24 hours'",
            'avg_sentiment': "SELECT AVG(sentiment_score) FROM sentiment_data WHERE timestamp > NOW() - INTERVAL '24 hours'",
            'db_size': "SELECT pg_database_size(current_database()) / 1024.0 / 1024.0"
        }
        
        try:
            async with self.pool.acquire() as conn:
                results = {}
                for key, query in queries.items():
                    results[key] = await conn.fetchval(query)
                
                # Calculate messages per second (rough estimate)
                if results['total_24h']:
                    results['messages_per_second'] = results['total_24h'] / (24 * 3600)
                else:
                    results['messages_per_second'] = 0
                
                return {
                    'total_messages': results['total'] or 0,
                    'messages_24h': results['total_24h'] or 0,
                    'unique_tickers': results['unique_tickers'] or 0,
                    'avg_sentiment_score': float(results['avg_sentiment'] or 0),
                    'messages_per_second': results['messages_per_second'],
                    'database_size_mb': float(results['db_size'] or 0),
                    'uptime_seconds': 0  # Would need to track this separately
                }
        except Exception as e:
            logger.error(f"Error fetching system stats: {e}")
            return {}
    
    async def get_available_tickers(self) -> List[str]:
        """Get list of tickers with recent data"""
        query = """
            SELECT DISTINCT ticker
            FROM sentiment_data
            WHERE timestamp > NOW() - INTERVAL '7 days'
            ORDER BY ticker
        """
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [row['ticker'] for row in rows]
        except Exception as e:
            logger.error(f"Error fetching tickers: {e}")
            return []
    
    async def get_latest_updates(self, minutes: int = 1) -> List[Dict[str, Any]]:
        """Get latest sentiment updates for WebSocket broadcasting"""
        query = """
            SELECT 
                ticker, source, sentiment, sentiment_score, 
                confidence, timestamp, text
            FROM sentiment_data
            WHERE timestamp > NOW() - INTERVAL '{} minutes'
            ORDER BY timestamp DESC
            LIMIT 100
        """.format(minutes)
        
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching latest updates: {e}")
            return []