"""
Stock Price API
Fetches real-time stock prices using Alpha Vantage API
"""

import os
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import httpx
from dotenv import load_dotenv

load_dotenv('../config/.env')

logger = logging.getLogger(__name__)


class StockAPI:
    """Client for fetching stock prices"""
    
    BASE_URL = "https://www.alphavantage.co/query"
    
    def __init__(self):
        """Initialize Stock API client"""
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.rate_limit = int(os.getenv('ALPHA_VANTAGE_RATE_LIMIT', '5'))
        
        if not self.api_key:
            logger.warning("Alpha Vantage API key not found. Using mock data.")
            self.client = None
        else:
            self.client = httpx.AsyncClient(timeout=30.0)
            logger.info("Stock API client initialized successfully")
        
        # Rate limiting
        self._last_request_time = 0
        self._request_interval = 12  # 5 requests per minute = 12 sec interval
    
    async def _rate_limit(self):
        """Implement rate limiting"""
        import time
        elapsed = time.time() - self._last_request_time
        if elapsed < self._request_interval:
            await asyncio.sleep(self._request_interval - elapsed)
        self._last_request_time = time.time()
    
    async def get_quote(self, ticker: str) -> Dict[str, Any]:
        """
        Get real-time quote for a ticker
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            
        Returns:
            Dictionary with price data
        """
        if not self.client:
            return self._get_mock_quote(ticker)
        
        try:
            await self._rate_limit()
            
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': ticker,
                'apikey': self.api_key
            }
            
            response = await self.client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"API error for {ticker}: {data['Error Message']}")
                return self._get_mock_quote(ticker)
            
            if 'Note' in data:
                logger.warning(f"API rate limit hit: {data['Note']}")
                return self._get_mock_quote(ticker)
            
            # Parse response
            quote = data.get('Global Quote', {})
            
            if not quote:
                logger.warning(f"No quote data for {ticker}")
                return self._get_mock_quote(ticker)
            
            return {
                'ticker': ticker,
                'price': float(quote.get('05. price', 0)),
                'open': float(quote.get('02. open', 0)),
                'high': float(quote.get('03. high', 0)),
                'low': float(quote.get('04. low', 0)),
                'volume': int(quote.get('06. volume', 0)),
                'previous_close': float(quote.get('08. previous close', 0)),
                'change': float(quote.get('09. change', 0)),
                'change_percent': quote.get('10. change percent', '0%').rstrip('%'),
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching quote for {ticker}: {e.response.status_code}")
            return self._get_mock_quote(ticker)
        except Exception as e:
            logger.error(f"Error fetching quote for {ticker}: {e}")
            return self._get_mock_quote(ticker)
    
    async def get_intraday(self, ticker: str, interval: str = '5min') -> Dict[str, Any]:
        """
        Get intraday time series data
        
        Args:
            ticker: Stock ticker symbol
            interval: Time interval (1min, 5min, 15min, 30min, 60min)
            
        Returns:
            Dictionary with time series data
        """
        if not self.client:
            return self._get_mock_intraday(ticker)
        
        try:
            await self._rate_limit()
            
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': ticker,
                'interval': interval,
                'apikey': self.api_key,
                'outputsize': 'compact'  # Last 100 data points
            }
            
            response = await self.client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for errors
            if 'Error Message' in data or 'Note' in data:
                return self._get_mock_intraday(ticker)
            
            # Parse time series
            time_series_key = f'Time Series ({interval})'
            time_series = data.get(time_series_key, {})
            
            prices = []
            for timestamp, values in list(time_series.items())[:20]:  # Last 20 data points
                prices.append({
                    'timestamp': timestamp,
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['5. volume'])
                })
            
            return {
                'ticker': ticker,
                'interval': interval,
                'data': prices
            }
            
        except Exception as e:
            logger.error(f"Error fetching intraday data for {ticker}: {e}")
            return self._get_mock_intraday(ticker)
    
    def _get_mock_quote(self, ticker: str) -> Dict[str, Any]:
        """Generate mock quote data"""
        import random
        
        # Base prices for common tickers
        base_prices = {
            'AAPL': 175.0,
            'MSFT': 380.0,
            'GOOGL': 140.0,
            'AMZN': 155.0,
            'TSLA': 240.0,
            'NVDA': 495.0,
            'META': 350.0,
            'NFLX': 450.0,
            'AMD': 120.0,
            'GME': 25.0,
            'AMC': 5.0
        }
        
        base_price = base_prices.get(ticker, 100.0)
        
        # Add random variation
        price = base_price * (1 + random.uniform(-0.02, 0.02))
        open_price = price * (1 + random.uniform(-0.01, 0.01))
        high = max(price, open_price) * (1 + random.uniform(0, 0.01))
        low = min(price, open_price) * (1 - random.uniform(0, 0.01))
        previous_close = base_price
        change = price - previous_close
        change_percent = (change / previous_close) * 100
        
        return {
            'ticker': ticker,
            'price': round(price, 2),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'volume': random.randint(1000000, 50000000),
            'previous_close': round(previous_close, 2),
            'change': round(change, 2),
            'change_percent': f"{change_percent:.2f}",
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _get_mock_intraday(self, ticker: str) -> Dict[str, Any]:
        """Generate mock intraday data"""
        import random
        from datetime import timedelta
        
        base_price = self._get_mock_quote(ticker)['price']
        
        now = datetime.utcnow()
        prices = []
        
        for i in range(20):
            timestamp = now - timedelta(minutes=5*i)
            price_var = base_price * (1 + random.uniform(-0.01, 0.01))
            
            prices.append({
                'timestamp': timestamp.isoformat(),
                'open': round(price_var, 2),
                'high': round(price_var * 1.005, 2),
                'low': round(price_var * 0.995, 2),
                'close': round(price_var, 2),
                'volume': random.randint(100000, 1000000)
            })
        
        return {
            'ticker': ticker,
            'interval': '5min',
            'data': prices
        }
    
    async def close(self):
        """Close the HTTP client"""
        if self.client:
            await self.client.aclose()


# Test function
async def test_api():
    """Test the stock API"""
    api = StockAPI()
    
    try:
        tickers = ['AAPL', 'TSLA', 'GME']
        
        for ticker in tickers:
            print(f"\n=== Quote for {ticker} ===")
            quote = await api.get_quote(ticker)
            print(f"Price: ${quote['price']}")
            print(f"Change: {quote['change']} ({quote['change_percent']}%)")
            print(f"Volume: {quote['volume']:,}")
        
        print(f"\n=== Intraday for AAPL ===")
        intraday = await api.get_intraday('AAPL')
        print(f"Got {len(intraday['data'])} data points")
        for point in intraday['data'][:3]:
            print(f"{point['timestamp']}: ${point['close']}")
    
    finally:
        await api.close()


if __name__ == '__main__':
    asyncio.run(test_api())