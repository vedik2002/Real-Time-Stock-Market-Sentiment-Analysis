"""
Twitter Scraper
Fetches stock mentions from Twitter using Twitter API v2
"""

import os
import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
import httpx
from dotenv import load_dotenv

load_dotenv('../config/.env')

logger = logging.getLogger(__name__)


class TwitterScraper:
    """Scraper for Twitter stock mentions"""
    
    BASE_URL = "https://api.twitter.com/2"
    
    def __init__(self):
        """Initialize Twitter API client"""
        self.bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        
        if not self.bearer_token:
            logger.warning("Twitter API token not found. Using mock data.")
            self.client = None
        else:
            self.headers = {
                'Authorization': f'Bearer {self.bearer_token}',
                'Content-Type': 'application/json'
            }
            self.client = httpx.AsyncClient(headers=self.headers, timeout=30.0)
            logger.info("Twitter client initialized successfully")
    
    async def get_ticker_mentions(self, ticker: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Search for ticker mentions on Twitter
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            limit: Maximum number of tweets to fetch
            
        Returns:
            List of tweet dictionaries
        """
        if not self.client:
            return self._get_mock_data(ticker, limit)
        
        try:
            # Build search query
            query = f"(${ticker} OR {ticker}) -is:retweet lang:en"
            
            # API endpoint
            url = f"{self.BASE_URL}/tweets/search/recent"
            
            # Parameters
            params = {
                'query': query,
                'max_results': min(limit, 100),  # API limit is 100
                'tweet.fields': 'created_at,public_metrics,author_id',
                'expansions': 'author_id',
                'user.fields': 'username'
            }
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract tweets
            tweets = []
            if 'data' in data:
                # Create user lookup
                users = {}
                if 'includes' in data and 'users' in data['includes']:
                    users = {user['id']: user['username'] for user in data['includes']['users']}
                
                for tweet in data['data']:
                    tweet_data = {
                        'id': tweet['id'],
                        'text': tweet['text'][:500],  # Limit text length
                        'author': users.get(tweet['author_id'], 'unknown'),
                        'url': f"https://twitter.com/user/status/{tweet['id']}",
                        'likes': tweet['public_metrics']['like_count'],
                        'retweets': tweet['public_metrics']['retweet_count'],
                        'created_at': tweet['created_at']
                    }
                    tweets.append(tweet_data)
            
            logger.info(f"Fetched {len(tweets)} tweets for {ticker}")
            return tweets
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching tweets: {e.response.status_code}")
            return self._get_mock_data(ticker, limit)
        except Exception as e:
            logger.error(f"Error in Twitter scraper: {e}")
            return self._get_mock_data(ticker, limit)
    
    async def get_trending_tickers(self, limit: int = 20) -> List[str]:
        """
        Get trending stock tickers on Twitter
        
        Returns:
            List of ticker symbols
        """
        if not self.client:
            return ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        
        try:
            # Get trending topics (requires elevated access)
            url = f"{self.BASE_URL}/trends/place"
            
            # For US trends
            params = {'id': '23424977'}  
            
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            
           
            import re
            ticker_pattern = r'\$([A-Z]{2,5})\b'
            
            data = response.json()
            tickers = set()
            
            for trend in data[0]['trends'][:50]:
                matches = re.findall(ticker_pattern, trend['name'])
                tickers.update(matches)
            
            return list(tickers)[:limit]
            
        except Exception as e:
            logger.error(f"Error getting trending tickers: {e}")
            return ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    
    def _get_mock_data(self, ticker: str, limit: int) -> List[Dict[str, Any]]:
        """Generate mock data when API is unavailable"""
        logger.info(f"Generating mock Twitter data for {ticker}")
        
        mock_texts = [
            f"${ticker} just hit a new ATH! 📈 #stocks #investing",
            f"Bought the dip on {ticker} today. Long term bullish 🚀",
            f"${ticker} earnings call tomorrow. What are your predictions?",
            f"Technical analysis: {ticker} forming a cup and handle pattern",
            f"Why ${ticker} is my top pick for 2024",
            f"Sold half my ${ticker} position to lock in profits",
            f"${ticker} PE ratio still looks attractive at these levels",
            f"Institutional money flowing into ${ticker} - very bullish signal",
            f"Bearish on ${ticker} short term, but long term holder",
            f"${ticker} breaking out! Time to add to position"
        ]
        
        tweets = []
        now = datetime.utcnow()
        
        for i in range(min(limit, len(mock_texts))):
            tweets.append({
                'id': f'mock_tw_{ticker}_{i}',
                'text': mock_texts[i % len(mock_texts)],
                'author': f'trader_{i}',
                'url': f'https://twitter.com/user/status/mock_{i}',
                'likes': (i + 1) * 20,
                'retweets': (i + 1) * 5,
                'created_at': (now - timedelta(hours=i)).isoformat()
            })
        
        return tweets
    
    async def close(self):
        """Close the HTTP client"""
        if self.client:
            await self.client.aclose()


# Test function
async def test_scraper():
    """Test the Twitter scraper"""
    scraper = TwitterScraper()
    
    try:
        tickers = ['AAPL', 'TSLA']
        
        for ticker in tickers:
            print(f"\n=== Testing {ticker} ===")
            tweets = await scraper.get_ticker_mentions(ticker, limit=5)
            
            for tweet in tweets[:2]:  # Show first 2
                print(f"\nTweet ID: {tweet['id']}")
                print(f"Text: {tweet['text'][:100]}...")
                print(f"Likes: {tweet['likes']}, Retweets: {tweet['retweets']}")
    
    finally:
        await scraper.close()


if __name__ == '__main__':
    asyncio.run(test_scraper())