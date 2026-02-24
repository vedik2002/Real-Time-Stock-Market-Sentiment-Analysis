"""
Reddit Scraper
Fetches stock mentions from Reddit using PRAW (Python Reddit API Wrapper)
"""

import os
import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
import praw
from prawcore.exceptions import ResponseException, RequestException
from dotenv import load_dotenv

load_dotenv('../config/.env')

logger = logging.getLogger(__name__)


class RedditScraper:
    """Scraper for Reddit stock mentions"""
    
    # Subreddits to monitor
    SUBREDDITS = [
        'wallstreetbets',
        'stocks', 
        'investing',
        'stockmarket',
        'options',
        'pennystocks',
        'swingtrading'
    ]
    
    def __init__(self):
        """Initialize Reddit API client"""
        self.client_id = os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = os.getenv('REDDIT_USER_AGENT', 'sentiment-analyzer/1.0')
        
        if not self.client_id or not self.client_secret:
            logger.warning("Reddit API credentials not found. Using mock data.")
            self.reddit = None
        else:
            try:
                self.reddit = praw.Reddit(
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    user_agent=self.user_agent
                )
                logger.info("Reddit client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Reddit client: {e}")
                self.reddit = None
    
    async def get_ticker_mentions(self, ticker: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Search for ticker mentions across multiple subreddits
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            limit: Maximum number of posts to fetch
            
        Returns:
            List of post dictionaries
        """
        if not self.reddit:
            return self._get_mock_data(ticker, limit)
        
        all_posts = []
        
        try:
            # Search across multiple subreddits
            for subreddit_name in self.SUBREDDITS[:3]:  # Limit to avoid rate limits
                try:
                    subreddit = self.reddit.subreddit(subreddit_name)
                    
                    # Search for ticker in post titles and body
                    search_query = f'${ticker} OR {ticker}'
                    
                    # Get posts from last 24 hours
                    posts = subreddit.search(
                        search_query,
                        sort='new',
                        time_filter='day',
                        limit=limit // len(self.SUBREDDITS[:3])
                    )
                    
                    for post in posts:
                    
                        if post.removed_by_category or '[removed]' in post.selftext:
                            continue
                        
                        post_data = {
                            'id': post.id,
                            'text': f"{post.title} {post.selftext}"[:500],  # Limit text length
                            'author': str(post.author) if post.author else '[deleted]',
                            'url': f"https://reddit.com{post.permalink}",
                            'upvotes': post.score,
                            'comments': post.num_comments,
                            'created_utc': datetime.fromtimestamp(post.created_utc).isoformat(),
                            'subreddit': subreddit_name
                        }
                        all_posts.append(post_data)
                    
                    # Rate limiting
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.warning(f"Error fetching from r/{subreddit_name}: {e}")
                    continue
            
            logger.info(f"Fetched {len(all_posts)} Reddit posts for {ticker}")
            return all_posts[:limit]
            
        except Exception as e:
            logger.error(f"Error in Reddit scraper: {e}")
            return self._get_mock_data(ticker, limit)
    
    async def get_trending_tickers(self, limit: int = 20) -> List[str]:
        """
        Get trending tickers from wallstreetbets
        
        Returns:
            List of ticker symbols
        """
        if not self.reddit:
            return ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
        
        try:
            subreddit = self.reddit.subreddit('wallstreetbets')
            hot_posts = subreddit.hot(limit=100)
            
            # Extract tickers using regex (simple approach)
            import re
            ticker_pattern = r'\$([A-Z]{1,5})\b'
            ticker_counts = {}
            
            for post in hot_posts:
                text = f"{post.title} {post.selftext}"
                tickers = re.findall(ticker_pattern, text)
                
                for ticker in tickers:
                    if len(ticker) >= 2:  # Filter out single letters
                        ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
            
            # Sort by frequency
            sorted_tickers = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)
            return [ticker for ticker, _ in sorted_tickers[:limit]]
            
        except Exception as e:
            logger.error(f"Error getting trending tickers: {e}")
            return ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    
    def _get_mock_data(self, ticker: str, limit: int) -> List[Dict[str, Any]]:
        """Generate mock data when API is unavailable"""
        logger.info(f"Generating mock data for {ticker}")
        
        mock_texts = [
            f"Just bought 100 shares of ${ticker}! To the moon! 🚀",
            f"{ticker} earnings report looking strong this quarter",
            f"Thinking of selling my ${ticker} position, thoughts?",
            f"DD: Why ${ticker} is undervalued right now",
            f"${ticker} breaking resistance at $150, bullish signal",
            f"Anyone else worried about ${ticker} recent news?",
            f"Long term hold on ${ticker}, great fundamentals",
            f"${ticker} options play for next week?",
            f"Why I'm bearish on ${ticker} short term",
            f"${ticker} institutional buying increasing"
        ]
        
        posts = []
        now = datetime.utcnow()
        
        for i in range(min(limit, len(mock_texts))):
            posts.append({
                'id': f'mock_{ticker}_{i}',
                'text': mock_texts[i % len(mock_texts)],
                'author': f'mock_user_{i}',
                'url': f'https://reddit.com/r/wallstreetbets/mock_{i}',
                'upvotes': (i + 1) * 10,
                'comments': (i + 1) * 5,
                'created_utc': (now - timedelta(hours=i)).isoformat(),
                'subreddit': 'wallstreetbets'
            })
        
        return posts


# Test function
async def test_scraper():
    """Test the Reddit scraper"""
    scraper = RedditScraper()
    
    tickers = ['AAPL', 'TSLA', 'GME']
    
    for ticker in tickers:
        print(f"\n=== Testing {ticker} ===")
        posts = await scraper.get_ticker_mentions(ticker, limit=5)
        
        for post in posts[:2]:  # Show first 2
            print(f"\nPost ID: {post['id']}")
            print(f"Text: {post['text'][:100]}...")
            print(f"Upvotes: {post['upvotes']}, Comments: {post['comments']}")
    
    print("\n=== Trending Tickers ===")
    trending = await scraper.get_trending_tickers(limit=10)
    print(trending)


if __name__ == '__main__':
    asyncio.run(test_scraper())