"""
Kafka Producer - Data Ingestion Service
Fetches data from Reddit, Twitter, and Stock APIs and publishes to Kafka
"""

import os
import json
import time
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

from reddit_scraper import RedditScraper
from twitter_scraper import TwitterScraper
from stock_api import StockAPI

# Load environment variables
load_dotenv('../config/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SentimentDataProducer:
    """Main producer class that orchestrates data collection and publishing"""
    
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock-mentions')
        self.tickers = os.getenv('STOCK_TICKERS', 'AAPL,MSFT,GOOGL').split(',')
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='gzip',
            linger_ms=10,  # Batch messages for 10ms
            batch_size=16384,  # 16KB batches
            max_in_flight_requests_per_connection=5,
            retries=3,
            acks='all'  # Wait for all replicas
        )
        
        # Initialize scrapers
        self.reddit = RedditScraper()
        self.twitter = TwitterScraper() if os.getenv('TWITTER_ENABLED', 'false').lower() == 'true' else None
        self.stock_api = StockAPI()
        
        # Metrics
        self.messages_sent = 0
        self.errors = 0
        self.start_time = time.time()
        
        logger.info(f"Producer initialized for tickers: {self.tickers}")
        logger.info(f"Kafka servers: {self.kafka_servers}")
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            self.errors += 1
            logger.error(f'Message delivery failed: {err}')
        else:
            self.messages_sent += 1
            if self.messages_sent % 100 == 0:
                logger.info(f'Messages sent: {self.messages_sent}')
    
    def send_message(self, data: Dict[str, Any]) -> None:
        """Send a message to Kafka with error handling"""
        try:
            # Use ticker as partition key for even distribution
            key = data.get('ticker', 'unknown')
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=data
            )
            
            # Add callback
            future.add_callback(lambda metadata: self.delivery_callback(None, metadata))
            future.add_errback(lambda e: self.delivery_callback(e, None))
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            self.errors += 1
    
    async def fetch_reddit_data(self, ticker: str) -> List[Dict[str, Any]]:
        """Fetch Reddit posts/comments for a ticker"""
        try:
            posts = await self.reddit.get_ticker_mentions(ticker, limit=50)
            
            messages = []
            for post in posts:
                message = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'ticker': ticker,
                    'source': 'reddit',
                    'source_id': post['id'],
                    'text': post['text'],
                    'author': post['author'],
                    'url': post['url'],
                    'upvotes': post['upvotes'],
                    'comments_count': post['comments'],
                    'created_utc': post['created_utc']
                }
                messages.append(message)
            
            return messages
        except Exception as e:
            logger.error(f"Error fetching Reddit data for {ticker}: {e}")
            return []
    
    async def fetch_twitter_data(self, ticker: str) -> List[Dict[str, Any]]:
        """Fetch Twitter mentions for a ticker"""
        if not self.twitter:
            return []
        
        try:
            tweets = await self.twitter.get_ticker_mentions(ticker, limit=50)
            
            messages = []
            for tweet in tweets:
                message = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'ticker': ticker,
                    'source': 'twitter',
                    'source_id': tweet['id'],
                    'text': tweet['text'],
                    'author': tweet['author'],
                    'url': tweet['url'],
                    'likes': tweet['likes'],
                    'retweets': tweet['retweets'],
                    'created_at': tweet['created_at']
                }
                messages.append(message)
            
            return messages
        except Exception as e:
            logger.error(f"Error fetching Twitter data for {ticker}: {e}")
            return []
    
    async def fetch_stock_prices(self, ticker: str) -> Dict[str, Any]:
        """Fetch current stock price"""
        try:
            price_data = await self.stock_api.get_quote(ticker)
            
            message = {
                'timestamp': datetime.utcnow().isoformat(),
                'ticker': ticker,
                'source': 'stock_price',
                'open': price_data['open'],
                'high': price_data['high'],
                'low': price_data['low'],
                'close': price_data['price'],
                'volume': price_data['volume'],
                'change_percent': price_data['change_percent']
            }
            
            return message
        except Exception as e:
            logger.error(f"Error fetching stock price for {ticker}: {e}")
            return None
    
    async def process_ticker(self, ticker: str) -> None:
        """Process all data sources for a single ticker"""
        logger.info(f"Processing ticker: {ticker}")
        
        # Fetch data concurrently
        tasks = [
            self.fetch_reddit_data(ticker),
            self.fetch_twitter_data(ticker),
            self.fetch_stock_prices(ticker)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Send Reddit messages
        for message in results[0] if isinstance(results[0], list) else []:
            self.send_message(message)
        
        # Send Twitter messages
        for message in results[1] if isinstance(results[1], list) else []:
            self.send_message(message)
        
        # Send stock price
        if results[2] and not isinstance(results[2], Exception):
            self.send_message(results[2])
    
    async def run(self):
        """Main run loop"""
        logger.info("Starting data producer...")
        
        iteration = 0
        while True:
            try:
                iteration += 1
                logger.info(f"\n=== Iteration {iteration} ===")
                
               
                tasks = [self.process_ticker(ticker.strip()) for ticker in self.tickers]
                await asyncio.gather(*tasks)
                
                
                self.producer.flush()
                
                # Log metrics
                elapsed = time.time() - self.start_time
                rate = self.messages_sent / elapsed if elapsed > 0 else 0
                logger.info(f"Metrics - Sent: {self.messages_sent}, Errors: {self.errors}, Rate: {rate:.2f} msg/s")
                
         
                await asyncio.sleep(30)  
                
            except KeyboardInterrupt:
                logger.info("Shutting down producer...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                await asyncio.sleep(5)
        
        # Cleanup
        self.producer.close()
        logger.info("Producer shut down gracefully")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        elapsed = time.time() - self.start_time
        return {
            'messages_sent': self.messages_sent,
            'errors': self.errors,
            'uptime_seconds': elapsed,
            'messages_per_second': self.messages_sent / elapsed if elapsed > 0 else 0
        }


def main():
 
    producer = SentimentDataProducer()
    
    try:
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        stats = producer.get_stats()
        logger.info(f"Final stats: {json.dumps(stats, indent=2)}")


if __name__ == '__main__':
    main()