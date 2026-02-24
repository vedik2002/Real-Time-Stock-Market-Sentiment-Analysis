"""
WebSocket Connection Manager
Handles WebSocket connections and broadcasting
"""

import json
import logging
from typing import Dict, Set, Any
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and message broadcasting"""
    
    def __init__(self):
        # Store active connections
        self.active_connections: Set[WebSocket] = set()
        
        # Store subscriptions per ticker
        self.subscriptions: Dict[str, Set[WebSocket]] = {}
        
        # Store connection metadata
        self.connection_data: Dict[WebSocket, Dict[str, Any]] = {}
    
    async def connect(self, websocket: WebSocket):
        """Accept and register a new WebSocket connection"""
        await websocket.accept()
        self.active_connections.add(websocket)
        self.connection_data[websocket] = {
            'connected_at': None,
            'subscriptions': set()
        }
        logger.info(f"New WebSocket connection. Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        
        # Remove from all subscriptions
        if websocket in self.connection_data:
            for ticker in self.connection_data[websocket].get('subscriptions', set()):
                if ticker in self.subscriptions:
                    self.subscriptions[ticker].discard(websocket)
                    if not self.subscriptions[ticker]:
                        del self.subscriptions[ticker]
            
            del self.connection_data[websocket]
        
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")
    
    async def subscribe(self, websocket: WebSocket, ticker: str):
        """Subscribe a connection to ticker updates"""
        if ticker not in self.subscriptions:
            self.subscriptions[ticker] = set()
        
        self.subscriptions[ticker].add(websocket)
        
        if websocket in self.connection_data:
            self.connection_data[websocket]['subscriptions'].add(ticker)
        
        logger.info(f"WebSocket subscribed to {ticker}")
    
    async def unsubscribe(self, websocket: WebSocket, ticker: str):
        """Unsubscribe a connection from ticker updates"""
        if ticker in self.subscriptions:
            self.subscriptions[ticker].discard(websocket)
            
            if not self.subscriptions[ticker]:
                del self.subscriptions[ticker]
        
        if websocket in self.connection_data:
            self.connection_data[websocket]['subscriptions'].discard(ticker)
        
        logger.info(f"WebSocket unsubscribed from {ticker}")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send a message to a specific connection"""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Error sending personal message: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: dict):
        """Broadcast a message to all connected clients"""
        disconnected = []
        
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to connection: {e}")
                disconnected.append(connection)

        for connection in disconnected:
            self.disconnect(connection)
    
    async def broadcast_to_ticker(self, ticker: str, message: dict):
        """Broadcast a message to all clients subscribed to a ticker"""
        if ticker not in self.subscriptions:
            return
        
        disconnected = []
        
        for connection in self.subscriptions[ticker]:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to ticker subscriber: {e}")
                disconnected.append(connection)
        
        
        for connection in disconnected:
            self.disconnect(connection)
    
    def get_connection_count(self) -> int:
        """Get number of active connections"""
        return len(self.active_connections)
    
    def get_subscription_count(self, ticker: str) -> int:
        """Get number of subscriptions for a ticker"""
        return len(self.subscriptions.get(ticker, set()))
    
    def get_all_subscriptions(self) -> Dict[str, int]:
        """Get subscription counts for all tickers"""
        return {ticker: len(connections) for ticker, connections in self.subscriptions.items()}