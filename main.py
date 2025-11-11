"""
Upstox WebSocket Server - Fixed Startup Issue

Problem: ws_client was None because startup() wasn't running properly
Solution: Proper async initialization
"""

import asyncio
import json
import ssl
import websockets
import requests
from datetime import datetime
from typing import List, Optional, Dict
import logging
from collections import defaultdict

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Protobuf
try:
    import MarketDataFeedV3_pb2 as pb
    from google.protobuf.json_format import MessageToDict
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False
    logging.warning("⚠️  Protobuf not available")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============= Config =============
# 👇 உங்கள் access token இங்கே போடுங்க
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN_HERE"


# ============= Pydantic Models =============

class TokenUpdate(BaseModel):
    access_token: str

class SubscriptionRequest(BaseModel):
    instruments: List[str]
    mode: str = 'full'

class UnsubscriptionRequest(BaseModel):
    instruments: List[str]

class ModeChangeRequest(BaseModel):
    instruments: List[str]
    new_mode: str


# ============= Connection Manager =============

class ConnectionManager:
    """Manage dashboard WebSocket connections"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self.lock:
            self.active_connections.append(websocket)
        logger.info(f"✅ Dashboard connected. Total: {len(self.active_connections)}")
    
    async def disconnect(self, websocket: WebSocket):
        async with self.lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        logger.info(f"❌ Dashboard disconnected. Total: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        """Broadcast to all dashboards"""
        if not self.active_connections:
            return
        
        disconnected = []
        async with self.lock:
            connections = self.active_connections.copy()
        
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Broadcast error: {e}")
                disconnected.append(connection)
        
        for conn in disconnected:
            await self.disconnect(conn)


manager = ConnectionManager()


# ============= Upstox WebSocket Client =============

class UpstoxWebSocketClient:
    """Upstox WebSocket client"""
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.is_connected = False
        self.should_run = True
        
        self.subscribed_instruments = {}
        self.latest_data = {}
        
        self.total_messages = 0
        self.reconnection_count = 0
        self.connection_start_time = None
        self.last_message_time = None
        
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        self.max_retries = 10
        self.base_retry_delay = 2
        self.current_retry = 0
    
    def get_authorization(self):
        """Get WebSocket URL"""
        headers = {
            'Accept': '*/*',
            'Authorization': f'Bearer {self.access_token}'
        }
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
        
        response = requests.get(url=url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    
    def decode_protobuf(self, buffer):
        """Decode protobuf"""
        if not PROTOBUF_AVAILABLE:
            return None
        
        try:
            feed_response = pb.FeedResponse()
            feed_response.ParseFromString(buffer)
            return feed_response
        except Exception as e:
            logger.error(f"Decode error: {e}")
            return None
    
    async def send_message(self, instruments: List[str], mode: str, method: str):
        """Send WebSocket message"""
        if not self.websocket or not self.is_connected:
            logger.warning("⚠️  Not connected")
            return False
        
        try:
            data = {
                "guid": f"guid_{datetime.now().timestamp()}",
                "method": method,
                "data": {
                    "mode": mode,
                    "instrumentKeys": instruments
                }
            }
            
            await self.websocket.send(json.dumps(data).encode('utf-8'))
            logger.info(f"✅ Sent {method}: {len(instruments)} instruments")
            return True
            
        except Exception as e:
            logger.error(f"❌ Send error: {e}")
            return False
    
    async def subscribe(self, instruments: List[str], mode: str):
        """Subscribe"""
        success = await self.send_message(instruments, mode, 'sub')
        if success:
            for inst in instruments:
                self.subscribed_instruments[inst] = mode
                logger.info(f"📊 Subscribed: {inst} ({mode})")
        return success
    
    async def unsubscribe(self, instruments: List[str]):
        """Unsubscribe"""
        mode = list(self.subscribed_instruments.values())[0] if self.subscribed_instruments else 'full'
        success = await self.send_message(instruments, mode, 'unsub')
        if success:
            for inst in instruments:
                self.subscribed_instruments.pop(inst, None)
                self.latest_data.pop(inst, None)
                logger.info(f"🔴 Unsubscribed: {inst}")
        return success
    
    async def change_mode(self, instruments: List[str], new_mode: str):
        """Change mode"""
        success = await self.send_message(instruments, new_mode, 'change_mode')
        if success:
            for inst in instruments:
                if inst in self.subscribed_instruments:
                    self.subscribed_instruments[inst] = new_mode
        return success
    
    async def resubscribe_all(self):
        """Resubscribe after reconnection"""
        if not self.subscribed_instruments:
            return
        
        logger.info(f"🔄 Resubscribing {len(self.subscribed_instruments)} instruments...")
        
        mode_groups = defaultdict(list)
        for inst, mode in self.subscribed_instruments.items():
            mode_groups[mode].append(inst)
        
        for mode, instruments in mode_groups.items():
            await self.subscribe(instruments, mode)
            await asyncio.sleep(0.5)
    
    async def connect(self):
        """Connect to Upstox"""
        while self.should_run and self.current_retry < self.max_retries:
            try:
                logger.info("🔐 Getting authorization...")
                auth_response = self.get_authorization()
                ws_url = auth_response["data"]["authorized_redirect_uri"]
                
                logger.info("📡 Connecting to Upstox WebSocket...")
                self.websocket = await websockets.connect(
                    ws_url,
                    ssl=self.ssl_context,
                    ping_interval=20,
                    ping_timeout=10
                )
                
                self.is_connected = True
                self.current_retry = 0
                self.connection_start_time = datetime.now()
                
                logger.info("✅ Connected to Upstox!")
                return True
                
            except Exception as e:
                self.is_connected = False
                self.current_retry += 1
                logger.error(f"❌ Connection failed (attempt {self.current_retry}): {e}")
                
                if self.current_retry >= self.max_retries:
                    logger.error("❌ Max retries reached")
                    return False
                
                retry_delay = min(self.base_retry_delay * (2 ** (self.current_retry - 1)), 60)
                logger.info(f"⏳ Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
        
        return False
    
    async def receive_and_broadcast(self):
        """Receive and broadcast data"""
        message_count = 0
        
        try:
            while self.is_connected and self.should_run:
                try:
                    message = await asyncio.wait_for(
                        self.websocket.recv(),
                        timeout=30.0
                    )
                    
                    self.last_message_time = datetime.now()
                    decoded_data = self.decode_protobuf(message)
                    
                    if decoded_data is None:
                        continue
                    
                    data_dict = MessageToDict(decoded_data)
                    message_count += 1
                    self.total_messages += 1
                    
                    # Market info
                    if message_count == 1 and data_dict.get('type') == 'market_info':
                        logger.info("📊 Market info received")
                        await manager.broadcast({
                            'type': 'market_info',
                            'data': data_dict
                        })
                        continue
                    
                    # Snapshot
                    if message_count == 2:
                        logger.info("📸 Snapshot received")
                    
                    # Live feed
                    if 'feeds' in data_dict:
                        for instrument_key, feed_data in data_dict['feeds'].items():
                            self.latest_data[instrument_key] = feed_data
                            
                            logger.info(f"💹 Live data: {instrument_key}")
                            
                            await manager.broadcast({
                                'type': 'live_feed',
                                'instrument': instrument_key,
                                'data': feed_data,
                                'timestamp': datetime.now().isoformat()
                            })
                            
                            logger.debug(f"✅ Broadcasted to {len(manager.active_connections)} dashboards")
                
                except asyncio.TimeoutError:
                    # Check if connection is still alive
                    try:
                        pong = await self.websocket.ping()
                        await asyncio.wait_for(pong, timeout=5)
                        logger.debug("Connection alive (ping/pong OK)")
                    except:
                        logger.warning("⚠️  Connection lost")
                        break
                
                except websockets.exceptions.ConnectionClosed:
                    logger.error("❌ Connection closed")
                    break
        
        except Exception as e:
            logger.error(f"❌ Receive error: {e}")
        
        finally:
            self.is_connected = False
    
    async def run(self):
        """Main run loop"""
        logger.info("🚀 Starting Upstox client...")
        
        try:
            while self.should_run:
                connected = await self.connect()
                
                if not connected:
                    logger.error("❌ Failed to connect")
                    break
                
                await asyncio.sleep(1)
                
                # Resubscribe
                if self.reconnection_count > 0:
                    await self.resubscribe_all()
                
                self.reconnection_count += 1
                
                # Broadcast status
                await manager.broadcast({
                    'type': 'status',
                    'data': self.get_status()
                })
                
                # Receive
                await self.receive_and_broadcast()
                
                # Disconnected
                await manager.broadcast({
                    'type': 'status',
                    'data': {'connected': False}
                })
                
                if self.should_run:
                    logger.info("🔄 Reconnecting in 2s...")
                    await asyncio.sleep(2)
        
        except Exception as e:
            logger.error(f"❌ Run error: {e}")
        
        logger.info("🛑 Client stopped")
    
    def stop(self):
        """Stop client"""
        self.should_run = False
        if self.websocket:
            asyncio.create_task(self.websocket.close())
    
    def get_status(self):
        """Get status"""
        uptime = None
        if self.connection_start_time:
            uptime = (datetime.now() - self.connection_start_time).total_seconds()
        
        return {
            "connected": self.is_connected,
            "subscriptions": len(self.subscribed_instruments),
            "instruments": list(self.subscribed_instruments.keys()),
            "total_messages": self.total_messages,
            "reconnections": self.reconnection_count,
            "uptime_seconds": int(uptime) if uptime else 0,
            "last_message": self.last_message_time.isoformat() if self.last_message_time else None
        }
    
    def update_token(self, new_token: str):
        """Update token"""
        self.access_token = new_token
        logger.info("🔑 Token updated")
        if self.websocket:
            asyncio.create_task(self.websocket.close())


# ============= FastAPI App =============

app = FastAPI(
    title="Upstox WebSocket Server",
    description="Real-time market data broadcasting",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global client
ws_client: Optional[UpstoxWebSocketClient] = None
client_task: Optional[asyncio.Task] = None


@app.on_event("startup")
async def startup():
    """Startup - இது தான் முக்கியம்!"""
    global ws_client, client_task
    
    logger.info("=" * 70)
    logger.info("🚀 Starting Upstox WebSocket Server")
    logger.info("=" * 70)
    
    # Check token
    if ACCESS_TOKEN == "YOUR_ACCESS_TOKEN_HERE":
        logger.error("❌ ERROR: Access token not set!")
        logger.error("   Fix: Update ACCESS_TOKEN at top of file")
        logger.error("=" * 70)
        # Don't exit - let server run for testing
    else:
        logger.info(f"✅ Token configured (length: {len(ACCESS_TOKEN)})")
    
    # Create client
    try:
        ws_client = UpstoxWebSocketClient(ACCESS_TOKEN)
        logger.info("✅ Client created")
        
        # Start client in background
        client_task = asyncio.create_task(ws_client.run())
        logger.info("✅ Client task started")
        
        # Give it a moment to initialize
        await asyncio.sleep(1)
        
        if ws_client.is_connected:
            logger.info("✅ Client connected to Upstox!")
        else:
            logger.warning("⚠️  Client not yet connected (will retry)")
        
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
        logger.error("   Server will run but subscriptions will fail")
    
    logger.info("=" * 70)
    logger.info("✅ Server ready!")
    logger.info("=" * 70)


@app.on_event("shutdown")
async def shutdown():
    """Shutdown"""
    global ws_client, client_task
    
    logger.info("🛑 Shutting down...")
    
    if ws_client:
        ws_client.stop()
    
    if client_task:
        client_task.cancel()
        try:
            await client_task
        except asyncio.CancelledError:
            pass
    
    logger.info("✅ Shutdown complete")


# ============= Endpoints =============

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "Upstox WebSocket Server",
        "version": "1.0.0",
        "status": "running",
        "client_status": "connected" if (ws_client and ws_client.is_connected) else "disconnected",
        "endpoints": {
            "status": "GET /status",
            "subscribe": "POST /subscribe",
            "unsubscribe": "POST /unsubscribe",
            "websocket": "WS /ws",
            "docs": "GET /docs"
        }
    }


@app.get("/health")
async def health():
    """Health check"""
    if not ws_client:
        return {
            "status": "unhealthy",
            "reason": "Client not initialized",
            "fix": "Check server logs and access token"
        }
    
    if not ws_client.is_connected:
        return {
            "status": "degraded",
            "reason": "Client not connected to Upstox",
            "subscriptions": len(ws_client.subscribed_instruments)
        }
    
    return {
        "status": "healthy",
        "connected": True,
        "subscriptions": len(ws_client.subscribed_instruments)
    }


@app.get("/status")
async def get_status():
    """Get status"""
    if not ws_client:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "Client not initialized",
                "fix": "Check server logs. Token might be invalid or startup failed."
            }
        )
    
    return ws_client.get_status()


@app.put("/token")
async def update_token(token_data: TokenUpdate):
    """Update token"""
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    
    ws_client.update_token(token_data.access_token)
    return {"status": "success", "message": "Token updated. Reconnecting..."}


@app.post("/subscribe")
async def subscribe(sub_req: SubscriptionRequest):
    """Subscribe"""
    if not ws_client:
        raise HTTPException(
            status_code=503,
            detail="Client not initialized. Check /health endpoint."
        )
    
    if not ws_client.is_connected:
        raise HTTPException(
            status_code=503,
            detail="Not connected to Upstox. Client is reconnecting..."
        )
    
    success = await ws_client.subscribe(sub_req.instruments, sub_req.mode)
    
    return {
        "status": "success" if success else "failed",
        "instruments": sub_req.instruments,
        "mode": sub_req.mode
    }


@app.post("/unsubscribe")
async def unsubscribe(unsub_req: UnsubscriptionRequest):
    """Unsubscribe"""
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    
    if not ws_client.is_connected:
        raise HTTPException(status_code=503, detail="Not connected")
    
    success = await ws_client.unsubscribe(unsub_req.instruments)
    return {"status": "success" if success else "failed"}


@app.post("/change-mode")
async def change_mode(mode_req: ModeChangeRequest):
    """Change mode"""
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    
    if not ws_client.is_connected:
        raise HTTPException(status_code=503, detail="Not connected")
    
    success = await ws_client.change_mode(mode_req.instruments, mode_req.new_mode)
    return {"status": "success" if success else "failed"}


@app.get("/subscriptions")
async def get_subscriptions():
    """Get subscriptions"""
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    
    return {
        "total": len(ws_client.subscribed_instruments),
        "subscriptions": ws_client.subscribed_instruments
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for dashboard"""
    await manager.connect(websocket)
    
    try:
        # Send initial status
        if ws_client:
            await websocket.send_json({
                'type': 'status',
                'data': ws_client.get_status()
            })
            
            # Send cached data
            for instrument, data in ws_client.latest_data.items():
                await websocket.send_json({
                    'type': 'live_feed',
                    'instrument': instrument,
                    'data': data
                })
        
        # Keep alive
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                await websocket.send_json({'type': 'ping'})
                
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await manager.disconnect(websocket)


# ============= Main =============

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🚀 Upstox WebSocket Server with Real Data")
    print("=" * 70)
    print()
    print("📡 API Server:  http://localhost:8000")
    print("🔌 WebSocket:   ws://localhost:8000/ws")
    print("📚 API Docs:    http://localhost:8000/docs")
    print("❤️  Health:      http://localhost:8000/health")
    print()
    
    if ACCESS_TOKEN == "YOUR_ACCESS_TOKEN_HERE":
        print("⚠️  WARNING: Access token not configured!")
        print("   Edit file and update ACCESS_TOKEN variable")
        print()
    else:
        print(f"✅ Token: {ACCESS_TOKEN[:20]}...{ACCESS_TOKEN[-10:]}")
        print()
    
    print("Press Ctrl+C to stop")
    print("=" * 70)
    print()
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )