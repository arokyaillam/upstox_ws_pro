"""
Upstox WebSocket Server with Real Data Broadcasting

Install:
pip install fastapi uvicorn websockets protobuf requests python-multipart

Run:
python upstox_server_realdata.py
"""

import asyncio
import json
import ssl
import websockets
import requests
from datetime import datetime
from typing import List, Optional, Dict, Set
import logging
from collections import defaultdict

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel
import uvicorn

# Import protobuf (you need MarketDataFeedV3_pb2.py)
try:
    import MarketDataFeedV3_pb2 as pb
    from google.protobuf.json_format import MessageToDict
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False
    logging.warning("Protobuf not available. Running in demo mode.")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    """Manage WebSocket connections to dashboard clients"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"✅ Dashboard client connected. Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"❌ Dashboard client disconnected. Total: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        """Broadcast data to all connected dashboard clients"""
        if not self.active_connections:
            return
        
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                disconnected.append(connection)
        
        # Remove disconnected clients
        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()


# ============= Upstox WebSocket Client =============

class UpstoxWebSocketClient:
    """Real Upstox WebSocket client with data broadcasting"""
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.is_connected = False
        self.should_reconnect = True
        
        # Subscriptions
        self.subscribed_instruments = {}  # {instrument: mode}
        
        # Stats
        self.total_messages = 0
        self.reconnection_count = 0
        self.connection_start_time = None
        self.last_message_time = None
        
        # Latest data cache
        self.latest_data = {}  # {instrument: latest_feed_data}
        
        # SSL
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        # Reconnection
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
        
        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Authorization failed: {e}")
            raise
    
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
    
    async def send_subscription(self, instruments: List[str], mode: str, method: str):
        """Send subscription request"""
        if not self.websocket or not self.is_connected:
            logger.warning("WebSocket not connected")
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
            
            binary_data = json.dumps(data).encode('utf-8')
            await self.websocket.send(binary_data)
            logger.info(f"✅ Sent {method} for {len(instruments)} instruments")
            return True
            
        except Exception as e:
            logger.error(f"Send error: {e}")
            return False
    
    async def subscribe(self, instruments: List[str], mode: str):
        """Subscribe"""
        success = await self.send_subscription(instruments, mode, 'sub')
        if success:
            for inst in instruments:
                self.subscribed_instruments[inst] = mode
        return success
    
    async def unsubscribe(self, instruments: List[str]):
        """Unsubscribe"""
        mode = list(self.subscribed_instruments.values())[0] if self.subscribed_instruments else 'full'
        success = await self.send_subscription(instruments, mode, 'unsub')
        if success:
            for inst in instruments:
                self.subscribed_instruments.pop(inst, None)
                self.latest_data.pop(inst, None)
        return success
    
    async def change_mode(self, instruments: List[str], new_mode: str):
        """Change mode"""
        success = await self.send_subscription(instruments, new_mode, 'change_mode')
        if success:
            for inst in instruments:
                if inst in self.subscribed_instruments:
                    self.subscribed_instruments[inst] = new_mode
        return success
    
    async def resubscribe_all(self):
        """Resubscribe after reconnection"""
        if not self.subscribed_instruments:
            return
        
        logger.info(f"🔄 Resubscribing to {len(self.subscribed_instruments)} instruments...")
        
        mode_groups = defaultdict(list)
        for inst, mode in self.subscribed_instruments.items():
            mode_groups[mode].append(inst)
        
        for mode, instruments in mode_groups.items():
            await self.subscribe(instruments, mode)
            await asyncio.sleep(0.5)
    
    async def connect(self):
        """Connect to Upstox WebSocket"""
        while self.should_reconnect and self.current_retry < self.max_retries:
            try:
                logger.info("🔐 Getting authorization...")
                auth_response = self.get_authorization()
                ws_url = auth_response["data"]["authorized_redirect_uri"]
                
                logger.info("📡 Connecting to Upstox...")
                self.websocket = await websockets.connect(
                    ws_url,
                    ssl=self.ssl_context,
                    ping_interval=20,
                    ping_timeout=10
                )
                
                self.is_connected = True
                self.current_retry = 0
                self.connection_start_time = datetime.now()
                
                logger.info("✅ Connected to Upstox WebSocket!")
                return True
                
            except Exception as e:
                self.is_connected = False
                self.current_retry += 1
                logger.error(f"Connection failed (attempt {self.current_retry}): {e}")
                
                if self.current_retry >= self.max_retries:
                    logger.error("Max retries reached")
                    return False
                
                retry_delay = min(self.base_retry_delay * (2 ** (self.current_retry - 1)), 60)
                logger.info(f"Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
        
        return False
    
    async def receive_and_broadcast(self):
        """Receive data and broadcast to dashboard clients"""
        message_count = 0
        
        try:
            while self.is_connected:
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
                    
                    # Handle different message types
                    if message_count == 1 and data_dict.get('type') == 'market_info':
                        logger.info("📊 Market info received")
                        await manager.broadcast({
                            'type': 'market_info',
                            'data': data_dict
                        })
                        continue
                    
                    if message_count == 2:
                        logger.info("📸 Initial snapshot received")
                    
                    # Process live feed
                    if 'feeds' in data_dict:
                        for instrument_key, feed_data in data_dict['feeds'].items():
                            # Cache latest data
                            self.latest_data[instrument_key] = feed_data
                            
                            # Broadcast to dashboard
                            await manager.broadcast({
                                'type': 'live_feed',
                                'instrument': instrument_key,
                                'data': feed_data,
                                'timestamp': datetime.now().isoformat()
                            })
                
                except asyncio.TimeoutError:
                    logger.warning("Message timeout")
                    if not self.websocket or self.websocket.closed:
                        break
                
                except websockets.exceptions.ConnectionClosed:
                    logger.error("Connection closed")
                    break
        
        except Exception as e:
            logger.error(f"Receive error: {e}")
        
        finally:
            self.is_connected = False
    
    async def run(self):
        """Main run loop"""
        try:
            while self.should_reconnect:
                connected = await self.connect()
                
                if not connected:
                    break
                
                await asyncio.sleep(1)
                
                # Resubscribe
                if self.reconnection_count > 0:
                    await self.resubscribe_all()
                
                self.reconnection_count += 1
                
                # Broadcast connection status
                await manager.broadcast({
                    'type': 'status',
                    'connected': True,
                    'subscriptions': len(self.subscribed_instruments)
                })
                
                # Start receiving
                await self.receive_and_broadcast()
                
                # Broadcast disconnection
                await manager.broadcast({
                    'type': 'status',
                    'connected': False
                })
                
                if self.should_reconnect:
                    await asyncio.sleep(2)
        
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.should_reconnect = False
    
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

# Global client
ws_client: Optional[UpstoxWebSocketClient] = None
ws_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ws_client, ws_task
    
    # Startup
    logger.info("🚀 Starting Upstox WebSocket Server")
    # IMPORTANT: Replace with your actual access token
    access_token = "YOUR_ACCESS_TOKEN_HERE"
    ws_client = UpstoxWebSocketClient(access_token)
    ws_task = asyncio.create_task(ws_client.run())
    logger.info("✅ Server ready!")
    
    yield
    
    # Shutdown
    logger.info("🛑 Server stopped")
    if ws_client:
        ws_client.should_reconnect = False
    if ws_task:
        ws_task.cancel()


app = FastAPI(
    title="Upstox WebSocket Server",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============= REST Endpoints =============

@app.get("/")
async def root():
    return {
        "name": "Upstox WebSocket Server",
        "status": "running",
        "websocket": "/ws",
        "endpoints": ["/status", "/subscribe", "/unsubscribe", "/token"]
    }


@app.get("/status")
async def get_status():
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    return ws_client.get_status()


@app.put("/token")
async def update_token(token_data: TokenUpdate):
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    ws_client.update_token(token_data.access_token)
    return {"status": "success", "message": "Token updated"}


@app.post("/subscribe")
async def subscribe(sub_req: SubscriptionRequest):
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    if not ws_client.is_connected:
        raise HTTPException(status_code=503, detail="Not connected")
    
    success = await ws_client.subscribe(sub_req.instruments, sub_req.mode)
    
    return {
        "status": "success" if success else "failed",
        "instruments": sub_req.instruments,
        "mode": sub_req.mode
    }


@app.post("/unsubscribe")
async def unsubscribe(unsub_req: UnsubscriptionRequest):
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    if not ws_client.is_connected:
        raise HTTPException(status_code=503, detail="Not connected")
    
    success = await ws_client.unsubscribe(unsub_req.instruments)
    
    return {
        "status": "success" if success else "failed",
        "instruments": unsub_req.instruments
    }


@app.post("/change-mode")
async def change_mode(mode_req: ModeChangeRequest):
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    if not ws_client.is_connected:
        raise HTTPException(status_code=503, detail="Not connected")
    
    success = await ws_client.change_mode(mode_req.instruments, mode_req.new_mode)
    
    return {
        "status": "success" if success else "failed",
        "instruments": mode_req.instruments,
        "new_mode": mode_req.new_mode
    }


@app.get("/subscriptions")
async def get_subscriptions():
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    return {
        "total": len(ws_client.subscribed_instruments),
        "subscriptions": ws_client.subscribed_instruments
    }


@app.get("/latest-data")
async def get_latest_data():
    """Get latest cached data for all instruments"""
    if not ws_client:
        raise HTTPException(status_code=503, detail="Client not initialized")
    return {
        "instruments": ws_client.latest_data
    }


# ============= WebSocket Endpoint for Real-time Data =============

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for dashboard to receive real-time data
    
    Dashboard connects here to get live market updates
    """
    await manager.connect(websocket)
    
    try:
        # Send initial status
        if ws_client:
            await websocket.send_json({
                'type': 'status',
                'data': ws_client.get_status()
            })
            
            # Send latest cached data
            for instrument, data in ws_client.latest_data.items():
                await websocket.send_json({
                    'type': 'live_feed',
                    'instrument': instrument,
                    'data': data
                })
        
        # Keep connection alive
        while True:
            # Receive any messages from client (like ping)
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                # Echo back or handle client messages
            except asyncio.TimeoutError:
                # Send ping to keep alive
                await websocket.send_json({'type': 'ping'})
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


if __name__ == "__main__":
    print("=" * 70)
    print("🚀 Upstox WebSocket Server with Real Data Broadcasting")
    print("=" * 70)
    print()
    print("📡 HTTP API:  http://localhost:8000")
    print("🔌 WebSocket: ws://localhost:8000/ws")
    print("📚 API Docs:  http://localhost:8000/docs")
    print()
    print("⚠️  IMPORTANT: Update access_token in startup() function!")
    print()
    print("Press Ctrl+C to stop")
    print("=" * 70)
    print()
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")