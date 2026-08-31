"""
Distributed Task Streamer & Worker Engine
High-throughput async job execution system with real-time WebSocket telemetry.
"""
import asyncio
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, ConfigDict, Field

# ==========================================
# 1. DOMAIN SCHEMAS & MODELS
# ==========================================

class TaskPriority(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

class TaskStatus(str, Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TaskCreate(BaseModel):
    title: str = Field(..., min_length=3, max_length=100)
    payload: dict = Field(default_factory=dict, description="Arbitrary task payload")
    priority: TaskPriority = Field(default=TaskPriority.MEDIUM)

class TaskRecord(BaseModel):
    id: UUID
    title: str
    priority: TaskPriority
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    execution_time_ms: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)

# ==========================================
# 2. WEBSOCKET CONNECTION MANAGER
# ==========================================

class ConnectionManager:
    """Manages active WebSocket subscriber connections."""
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

# ==========================================
# 3. ASYNC TASK QUEUE & WORKER ENGINE
# ==========================================

class DistributedTaskEngine:
    def __init__(self, notifier: ConnectionManager):
        self.tasks: Dict[UUID, TaskRecord] = {}
        self.queue: asyncio.Queue = asyncio.Queue()
        self.notifier = notifier
        self._is_running = False

    async def enqueue_task(self, data: TaskCreate) -> TaskRecord:
        task_id = uuid4()
        now = datetime.utcnow()
        record = TaskRecord(
            id=task_id,
            title=data.title,
            priority=data.priority,
            status=TaskStatus.QUEUED,
            created_at=now,
            updated_at=now
        )
        self.tasks[task_id] = record
        await self.queue.put(task_id)
        
        # Broadcast queued event
        await self.notifier.broadcast({
            "event": "TASK_QUEUED",
            "task_id": str(task_id),
            "status": record.status.value
        })
        return record

    async def start_worker(self):
        self._is_running = True
        while self._is_running:
            task_id = await self.queue.get()
            await self._process(task_id)
            self.queue.task_done()

    async def _process(self, task_id: UUID):
        record = self.tasks.get(task_id)
        if not record:
            return

        # Update to PROCESSING
        record.status = TaskStatus.PROCESSING
        record.updated_at = datetime.utcnow()
        await self.notifier.broadcast({
            "event": "TASK_PROCESSING",
            "task_id": str(task_id),
            "status": record.status.value
        })

        # Simulate async heavy computation
        start_time = asyncio.get_event_loop().time()
        await asyncio.sleep(0.05)
        duration = (asyncio.get_event_loop().time() - start_time) * 1000

        # Mark COMPLETED
        record.status = TaskStatus.COMPLETED
        record.execution_time_ms = round(duration, 2)
        record.updated_at = datetime.utcnow()

        await self.notifier.broadcast({
            "event": "TASK_COMPLETED",
            "task_id": str(task_id),
            "status": record.status.value,
            "execution_time_ms": record.execution_time_ms
        })

# ==========================================
# 4. FASTAPI APPLICATION SETUP
# ==========================================

app = FastAPI(
    title="Distributed Task Streamer API",
    version="1.0.0",
    description="High-throughput asynchronous task processing engine with WebSocket telemetry."
)

manager = ConnectionManager()
engine = DistributedTaskEngine(notifier=manager)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(engine.start_worker())

@app.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy", "service": "task-streamer", "active_ws": len(manager.active_connections)}

@app.post("/api/v1/tasks", response_model=TaskRecord, status_code=status.HTTP_202_ACCEPTED, tags=["Tasks"])
async def submit_task(payload: TaskCreate):
    return await engine.enqueue_task(payload)

@app.get("/api/v1/tasks/{task_id}", response_model=TaskRecord, tags=["Tasks"])
async def get_task_status(task_id: UUID):
    task = engine.tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.websocket("/ws/telemetry")
async def websocket_telemetry_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
