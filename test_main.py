"""
Integration and Unit Tests for Distributed Task Streamer Engine
"""
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from main import app, TaskCreate, TaskPriority, TaskStatus, DistributedTaskEngine, ConnectionManager

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_enqueue_task_endpoint():
    payload = {
        "title": "Process Data Batch",
        "payload": {"records": 500},
        "priority": "HIGH"
    }
    response = client.post("/api/v1/tasks", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["title"] == "Process Data Batch"
    assert data["status"] in ["QUEUED", "PROCESSING", "COMPLETED"]

def test_get_nonexistent_task_returns_404():
    response = client.get("/api/v1/tasks/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_engine_task_lifecycle():
    manager = ConnectionManager()
    engine = DistributedTaskEngine(notifier=manager)
    
    task_input = TaskCreate(title="Worker Test Task", priority=TaskPriority.LOW)
    record = await engine.enqueue_task(task_input)
    
    assert record.status == TaskStatus.QUEUED
    assert record.id in engine.tasks
    
    # Process explicitly
    await engine._process(record.id)
    assert engine.tasks[record.id].status == TaskStatus.COMPLETED
    assert engine.tasks[record.id].execution_time_ms is not None
