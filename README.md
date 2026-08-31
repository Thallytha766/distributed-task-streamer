# ⚡ Distributed Task Streamer & Worker Engine

[![Python Version](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg?logo=fastapi)](https://fastapi.tiangolo.com)

A high-performance, asynchronous background task streaming engine built with FastAPI, asyncio worker queues, and real-time WebSocket telemetry broadcasting.

---

## 🎯 Architecture Overview

```text
[ Client Request ] ---> [ FastAPI Ingestion API ] (HTTP 202 Accepted)
                               |
                               v
                       [ Async IO Queue ]
                               |
                               v
                      [ Background Worker ] ---> [ Business Execution ]
                               |
                               v
                    [ WebSocket Broadcaster ] ---> [ Connected Clients ]
🚀 Key Features
 Non-Blocking Ingestion: Returns HTTP 202 Accepted immediately, offloading intensive workloads to async queue workers.
 Real-Time Telemetry: Broadcasts state transitions (QUEUED -> PROCESSING -> COMPLETED) to all connected clients via WebSockets.
 Fail-Fast Typing: Fully validated domain payloads using Pydantic v2.
 Automated Test Coverage: Comprehensive test suite validating lifecycle events and edge cases using Pytest and HTTPX.
🛠️ Quick Start
1. Clone the repository
git clone https://github.com/Thallytha766/distributed-task-streamer.git
cd distributed-task-streamer
2. Install dependencies
pip install -r requirements.txt
3. Run application
uvicorn main:app --reload
Interactive Swagger docs available at: http://localhost:8000/docs
4. Run automated test suite
pytest test_main.py -v


