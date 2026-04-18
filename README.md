# Flight Booking Processing System

A Python-based flight booking system composed of three services that work together to process, confirm, and cancel flight reservations.

---

## Architecture

```
flight_booking_service.py   ──►  MCP Server (:8080)  ──►  Booking confirmed
        │                              │
        └──► Mock Status API (:9090)   └──► Failures → dlq_messages.json
        │
        └──► SQLite (flight_bookings.db)
```

| File | Description |
|------|-------------|
| `flight_booking_service.py` | Main async processing service |
| `mcp_server.py` | MCP Server — JSON-RPC 2.0 tool server |
| `mock_status_api.py` | Mock REST API for booking status updates |
| `ui.py` | Streamlit dashboard |
| `task1_python_service.txt` | Task 1 — detailed implementation notes |
| `task2_mcp_server.txt` | Task 2 — detailed implementation notes |

---

## Services

### MCP Server (`mcp_server.py`)
Runs on **http://localhost:8080**

Exposes three tools via JSON-RPC 2.0:

| Tool | Description |
|------|-------------|
| `process_booking` | Validates seats, calculates fare, confirms booking |
| `cancel_booking` | Cancels a confirmed booking and releases seats |
| `check_seat_availability` | Checks available seats without reserving |

Interactive docs: [http://localhost:8080/docs](http://localhost:8080/docs)

---

### Flight Booking Service (`flight_booking_service.py`)
Fetches `PENDING` bookings from SQLite and for each one:

1. Calls `check_seat_availability` on the MCP Server
2. Calculates total fare:
   ```
   subtotal = base_fare × class_multiplier × num_seats + $250 fee
   total    = subtotal × 1.16 (16% tax)
   ```
   Multipliers: `ECONOMY ×1.0` | `BUSINESS ×1.85` | `FIRST ×2.60`
3. Updates status via REST API (`PATCH /api/bookings/{id}/status`)
4. Publishes confirmation via `process_booking` MCP tool
5. On failure → writes to `dlq_messages.json` (simulates AWS SQS DLQ)

---

### Mock Status API (`mock_status_api.py`)
Runs on **http://localhost:9090**

| Endpoint | Description |
|----------|-------------|
| `PATCH /api/bookings/{id}/status` | Update booking status |
| `GET /api/bookings` | List all status updates |
| `GET /health` | Health check |

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Terminal 1 — MCP Server
uvicorn mcp_server:app --port 8080

# Terminal 2 — Mock Status API
uvicorn mock_status_api:app --port 9090

# Terminal 3 — Process bookings
python flight_booking_service.py

# Optional — Streamlit UI
streamlit run ui.py
```

---

## Streamlit Dashboard

```bash
streamlit run ui.py
```

| Section | Description |
|---------|-------------|
| Dashboard | All bookings with live status metrics |
| Nueva Reserva | Form to add a new PENDING booking |
| Procesar | Run the booking service and view logs |
| Disponibilidad | Check seat availability via MCP |
| Cancelar | Cancel a confirmed booking via MCP |
| DLQ | View failed messages |

---

## Configuration

All URLs and paths can be overridden via `.env`:

```env
MCP_SERVER_URL=http://localhost:8080
STATUS_API_URL=http://localhost:9090/api/bookings
DATABASE_URL=sqlite:///./flight_bookings.db
```

---

## Dependencies

```
fastapi
uvicorn
sqlalchemy
aiohttp
python-dotenv
pydantic
streamlit
requests
```
