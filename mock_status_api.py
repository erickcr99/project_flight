"""
Mock REST API — Status de Reservas
Simula el servicio externo que asigna el estado de una reserva.
Corre en: http://localhost:9090
"""

import logging
from datetime import datetime
from typing import Optional
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("mock_status_api")

app = FastAPI(title="Mock Status API", version="1.0.0")

# Guarda en memoria los estados asignados
STATUS_LOG = {}


class StatusUpdate(BaseModel):
    status: str
    total_fare: Optional[float] = None
    updated_at: Optional[str] = None


@app.patch("/api/bookings/{booking_id}/status")
async def update_status(booking_id: str, payload: StatusUpdate):
    logger.info("PATCH /api/bookings/%s/status → %s", booking_id, payload.status)
    STATUS_LOG[booking_id] = {
        "booking_id": booking_id,
        "status":     payload.status,
        "total_fare": payload.total_fare,
        "updated_at": payload.updated_at or datetime.utcnow().isoformat(),
    }
    return JSONResponse({"ok": True, "booking_id": booking_id, "status": payload.status})


@app.get("/api/bookings")
async def list_statuses():
    return {"bookings": list(STATUS_LOG.values())}


@app.get("/health")
async def health():
    return {"status": "ok"}
