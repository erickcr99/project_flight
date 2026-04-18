"""
MCP Server — Flight Booking Operations
Corre en: http://localhost:8080
Docs en:  http://localhost:8080/docs
"""

import logging
import os
import random
import sqlite3
import string
import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("mcp_server")


# ══════════════════════════════════════════════════════════════════════════════
#  JSON-RPC 2.0 SCHEMAS
# ══════════════════════════════════════════════════════════════════════════════

class JsonRpcRequest(BaseModel):
    jsonrpc: Literal["2.0"]
    id:      Union[str, int]
    method:  str
    params:  Dict[str, Any] = {}


class JsonRpcError(BaseModel):
    code:    int
    message: str
    data:    Optional[Any] = None


class JsonRpcResponse(BaseModel):
    jsonrpc: Literal["2.0"] = "2.0"
    id:      Union[str, int, None]
    result:  Optional[Dict[str, Any]] = None
    error:   Optional[JsonRpcError]   = None


class RpcErrorCode:
    PARSE_ERROR       = -32700
    INVALID_REQUEST   = -32600
    METHOD_NOT_FOUND  = -32601
    INVALID_PARAMS    = -32602
    INTERNAL_ERROR    = -32603
    SEAT_UNAVAILABLE  = -32000
    BOOKING_NOT_FOUND = -32001
    ALREADY_CANCELLED = -32002


# ══════════════════════════════════════════════════════════════════════════════
#  TOOL INPUT SCHEMAS
# ══════════════════════════════════════════════════════════════════════════════

class SeatAvailabilityInput(BaseModel):
    flight_number: str = Field(..., min_length=2, max_length=10)
    seat_class:    str = Field(..., pattern="^(ECONOMY|BUSINESS|FIRST)$")
    num_seats:     int = Field(..., ge=1, le=9)
    departure_dt:  str

class ProcessBookingInput(BaseModel):
    booking_id:     str   = Field(..., min_length=1)
    passenger_name: str   = Field(..., min_length=1)
    flight_number:  str   = Field(..., min_length=2)
    seat_class:     str   = Field(..., pattern="^(ECONOMY|BUSINESS|FIRST)$")
    num_seats:      int   = Field(..., ge=1, le=9)
    origin:         str   = Field(..., min_length=3, max_length=3)
    destination:    str   = Field(..., min_length=3, max_length=3)
    departure_dt:   str
    base_fare:      float = Field(..., gt=0)
    total_fare:     float = Field(..., gt=0)
    status:         str

    @field_validator("departure_dt")
    @classmethod
    def validate_iso_datetime(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v)
        except ValueError:
            raise ValueError("departure_dt must be ISO-8601")
        return v


class CancelBookingInput(BaseModel):
    booking_id: str = Field(..., min_length=1)
    reason:     str = Field(..., min_length=1, max_length=500)


# ══════════════════════════════════════════════════════════════════════════════
#  TOOL DEFINITIONS (para /tools)
# ══════════════════════════════════════════════════════════════════════════════

TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "process_booking",
        "description": "Valida asientos, calcula tarifa y confirma la reserva.",
        "inputSchema": {
            "type": "object",
            "required": ["booking_id", "passenger_name", "flight_number",
                         "seat_class", "num_seats", "origin", "destination",
                         "departure_dt", "base_fare", "total_fare", "status"],
            "properties": {
                "booking_id":     {"type": "string"},
                "passenger_name": {"type": "string"},
                "flight_number":  {"type": "string"},
                "seat_class":     {"type": "string", "enum": ["ECONOMY", "BUSINESS", "FIRST"]},
                "num_seats":      {"type": "integer", "minimum": 1, "maximum": 9},
                "origin":         {"type": "string"},
                "destination":    {"type": "string"},
                "departure_dt":   {"type": "string"},
                "base_fare":      {"type": "number"},
                "total_fare":     {"type": "number"},
                "status":         {"type": "string"},
            },
        },
    },
    {
        "name": "cancel_booking",
        "description": "Cancela una reserva existente y libera los asientos.",
        "inputSchema": {
            "type": "object",
            "required": ["booking_id", "reason"],
            "properties": {
                "booking_id": {"type": "string"},
                "reason":     {"type": "string"},
            },
        },
    },
    {
        "name": "check_seat_availability",
        "description": "Verifica disponibilidad de asientos en un vuelo.",
        "inputSchema": {
            "type": "object",
            "required": ["flight_number", "seat_class", "num_seats", "departure_dt"],
            "properties": {
                "flight_number": {"type": "string"},
                "seat_class":   {"type": "string"},
                "num_seats":    {"type": "integer"},
                "departure_dt": {"type": "string"},
            },
        },
    },
]


# ══════════════════════════════════════════════════════════════════════════════
#  IN-MEMORY STORES
# ══════════════════════════════════════════════════════════════════════════════

DATABASE_PATH = os.getenv("DATABASE_PATH", "./flight_bookings.db")

FLIGHT_INVENTORY: Dict[str, Dict[str, int]] = {
    "MX204": {"ECONOMY": 120, "BUSINESS": 20, "FIRST": 8},
    "AA100": {"ECONOMY": 150, "BUSINESS": 30, "FIRST": 12},
    "UA550": {"ECONOMY": 100, "BUSINESS": 25, "FIRST": 6},
}

BOOKINGS_STORE: Dict[str, Dict[str, Any]] = {}


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _generate_confirmation_code() -> str:
    return "CONF-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

def _get_seats(flight: str, cls: str) -> int:
    return FLIGHT_INVENTORY.get(flight, {}).get(cls.upper(), 50)

def _reserve(flight: str, cls: str, n: int):
    inv = FLIGHT_INVENTORY.setdefault(flight, {})
    inv[cls.upper()] = max(0, inv.get(cls.upper(), 50) - n)

def _release(flight: str, cls: str, n: int):
    inv = FLIGHT_INVENTORY.setdefault(flight, {})
    inv[cls.upper()] = inv.get(cls.upper(), 0) + n


class _RpcAppError(Exception):
    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


# ══════════════════════════════════════════════════════════════════════════════
#  TOOL HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

def handle_check_seat_availability(params: Dict[str, Any]) -> Dict[str, Any]:
    data = SeatAvailabilityInput(**params)
    remaining = _get_seats(data.flight_number, data.seat_class)
    available = remaining >= data.num_seats
    logger.info("check_seats flight=%s class=%s req=%d avail=%d → %s",
                data.flight_number, data.seat_class, data.num_seats, remaining,
                "OK" if available else "NO")
    return {
        "available":       available,
        "seats_requested": data.num_seats,
        "seats_remaining": remaining,
        "flight_number":   data.flight_number,
        "seat_class":      data.seat_class,
        "checked_at":      datetime.utcnow().isoformat(),
    }


def handle_process_booking(params: Dict[str, Any]) -> Dict[str, Any]:
    data = ProcessBookingInput(**params)
    remaining = _get_seats(data.flight_number, data.seat_class)

    if remaining < data.num_seats:
        raise _RpcAppError(
            RpcErrorCode.SEAT_UNAVAILABLE,
            f"Sin asientos en {data.flight_number} ({data.seat_class}): "
            f"solicitados {data.num_seats}, disponibles {remaining}.",
            {"seats_remaining": remaining},
        )

    if data.total_fare < data.base_fare:
        raise _RpcAppError(RpcErrorCode.INVALID_PARAMS,
                           "total_fare no puede ser menor que base_fare.")

    _reserve(data.flight_number, data.seat_class, data.num_seats)
    code = _generate_confirmation_code()

    record = {
        "booking_id":        data.booking_id,
        "passenger_name":    data.passenger_name,
        "flight_number":     data.flight_number,
        "seat_class":        data.seat_class,
        "num_seats":         data.num_seats,
        "origin":            data.origin,
        "destination":       data.destination,
        "departure_dt":      data.departure_dt,
        "base_fare":         data.base_fare,
        "total_fare":        data.total_fare,
        "status":            "CONFIRMED",
        "confirmation_code": code,
        "processed_at":      datetime.utcnow().isoformat(),
    }
    BOOKINGS_STORE[data.booking_id] = record
    logger.info("process_booking ✓ id=%s code=%s fare=%.2f",
                data.booking_id, code, data.total_fare)
    return record


def _db_get_booking(booking_id: str) -> Optional[Dict[str, Any]]:
    """Lee una reserva desde SQLite como fallback al store en memoria."""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM flight_bookings WHERE booking_id = ?", (booking_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as exc:
        logger.warning("SQLite read error: %s", exc)
        return None


def _db_update_status(booking_id: str, status: str):
    """Actualiza el status de una reserva en SQLite."""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute(
            "UPDATE flight_bookings SET status = ? WHERE booking_id = ?",
            (status, booking_id),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("SQLite write error: %s", exc)


def handle_cancel_booking(params: Dict[str, Any]) -> Dict[str, Any]:
    data = CancelBookingInput(**params)

    # Busca en memoria; si no está, consulta SQLite (p.ej. tras reinicio del servidor)
    record = BOOKINGS_STORE.get(data.booking_id)
    if not record:
        db_row = _db_get_booking(data.booking_id)
        if db_row:
            record = {
                "booking_id":    db_row["booking_id"],
                "passenger_name": db_row["passenger_name"],
                "flight_number": db_row["flight_number"],
                "seat_class":    db_row["seat_class"],
                "num_seats":     db_row["num_seats"],
                "status":        db_row["status"],
            }
            BOOKINGS_STORE[data.booking_id] = record

    if not record:
        raise _RpcAppError(RpcErrorCode.BOOKING_NOT_FOUND,
                           f"Reserva {data.booking_id!r} no encontrada.")
    if record.get("status") == "CANCELLED":
        raise _RpcAppError(RpcErrorCode.ALREADY_CANCELLED,
                           f"Reserva {data.booking_id!r} ya está cancelada.")

    _release(record["flight_number"], record["seat_class"], record["num_seats"])
    record["status"]        = "CANCELLED"
    record["cancel_reason"] = data.reason
    record["cancelled_at"]  = datetime.utcnow().isoformat()
    _db_update_status(data.booking_id, "CANCELLED")

    logger.info("cancel_booking ✓ id=%s", data.booking_id)
    return {
        "booking_id":     data.booking_id,
        "cancelled":      True,
        "cancel_reason":  data.reason,
        "cancelled_at":   record["cancelled_at"],
        "seats_released": record["num_seats"],
    }


TOOL_HANDLERS = {
    "process_booking":         handle_process_booking,
    "cancel_booking":          handle_cancel_booking,
    "check_seat_availability": handle_check_seat_availability,
}


# ══════════════════════════════════════════════════════════════════════════════
#  FASTAPI APP
# ══════════════════════════════════════════════════════════════════════════════

app = FastAPI(title="MCP Server — Flight Booking", version="1.0.0")


def _ok(rpc_id, result):
    return JSONResponse(JsonRpcResponse(id=rpc_id, result=result).model_dump())

def _err(rpc_id, code, message, data=None):
    return JSONResponse(
        JsonRpcResponse(id=rpc_id, error=JsonRpcError(code=code, message=message, data=data)).model_dump()
    )


@app.get("/tools", summary="Listar herramientas disponibles")
async def list_tools():
    return {
        "tools":      TOOL_DEFINITIONS,
        "tool_count": len(TOOL_DEFINITIONS),
        "server":     "MCP Flight Booking Server v1.0.0",
        "timestamp":  datetime.utcnow().isoformat(),
    }


@app.post("/rpc", summary="JSON-RPC 2.0 endpoint")
async def rpc_endpoint(rpc_req: JsonRpcRequest):
    rpc_id = rpc_req.id
    logger.info("RPC ← id=%s method=%s", rpc_id, rpc_req.method)

    handler = TOOL_HANDLERS.get(rpc_req.method)
    if not handler:
        return _err(rpc_id, RpcErrorCode.METHOD_NOT_FOUND,
                    f"Método no encontrado: {rpc_req.method!r}")
    try:
        result = handler(rpc_req.params)
        logger.info("RPC → id=%s OK", rpc_id)
        return _ok(rpc_id, result)
    except _RpcAppError as exc:
        logger.warning("RPC → id=%s AppError [%d] %s", rpc_id, exc.code, exc.message)
        return _err(rpc_id, exc.code, exc.message, exc.data)
    except Exception as exc:
        err_str = str(exc)
        logger.error("RPC → id=%s Error: %s", rpc_id, err_str)
        if "validation" in err_str.lower():
            return _err(rpc_id, RpcErrorCode.INVALID_PARAMS, f"Parámetros inválidos: {err_str}")
        return _err(rpc_id, RpcErrorCode.INTERNAL_ERROR, f"Error interno: {err_str}")


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
