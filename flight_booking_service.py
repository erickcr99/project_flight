"""
Flight Booking Processing Service
Conecta con:
  - SQLite (base de datos local)
  - MCP Server en http://localhost:8080
  - Mock Status API en http://localhost:9090
"""

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine, select, text
from sqlalchemy.orm import DeclarativeBase, Session

load_dotenv()

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL",  "http://localhost:8080")
STATUS_API_URL = os.getenv("STATUS_API_URL",  "http://localhost:9090/api/bookings")
DATABASE_URL   = os.getenv("DATABASE_URL",    "sqlite:///./flight_bookings.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("flight_booking_service")


# ══════════════════════════════════════════════════════════════════════════════
#  ENUMS Y DATACLASSES
# ══════════════════════════════════════════════════════════════════════════════

class BookingStatus(str, Enum):
    PENDING   = "PENDING"
    CONFIRMED = "CONFIRMED"
    FAILED    = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class BookingRecord:
    booking_id:     str
    passenger_name: str
    flight_number:  str
    seat_class:     str
    num_seats:      int
    origin:         str
    destination:    str
    departure_dt:   datetime
    base_fare:      float
    status:         BookingStatus
    created_at:     datetime


@dataclass
class ProcessedBooking:
    booking_id:     str
    passenger_name: str
    flight_number:  str
    seat_class:     str
    num_seats:      int
    origin:         str
    destination:    str
    departure_dt:   str
    base_fare:      float
    total_fare:     float
    status:         BookingStatus
    confirmation:   Optional[str] = None
    error:          Optional[str] = None
    processed_at:   str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ══════════════════════════════════════════════════════════════════════════════
#  BASE DE DATOS
# ══════════════════════════════════════════════════════════════════════════════

class Base(DeclarativeBase):
    pass


class FlightBooking(Base):
    __tablename__ = "flight_bookings"
    id             = Column(Integer,  primary_key=True, autoincrement=True)
    booking_id     = Column(String,   nullable=False, unique=True, index=True)
    passenger_name = Column(String,   nullable=False)
    flight_number  = Column(String,   nullable=False)
    seat_class     = Column(String,   nullable=False, default="ECONOMY")
    num_seats      = Column(Integer,  nullable=False, default=1)
    origin         = Column(String,   nullable=False)
    destination    = Column(String,   nullable=False)
    departure_dt   = Column(DateTime, nullable=False)
    base_fare      = Column(Float,    nullable=False)
    status         = Column(String,   nullable=False, default=BookingStatus.PENDING)
    created_at     = Column(DateTime, default=datetime.utcnow)


def create_db_engine():
    engine = create_engine(DATABASE_URL, echo=False)
    Base.metadata.create_all(engine)
    _seed_demo_data(engine)
    return engine


def _seed_demo_data(engine):
    with Session(engine) as session:
        count = session.execute(text("SELECT COUNT(*) FROM flight_bookings")).scalar()
        if count:
            logger.info("Ya existen %d reservas en la BD, no se insertan demos.", count)
            return
        demos = [
            FlightBooking(
                booking_id="BK-001", passenger_name="Alice Martínez",
                flight_number="MX204", seat_class="ECONOMY",
                num_seats=2, origin="MEX", destination="LAX",
                departure_dt=datetime(2026, 5, 10, 8, 30),
                base_fare=3500.00, status=BookingStatus.PENDING,
            ),
            FlightBooking(
                booking_id="BK-002", passenger_name="Carlos Ruiz",
                flight_number="AA100", seat_class="BUSINESS",
                num_seats=1, origin="MEX", destination="JFK",
                departure_dt=datetime(2026, 5, 12, 14, 0),
                base_fare=12000.00, status=BookingStatus.PENDING,
            ),
            FlightBooking(
                booking_id="BK-003", passenger_name="Diana López",
                flight_number="UA550", seat_class="FIRST",
                num_seats=1, origin="GDL", destination="ORD",
                departure_dt=datetime(2026, 5, 15, 9, 15),
                base_fare=25000.00, status=BookingStatus.PENDING,
            ),
        ]
        session.add_all(demos)
        session.commit()
        logger.info("✓ 3 reservas demo insertadas en la BD.")


def fetch_pending_bookings(engine) -> List[BookingRecord]:
    with Session(engine) as session:
        rows = session.execute(
            select(FlightBooking).where(FlightBooking.status == BookingStatus.PENDING)
        ).scalars().all()
        return [
            BookingRecord(
                booking_id=r.booking_id, passenger_name=r.passenger_name,
                flight_number=r.flight_number, seat_class=r.seat_class,
                num_seats=r.num_seats, origin=r.origin,
                destination=r.destination, departure_dt=r.departure_dt,
                base_fare=r.base_fare, status=BookingStatus(r.status),
                created_at=r.created_at,
            )
            for r in rows
        ]


def update_booking_status(engine, booking_id: str, status: BookingStatus):
    with Session(engine) as session:
        row = session.execute(
            select(FlightBooking).where(FlightBooking.booking_id == booking_id)
        ).scalar_one_or_none()
        if row:
            row.status = status.value
            session.commit()


# ══════════════════════════════════════════════════════════════════════════════
#  CÁLCULO DE TARIFA
# ══════════════════════════════════════════════════════════════════════════════

SEAT_MULTIPLIER = {"ECONOMY": 1.00, "BUSINESS": 1.85, "FIRST": 2.60}
TAX_RATE        = 0.16
SERVICE_FEE     = 250.0


def calculate_total_fare(booking: BookingRecord) -> float:
    multiplier = SEAT_MULTIPLIER.get(booking.seat_class.upper(), 1.0)
    subtotal   = booking.base_fare * multiplier * booking.num_seats + SERVICE_FEE
    total      = round(subtotal * (1 + TAX_RATE), 2)
    logger.info(
        "Tarifa %s: base=%.2f × %.2f × %d asientos + cuota → total=%.2f MXN",
        booking.booking_id, booking.base_fare, multiplier, booking.num_seats, total,
    )
    return total


# ══════════════════════════════════════════════════════════════════════════════
#  MCP CLIENT
# ══════════════════════════════════════════════════════════════════════════════

class MCPClient:
    def __init__(self, base_url: str, session: aiohttp.ClientSession):
        self.base_url = base_url.rstrip("/")
        self.session  = session

    async def call_tool(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        payload = {"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method, "params": params}
        async with self.session.post(
            f"{self.base_url}/rpc", json=payload,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()
        if "error" in body and body["error"]:
            raise RuntimeError(f"MCP [{body['error']['code']}]: {body['error']['message']}")
        return body["result"]

    async def check_seat_availability(self, booking: BookingRecord) -> bool:
        result = await self.call_tool("check_seat_availability", {
            "flight_number": booking.flight_number,
            "seat_class":    booking.seat_class,
            "num_seats":     booking.num_seats,
            "departure_dt":  booking.departure_dt.isoformat(),
        })
        return result.get("available", False)

    async def process_booking(self, processed: ProcessedBooking) -> str:
        result = await self.call_tool("process_booking", asdict(processed))
        return result.get("confirmation_code", "N/A")


# ══════════════════════════════════════════════════════════════════════════════
#  REST API CLIENT
# ══════════════════════════════════════════════════════════════════════════════

class StatusAPIClient:
    def __init__(self, base_url: str, session: aiohttp.ClientSession):
        self.base_url = base_url.rstrip("/")
        self.session  = session

    async def assign_status(self, booking_id: str, status: BookingStatus, total_fare: float):
        url     = f"{self.base_url}/{booking_id}/status"
        payload = {"status": status.value, "total_fare": total_fare,
                   "updated_at": datetime.utcnow().isoformat()}
        logger.info("→ REST PATCH %s  status=%s", url, status.value)
        async with self.session.patch(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            resp.raise_for_status()
            return await resp.json()


# ══════════════════════════════════════════════════════════════════════════════
#  DLQ LOCAL  (simula AWS SQS escribiendo a un archivo JSON)
# ══════════════════════════════════════════════════════════════════════════════

class LocalDLQ:
    """Guarda los mensajes fallidos en dlq_messages.json en lugar de AWS SQS."""

    def __init__(self, filepath: str = "dlq_messages.json"):
        self.filepath = filepath

    def send(self, booking_id: str, error: str, payload: Dict[str, Any]):
        message = {
            "booking_id": booking_id,
            "error":      error,
            "payload":    payload,
            "failed_at":  datetime.utcnow().isoformat(),
        }
        # Lee mensajes existentes
        try:
            with open(self.filepath, "r") as f:
                messages = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            messages = []

        messages.append(message)

        with open(self.filepath, "w") as f:
            json.dump(messages, f, indent=2, ensure_ascii=False)

        logger.warning("DLQ local ✓ falla guardada para %s → %s", booking_id, self.filepath)


# ══════════════════════════════════════════════════════════════════════════════
#  PROCESADOR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

class FlightBookingProcessor:
    def __init__(self, mcp: MCPClient, status: StatusAPIClient, dlq: LocalDLQ, engine):
        self.mcp    = mcp
        self.status = status
        self.dlq    = dlq
        self.engine = engine

    async def process(self, booking: BookingRecord) -> ProcessedBooking:
        logger.info("━━ Procesando reserva %s (%s) ━━", booking.booking_id, booking.passenger_name)

        processed = ProcessedBooking(
            booking_id=booking.booking_id, passenger_name=booking.passenger_name,
            flight_number=booking.flight_number, seat_class=booking.seat_class,
            num_seats=booking.num_seats, origin=booking.origin,
            destination=booking.destination,
            departure_dt=booking.departure_dt.isoformat(),
            base_fare=booking.base_fare, total_fare=0.0,
            status=BookingStatus.PENDING,
        )

        try:
            # 1. Validar disponibilidad de asientos (MCP)
            available = await self.mcp.check_seat_availability(booking)
            if not available:
                raise ValueError(
                    f"Sin asientos en vuelo {booking.flight_number} "
                    f"({booking.seat_class}) para {booking.num_seats} lugar(es)."
                )

            # 2. Calcular tarifa total
            processed.total_fare = calculate_total_fare(booking)

            # 3. Asignar estado via REST API
            await self.status.assign_status(
                booking.booking_id, BookingStatus.CONFIRMED, processed.total_fare
            )
            processed.status = BookingStatus.CONFIRMED

            # 4. Publicar confirmación via MCP
            code = await self.mcp.process_booking(processed)
            processed.confirmation = code

            # 5. Actualizar BD
            update_booking_status(self.engine, booking.booking_id, BookingStatus.CONFIRMED)

            logger.info(
                "✅ %s CONFIRMADA — tarifa=%.2f MXN | código=%s",
                booking.booking_id, processed.total_fare, code,
            )

        except Exception as exc:
            error_msg = str(exc)
            logger.error("❌ %s FALLÓ — %s", booking.booking_id, error_msg)
            processed.status = BookingStatus.FAILED
            processed.error  = error_msg
            self.dlq.send(booking.booking_id, error_msg, asdict(processed))
            update_booking_status(self.engine, booking.booking_id, BookingStatus.FAILED)

        return processed


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def run_service():
    logger.info("╔══════════════════════════════════════╗")
    logger.info("║  Flight Booking Service — iniciando  ║")
    logger.info("╚══════════════════════════════════════╝")

    engine   = create_db_engine()
    bookings = fetch_pending_bookings(engine)
    logger.info("BD: %d reserva(s) PENDING encontradas.", len(bookings))

    if not bookings:
        logger.info("Sin reservas pendientes. Fin.")
        return []

    dlq = LocalDLQ("dlq_messages.json")

    semaphore = asyncio.Semaphore(5)

    async with aiohttp.ClientSession(
        headers={"Content-Type": "application/json", "Accept": "application/json"}
    ) as http_session:
        mcp_client    = MCPClient(MCP_SERVER_URL, http_session)
        status_client = StatusAPIClient(STATUS_API_URL, http_session)
        processor     = FlightBookingProcessor(mcp_client, status_client, dlq, engine)

        async def bounded(b):
            async with semaphore:
                return await processor.process(b)

        results = await asyncio.gather(*[bounded(b) for b in bookings])

    confirmed = [r for r in results if r.status == BookingStatus.CONFIRMED]
    failed    = [r for r in results if r.status == BookingStatus.FAILED]

    logger.info("╔══════════════════════════════╗")
    logger.info("║        RESUMEN FINAL         ║")
    logger.info("╠══════════════════════════════╣")
    logger.info("║  Confirmadas : %d             ║", len(confirmed))
    logger.info("║  Fallidas    : %d             ║", len(failed))
    logger.info("╚══════════════════════════════╝")

    for r in results:
        icon = "✅" if r.status == BookingStatus.CONFIRMED else "❌"
        logger.info(
            "%s [%s] %s | tarifa=%.2f | código=%s | error=%s",
            icon, r.status.value, r.booking_id,
            r.total_fare, r.confirmation, r.error,
        )

    return list(results)


if __name__ == "__main__":
    asyncio.run(run_service())
