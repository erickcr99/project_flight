"""
Dashboard de Reservas de Vuelo — Streamlit UI
Corre con: streamlit run ui.py
"""

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime

import requests
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from flight_booking_service import (
    Base, BookingStatus, FlightBooking,
    fetch_pending_bookings,
)

load_dotenv()

MCP_URL      = os.getenv("MCP_SERVER_URL", "http://localhost:8080")
DATABASE_URL = os.getenv("DATABASE_URL",   "sqlite:///./flight_bookings.db")
DLQ_FILE     = "dlq_messages.json"

FLIGHTS  = ["MX204", "AA100", "UA550"]
CLASSES  = ["ECONOMY", "BUSINESS", "FIRST"]
AIRPORTS = ["MEX", "GDL", "LAX", "JFK", "ORD", "MIA", "DFW", "SFO"]

st.set_page_config(page_title="Flight Booking", page_icon="✈️", layout="wide")


# ── Engine (cacheado) ─────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    engine = create_engine(DATABASE_URL, echo=False)
    Base.metadata.create_all(engine)
    return engine


# ── Helpers ───────────────────────────────────────────────────────────────────

STATUS_ICON = {"CONFIRMED": "🟢", "PENDING": "🟡", "FAILED": "🔴", "CANCELLED": "⚫"}


def all_bookings(engine):
    with Session(engine) as s:
        rows = s.execute(
            select(FlightBooking).order_by(FlightBooking.id.desc())
        ).scalars().all()
        return [
            {
                "ID":          r.booking_id,
                "Pasajero":    r.passenger_name,
                "Vuelo":       r.flight_number,
                "Clase":       r.seat_class,
                "Asientos":    r.num_seats,
                "Origen":      r.origin,
                "Destino":     r.destination,
                "Salida":      r.departure_dt.strftime("%Y-%m-%d %H:%M") if r.departure_dt else "",
                "Tarifa Base": f"${r.base_fare:,.2f}",
                "Estado":      f"{STATUS_ICON.get(r.status, '⚪')} {r.status}",
                "Creada":      r.created_at.strftime("%Y-%m-%d %H:%M") if r.created_at else "",
            }
            for r in rows
        ]


def rpc_call(method: str, params: dict):
    payload = {"jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method, "params": params}
    try:
        r = requests.post(f"{MCP_URL}/rpc", json=payload, timeout=10)
        r.raise_for_status()
        body = r.json()
        if body.get("error"):
            return None, body["error"]["message"]
        return body["result"], None
    except requests.exceptions.ConnectionError:
        return None, "No se pudo conectar al MCP Server (¿está corriendo en :8080?)"
    except Exception as exc:
        return None, str(exc)


def service_status():
    statuses = {}
    for name, url in [("MCP :8080", f"{MCP_URL}/health"), ("Status API :9090", "http://localhost:9090/health")]:
        try:
            requests.get(url, timeout=2).raise_for_status()
            statuses[name] = True
        except Exception:
            statuses[name] = False
    return statuses


# ── Dashboard ─────────────────────────────────────────────────────────────────

def page_dashboard(engine):
    st.header("📊 Reservas")

    bookings = all_bookings(engine)
    total     = len(bookings)
    confirmed = sum(1 for b in bookings if "CONFIRMED" in b["Estado"])
    pending   = sum(1 for b in bookings if "PENDING"   in b["Estado"])
    failed    = sum(1 for b in bookings if "FAILED"    in b["Estado"])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total",        total)
    c2.metric("Confirmadas",  confirmed)
    c3.metric("Pendientes",   pending)
    c4.metric("Fallidas",     failed)

    st.divider()
    if st.button("🔄 Actualizar"):
        st.rerun()

    if not bookings:
        st.info("No hay reservas registradas.")
        return

    st.dataframe(bookings, use_container_width=True, hide_index=True)


# ── Nueva Reserva ─────────────────────────────────────────────────────────────

def page_nueva_reserva(engine):
    st.header("➕ Nueva Reserva")

    with st.form("form_reserva"):
        c1, c2 = st.columns(2)
        with c1:
            booking_id     = st.text_input("ID de Reserva", value=f"BK-{uuid.uuid4().hex[:6].upper()}")
            passenger_name = st.text_input("Nombre del Pasajero")
            flight_number  = st.selectbox("Vuelo", FLIGHTS)
            seat_class     = st.selectbox("Clase", CLASSES)
            num_seats      = st.number_input("Asientos", min_value=1, max_value=9, value=1)
        with c2:
            origin      = st.selectbox("Origen", AIRPORTS)
            destination = st.selectbox("Destino", AIRPORTS, index=1)
            dep_date    = st.date_input("Fecha de Salida")
            dep_time    = st.time_input("Hora de Salida")
            base_fare   = st.number_input("Tarifa Base (MXN)", min_value=100.0, value=3500.0, step=100.0)

        submitted = st.form_submit_button("Guardar Reserva", type="primary")

    if submitted:
        if not passenger_name:
            st.error("El nombre del pasajero es requerido.")
            return
        if origin == destination:
            st.error("Origen y destino no pueden ser iguales.")
            return

        dep_dt = datetime.combine(dep_date, dep_time)
        with Session(engine) as session:
            if session.execute(
                select(FlightBooking).where(FlightBooking.booking_id == booking_id)
            ).scalar_one_or_none():
                st.error(f"Ya existe una reserva con ID {booking_id}.")
                return

            session.add(FlightBooking(
                booking_id=booking_id, passenger_name=passenger_name,
                flight_number=flight_number, seat_class=seat_class,
                num_seats=num_seats, origin=origin, destination=destination,
                departure_dt=dep_dt, base_fare=base_fare,
                status=BookingStatus.PENDING,
            ))
            session.commit()

        st.success(f"✅ Reserva **{booking_id}** guardada como PENDING.")


# ── Procesar Reservas ─────────────────────────────────────────────────────────

def page_procesar(engine):
    st.header("⚡ Procesar Reservas Pendientes")

    pending = fetch_pending_bookings(engine)

    if not pending:
        st.info("No hay reservas pendientes para procesar.")
        return

    st.write(f"**{len(pending)} reserva(s) pendiente(s):**")
    for b in pending:
        st.write(f"- `{b.booking_id}` — {b.passenger_name} | {b.flight_number} {b.seat_class} × {b.num_seats}")

    st.warning("Asegúrate de que el MCP Server (:8080) y el Status API (:9090) estén corriendo antes de procesar.")

    if st.button("🚀 Procesar Ahora", type="primary"):
        with st.spinner("Procesando reservas..."):
            result = subprocess.run(
                [sys.executable, "flight_booking_service.py"],
                capture_output=True, text=True,
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )

        if result.returncode == 0:
            st.success("✅ Procesamiento completado.")
        else:
            st.error("❌ El servicio terminó con errores.")

        if result.stdout:
            with st.expander("Log de salida"):
                st.code(result.stdout)
        if result.stderr:
            with st.expander("Stderr"):
                st.code(result.stderr)

        st.rerun()


# ── Verificar Disponibilidad ──────────────────────────────────────────────────

def page_verificar():
    st.header("🔍 Verificar Disponibilidad de Asientos")

    with st.form("form_seats"):
        c1, c2 = st.columns(2)
        with c1:
            flight  = st.selectbox("Vuelo", FLIGHTS)
            clase   = st.selectbox("Clase", CLASSES)
            seats   = st.number_input("Asientos requeridos", min_value=1, max_value=9, value=1)
        with c2:
            dep_d = st.date_input("Fecha de Salida")
            dep_t = st.time_input("Hora")

        check = st.form_submit_button("Verificar", type="primary")

    if check:
        result, err = rpc_call("check_seat_availability", {
            "flight_number": flight,
            "seat_class":    clase,
            "num_seats":     seats,
            "departure_dt":  datetime.combine(dep_d, dep_t).isoformat(),
        })
        if err:
            st.error(f"Error: {err}")
        elif result["available"]:
            st.success(
                f"✅ **{result['seats_remaining']} asientos** disponibles en {flight} ({clase}). "
                f"Solicitas {seats} — aprobado."
            )
        else:
            st.error(
                f"❌ Sin disponibilidad en {flight} ({clase}). "
                f"Disponibles: {result['seats_remaining']}, solicitados: {seats}."
            )


# ── Cancelar Reserva ──────────────────────────────────────────────────────────

def page_cancelar():
    st.header("❌ Cancelar Reserva")
    st.info("Solo se pueden cancelar reservas que hayan sido procesadas (estado CONFIRMED) por el MCP Server.")

    with st.form("form_cancel"):
        booking_id = st.text_input("ID de Reserva (ej. BK-001)")
        reason     = st.text_area("Motivo de cancelación")
        cancel_btn = st.form_submit_button("Cancelar Reserva", type="primary")

    if cancel_btn:
        if not booking_id or not reason:
            st.error("ID y motivo son requeridos.")
            return

        result, err = rpc_call("cancel_booking", {"booking_id": booking_id, "reason": reason})

        if err:
            st.error(f"Error: {err}")
        else:
            st.success(
                f"✅ Reserva `{result['booking_id']}` cancelada. "
                f"Asientos liberados: {result['seats_released']}."
            )


# ── DLQ ───────────────────────────────────────────────────────────────────────

def page_dlq():
    st.header("🚨 Dead Letter Queue")

    try:
        with open(DLQ_FILE, "r") as f:
            messages = json.load(f)
    except FileNotFoundError:
        st.info("No hay mensajes en la DLQ — sin fallos registrados.")
        return
    except json.JSONDecodeError:
        st.error("Archivo DLQ corrupto.")
        return

    if not messages:
        st.info("La DLQ está vacía.")
        return

    st.warning(f"**{len(messages)} mensaje(s) fallido(s)**")

    for msg in reversed(messages):
        with st.expander(f"❌  {msg['booking_id']}  —  {msg['failed_at']}"):
            st.write(f"**Error:** {msg['error']}")
            st.json(msg["payload"])


# ── App principal ─────────────────────────────────────────────────────────────

def main():
    engine = get_engine()

    with st.sidebar:
        st.title("✈️ Flight Booking")
        st.divider()
        page = st.radio(
            "nav",
            ["📊 Dashboard", "➕ Nueva Reserva", "⚡ Procesar", "🔍 Disponibilidad", "❌ Cancelar", "🚨 DLQ"],
            label_visibility="collapsed",
        )
        st.divider()

        st.caption("Estado de servicios")
        for name, ok in service_status().items():
            (st.success if ok else st.error)(f"{'✓' if ok else '✗'}  {name}")

    pages = {
        "📊 Dashboard":     lambda: page_dashboard(engine),
        "➕ Nueva Reserva": lambda: page_nueva_reserva(engine),
        "⚡ Procesar":      lambda: page_procesar(engine),
        "🔍 Disponibilidad": page_verificar,
        "❌ Cancelar":       page_cancelar,
        "🚨 DLQ":            page_dlq,
    }
    pages[page]()


if __name__ == "__main__":
    main()
