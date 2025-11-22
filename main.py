from fastapi import (
    FastAPI,
    Query,
    HTTPException,
    status,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Tuple, List
from uuid import uuid4, UUID
from datetime import datetime, timedelta
import qrcode
import base64
import io
import random
import re
import asyncio

# ------------------------------------------------------------------------------
# FastAPI app initialization
# ------------------------------------------------------------------------------
app = FastAPI(title="Notification + PromptPay Payment Gateway")

# ------------------------------------------------------------------------------
# MODELS
# ------------------------------------------------------------------------------

class PaymentStatusResponse(BaseModel):
    payment_id: UUID
    base_amount: float
    pay_amount: float
    discount: float
    unique_suffix: int        # จำนวนสตางค์ที่หักเป็นส่วนลด (0–99)
    description: Optional[str]
    prompay_id: str
    payload: str
    status: Literal["PENDING", "PAID", "CANCELLED"]
    created_at: datetime
    qr_base64: Optional[str] = None  # QR เป็น base64


class NotificationResponse(BaseModel):
    status: str
    message: str
    data_received: dict
    parsed_amount: Optional[float] = None
    auto_matched: Optional[PaymentStatusResponse] = None


class PaymentCreateRequest(BaseModel):
    amount: float = Field(
        ...,
        gt=0,
        description="Base amount before discount (THB). System will apply 0–1% discount and unique final amount."
    )
    description: Optional[str] = Field(None, max_length=200)
    prompay_id: Optional[str] = Field(
        None,
        description="PromptPay ID / phone (Thai). If omitted, use default merchant PromptPay ID."
    )


class MarkPaidByAmountRequest(BaseModel):
    amount: float = Field(..., gt=0, description="Received amount in THB (exact pay_amount).")


# ------------------------------------------------------------------------------
# IN-MEMORY "DATABASE"
# ------------------------------------------------------------------------------

DEFAULT_PROMPTPAY_ID = "0805471749"  # PromptPay หลักของคุณ

payments_db: Dict[UUID, dict] = {}  # payment_id -> payment data

# เก็บยอดที่จ่ายจริง (pay_amount) ที่ถูกใช้ล่าสุด
RECENT_AMOUNT_WINDOW = timedelta(minutes=10)
recent_pay_amounts: Dict[float, datetime] = {}  # pay_amount -> last_used_time


# ------------------------------------------------------------------------------
# WEBSOCKET CONNECTION MANAGERS
# ------------------------------------------------------------------------------

class ConnectionManager:
    """
    สำหรับ websocket ที่รับ event payment ทั้งระบบ (/ws/payments)
    """
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"[WS] /ws/payments connected: {len(self.active_connections)} active")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        print(f"[WS] /ws/payments disconnected: {len(self.active_connections)} active")

    async def broadcast(self, message: dict):
        dead: List[WebSocket] = []
        for ws in self.active_connections:
            try:
                await ws.send_json(message)
            except Exception as e:
                print(f"[WS] /ws/payments send error: {e}")
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


class PrompaySubscriptionManager:
    """
    สำหรับ websocket ที่ subscribe ตาม prompay_id
    path: /ws/prompay/{prompay_id}
    """
    def __init__(self):
        self.subscribers: Dict[str, List[WebSocket]] = {}

    async def connect(self, prompay_id: str, websocket: WebSocket):
        await websocket.accept()
        self.subscribers.setdefault(prompay_id, []).append(websocket)
        print(
            f"[WS] /ws/prompay/{prompay_id} connected: "
            f"{len(self.subscribers[prompay_id])} active for this id"
        )

    def disconnect(self, websocket: WebSocket):
        empty_keys = []
        for pid, conns in self.subscribers.items():
            if websocket in conns:
                conns.remove(websocket)
                print(
                    f"[WS] /ws/prompay/{pid} disconnected: "
                    f"{len(conns)} active for this id"
                )
            if not conns:
                empty_keys.append(pid)
        for k in empty_keys:
            del self.subscribers[k]

    async def send_to_prompay(self, prompay_id: str, message: dict):
        conns = self.subscribers.get(prompay_id)
        if not conns:
            return
        dead: List[WebSocket] = []
        for ws in conns:
            try:
                await ws.send_json(message)
            except Exception as e:
                print(f"[WS] /ws/prompay/{prompay_id} send error: {e}")
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager_all = ConnectionManager()
manager_prompay = PrompaySubscriptionManager()


async def notify_payment(payment: dict, event: str):
    """
    broadcast event payment ให้ทุก connection ใน /ws/payments
    """
    if not manager_all.active_connections:
        return

    payload = PaymentStatusResponse(**payment)
    message = {
        "type": "payment_update",
        "event": event,
        "payment": jsonable_encoder(payload),
        "server_time_utc": datetime.utcnow().isoformat(),
    }

    print("[WS] broadcast /ws/payments:", event, "payment_id=", payment["payment_id"])
    await manager_all.broadcast(message)


async def notify_notification_prompay(
    prompay_id: str,
    title: str,
    msg: str,
    parsed_amount: Optional[float],
    matched_payment: Optional[dict],
):
    """
    ส่ง event notification ไปยัง client ที่ connect /ws/prompay/{prompay_id}
    - matched_payment: dict ของ payment ถ้า match ได้, ถ้าไม่ก็ None
    """
    payment_payload = None
    if matched_payment is not None:
        payment_payload = jsonable_encoder(PaymentStatusResponse(**matched_payment))

    message = {
        "type": "notification",
        "prompay_id": prompay_id,
        "title": title,
        "msg": msg,
        "parsed_amount": parsed_amount,
        "matched": matched_payment is not None,
        "payment": payment_payload,
        "server_time_utc": datetime.utcnow().isoformat(),
    }
    print(
        "🔔 WS SEND NOTIFICATION /ws/prompay:",
        prompay_id,
        "amount=", parsed_amount,
        "matched=", matched_payment is not None,
    )
    await manager_prompay.send_to_prompay(prompay_id, message)


# ------------------------------------------------------------------------------
# HELPER: UNIQUE pay_amount (หลังหักส่วนลด)
# ------------------------------------------------------------------------------

def clean_old_pay_amounts() -> None:
    now = datetime.utcnow()
    to_delete = []
    for amt, used_at in list(recent_pay_amounts.items()):
        if now - used_at > RECENT_AMOUNT_WINDOW:
            to_delete.append(amt)
    for a in to_delete:
        del recent_pay_amounts[a]


def pick_discount_for_base_amount(base_amount: float) -> Tuple[float, int, float]:
    base_amount = round(base_amount, 2)
    clean_old_pay_amounts()
    now = datetime.utcnow()

    if base_amount < 1:
        pay_amount = base_amount
        recent_pay_amounts[pay_amount] = now
        return 0.0, 0, pay_amount

    max_suffix_by_percent = int(base_amount)
    max_suffix = min(99, max_suffix_by_percent)

    candidate_suffixes = list(range(0, max_suffix + 1))
    random.shuffle(candidate_suffixes)

    chosen_discount = None
    chosen_suffix = None
    chosen_pay_amount = None

    for suffix in candidate_suffixes:
        discount = round(suffix / 100.0, 2)
        pay_amount = round(base_amount - discount, 2)
        if pay_amount <= 0:
            continue

        if pay_amount not in recent_pay_amounts:
            chosen_discount = discount
            chosen_suffix = suffix
            chosen_pay_amount = pay_amount
            break

    if chosen_discount is None:
        chosen_discount = 0.0
        chosen_suffix = 0
        chosen_pay_amount = base_amount

    recent_pay_amounts[chosen_pay_amount] = now

    max_discount = round(base_amount * 0.01, 2)
    if chosen_discount > max_discount:
        chosen_discount = max_discount
        chosen_pay_amount = round(base_amount - chosen_discount, 2)

    return chosen_discount, chosen_suffix, chosen_pay_amount


# ------------------------------------------------------------------------------
# HELPER FUNCTIONS: PromptPay
# ------------------------------------------------------------------------------

def format_tag(tag: str, value: str) -> str:
    length = f"{len(value):02d}"
    return f"{tag}{length}{value}"


def crc16_ccitt(data: str, poly: int = 0x1021, init: int = 0xFFFF) -> int:
    crc = init
    for ch in data.encode("utf-8"):
        crc ^= ch << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ poly
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc


def generate_promptpay_payload(phone_or_id: str, amount: Optional[float] = None) -> str:
    payload = format_tag("00", "01")
    payload += format_tag("01", "11")

    if len(phone_or_id) == 10 and phone_or_id.startswith("0"):
        mobile = "0" + phone_or_id[1:]
        sub_id = format_tag("01", mobile)
    else:
        sub_id = format_tag("02", phone_or_id)

    aid = format_tag("00", "A000000677010111")
    merchant_account_info_value = aid + sub_id
    payload += format_tag("29", merchant_account_info_value)

    payload += format_tag("52", "0000")
    payload += format_tag("53", "764")

    if amount is not None:
        payload += format_tag("54", f"{amount:.2f}")

    payload += format_tag("58", "TH")
    payload += format_tag("59", "PROMPTPAY USER")
    payload += format_tag("60", "BANGKOK")

    data_for_crc = payload + "6304"
    crc = f"{crc16_ccitt(data_for_crc):04X}"
    payload += format_tag("63", crc)

    return payload


def generate_qr_base64(payload: str) -> str:
    qr = qrcode.QRCode(
        version=1,
        box_size=10,
        border=4,
    )
    qr.add_data(payload)
    qr.make(fit=True)
    img = qr.make_image()

    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)

    return base64.b64encode(buffer.read()).decode("utf-8")


# ------------------------------------------------------------------------------
# HELPER: MATCH PAYMENT BY AMOUNT + PROMPAY_ID
# ------------------------------------------------------------------------------

def mark_payment_by_amount_internal(amount: float, prompay_id: Optional[str] = None) -> dict:
    """
    ใช้ยอดเงิน (amount) + prompay_id หา payment ที่ PENDING และ pay_amount ตรงกัน
    ถ้าเจอหลายรายการ เลือกตัวที่สร้างล่าสุด
    ถ้าไม่เจอ -> raise ValueError
    """
    target_amount = round(amount, 2)

    candidates = [
        p for p in payments_db.values()
        if p["status"] == "PENDING"
        and round(p["pay_amount"], 2) == target_amount
        and (prompay_id is None or p["prompay_id"] == prompay_id)
    ]

    if not candidates:
        raise ValueError("No pending payment matched with this amount and prompay_id.")

    candidates.sort(key=lambda p: p["created_at"], reverse=True)
    payment = candidates[0]
    payment_id = payment["payment_id"]

    payment["status"] = "PAID"
    payments_db[payment_id] = payment
    return payment


# ------------------------------------------------------------------------------
# HELPER: PARSE AMOUNT FROM THAI NOTI TEXT
# ------------------------------------------------------------------------------

def extract_amount_from_thai_notification(msg: str) -> Optional[float]:
    cleaned = msg.replace(",", "")
    pattern = r'([0-9]+(?:\.[0-9]{1,2})?)\s*บาท'
    m = re.search(pattern, cleaned)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


# ------------------------------------------------------------------------------
# NOTIFICATION ENDPOINT (auto match + WS notify)
# ------------------------------------------------------------------------------

@app.post("/noti/", response_model=NotificationResponse)
async def create_notification(
    title: str = Query(..., min_length=1, max_length=100),
    msg: str = Query(..., min_length=1, max_length=2000),
    prompay_id: str = Query(..., min_length=1, max_length=50),
):
    """
    รับ noti จาก Android:
    - extract amount จาก msg
    - พยายาม match payment (amount + prompay_id)
    - ถ้า match ได้ -> mark PAID และคืน auto_matched
    - ส่ง event ไป WebSocket /ws/prompay/{prompay_id} เสมอ (type = notification)
    """
    print(f"[NOTI] Title: {title} | Msg: {msg} | PromptPay: {prompay_id}")

    parsed_amount = extract_amount_from_thai_notification(msg)
    auto_matched_payment: Optional[PaymentStatusResponse] = None
    matched_payment_dict: Optional[dict] = None

    if parsed_amount is not None:
        try:
            matched_payment_dict = mark_payment_by_amount_internal(parsed_amount, prompay_id=prompay_id)
            auto_matched_payment = PaymentStatusResponse(**matched_payment_dict)
            print(
                f"[NOTI] Auto matched payment_id={matched_payment_dict['payment_id']} "
                f"amount={parsed_amount} prompay_id={prompay_id}"
            )
            # broadcast ให้ /ws/payments ด้วย (optional)
            await notify_payment(matched_payment_dict, event="PAID_BY_NOTIFICATION")
        except ValueError as e:
            print(
                f"[NOTI] No matching payment for amount={parsed_amount} "
                f"prompay_id={prompay_id}: {e}"
            )

    # ส่ง noti ไปยัง /ws/prompay/{prompay_id} เสมอ
    await notify_notification_prompay(
        prompay_id=prompay_id,
        title=title,
        msg=msg,
        parsed_amount=parsed_amount,
        matched_payment=matched_payment_dict,
    )

    return NotificationResponse(
        status="success",
        message="Notification received.",
        data_received={
            "title": title,
            "msg": msg,
            "prompay_id": prompay_id,
        },
        parsed_amount=parsed_amount,
        auto_matched=auto_matched_payment
    )


# ------------------------------------------------------------------------------
# PAYMENT ENDPOINTS (ของเดิม)
# ------------------------------------------------------------------------------

@app.post("/payments/qr", response_model=PaymentStatusResponse)
async def create_payment_qr(body: PaymentCreateRequest):
    prompay_id = body.prompay_id or DEFAULT_PROMPTPAY_ID

    base_amount = round(body.amount, 2)
    discount, suffix, pay_amount = pick_discount_for_base_amount(base_amount)

    if pay_amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Calculated pay_amount <= 0. Please check base amount."
        )

    payload = generate_promptpay_payload(prompay_id, pay_amount)
    qr_b64 = generate_qr_base64(payload)

    payment_id = uuid4()
    now = datetime.utcnow()

    payment_data = {
        "payment_id": payment_id,
        "base_amount": base_amount,
        "pay_amount": pay_amount,
        "discount": discount,
        "unique_suffix": suffix,
        "description": body.description,
        "prompay_id": prompay_id,
        "payload": payload,
        "status": "PENDING",
        "created_at": now,
        "qr_base64": qr_b64,
    }

    payments_db[payment_id] = payment_data

    await notify_payment(payment_data, event="CREATED")

    return PaymentStatusResponse(**payment_data)


@app.get("/payments/{payment_id}", response_model=PaymentStatusResponse)
async def get_payment_status(payment_id: UUID):
    payment = payments_db.get(payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentStatusResponse(**payment)


@app.post("/payments/{payment_id}/mark-paid", response_model=PaymentStatusResponse)
async def mark_payment_paid(payment_id: UUID):
    payment = payments_db.get(payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    payment["status"] = "PAID"
    payments_db[payment_id] = payment

    await notify_payment(payment, event="PAID_BY_ID")
    return PaymentStatusResponse(**payment)


@app.post("/payments/mark-paid-by-amount", response_model=PaymentStatusResponse)
async def mark_payment_paid_by_amount(body: MarkPaidByAmountRequest):
    target_amount = round(body.amount, 2)
    candidates = [
        p for p in payments_db.values()
        if p["status"] == "PENDING" and round(p["pay_amount"], 2) == target_amount
    ]
    if not candidates:
        raise HTTPException(status_code=404, detail="No pending payment matched with this amount.")
    candidates.sort(key=lambda p: p["created_at"], reverse=True)
    payment = candidates[0]
    payment["status"] = "PAID"
    payments_db[payment["payment_id"]] = payment

    await notify_payment(payment, event="PAID_BY_AMOUNT")

    return PaymentStatusResponse(**payment)


@app.post("/payments/{payment_id}/cancel", response_model=PaymentStatusResponse)
async def cancel_payment(payment_id: UUID):
    payment = payments_db.get(payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    if payment["status"] == "PAID":
        raise HTTPException(
            status_code=400,
            detail="Cannot cancel a PAID payment."
        )

    payment["status"] = "CANCELLED"
    payments_db[payment_id] = payment

    await notify_payment(payment, event="CANCELLED")
    return PaymentStatusResponse(**payment)


# ------------------------------------------------------------------------------
# HEALTH CHECK
# ------------------------------------------------------------------------------

@app.get("/health", tags=["system"])
async def health_check():
    return {
        "status": "ok",
        "server_time_utc": datetime.utcnow().isoformat(),
        "payments_count": len(payments_db),
    }


# ------------------------------------------------------------------------------
# WEBSOCKET ENDPOINTS
# ------------------------------------------------------------------------------

@app.websocket("/ws/payments")
async def websocket_payments(ws: WebSocket):
    await manager_all.connect(ws)

    await ws.send_json({
        "type": "welcome",
        "message": "connected to /ws/payments",
        "server_time_utc": datetime.utcnow().isoformat(),
    })

    try:
        while True:
            data = await ws.receive_text()
            if data.lower() == "ping":
                await ws.send_json({
                    "type": "pong",
                    "server_time_utc": datetime.utcnow().isoformat(),
                })
            else:
                await ws.send_json({
                    "type": "echo",
                    "received": data,
                    "server_time_utc": datetime.utcnow().isoformat(),
                })
    except WebSocketDisconnect:
        manager_all.disconnect(ws)


@app.websocket("/ws/prompay/{prompay_id}")
async def websocket_prompay(prompay_id: str, ws: WebSocket):
    await manager_prompay.connect(prompay_id, ws)

    await ws.send_json({
        "type": "welcome",
        "message": f"connected to /ws/prompay/{prompay_id}",
        "prompay_id": prompay_id,
        "server_time_utc": datetime.utcnow().isoformat(),
    })

    try:
        while True:
            data = await ws.receive_text()
            if data.lower() == "ping":
                await ws.send_json({
                    "type": "pong",
                    "prompay_id": prompay_id,
                    "server_time_utc": datetime.utcnow().isoformat(),
                })
            else:
                await ws.send_json({
                    "type": "echo",
                    "prompay_id": prompay_id,
                    "received": data,
                    "server_time_utc": datetime.utcnow().isoformat(),
                })
    except WebSocketDisconnect:
        manager_prompay.disconnect(ws)
