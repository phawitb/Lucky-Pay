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
    user_id: Optional[str] = None
    discount_mode: Optional[str] = None


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
        description="Base amount before discount (THB). System will apply discount depending on mode."
    )
    description: Optional[str] = Field(None, max_length=200)
    prompay_id: Optional[str] = Field(
        None,
        description="PromptPay ID / phone (Thai). If omitted, use default merchant PromptPay ID."
    )
    user_id: Optional[str] = Field(
        None,
        description="User identifier used to keep discount stable within a short window."
    )
    discount: Optional[str] = Field(
        None,
        description=(
            "Discount mode: "
            "'none' = queue incremental discount 0.00,0.01,0.02,... (per base_amount+prompay_id, 5 min window); "
            "'auto' = random 0–2%; "
            "'<x>percen' e.g. '0.05percen' => random 0–5%; "
            "'<x>bath' e.g. '2.2bath' => random 0–2.2 THB. "
            "If omitted, behaves like 'auto'."
        )
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

# เก็บ discount เดิมของ user_id + base_amount ภายใน 10 นาที (สำหรับโหมดที่สุ่ม)
USER_DISCOUNT_WINDOW = timedelta(minutes=10)
user_recent_discounts: Dict[Tuple[str, float], dict] = {}  # (user_id, base_amount) -> {discount, pay_amount, mode, used_at}

# NONE-mode queue window
NONE_MODE_QUEUE_WINDOW = timedelta(minutes=5)


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
# HELPER: CLEANERS
# ------------------------------------------------------------------------------

def clean_old_pay_amounts() -> None:
    now = datetime.utcnow()
    to_delete = []
    for amt, used_at in list(recent_pay_amounts.items()):
        if now - used_at > RECENT_AMOUNT_WINDOW:
            to_delete.append(amt)
    for a in to_delete:
        del recent_pay_amounts[a]


def clean_old_user_discounts() -> None:
    now = datetime.utcnow()
    to_delete = []
    for key, data in list(user_recent_discounts.items()):
        if now - data["used_at"] > USER_DISCOUNT_WINDOW:
            to_delete.append(key)
    for k in to_delete:
        del user_recent_discounts[k]


def compute_unique_suffix(base_amount: float, discount: float) -> int:
    """
    แปลง discount (บาท) เป็น unique_suffix (สตางค์) 0–99 (แค่ให้ client แสดง)
    ถ้าเกิน 0.99 บาทจะถูก clamp เป็น 99
    """
    satang = int(round(discount * 100))
    if satang < 0:
        satang = 0
    if satang > 99:
        satang = 99
    return satang


def resolve_discount_mode(raw_mode: Optional[str]) -> str:
    if raw_mode is None or raw_mode.strip() == "":
        return "auto"
    m = raw_mode.strip().lower()
    if m in ("none", "auto"):
        return m
    if m.endswith("percen") or m.endswith("bath"):
        return m
    # ค่าอื่นๆ ที่ไม่รู้จัก treat เป็น auto
    return "auto"


# ------------------------------------------------------------------------------
# HELPER: DISCOUNT CALCULATION (non-none modes)
# ------------------------------------------------------------------------------

def pick_discount_for_base_amount(
    base_amount: float,
    discount_mode: str,
    user_id: Optional[str] = None,
) -> Tuple[float, int, float]:
    """
    คืนค่า: (discount_thb, unique_suffix, pay_amount)
    - discount_mode:
        auto   : สุ่ม 0–2% ของ base_amount
        Xpercen: เช่น 0.05percen => 0–5%
        Xbath  : เช่น 2.2bath   => 0–2.2 บาท
    - ถ้ามี user_id เดิมที่เรียกด้วย base_amount เดิมใน 10 นาทีที่ผ่านมา
      จะ reuse discount เดิม (กันกดวนเพื่อลุ้นส่วนลด)
    - ยอดจ่ายสุดท้ายต้อง >= 1 บาท
    """
    base_amount = round(base_amount, 2)
    if base_amount < 1.0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Base amount must be at least 1 THB because final pay_amount must be >= 1 THB."
        )

    discount_mode = resolve_discount_mode(discount_mode)
    if discount_mode == "none":
        # โหมด none ไม่ควรเข้าฟังก์ชันนี้ ให้ไปใช้ compute_none_mode_discount แทน
        raise RuntimeError("pick_discount_for_base_amount should not be called with discount_mode='none'")

    clean_old_pay_amounts()
    clean_old_user_discounts()
    now = datetime.utcnow()

    # ถ้ามี user_id และมี cache เดิมให้ใช้ซ้ำ
    if user_id is not None:
        key = (user_id, base_amount)
        cached = user_recent_discounts.get(key)
        if cached and now - cached["used_at"] <= USER_DISCOUNT_WINDOW:
            discount = cached["discount"]
            pay_amount = cached["pay_amount"]
            suffix = compute_unique_suffix(base_amount, discount)
            recent_pay_amounts[pay_amount] = now
            return discount, suffix, pay_amount

    # คำนวณ max_discount_thb ตามโหมด
    max_discount_thb = 0.0
    if discount_mode == "auto":
        max_discount_thb = round(base_amount * 0.02, 2)  # 0–2%
    elif discount_mode.endswith("percen"):
        num_part = discount_mode[:-6]
        try:
            v = float(num_part)
            max_percent = max(0.0, v * 100.0)  # 0.05 -> 5%
        except ValueError:
            max_percent = 2.0
        max_discount_thb = round(base_amount * (max_percent / 100.0), 2)
    elif discount_mode.endswith("bath"):
        num_part = discount_mode[:-4]
        try:
            max_discount_thb = max(0.0, round(float(num_part), 2))
        except ValueError:
            max_discount_thb = round(base_amount * 0.02, 2)
    else:
        max_discount_thb = round(base_amount * 0.02, 2)

    # จำกัดให้ยอดสุดท้ายไม่ต่ำกว่า 1 บาท
    max_allowed_discount = round(base_amount - 1.0, 2)
    if max_allowed_discount < 0:
        max_allowed_discount = 0.0
    if max_discount_thb > max_allowed_discount:
        max_discount_thb = max_allowed_discount

    # กรณีไม่มีส่วนลดได้เลย (เช่น amount ใกล้ 1 บาทมาก)
    if max_discount_thb <= 0:
        discount = 0.0
        pay_amount = base_amount
        suffix = 0
        recent_pay_amounts[pay_amount] = now
        if user_id is not None:
            key = (user_id, base_amount)
            user_recent_discounts[key] = {
                "discount": discount,
                "pay_amount": pay_amount,
                "mode": discount_mode,
                "used_at": now,
            }
        return discount, suffix, pay_amount

    # สุ่ม discount ให้ pay_amount ไม่เบิ้ลกับอันที่ใช้ล่าสุด (เท่าที่ทำได้)
    discount = 0.0
    pay_amount = base_amount
    for _ in range(20):
        discount_candidate = round(random.uniform(0, max_discount_thb), 2)
        pay_candidate = round(base_amount - discount_candidate, 2)
        if pay_candidate < 1.0:
            continue
        if pay_candidate not in recent_pay_amounts:
            discount = discount_candidate
            pay_amount = pay_candidate
            break
    else:
        # ถ้าลองหลายรอบแล้วยังชนทั้งหมด ก็ไม่ลด
        discount = 0.0
        pay_amount = base_amount

    suffix = compute_unique_suffix(base_amount, discount)
    recent_pay_amounts[pay_amount] = now

    # cache ให้ user_id
    if user_id is not None:
        key = (user_id, base_amount)
        user_recent_discounts[key] = {
            "discount": discount,
            "pay_amount": pay_amount,
            "mode": discount_mode,
            "used_at": now,
        }

    return discount, suffix, pay_amount


# ------------------------------------------------------------------------------
# HELPER: DISCOUNT CALCULATION (none mode queue)
# ------------------------------------------------------------------------------

def compute_none_mode_discount(
    base_amount: float,
    prompay_id: str,
) -> Tuple[float, int, float]:
    """
    none-mode:
    - ถ้าไม่มีคิวที่ base_amount + prompay_id เดียวกันภายใน 5 นาที -> discount = 0.00
    - ถ้ามีคิว PENDING (none-mode) อยู่แล้ว n รายการ (ยังไม่เกิน 5 นาที)
      ใบใหม่จะได้ discount = 0.01 * n (บาท) เช่น 0.01, 0.02, 0.03, ...
    - ใบที่เกิน 5 นาที จะโดน expire_old_none_mode_payments() CANCELLED ออกไปก่อนแล้ว
    - ยอดสุดท้ายต้อง >= 1 บาท
    """
    now = datetime.utcnow()
    base_amount = round(base_amount, 2)

    # คิวที่กำลังรออยู่ (none-mode เท่านั้น) และยังไม่เกิน 5 นาที
    pending_same = [
        p for p in payments_db.values()
        if p["status"] == "PENDING"
        and p.get("discount_mode") == "none"
        and round(p["base_amount"], 2) == base_amount
        and p["prompay_id"] == prompay_id
        and now - p["created_at"] < NONE_MODE_QUEUE_WINDOW
    ]

    # ลำดับคิว (0 = คนแรก discount 0.00, 1 = 0.01, 2 = 0.02, ...)
    n = len(pending_same)
    discount = round(0.01 * n, 2)

    # จำกัดไม่ให้ส่วนลดทำให้ยอดต่ำกว่า 1 บาท
    max_allowed_discount = round(base_amount - 1.0, 2)
    if max_allowed_discount < 0:
        max_allowed_discount = 0.0
    if discount > max_allowed_discount:
        discount = max_allowed_discount

    pay_amount = round(base_amount - discount, 2)
    if pay_amount < 1.0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Calculated pay_amount < 1 in none mode. Please check base amount."
        )

    suffix = compute_unique_suffix(base_amount, discount)
    # ไม่จำเป็นต้อง unique global, แต่เก็บไว้ monitor ได้
    recent_pay_amounts[pay_amount] = now

    return discount, suffix, pay_amount


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
# HELPER: expire old NONE-mode payments
# ------------------------------------------------------------------------------

async def expire_old_none_mode_payments():
    """
    สำหรับ discount_mode = 'none' ถ้ารอเกิน 5 นาทีแล้วยังไม่จ่าย ให้หลุดอัตโนมัติ (CANCELLED)
    """
    if not payments_db:
        return
    now = datetime.utcnow()
    for payment in list(payments_db.values()):
        if (
            payment["status"] == "PENDING"
            and payment.get("discount_mode") == "none"
            and now - payment["created_at"] > NONE_MODE_QUEUE_WINDOW
        ):
            payment["status"] = "CANCELLED"
            payments_db[payment["payment_id"]] = payment
            print(f"[EXPIRE] payment_id={payment['payment_id']} expired (none mode > 5 min)")
            await notify_payment(payment, event="EXPIRED")


# ------------------------------------------------------------------------------
# PAYMENT ENDPOINTS
# ------------------------------------------------------------------------------

@app.post("/payments/qr", response_model=PaymentStatusResponse)
async def create_payment_qr(body: PaymentCreateRequest):
    prompay_id = body.prompay_id or DEFAULT_PROMPTPAY_ID

    base_amount = round(body.amount, 2)
    if base_amount < 1.0:
        # ตาม requirement: ยอดเงินรวมที่ต้องจ่ายสุดท้าย ต้องมากกว่าหรือเท่า 1 บาท
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Base amount must be at least 1 THB because final pay_amount must be >= 1 THB."
        )

    # เคลียร์ / expire queue ของ none-mode ก่อน (ใบเก่าเกิน 5 นาทีจะโดน CANCELLED)
    await expire_old_none_mode_payments()

    discount_mode = resolve_discount_mode(body.discount)

    if discount_mode == "none":
        # ใช้คิวแบบ incremental: 0.00, 0.01, 0.02, ...
        discount, suffix, pay_amount = compute_none_mode_discount(
            base_amount=base_amount,
            prompay_id=prompay_id,
        )
    else:
        # โหมดอื่นใช้ random ตาม config + ป้องกันกดลุ้นซ้ำด้วย user_id
        discount, suffix, pay_amount = pick_discount_for_base_amount(
            base_amount=base_amount,
            discount_mode=discount_mode,
            user_id=body.user_id,
        )

    if pay_amount < 1.0:
        # safety double-check
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Calculated pay_amount < 1. Please check base amount and discount settings."
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
        "user_id": body.user_id,
        "discount_mode": discount_mode,
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
