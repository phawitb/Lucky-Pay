from fastapi import FastAPI, Query, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Tuple
from uuid import uuid4, UUID
from datetime import datetime, timedelta
import qrcode
import base64
import io
import random
import re

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
    phone_number: str
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
    phone_number: Optional[str] = Field(
        None,
        description="PromptPay phone (Thai). If omitted, use default merchant number."
    )


class MarkPaidByAmountRequest(BaseModel):
    amount: float = Field(..., gt=0, description="Received amount in THB (exact pay_amount).")


# ------------------------------------------------------------------------------
# IN-MEMORY "DATABASE"
# ------------------------------------------------------------------------------

DEFAULT_PROMPTPAY_PHONE = "0805471749"  # เบอร์ PromptPay หลักของคุณ

payments_db: Dict[UUID, dict] = {}  # payment_id -> payment data

# เก็บยอดที่จ่ายจริง (pay_amount) ที่ถูกใช้ล่าสุด
RECENT_AMOUNT_WINDOW = timedelta(minutes=10)
recent_pay_amounts: Dict[float, datetime] = {}  # pay_amount -> last_used_time


# ------------------------------------------------------------------------------
# HELPER: UNIQUE pay_amount (หลังหักส่วนลด)
# ------------------------------------------------------------------------------

def clean_old_pay_amounts() -> None:
    """ลบ pay_amount ที่เก่ากว่า RECENT_AMOUNT_WINDOW ออกจาก memory."""
    now = datetime.utcnow()
    to_delete = []
    for amt, used_at in recent_pay_amounts.items():
        if now - used_at > RECENT_AMOUNT_WINDOW:
            to_delete.append(amt)
    for a in to_delete:
        del recent_pay_amounts[a]


def pick_discount_for_base_amount(base_amount: float) -> Tuple[float, int, float]:
    """
    เลือกส่วนลดสำหรับ base_amount ให้:
      - discount อยู่ระหว่าง 0–1% ของ base_amount
      - pay_amount = base_amount - discount
      - pay_amount ไม่ซ้ำกับยอดอื่นในช่วงเวลา RECENT_AMOUNT_WINDOW

    return: (discount, suffix, pay_amount)
      - discount: จำนวนเงินส่วนลด (เช่น 0.63)
      - suffix: จำนวนสตางค์ที่ใช้หัก (63)
      - pay_amount: ยอดที่ให้ลูกค้าจ่ายจริง
    """
    base_amount = round(base_amount, 2)
    clean_old_pay_amounts()
    now = datetime.utcnow()

    # ยอดเล็กมาก (<1) ไม่ให้เล่นส่วนลด เพื่อความปลอดภัย
    if base_amount < 1:
        pay_amount = base_amount
        recent_pay_amounts[pay_amount] = now
        return 0.0, 0, pay_amount

    # ส่วนลดไม่เกิน 1% → suffix/100 <= 0.01 * base_amount
    # => suffix <= base_amount
    max_suffix_by_percent = int(base_amount)
    max_suffix = min(99, max_suffix_by_percent)

    # ลอง suffix ตั้งแต่ 0 (ไม่ลด) ถึง max_suffix
    candidate_suffixes = list(range(0, max_suffix + 1))
    random.shuffle(candidate_suffixes)  # สุ่มลำดับให้กระจาย ๆ

    chosen_discount = None
    chosen_suffix = None
    chosen_pay_amount = None

    for suffix in candidate_suffixes:
        discount = round(suffix / 100.0, 2)
        pay_amount = round(base_amount - discount, 2)
        if pay_amount <= 0:
            continue

        # ถ้า pay_amount ยังไม่ถูกใช้ในช่วงเวลาใกล้ ๆ → ใช้อันนี้
        if pay_amount not in recent_pay_amounts:
            chosen_discount = discount
            chosen_suffix = suffix
            chosen_pay_amount = pay_amount
            break

    # ถ้าหาไม่เจอเลย (rare case) → ยอมใช้ base_amount ตรง ๆ
    if chosen_discount is None:
        chosen_discount = 0.0
        chosen_suffix = 0
        chosen_pay_amount = base_amount

    # บันทึกว่า pay_amount นี้ถูกใช้ในตอนนี้
    recent_pay_amounts[chosen_pay_amount] = now

    # กัน rounding ทำให้ส่วนลดเกิน 1% เล็กน้อย (เผื่อ ๆ)
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
    """
    Compute CRC16-CCITT (0xFFFF initial value), polynomial 0x1021.
    """
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
    """
    Generate EMVCo-compliant PromptPay payload for:
      - Thai phone number (10 digits starting with 0)
      - Or national ID / tax ID
    """
    # 00: Payload format indicator
    payload = format_tag("00", "01")

    # 01: POI method (11 = dynamic)
    payload += format_tag("01", "11")

    # Merchant account info (tag 29)
    #   00: AID (A000000677010111)
    #   01: mobile number (or 02: ID)
    if len(phone_or_id) == 10 and phone_or_id.startswith("0"):
        # phone -> convert to 66X...
        # mobile = "66" + phone_or_id[1:]
        mobile = "0" + phone_or_id[1:]
        sub_id = format_tag("01", mobile)
    else:
        # assume national ID
        sub_id = format_tag("02", phone_or_id)

    aid = format_tag("00", "A000000677010111")
    merchant_account_info_value = aid + sub_id
    payload += format_tag("29", merchant_account_info_value)

    # 52: Merchant category code
    payload += format_tag("52", "0000")

    # 53: Currency (764 = THB)
    payload += format_tag("53", "764")

    # 54: Amount (optional)
    if amount is not None:
        payload += format_tag("54", f"{amount:.2f}")

    # 58: Country code
    payload += format_tag("58", "TH")

    # 59: Merchant name (placeholder)
    payload += format_tag("59", "PROMPTPAY USER")

    # 60: Merchant city
    payload += format_tag("60", "BANGKOK")

    # 63: CRC
    data_for_crc = payload + "6304"
    crc = f"{crc16_ccitt(data_for_crc):04X}"
    payload += format_tag("63", crc)

    return payload


def generate_qr_base64(payload: str) -> str:
    """
    Generate a QR PNG image and return as base64 string.
    """
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
# HELPER: MATCH PAYMENT BY AMOUNT (ใช้ได้ทั้ง endpoint และ noti)
# ------------------------------------------------------------------------------

def mark_payment_by_amount_internal(amount: float) -> dict:
    """
    ใช้ยอดเงิน (amount) หา payment ที่ PENDING และ pay_amount ตรงกัน
    ถ้าเจอหลายรายการ เลือกตัวที่สร้างล่าสุด
    ถ้าไม่เจอ -> raise ValueError
    """
    target_amount = round(amount, 2)

    candidates = [
        p for p in payments_db.values()
        if p["status"] == "PENDING" and round(p["pay_amount"], 2) == target_amount
    ]

    if not candidates:
        raise ValueError("No pending payment matched with this amount.")

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
    """
    พยายามดึงยอดเงินจากข้อความ noti ภาษาไทย เช่น:
    'รายการเงินเข้า 1.12 บาท เข้าบัญชี X-1787 วันที่ 21'

    logic:
    - ตัด comma ออก (เผื่อรูปแบบ 1,234.56)
    - หา pattern 'ตัวเลข(ทศนิยม) ... บาท'
    """
    cleaned = msg.replace(",", "")
    # ดึงตัวเลขก่อนคำว่า "บาท"
    pattern = r'([0-9]+(?:\.[0-9]{1,2})?)\s*บาท'
    m = re.search(pattern, cleaned)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


# ------------------------------------------------------------------------------
# NOTIFICATION ENDPOINT (รับ noti จาก Android + auto match payment)
# ------------------------------------------------------------------------------

@app.post("/noti/", response_model=NotificationResponse)
def create_notification(
    title: str = Query(
        ...,
        min_length=1,
        max_length=100,
        description="Notification title from Android."
    ),
    msg: str = Query(
        ...,
        min_length=1,
        max_length=2000,
        description="Notification text from Android (e.g. bank incoming transfer message)."
    )
):
    """
    รับ noti จาก Android

    ตัวอย่าง msg:
      'รายการเงินเข้า 999.37 บาท เข้าบัญชี X-1787 วันที่ 21'

    flow:
    - log noti
    - extract amount จาก msg
    - ถ้าเจอ amount -> พยายาม match payment จาก pay_amount และ mark เป็น PAID
    """
    print(f"[NOTI] Title: {title} | Msg: {msg}")

    parsed_amount = extract_amount_from_thai_notification(msg)
    auto_matched_payment: Optional[PaymentStatusResponse] = None

    if parsed_amount is not None:
        try:
            payment_dict = mark_payment_by_amount_internal(parsed_amount)
            auto_matched_payment = PaymentStatusResponse(**payment_dict)
            print(f"[NOTI] Auto matched payment_id={payment_dict['payment_id']} amount={parsed_amount}")
        except ValueError as e:
            print(f"[NOTI] No matching payment for amount={parsed_amount}: {e}")

    return NotificationResponse(
        status="success",
        message="Notification received.",
        data_received={
            "title": title,
            "msg": msg
        },
        parsed_amount=parsed_amount,
        auto_matched=auto_matched_payment
    )


# ------------------------------------------------------------------------------
# PAYMENT GATEWAY ENDPOINTS (PromptPay + unique final amount)
# ------------------------------------------------------------------------------

@app.post("/payments/qr", response_model=PaymentStatusResponse)
def create_payment_qr(body: PaymentCreateRequest):
    """
    Create a payment and generate a PromptPay QR code to receive money.

    Logic:
    - body.amount = base_amount (ยอดเต็มก่อนลด)
    - ระบบสุ่มส่วนลด 0–1% แล้วดูให้ pay_amount (ยอดจริง) ไม่ซ้ำในช่วงเวลาใกล้ ๆ
    """
    phone_number = body.phone_number or DEFAULT_PROMPTPAY_PHONE

    base_amount = round(body.amount, 2)

    # ใช้ helper เลือกส่วนลดให้ได้ pay_amount ที่ไม่ซ้ำ
    discount, suffix, pay_amount = pick_discount_for_base_amount(base_amount)

    if pay_amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Calculated pay_amount <= 0. Please check base amount."
        )

    payload = generate_promptpay_payload(phone_number, pay_amount)
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
        "phone_number": phone_number,
        "payload": payload,
        "status": "PENDING",
        "created_at": now,
        "qr_base64": qr_b64,
    }

    payments_db[payment_id] = payment_data

    return PaymentStatusResponse(**payment_data)


@app.get("/payments/{payment_id}", response_model=PaymentStatusResponse)
def get_payment_status(payment_id: UUID):
    """
    Check payment status (PENDING / PAID / CANCELLED).
    """
    payment = payments_db.get(payment_id)
    if not payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found"
        )
    return PaymentStatusResponse(**payment)


@app.post("/payments/{payment_id}/mark-paid", response_model=PaymentStatusResponse)
def mark_payment_paid(payment_id: UUID):
    """
    Mark payment as PAID by payment_id (manual).
    """
    payment = payments_db.get(payment_id)
    if not payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found"
        )

    payment["status"] = "PAID"
    payments_db[payment_id] = payment

    return PaymentStatusResponse(**payment)


@app.post("/payments/mark-paid-by-amount", response_model=PaymentStatusResponse)
def mark_payment_paid_by_amount(body: MarkPaidByAmountRequest):
    """
    Mark payment as PAID using only the received amount.
    ใช้สำหรับ debug หรือ call ตรง ๆ ได้
    """
    try:
        payment = mark_payment_by_amount_internal(body.amount)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    return PaymentStatusResponse(**payment)


@app.post("/payments/{payment_id}/cancel", response_model=PaymentStatusResponse)
def cancel_payment(payment_id: UUID):
    """
    Cancel a payment (if still pending).
    """
    payment = payments_db.get(payment_id)
    if not payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found"
        )

    if payment["status"] == "PAID":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot cancel a PAID payment."
        )

    payment["status"] = "CANCELLED"
    payments_db[payment_id] = payment

    return PaymentStatusResponse(**payment)
