import json
import time
from websocket import create_connection, WebSocketConnectionClosedException

SERVER_URL = "wss://200ef0723c0a.ngrok-free.app"  # เปลี่ยนตาม ngrok
PROMPAY_ID = "0805471749"

RECONNECT_DELAY_SECONDS = 3  # เวลาหน่วงก่อนลองต่อใหม่

def listen_payment_sync(prompay_id: str):
    url = f"{SERVER_URL}/ws/prompay/{prompay_id}"

    while True:
        print(f"\n[CLIENT] Connecting to {url} ...")
        try:
            ws = create_connection(url)
            print("[CLIENT] Connected.")

            # ส่ง ping ไปทักทาย (ไม่จำเป็น แต่ใช้เช็คว่าเชื่อมได้)
            ws.send("ping")

            while True:
                msg = ws.recv()
                print("[CLIENT] Raw message:", msg)  # debug ดูของจริงจาก server

                try:
                    data = json.loads(msg)
                except json.JSONDecodeError:
                    print("[CLIENT] Non-JSON message:", msg)
                    continue

                print("[CLIENT] Received:", data)

                msg_type = data.get("type")

                if msg_type == "welcome":
                    print(f"[CLIENT] Welcome from server for prompay_id={data.get('prompay_id')}")
                elif msg_type == "pong":
                    print("[CLIENT] Pong from server")
                elif msg_type == "payment_paid":
                    payment = data.get("payment") or {}
                    print(">>> Payment PAID for prompay_id:", data.get("prompay_id"))
                    print("    payment_id:", payment.get("payment_id"))
                    print("    status    :", payment.get("status"))
                    print("    amount    :", payment.get("pay_amount"))
                else:
                    print("[CLIENT] Other message type:", msg_type)

        except KeyboardInterrupt:
            print("\n[CLIENT] Stopped by user (Ctrl+C).")
            try:
                ws.close()
            except Exception:
                pass
            break

        except (ConnectionRefusedError, WebSocketConnectionClosedException) as e:
            print(f"[CLIENT] Connection error: {e}")
            print(f"[CLIENT] Reconnecting in {RECONNECT_DELAY_SECONDS} seconds...")
            time.sleep(RECONNECT_DELAY_SECONDS)
            continue

        except Exception as e:
            print(f"[CLIENT] Unexpected error: {e}")
            print(f"[CLIENT] Reconnecting in {RECONNECT_DELAY_SECONDS} seconds...")
            time.sleep(RECONNECT_DELAY_SECONDS)
            continue

        print(f"[CLIENT] Disconnected. Reconnecting in {RECONNECT_DELAY_SECONDS} seconds...")
        time.sleep(RECONNECT_DELAY_SECONDS)

if __name__ == "__main__":
    listen_payment_sync(PROMPAY_ID)
