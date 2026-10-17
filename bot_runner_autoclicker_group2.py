# ==============================================================================
# ATF TAP V2 AUTO-CLICKER — نسخه ابری (GitHub Actions + Cloudflare Worker)
# ==============================================================================
# وضعیت اجرا:
#   - اجرا روی رانرهای گیت‌هاب با ترایگر Cron-Job.org (هر ۶ ساعت).
#   - توقف تمیز و خودکار در دقیقه ۳۵۰ (۵ ساعت و ۵۰ دقیقه).
#   - پشتیبانی کامل از سیستم Tap V2 (انرژی، تپ دسته‌ای، توالی seq و توکن سشن).
#   - دریافت خودکار پارامترهای انرژی هر اکانت متناسب با لول همان اکانت.
# ==============================================================================

import os
import json
import asyncio
import time
import uuid
import random
import urllib.parse
import re
import sys
import aiohttp
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import RequestWebViewRequest
from pathlib import Path
from dotenv import load_dotenv

# فعال‌سازی نمایش لحظه‌ای لاگ‌ها در کنسول GitHub Actions
sys.stdout.reconfigure(line_buffering=True)

# پیدا کردن دقیق فایل .env در کنار خود فایل پایتون (برای اجرای لوکال)
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

API_ID = int(os.getenv("TG_API_ID") or 0)
API_HASH = os.getenv("TG_API_HASH", "")
BOT_USERNAME = "ATF_AIRDROP_bot"

if not API_ID or not API_HASH:
    raise ValueError("TG_API_ID or TG_API_HASH is missing! Set them in GitHub Secrets or .env.")

BASE_URL = "https://atf-bot-runner-autoclicker-group2.alibotrunner2.workers.dev/miner/index.php"

# سقف زمان اجرا: ۵ ساعت و ۵۰ دقیقه (۳۵۰ دقیقه = ۲۱۰۰۰ ثانیه)
MAX_RUN_SECONDS = (5 * 3600) + (50 * 60)

# ۱. اولویت اول: خواندن از سکرت گیت‌هاب (ACCOUNTS_JSON)
# ۲. اولویت دوم: خواندن از فایل محلی accounts.json
RAW_ACCOUNTS = os.getenv("ACCOUNTS_JSON")
if RAW_ACCOUNTS:
    try:
        ACCOUNTS = json.loads(RAW_ACCOUNTS)
    except Exception as e:
        print(f"Error parsing ACCOUNTS_JSON from env: {e}", flush=True)
        ACCOUNTS = []
elif os.path.exists("accounts.json"):
    try:
        with open("accounts.json", "r", encoding="utf-8") as f:
            ACCOUNTS = json.load(f)
    except Exception as e:
        print(f"Error reading local accounts.json: {e}", flush=True)
        ACCOUNTS = []
else:
    ACCOUNTS = []

# مدیریت صف درخواست‌ها برای جلوگیری از ارسال همزمان چند اکانت در یک ثانیه
QUEUE_LOCK = asyncio.Lock()
LAST_REQUEST_TIME = 0.0
MIN_REQUEST_GAP = 1.3


def to_base36(num: int) -> str:
    """تبدیل عدد به مبنای ۳۶ دقیقاً مشابه جاوااسکریپت"""
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    if num <= 0:
        return "0"
    res = []
    while num > 0:
        num, rem = divmod(num, 36)
        res.append(chars[rem])
    return "".join(reversed(res))


def generate_client_instance() -> str:
    """معادل دقیق تابع clientId() در فرانت‌اند"""
    parts = [to_base36(random.getrandbits(32)) for _ in range(4)]
    return "c_" + "".join(parts)


def generate_batch_id() -> str:
    """معادل دقیق تابع batchId() در فرانت‌اند"""
    now_b36 = to_base36(int(time.time() * 1000))
    rand_parts = [to_base36(random.getrandbits(32)) for _ in range(3)]
    return f"b_{now_b36}_{''.join(rand_parts)}"


def extract_user_from_init_data(init_data: str) -> dict:
    """استخراج tg_id و username از رشته initData تلگرام"""
    try:
        parsed = urllib.parse.parse_qs(init_data)
        user_str = parsed.get("user", ["{}"])[0]
        user_obj = json.loads(user_str)
        return {
            "tg_id": user_obj.get("id"),
            "username": user_obj.get("username", "")
        }
    except Exception:
        return {"tg_id": None, "username": ""}


def get_account_headers(acc: dict, init_data: str = "", tma_session_token: str = "") -> dict:
    ua = acc.get("user_agent") or (
        "Mozilla/5.0 (Linux; Android 14; SM-S928B Build/UP1A.231005.007; wv) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/128.0.6613.88 Mobile Safari/537.36"
    )
    match = re.search(r"Chrome/(\d+)", ua)
    chrome_ver = match.group(1) if match else "128"

    headers = {
        "User-Agent": ua,
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-CH-UA": f'"Chromium";v="{chrome_ver}", "Not;A=Brand";v="24", "Android WebView";v="{chrome_ver}"',
        "Sec-CH-UA-Mobile": "?1",
        "Sec-CH-UA-Platform": '"Android"',
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://atfminers.asloni.online",
        "Referer": "https://atfminers.asloni.online/miner/index.html"
    }
    if init_data:
        headers["x-telegram-init-data"] = init_data
    if tma_session_token:
        headers["x-atf-tma-session"] = tma_session_token
    return headers


async def wait_for_account_gap():
    global LAST_REQUEST_TIME
    async with QUEUE_LOCK:
        now = time.time()
        elapsed = now - LAST_REQUEST_TIME
        target_gap = MIN_REQUEST_GAP + random.uniform(0.1, 0.5)
        if elapsed < target_gap:
            await asyncio.sleep(target_gap - elapsed)
        LAST_REQUEST_TIME = time.time()


async def fetch_init_data(session_str: str) -> str:
    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()
    try:
        bot_peer = await client.get_input_entity(BOT_USERNAME)
        web_view = await client(RequestWebViewRequest(
            peer=bot_peer,
            bot=bot_peer,
            platform="android",
            from_bot_menu=False,
            url="https://atfminers.asloni.online/miner/"
        ))
        match = re.search(r"#tgWebAppData=([^&]+)", web_view.url)
        if match:
            return urllib.parse.unquote(match.group(1))

        parsed_url = urllib.parse.urlparse(web_view.url)
        params = urllib.parse.parse_qs(parsed_url.fragment)
        return params.get("tgWebAppData", [""])[0]
    finally:
        await client.disconnect()


async def api_post(session: aiohttp.ClientSession, action: str, payload: dict, headers: dict) -> tuple[int, dict]:
    await wait_for_account_gap()
    url = f"{BASE_URL}?action={action}&t={int(time.time() * 1000)}"
    try:
        async with session.post(
            url,
            data=json.dumps(payload),
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            status_code = resp.status
            data = await resp.json(content_type=None)
            return status_code, (data if isinstance(data, dict) else {})
    except Exception as e:
        return 0, {"error": str(e)}


async def boost_worker(acc: dict, worker_index: int):
    # شروع پلکانی اکانت‌ها برای طبیعی بودن ترافیک
    await asyncio.sleep(worker_index * random.uniform(2.5, 4.5))

    acc_name = acc.get("name", f"Account {worker_index + 1}")
    device_id = acc.get("device_id", f"dev-{uuid.uuid4()}")
    client_instance = generate_client_instance()

    async with aiohttp.ClientSession() as session:
        # شبیه‌سازی باز کردن اولیه صفحه مینی‌اپ
        try:
            async with session.get(BASE_URL, headers=get_account_headers(acc), timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await resp.text()
            await asyncio.sleep(random.uniform(1.0, 2.5))
        except Exception:
            pass

        while True:
            try:
                init_data = await fetch_init_data(acc["session"])
                if not init_data:
                    print(f"[{acc_name}] Failed to fetch initData. Retrying in 20s...", flush=True)
                    await asyncio.sleep(20)
                    continue

                extracted = extract_user_from_init_data(init_data)
                tg_id = extracted["tg_id"]
                username = extracted["username"]

                # ۱. لاگین و دریافت توکن سشن (tma_session_token)
                login_headers = get_account_headers(acc, init_data=init_data)
                login_payload = {
                    "initData": init_data,
                    "request_id": str(uuid.uuid4()),
                    "device_id": device_id
                }
                if tg_id:
                    login_payload["tg_id"] = tg_id
                if username:
                    login_payload["username"] = username

                status_code, login_data = await api_post(session, "login", login_payload, login_headers)
                if status_code != 200 or login_data.get("status") != "success":
                    print(f"[{acc_name}] Login failed (HTTP {status_code}). Retrying in 15s...", flush=True)
                    await asyncio.sleep(15)
                    continue

                user_info = login_data.get("user", {})
                tg_id = str(user_info.get("tg_id") or tg_id)
                tma_session_token = login_data.get("tma_session_token", "")
                miner_level = user_info.get("miner_level", "?")
                pending_reward = float(user_info.get("pending_reward", 0.0))

                print(
                    f"[{acc_name}] Session Active | Level: {miner_level} | "
                    f"Pending: {pending_reward:.4f} | SessionToken: {'OK' if tma_session_token else 'NONE'}",
                    flush=True
                )

                auth_headers = get_account_headers(
                    acc,
                    init_data=init_data,
                    tma_session_token=tma_session_token
                )

                session_start = time.time()
                need_state_sync = True

                session_nonce = ""
                seq = 1
                energy = 0.0
                max_energy = 1000.0
                regen_per_sec = 1.0
                energy_cost = 1.5
                max_taps_per_sec = 8
                last_energy_update = time.time()

                # هر ۴۵ دقیقه یک‌بار initData تلگرام رفرش می‌شود
                while time.time() - session_start < random.randint(2500, 2850):

                    # ۲. دریافت وضعیت انرژی و شمارنده اختصاصی اکانت از tap_v2_state
                    if need_state_sync:
                        state_payload = {
                            "initData": init_data,
                            "request_id": str(uuid.uuid4()),
                            "device_id": device_id,
                            "tg_id": str(tg_id),
                            "client_instance": client_instance
                        }
                        st_code, st_data = await api_post(session, "tap_v2_state", state_payload, auth_headers)

                        if st_code != 200 or st_data.get("status") != "success":
                            print(f"[{acc_name}] tap_v2_state failed (HTTP {st_code}): {st_data}. Re-logging in 12s...", flush=True)
                            await asyncio.sleep(12)
                            break

                        if st_data.get("tap_locked"):
                            retry_after = int(st_data.get("retry_after") or 60)
                            print(f"[{acc_name}] Tap locked on state check! Sleeping {retry_after + 5}s...", flush=True)
                            await asyncio.sleep(retry_after + 5)
                            continue

                        session_nonce = str(st_data.get("session_nonce") or "")
                        seq = int(st_data.get("last_seq") or 0) + 1
                        energy = float(st_data.get("energy") or 0.0)
                        max_energy = float(st_data.get("max_energy") or 400.0)
                        regen_per_sec = float(st_data.get("regen_per_sec") or 1.0)
                        energy_cost = max(0.25, float(st_data.get("energy_cost") or 1.5))
                        max_taps_per_sec = int(st_data.get("max_taps_per_sec") or 8)
                        blocked_until = int(st_data.get("boost_finishing_until") or 0)
                        last_energy_update = time.time()
                        need_state_sync = False

                        print(
                            f"[{acc_name}] State Synced -> Energy: {energy:.0f}/{max_energy:.0f} | "
                            f"Cost: {energy_cost} | Regen: {regen_per_sec:.2f}/s | Seq: {seq}",
                            flush=True
                        )

                        now_sec = int(time.time())
                        if blocked_until > now_sec:
                            wait_blk = blocked_until - now_sec + 2
                            await asyncio.sleep(wait_blk)

                    # محاسبه انرژی شارژ شده
                    now = time.time()
                    energy = min(max_energy, energy + (max(0.0, now - last_energy_update) * regen_per_sec))
                    last_energy_update = now

                    max_affordable_taps = int(energy // energy_cost)
                    min_batch_threshold = min(25, int(max_energy // energy_cost))

                    # ۳. استراحت هوشمند تا پر شدن مخزن انرژی (بدون هدر رفتن حتی ۱ واحد انرژی)
                    if max_affordable_taps < max(5, min_batch_threshold):
                        target_energy = max_energy * random.uniform(0.78, 0.94)
                        needed_energy = max(0.0, target_energy - energy)
                        recharge_seconds = (needed_energy / regen_per_sec) if regen_per_sec > 0 else 300.0
                        recharge_seconds = min(max(recharge_seconds, 30.0), 950.0)

                        print(
                            f"[{acc_name}] Energy depleted ({energy:.0f}/{max_energy:.0f}). "
                            f"Sleeping {recharge_seconds:.0f}s to refill...",
                            flush=True
                        )
                        await asyncio.sleep(recharge_seconds)
                        need_state_sync = True
                        continue

                    # ۴. جمع کردن تپ‌ها در بازه ۹ تا ۱۴ ثانیه‌ای با سرعت ایمن انسانی
                    batch_window_sec = random.uniform(9.0, 14.0)
                    await asyncio.sleep(batch_window_sec)

                    now = time.time()
                    energy = min(max_energy, energy + (max(0.0, now - last_energy_update) * regen_per_sec))
                    last_energy_update = now
                    max_affordable_taps = int(energy // energy_cost)

                    safe_rate = random.uniform(3.6, min(5.6, float(max_taps_per_sec) - 1.5))
                    desired_taps = int(batch_window_sec * safe_rate)
                    taps_to_send = max(1, min(desired_taps, max_affordable_taps, 120))

                    # ۵. ارسال بسته تپ‌ها به سرور (tap_v2_sync)
                    sync_payload = {
                        "initData": init_data,
                        "request_id": str(uuid.uuid4()),
                        "device_id": device_id,
                        "tg_id": str(tg_id),
                        "client_instance": client_instance,
                        "session_nonce": session_nonce,
                        "seq": seq,
                        "batch_id": generate_batch_id(),
                        "taps": taps_to_send
                    }

                    sync_code, sync_data = await api_post(session, "tap_v2_sync", sync_payload, auth_headers)

                    if sync_code == 429:
                        print(f"[{acc_name}] Rate limited (429)! Pausing 10s...", flush=True)
                        await asyncio.sleep(10)
                        continue

                    if sync_code != 200 or sync_data.get("status") != "success":
                        reason = sync_data.get("reason", "")
                        if reason == "tap_autoclick_locked":
                            retry_after = int(sync_data.get("retry_after") or 60)
                            lock_lvl = sync_data.get("lock_level", "?")
                            print(
                                f"[{acc_name}] Autoclick lock triggered (lvl {lock_lvl})! "
                                f"Sleeping {retry_after + 10}s...",
                                flush=True
                            )
                            await asyncio.sleep(retry_after + 10)
                            need_state_sync = True
                            continue

                        print(f"[{acc_name}] Sync rejected ({sync_data}). Resyncing state in 6s...", flush=True)
                        need_state_sync = True
                        await asyncio.sleep(6)
                        continue

                    # ۶. آپدیت وضعیت از پاسخ سرور
                    accepted = sync_data.get("accepted", taps_to_send)
                    rejected = sync_data.get("rejected", 0)
                    reward = float(sync_data.get("reward") or 0.0)
                    pending_reward = float(sync_data.get("pending_reward") or pending_reward)

                    seq = int(sync_data.get("next_seq") or (seq + 1))
                    energy = float(sync_data.get("energy") or 0.0)
                    max_energy = float(sync_data.get("max_energy") or max_energy)
                    regen_per_sec = float(sync_data.get("regen_per_sec") or regen_per_sec)
                    energy_cost = max(0.25, float(sync_data.get("energy_cost") or energy_cost))
                    last_energy_update = time.time()

                    print(
                        f"[{acc_name}] Tap Batch OK: +{accepted} taps (Rej:{rejected}) | "
                        f"+{reward:.4f} ATF (Pending: {pending_reward:.4f}) | "
                        f"Energy: {energy:.0f}/{max_energy:.0f}",
                        flush=True
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[{acc_name}] Worker error: {e}. Retrying in 10s...", flush=True)
                await asyncio.sleep(10)


async def main():
    if not ACCOUNTS:
        print("No accounts found in ACCOUNTS_JSON environment variable or accounts.json file.", flush=True)
        return

    start_time = time.time()

    print("==================================================", flush=True)
    print(">>> ATF Tap V2 Auto-Clicker (Cloud Runner)", flush=True)
    print(f">>> Running {len(ACCOUNTS)} Accounts Concurrently", flush=True)
    print(">>> Scheduled Auto-Stop: 5 Hours and 50 Minutes", flush=True)
    print("==================================================", flush=True)

    tasks = [asyncio.create_task(boost_worker(acc, idx)) for idx, acc in enumerate(ACCOUNTS)]

    try:
        while time.time() - start_time < MAX_RUN_SECONDS:
            await asyncio.sleep(30)
    finally:
        print("\n[SYSTEM] 5 hours and 50 minutes reached. Stopping bot gracefully...", flush=True)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOPPED] Auto-clicker stopped gracefully.", flush=True)