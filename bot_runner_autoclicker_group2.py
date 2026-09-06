# ==============================================================================
# ATF BOOST AUTO-CLICKER — نسخه ابری (GitHub Actions + Cloudflare Worker)
# ==============================================================================
# وضعیت اجرا:
#   - اجرا روی رانرهای گیت‌هاب (GitHub Actions Runner) با ترایگر کران‌جاب خارجی.
#   - مسیریابی درخواست‌ها از طریق پراکسی معکوس Cloudflare Worker جهت پنهان‌سازی IP
#     دیتاسنتری گیت‌هاب و تطبیق ساختار هدرها با شبکه داخلی کلودفلر سرور مقصد.
#
# ویژگی‌های عملکردی و تفاوت‌ها با نسخه لوکال:
#   - دریافت لیست اکانت‌ها به صورت ایمن از طریق GitHub Secrets (متغیر ACCOUNTS_JSON).
#   - تایمر هوشمند خروج اضطراری (MAX_RUN_SECONDS): توقف تمیز برنامه در دقیقه ۳۵۰
#     (۵ ساعت و ۵۰ دقیقه) جهت پیشگیری از خطای Kill شدن اجباری سرور گیت‌هاب (سقف ۶ ساعته).
#   - سازگار با اجرای چرخشی ۲۴ ساعته در کنار جاب‌های دوره‌ای Cron-Job.org.
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

# پیدا کردن دقیق فایل .env در کنار خود فایل پایتون
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

API_ID = int(os.getenv("TG_API_ID") or 0)
API_HASH = os.getenv("TG_API_HASH", "")
BOT_USERNAME = "ATF_AIRDROP_bot"

BASE_URL = "https://atf-bot-runner-autoclicker-group2.alibotrunner2.workers.dev/miner/index.php"

# سقف زمان اجرا: ۵ ساعت و ۵۵ دقیقه (به ثانیه)
MAX_RUN_SECONDS = (5 * 3600) + (55 * 60)

# ۱. اولویت اول: خواندن از سکرت گیت‌هاب (برای سرور)
# ۲. اولویت دوم: خواندن از فایل محلی accounts.json (برای لپ‌تاپ و ترموکس)
RAW_ACCOUNTS = os.getenv("ACCOUNTS_JSON")
if RAW_ACCOUNTS:
    try:
        ACCOUNTS = json.loads(RAW_ACCOUNTS)
    except Exception as e:
        print(f"Error parsing ACCOUNTS_JSON from env: {e}")
        ACCOUNTS = []
elif os.path.exists("accounts.json"):
    try:
        with open("accounts.json", "r", encoding="utf-8") as f:
            ACCOUNTS = json.load(f)
    except Exception as e:
        print(f"Error reading local accounts.json: {e}")
        ACCOUNTS = []
else:
    ACCOUNTS = []

def get_account_headers(acc):
    ua = acc.get("user_agent") or "Mozilla/5.0 (Linux; Android 14; SM-S928B Build/UP1A.231005.007; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/128.0.6613.88 Mobile Safari/537.36"
    match = re.search(r"Chrome/(\d+)", ua)
    chrome_ver = match.group(1) if match else "128"
    
    return {
        "User-Agent": ua,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-CH-UA": f'"Chromium";v="{chrome_ver}", "Not;A=Brand";v="24", "Android WebView";v="{chrome_ver}"',
        "Sec-CH-UA-Mobile": "?1",
        "Sec-CH-UA-Platform": '"Android"',
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://atfminers.asloni.online",
        "Referer": "https://atfminers.asloni.online/miner/index.php"
    }

# مدیریت صف و تنظیم فاصله 0.8 تا 1.2 ثانیه بین هر دو اکانت متوالی
QUEUE_LOCK = asyncio.Lock()
LAST_REQUEST_TIME = 0.0
GLOBAL_PAUSE_UNTIL = 0.0
COOLDOWN_DELAY = 9.2

async def wait_for_account_gap():
    global LAST_REQUEST_TIME, GLOBAL_PAUSE_UNTIL
    async with QUEUE_LOCK:
        now = time.time()
        
        if now < GLOBAL_PAUSE_UNTIL:
            wait_time = GLOBAL_PAUSE_UNTIL - now
            await asyncio.sleep(wait_time)
            now = time.time()

        num_accounts = len(ACCOUNTS) if len(ACCOUNTS) > 0 else 1
        calculated_gap = COOLDOWN_DELAY / num_accounts
        target_gap = max(calculated_gap, 1.3)
        
        elapsed = now - LAST_REQUEST_TIME
        if elapsed < target_gap:
            await asyncio.sleep(target_gap - elapsed)
        LAST_REQUEST_TIME = time.time()

async def fetch_init_data(session_str):
    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()
    bot_peer = await client.get_input_entity(BOT_USERNAME)

    web_view = await client(RequestWebViewRequest(
        peer=bot_peer,
        bot=bot_peer,
        platform="android",
        from_bot_menu=False,
        url="https://atfminers.asloni.online/miner/"
    ))
    await client.disconnect()

    match = re.search(r"#tgWebAppData=([^&]+)", web_view.url)
    if match:
        return urllib.parse.unquote(match.group(1))

    parsed_url = urllib.parse.urlparse(web_view.url)
    params = urllib.parse.parse_qs(parsed_url.fragment)
    return params.get("tgWebAppData", [""])[0]

async def boost_worker(acc):
    global GLOBAL_PAUSE_UNTIL
    acc_name = acc.get("name", "Account")
    device_id = acc.get("device_id", "")
    acc_headers = get_account_headers(acc)

    async with aiohttp.ClientSession(headers=acc_headers) as session:
        # شبیه‌سازی لود اولیه صفحه مینی‌اپ در مرورگر
        try:
            async with session.get(f"{BASE_URL}", timeout=aiohttp.ClientTimeout(total=10)) as landing_resp:
                await landing_resp.text()
            await asyncio.sleep(random.uniform(1.0, 2.5))
        except Exception:
            pass

        while True:
            try:
                init_data = await fetch_init_data(acc["session"])

                login_payload = {
                    "initData": init_data,
                    "device_id": device_id,
                    "request_id": str(uuid.uuid4())
                }

                # لاگین با حفظ فاصله نسبت به اکانت قبلی
                await wait_for_account_gap()
                async with session.post(f"{BASE_URL}?action=login&t={int(time.time()*1000)}", data=login_payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    login_json = await resp.json(content_type=None)
                    if not login_json or login_json.get("status") != "success":
                        print(f"[{acc_name}] Login failed. Retrying in 10s...")
                        await asyncio.sleep(10)
                        continue

                    user_info = login_json.get("user", {})
                    tg_id = user_info.get("tg_id")
                    current_preview = float(user_info.get("pending_reward", 1.0))
                    print(f"[{acc_name}] Session Active | Synced Preview: {current_preview:.4f}")

                session_start = time.time()
                last_break_time = time.time()
                last_tap_time = time.time()

                while time.time() - session_start < 2700:
                    time_since_last_tap = time.time() - last_tap_time
                    if time_since_last_tap < COOLDOWN_DELAY:
                        await asyncio.sleep(COOLDOWN_DELAY - time_since_last_tap)

                    current_preview += round(random.uniform(0.002, 0.008), 4)

                    boost_payload = {
                        "device_id": device_id,
                        "display_preview": f"{current_preview:.4f}",
                        "initData": init_data,
                        "request_id": str(uuid.uuid4()),
                        "tg_id": tg_id
                    }

                    # تضمین رعایت فاصله قبل از شلیک هر رکوئست بوست
                    await wait_for_account_gap()
                    async with session.post(f"{BASE_URL}?action=activate_boost&t={int(time.time()*1000)}", data=boost_payload, timeout=aiohttp.ClientTimeout(total=10)) as b_resp:
                        if b_resp.status == 200:
                            last_tap_time = time.time()
                            try:
                                b_json = await b_resp.json(content_type=None)
                                if "pending_reward" in b_json:
                                    current_preview = float(b_json["pending_reward"])
                            except Exception:
                                pass
                            print(f"[{acc_name}] Tap Triggered -> Boost Active")
                        elif b_resp.status == 429:
                            pause_duration = 1.5
                            print(f"[{acc_name}] Rate limited (429)! Pausing for {pause_duration:.1f}s...")
                            await asyncio.sleep(pause_duration)
                            continue
                        else:
                            print(f"[{acc_name}] Boost HTTP status: {b_resp.status}")

                    current_time = time.time()

                    # وقفه کوتاه خستگی
                    next_break_interval = random.randint(900, 1200)
                    if current_time - last_break_time > next_break_interval:
                        micro_break = random.uniform(20.0, 40.0)
                        print(f"[{acc_name}] Human short break: resting for {micro_break:.1f}s...")
                        await asyncio.sleep(micro_break)
                        last_break_time = time.time()

                    # استراحت پایان سشن
                    elif (current_time - session_start) > random.randint(3600, 4800):
                        long_break = random.uniform(90.0, 120.0)
                        print(f"[{acc_name}] Session fatigue break: resting for {long_break:.1f}s...")
                        await asyncio.sleep(long_break)
                        session_start = time.time()
                        last_break_time = time.time()

            except Exception as e:
                print(f"[{acc_name}] Worker error: {e}. Retrying in 5s...")
                await asyncio.sleep(5)

async def main():
    if not ACCOUNTS:
        print("No accounts found in ACCOUNTS_JSON environment variable or accounts.json file.")
        return

    start_time = time.time()

    print("==================================================")
    print(">>> ATF Boost Auto-Clicker (Optimized Delays)")
    print(f">>> Running {len(ACCOUNTS)} Accounts Concurrently")
    print(f">>> Scheduled Auto-Stop: 5 Hours and 55 Minutes")
    print(">>> Press Ctrl + C to stop.")
    print("==================================================")

    tasks = [boost_worker(acc) for acc in ACCOUNTS]
    worker_task = asyncio.gather(*tasks)

    # حلقه بررسی گذر زمان تا رسیدن به سقف ۵ ساعت و ۵۰ دقیقه
    while time.time() - start_time < MAX_RUN_SECONDS:
        await asyncio.sleep(30)

    print("\n[SYSTEM] 5 hours and 55 minutes reached. Stopping bot gracefully...")
    worker_task.cancel()
    sys.exit(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOPPED] Auto-clicker stopped gracefully.")