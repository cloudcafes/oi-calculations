"""
nifty_notify.py
===============
Called by fetch-snapshot.py after every cron run.

Workflow:
  1. Reads latest-calculations.txt  (written by fetch-snapshot.py)
  2. Reads ai-query.txt              (your standing prompt / instructions for the AI)
  3. Combines both and sends to Gemini for analysis
  4. Writes ai-response.txt          (Gemini response stored to disk)
  5. Sends ai-response.txt content to Telegram

All credentials are hardcoded here as specified.
"""

import os
import datetime
import urllib3

import requests
from google import genai

# ==========================================
# CREDENTIALS  (hardcoded as instructed)
# ==========================================
GEMINI_API_KEY     = "AIzaSyBumWb-P3PGfc6efmFlc_qDTs61Abq1eYI"
GEMINI_MODEL       = "gemini-3.1-flash-lite-preview"          # stable public model; update when gemini-3.1-pro-preview goes GA
TELEGRAM_BOT_TOKEN = "8747682342:AAG5f--5bePDBGjTFQDw0B7rLNGZFNkzQU8"
TELEGRAM_CHAT_ID   = "-1003800058836"

# ==========================================
# FILE PATHS  (all in same directory as this script)
# ==========================================
_DIR              = os.path.dirname(os.path.abspath(__file__))
LATEST_CALC_FILE  = os.path.join(_DIR, "latest-calculations.txt")
AI_QUERY_FILE     = os.path.join(_DIR, "ai-query.txt")
AI_RESPONSE_FILE  = os.path.join(_DIR, "ai-response.txt")

# Suppress SSL warnings (matches existing codebase pattern)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==========================================
# TELEGRAM
# ==========================================

def send_telegram_message(text: str) -> bool:
    """
    Sends a message to Telegram.
    Splits automatically if the message exceeds 4,000 characters.
    """
    if not TELEGRAM_BOT_TOKEN or "YOUR_" in TELEGRAM_BOT_TOKEN:
        print("Telegram skipped: bot token not configured.")
        return False

    # Markdown cleanup to prevent Telegram 400 parse errors
    clean = text.replace('**', '*').replace('##', '')

    max_len = 4000

    if len(clean) <= max_len:
        return _send_chunk(clean)

    print(f"Message too long ({len(clean)} chars) — splitting into parts...")
    lines   = clean.split('\n')
    current = ""
    part    = 1
    success = True

    for line in lines:
        if len(current) + len(line) + 1 > max_len:
            if current:
                if not _send_chunk(f"Part {part}:\n\n{current}"):
                    success = False
                part   += 1
                current = line
        else:
            current = (current + "\n" + line) if current else line

    if current:
        if not _send_chunk(f"Part {part}:\n\n{current}"):
            success = False

    return success


def _send_chunk(text: str) -> bool:
    """Sends a single payload to the Telegram Bot API."""
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}

    try:
        resp = requests.post(url, json=payload, verify=False, timeout=15)
        if resp.status_code == 200:
            print("Telegram message sent.")
            return True
        else:
            print(f"Telegram API error {resp.status_code}: {resp.text}")
            return False
    except Exception as e:
        print(f"Telegram send failed: {e}")
        return False


# ==========================================
# FILE READERS
# ==========================================

def _read_file(path: str, label: str) -> str:
    """Reads a text file and returns its contents, or an empty string on error."""
    if not os.path.exists(path):
        print(f"{label} not found: {path}")
        return ""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading {label}: {e}")
        return ""


def _write_file(path: str, content: str) -> bool:
    """Writes content to a file. Returns True on success."""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"Error writing {path}: {e}")
        return False


# ==========================================
# GEMINI AI ANALYSIS
# ==========================================

def get_ai_analysis() -> str:
    """
    Combines latest-calculations.txt + ai-query.txt,
    sends to Gemini, writes ai-response.txt,
    and returns the response text.
    """

    # 1. Read inputs
    calc_content  = _read_file(LATEST_CALC_FILE, "latest-calculations.txt")
    query_content = _read_file(AI_QUERY_FILE,    "ai-query.txt")

    if not calc_content:
        msg = "AI analysis skipped: latest-calculations.txt is empty or missing."
        print(msg)
        return msg

    # 2. Combine into prompt
    # The query file provides the standing instructions / role for the AI.
    # The calculations file provides the current market snapshot.
    if query_content:
        combined_prompt = (
            f"{query_content.strip()}\n\n"
            f"{'=' * 60}\n"
            f"CURRENT MARKET SNAPSHOT DATA:\n"
            f"{'=' * 60}\n\n"
            f"{calc_content.strip()}"
        )
    else:
        # No query file — ask for a generic analysis
        combined_prompt = (
            "You are an expert Nifty options trader. "
            "Analyze the following market snapshot data and provide:\n"
            "1. Market direction assessment\n"
            "2. Key support and resistance levels\n"
            "3. Trade recommendation (CALL / PUT / NO TRADE) with reasoning\n"
            "4. Risk factors and things to watch\n"
            "5. Confidence level and probability of success\n\n"
            f"{'=' * 60}\n"
            f"MARKET SNAPSHOT:\n"
            f"{'=' * 60}\n\n"
            f"{calc_content.strip()}"
        )

    # 3. Call Gemini
    if not GEMINI_API_KEY or "YOUR_" in GEMINI_API_KEY:
        msg = "AI analysis skipped: Gemini API key not configured."
        print(msg)
        return msg

    ai_response_text = None
    used_model       = "None"

    try:
        print(f"Requesting analysis from Gemini ({GEMINI_MODEL})...")
        client   = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model    = GEMINI_MODEL,
            contents = [combined_prompt],
        )
        ai_response_text = response.text
        used_model       = GEMINI_MODEL
        print(f"Gemini response received ({len(ai_response_text)} chars).")

    except Exception as e:
        print(f"Gemini API call failed: {e}")
        ai_response_text = None

    if not ai_response_text:
        msg = "AI analysis failed — Gemini returned no response."
        print(msg)
        _write_file(AI_RESPONSE_FILE, msg)
        return msg

    # 4. Build full response document
    timestamp    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_content = (
        f"Model Used      : {used_model}\n"
        f"Generated At    : {timestamp}\n"
        f"{'=' * 60}\n"
        f"AI ANALYSIS\n"
        f"{'=' * 60}\n\n"
        f"{ai_response_text}"
    )

    # 5. Write ai-response.txt
    if _write_file(AI_RESPONSE_FILE, full_content):
        print(f"AI response written to: {AI_RESPONSE_FILE}")
    else:
        print("Warning: could not write ai-response.txt")

    return full_content


# ==========================================
# MAIN
# ==========================================

def main():
    print("\n" + "=" * 50)
    print("nifty_notify.py — Telegram + AI Notifier")
    print("=" * 50 + "\n")

    # Step 1: Get AI analysis (reads calc file + query file, calls Gemini)
    ai_response = get_ai_analysis()

    # Step 2: Send AI response to Telegram
    if ai_response:
        print("Sending AI analysis to Telegram...")
        send_telegram_message(ai_response)
    else:
        print("No AI response to send.")

    print("\nnifty_notify.py complete.\n")


if __name__ == "__main__":
    main()