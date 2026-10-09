import json
import time
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import websocket


# ============================================================
# CONFIGURATION
# ============================================================

BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams="

TELEGRAM_BOT_TOKEN = "8759728528:AAEsBF7X6d1wU8VBpD6iqt0c7SVGjU9dqGk"
TELEGRAM_CHAT_ID = "6949295630"

TIMEFRAMES = ["1h", "4h", "1d"]

MIN_PRICE = 0.000002
MAX_PRICE = 0.3000

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

KLINE_LIMIT = 200

# Breakout filter
ENABLE_BREAKOUT_FILTER = True
BREAKOUT_LOOKBACK_BARS = 5

# Volume filter
ENABLE_VOLUME_FILTER = True
VOLUME_MULTIPLIER = 1.3
VOLUME_LOOKBACK_BARS = 20

# Near-cross alert tolerance
TOUCH_DISTANCE_PCT = 0.00005

SYMBOLS_PER_CONNECTION = 50

REQUEST_TIMEOUT = 15
RECONNECT_DELAY = 3

EXCLUDED_KEYWORDS = ("UP", "DOWN", "BULL", "BEAR")


# ============================================================
# GLOBAL STATE
# ============================================================

http = requests.Session()

state_lock = threading.RLock()
telegram_lock = threading.Lock()

states = {}
early_alerted = set()
confirmed_alerted = set()
touch_alerted = set()

last_prices = {}
executor = ThreadPoolExecutor(max_workers=12)


# ============================================================
# HTTP AND TELEGRAM
# ============================================================

def http_get(url, params=None):
    response = http.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    return response.json()


def send_telegram(message):
    if (
        not TELEGRAM_BOT_TOKEN
        or TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN"
        or not TELEGRAM_CHAT_ID
        or TELEGRAM_CHAT_ID == "YOUR_CHAT_ID"
    ):
        print("[CONFIG ERROR] Set Telegram token and chat ID.")
        return False

    url = (
        "https://api.telegram.org/bot"
        + TELEGRAM_BOT_TOKEN
        + "/sendMessage"
    )

    try:
        with telegram_lock:
            response = http.post(
                url,
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message
                },
                timeout=REQUEST_TIMEOUT
            )

        if response.status_code != 200:
            print("[TELEGRAM ERROR]", response.text)
            return False

        result = response.json()

        if not result.get("ok"):
            print("[TELEGRAM ERROR]", result)
            return False

        print("[TELEGRAM SENT]", message.splitlines()[0])
        return True

    except Exception as exc:
        print("[TELEGRAM ERROR]", exc)
        return False


# ============================================================
# BINANCE SYMBOLS
# ============================================================

def load_symbols():
    data = http_get(
        BINANCE_BASE_URL + "/api/v3/exchangeInfo"
    )

    result = []

    for item in data.get("symbols", []):
        symbol = item.get("symbol", "")

        if item.get("status") != "TRADING":
            continue

        if item.get("quoteAsset") != "USDT":
            continue

        if not item.get("isSpotTradingAllowed", False):
            continue

        if any(word in symbol for word in EXCLUDED_KEYWORDS):
            continue

        result.append(symbol)

    return sorted(set(result))


def get_historical_klines(symbol, interval):
    return http_get(
        BINANCE_BASE_URL + "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": KLINE_LIMIT
        }
    )


def parse_closed_bars(klines, current_open_time):
    bars = []

    for k in klines:
        if int(k[0]) >= current_open_time:
            continue

        bars.append({
            "open_time": int(k[0]),
            "close": float(k[4]),
            "high": float(k[2]),
            "low": float(k[3]),
            "volume": float(k[5])
        })

    return bars[-KLINE_LIMIT:]


# ============================================================
# MACD
# ============================================================

def calculate_macd(closes):
    if len(closes) < MACD_SLOW + MACD_SIGNAL + 5:
        return None

    series = pd.Series(closes, dtype="float64")

    fast = series.ewm(
        span=MACD_FAST,
        adjust=False
    ).mean()

    slow = series.ewm(
        span=MACD_SLOW,
        adjust=False
    ).mean()

    dif = fast - slow

    dea = dif.ewm(
        span=MACD_SIGNAL,
        adjust=False
    ).mean()

    return float(dif.iloc[-1]), float(dea.iloc[-1])


def get_cross(previous_dif, previous_dea, dif, dea):
    if previous_dif <= previous_dea and dif > dea:
        return "BULLISH"

    if previous_dif >= previous_dea and dif < dea:
        return "BEARISH"

    return None


# ============================================================
# BREAKOUT AND VOLUME FILTERS
# ============================================================

def passes_breakout_filter(direction, price, closed_bars):
    if not ENABLE_BREAKOUT_FILTER:
        return True

    count = BREAKOUT_LOOKBACK_BARS

    if count < 1 or len(closed_bars) < count:
        return False

    previous_bars = closed_bars[-count:]

    if direction == "BULLISH":
        previous_high = max(
            bar["high"] for bar in previous_bars
        )
        return price > previous_high

    if direction == "BEARISH":
        previous_low = min(
            bar["low"] for bar in previous_bars
        )
        return price < previous_low

    return False


def passes_volume_filter(current_volume, closed_bars):
    if not ENABLE_VOLUME_FILTER:
        return True

    count = VOLUME_LOOKBACK_BARS

    if count < 1 or len(closed_bars) < count:
        return False

    previous_volumes = [
        bar["volume"]
        for bar in closed_bars[-count:]
    ]

    average_volume = sum(previous_volumes) / count

    if average_volume <= 0:
        return False

    return current_volume >= average_volume * VOLUME_MULTIPLIER


def passes_all_filters(direction, price, current_volume, closed_bars):
    breakout_ok = passes_breakout_filter(
        direction,
        price,
        closed_bars
    )

    volume_ok = passes_volume_filter(
        current_volume,
        closed_bars
    )

    return breakout_ok and volume_ok


# ============================================================
# ALERT MESSAGE
# ============================================================

def candle_time_text(open_time):
    return datetime.fromtimestamp(
        open_time / 1000,
        tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M UTC")


def alert_message(kind, symbol, interval, price, direction,
                  open_time):

    if direction == "BULLISH":
        emoji = "🟢"
    else:
        emoji = "🔴"

    if kind == "EARLY":
        title = f"{emoji} MACD EARLY {direction} CROSS"
    elif kind == "CONFIRMED":
        title = f"✅ MACD CONFIRMED {direction} CROSS"
    else:
        title = f"🟡 MACD {direction} NEAR CROSS"

    base_asset = symbol[:-4]
    binance_url = (
        f"https://www.binance.com/en/trade/"
        f"{base_asset}_USDT?type=spot"
    )

    return (
        f"{title}\n\n"
        f"Coin: {symbol}\n"
        f"Timeframe: {interval}\n"
        f"Price: {price:.8f}\n"
        f"Candle: {candle_time_text(open_time)}\n"
        f"Direction: {direction}\n\n"
        f"Binance Spot:\n{binance_url}"
    )


def send_once(alert_set, key, message):
    with state_lock:
        if key in alert_set:
            return

        alert_set.add(key)

    if not send_telegram(message):
        with state_lock:
            alert_set.discard(key)


# ============================================================
# STATE INITIALIZATION
# ============================================================

def initialize_state(symbol, interval, kline):
    open_time = int(kline["t"])

    historical = get_historical_klines(
        symbol,
        interval
    )

    closed_bars = parse_closed_bars(
        historical,
        open_time
    )

    closes = [bar["close"] for bar in closed_bars]
    baseline = calculate_macd(closes)

    if baseline is None:
        return None

    return {
        "closed_bars": closed_bars,
        "open_time": open_time,
        "live_price": float(kline["c"]),
        "live_high": float(kline["h"]),
        "live_low": float(kline["l"]),
        "live_volume": float(kline["v"]),
        "dif": baseline[0],
        "dea": baseline[1],
        "baseline_dif": baseline[0],
        "baseline_dea": baseline[1],
        "touch_sent": False,
        "finalized": False
    }


# ============================================================
# KLINE PROCESSOR
# ============================================================

def process_kline(symbol, interval, kline):
    open_time = int(kline["t"])
    price = float(kline["c"])
    high = float(kline["h"])
    low = float(kline["l"])
    volume = float(kline["v"])
    is_closed = bool(kline["x"])

    with state_lock:
        last_prices[symbol] = price

        if not MIN_PRICE <= price <= MAX_PRICE:
            return

        key = (symbol, interval)
        current = states.get(key)

    if current is None:
        try:
            new_state = initialize_state(
                symbol,
                interval,
                kline
            )
        except Exception as exc:
            print("[INIT ERROR]", symbol, interval, exc)
            return

        if new_state is None:
            return

        with state_lock:
            states.setdefault(key, new_state)

        return

    with state_lock:
        current = states.get(key)

        if current is None:
            return

        if open_time < current["open_time"]:
            return

        if open_time > current["open_time"]:
            if not current["finalized"]:
                current["closed_bars"].append({
                    "open_time": current["open_time"],
                    "close": current["live_price"],
                    "high": current["live_high"],
                    "low": current["live_low"],
                    "volume": current["live_volume"]
                })

                current["closed_bars"] = (
                    current["closed_bars"][-KLINE_LIMIT:]
                )

            closed_bars = current["closed_bars"]

            baseline = calculate_macd([
                bar["close"] for bar in closed_bars
            ])

            if baseline is None:
                states.pop(key, None)
                return

            current = {
                "closed_bars": closed_bars,
                "open_time": open_time,
                "live_price": price,
                "live_high": high,
                "live_low": low,
                "live_volume": volume,
                "dif": baseline[0],
                "dea": baseline[1],
                "baseline_dif": baseline[0],
                "baseline_dea": baseline[1],
                "touch_sent": False,
                "finalized": False
            }

            states[key] = current
            return

        if current["finalized"]:
            return

        closed_bars = current["closed_bars"]

        values = calculate_macd(
            [bar["close"] for bar in closed_bars] + [price]
        )

        if values is None:
            return

        dif, dea = values

        previous_dif = current["dif"]
        previous_dea = current["dea"]

        direction = get_cross(
            previous_dif,
            previous_dea,
            dif,
            dea
        )

        candle = current["open_time"]

        current["dif"] = dif
        current["dea"] = dea
        current["live_price"] = price
        current["live_high"] = high
        current["live_low"] = low
        current["live_volume"] = volume

        # Early signal: only before candle close.
        if direction and not is_closed:
            filters_ok = passes_all_filters(
                direction,
                price,
                volume,
                closed_bars
            )

            if filters_ok:
                alert_key = (
                    symbol,
                    interval,
                    candle,
                    direction
                )

                message = alert_message(
                    "EARLY",
                    symbol,
                    interval,
                    price,
                    direction,
                    candle
                )

                executor.submit(
                    send_once,
                    early_alerted,
                    alert_key,
                    message
                )

        # Near-cross signal.
        distance = abs(dif - dea)
        tolerance = max(
            abs(price) * TOUCH_DISTANCE_PCT,
            1e-12
        )

        if (
            not current["touch_sent"]
            and distance <= tolerance
        ):
            touch_direction = (
                "BULLISH" if dif <= dea else "BEARISH"
            )

            filters_ok = passes_all_filters(
                touch_direction,
                price,
                volume,
                closed_bars
            )

            if filters_ok:
                current["touch_sent"] = True

                touch_key = (
                    symbol,
                    interval,
                    candle
                )

                message = alert_message(
                    "TOUCH",
                    symbol,
                    interval,
                    price,
                    touch_direction,
                    candle
                )

                executor.submit(
                    send_once,
                    touch_alerted,
                    touch_key,
                    message
                )

        # Confirmed signal: candle has closed.
        if is_closed:
            confirmed_direction = get_cross(
                current["baseline_dif"],
                current["baseline_dea"],
                dif,
                dea
            )

            if confirmed_direction:
                filters_ok = passes_all_filters(
                    confirmed_direction,
                    price,
                    volume,
                    closed_bars
                )

                if filters_ok:
                    confirm_key = (
                        symbol,
                        interval,
                        candle,
                        confirmed_direction
                    )

                    message = alert_message(
                        "CONFIRMED",
                        symbol,
                        interval,
                        price,
                        confirmed_direction,
                        candle
                    )

                    executor.submit(
                        send_once,
                        confirmed_alerted,
                        confirm_key,
                        message
                    )

            closed_bars.append({
                "open_time": candle,
                "close": price,
                "high": high,
                "low": low,
                "volume": volume
            })

            current["closed_bars"] = (
                closed_bars[-KLINE_LIMIT:]
            )
            current["finalized"] = True


# ============================================================
# WEBSOCKET
# ============================================================

def on_message(ws, message):
    try:
        payload = json.loads(message)
        data = payload.get("data", {})

        if data.get("e") != "kline":
            return

        kline = data.get("k", {})
        symbol = data.get("s", "")
        interval = kline.get("i", "")

        if interval not in TIMEFRAMES:
            return

        process_kline(symbol, interval, kline)

    except Exception as exc:
        print("[MESSAGE ERROR]", exc)


def on_error(ws, error):
    print("[WEBSOCKET ERROR]", error)


def on_close(ws, code, message):
    print("[WEBSOCKET CLOSED]", code, message)


def websocket_worker(streams):
    url = BINANCE_WS_URL + "/".join(streams)

    while True:
        try:
            app = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            app.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as exc:
            print("[CONNECTION ERROR]", exc)

        print(
            f"[INFO] Reconnecting in {RECONNECT_DELAY}s"
        )
        time.sleep(RECONNECT_DELAY)


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("BINANCE SPOT MACD LIVE SCANNER")
    print("=" * 60)
    print("Timeframes:", ", ".join(TIMEFRAMES))
    print("Price range:", MIN_PRICE, "-", MAX_PRICE)
    print(
        "MACD:",
        MACD_FAST,
        MACD_SLOW,
        MACD_SIGNAL
    )
    print("Breakout filter:", ENABLE_BREAKOUT_FILTER)
    print("Breakout lookback:", BREAKOUT_LOOKBACK_BARS)
    print("Volume filter:", ENABLE_VOLUME_FILTER)
    print("Volume multiplier:", VOLUME_MULTIPLIER)
    print("Volume lookback:", VOLUME_LOOKBACK_BARS)
    print("=" * 60)

    try:
        symbols = load_symbols()
    except Exception as exc:
        print("[FATAL] Cannot load Binance symbols:", exc)
        return

    print(
        f"[INFO] Loaded {len(symbols)} active USDT Spot pairs."
    )

    streams = []

    for symbol in symbols:
        for interval in TIMEFRAMES:
            streams.append(
                f"{symbol.lower()}@kline_{interval}"
            )

    streams_per_connection = (
        SYMBOLS_PER_CONNECTION * len(TIMEFRAMES)
    )

    groups = [
        streams[i:i + streams_per_connection]
        for i in range(0, len(streams), streams_per_connection)
    ]

    print(f"[INFO] WebSocket connections: {len(groups)}")

    for group in groups:
        threading.Thread(
            target=websocket_worker,
            args=(group,),
            daemon=True
        ).start()

    try:
        while True:
            time.sleep(60)

            with state_lock:
                tracked = len(states)

            print(
                f"[HEARTBEAT] Tracked: {tracked} | "
                f"UTC: {datetime.now(timezone.utc).strftime('%H:%M:%S')}"
            )

    except KeyboardInterrupt:
        print("[INFO] Scanner stopped.")


if __name__ == "__main__":
    main()
