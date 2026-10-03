import asyncio
import json
from collections import Counter, deque
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI(title="Open ProTrader Analysis Engine")

# Configuration & In-Memory Storage
DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"  # Default public app_id
TICK_HISTORY_SIZE = 100

# Store recent tick digits per symbol
tick_buffers = {
    "R_10": deque(maxlen=TICK_HISTORY_SIZE),
    "R_25": deque(maxlen=TICK_HISTORY_SIZE),
    "R_50": deque(maxlen=TICK_HISTORY_SIZE),
    "R_75": deque(maxlen=TICK_HISTORY_SIZE),
    "R_100": deque(maxlen=TICK_HISTORY_SIZE),
    "1HZ10V": deque(maxlen=TICK_HISTORY_SIZE),
    "1HZ100V": deque(maxlen=TICK_HISTORY_SIZE),
}

# --- Signal Algorithms ---

def analyze_digits(digits: deque) -> dict:
    """Processes digit history to calculate statistical probability metrics."""
    if len(digits) < 10:
        return {"status": "Gathering data..."}

    total = len(digits)
    counts = Counter(digits)
    
    # 1. Even / Odd Bias
    evens = sum(1 for d in digits if d % 2 == 0)
    odds = total - evens
    even_pct = round((evens / total) * 100, 1)
    odd_pct = round((odds / total) * 100, 1)
    
    # 2. Over / Under Ratios
    over_4 = sum(1 for d in digits if d > 4)
    under_5 = sum(1 for d in digits if d < 5)
    
    # 3. Last Digit Match (Most frequent digit)
    most_common_digit, most_common_count = counts.most_common(1)[0]
    
    # 4. Generate Signal Output
    even_odd_signal = "EVEN" if even_pct > 60 else ("ODD" if odd_pct > 60 else "NEUTRAL")
    over_under_signal = "OVER 4" if (over_4 / total) > 0.6 else ("UNDER 5" if (under_5 / total) > 0.6 else "NEUTRAL")

    return {
        "ticks_analyzed": total,
        "latest_digit": digits[-1],
        "even_odd_ratio": {"even_pct": even_pct, "odd_pct": odd_pct},
        "most_frequent_digit": {"digit": most_common_digit, "frequency_pct": round((most_common_count / total) * 100, 1)},
        "signals": {
            "even_odd": even_odd_signal,
            "over_under": over_under_signal,
            "match_bias": most_common_digit
        }
    }

# --- Deriv WebSocket Ingestion Loop ---

async def connect_deriv_stream():
    """Background task to stream ticks from Deriv for selected Volatility Indices."""
    symbols = list(tick_buffers.keys())
    
    while True:
        try:
            async with websockets.connect(DERIV_WS_URL) as ws:
                # Subscribe to ticks for each symbol
                for symbol in symbols:
                    req = {"ticks": symbol}
                    await ws.send(json.dumps(req))

                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("msg_type") == "tick":
                        tick = data.get("tick", {})
                        symbol = tick.get("symbol")
                        quote = str(tick.get("quote", ""))
                        
                        if symbol in tick_buffers and quote:
                            last_digit = int(quote[-1])
                            tick_buffers[symbol].append(last_digit)

        except Exception as e:
            print(f"Deriv WS Connection dropped: {e}. Reconnecting in 3 seconds...")
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(connect_deriv_stream())

# --- WebSocket Endpoint for Dashboard ---

@app.websocket("/ws/signals")
async def websocket_signals(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Broadcast metrics for all symbols every 500ms
            payload = {}
            for symbol, digits in tick_buffers.items():
                payload[symbol] = analyze_digits(digits)
            
            await websocket.send_json(payload)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass

# --- Front-End Single-Page UI ---

@app.get("/")
async def get_dashboard():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Open ProTrader Analysis Dashboard</title>
        <style>
            body { font-family: monospace; background: #0f172a; color: #f8fafc; padding: 20px; }
            h1 { color: #38bdf8; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 15px; }
            .card { background: #1e293b; padding: 15px; border-radius: 8px; border: 1px solid #334155; }
            .badge { padding: 4px 8px; border-radius: 4px; font-weight: bold; }
            .signal-green { background: #166534; color: #4ade80; }
            .signal-blue { background: #1e40af; color: #60a5fa; }
            .digit { font-size: 2em; font-weight: bold; color: #facc15; }
        </style>
    </head>
    <body>
        <h1>Open ProTrader Analysis Engine (Unrestricted)</h1>
        <div id="grid" class="grid">Loading analysis...</div>

        <script>
            const ws = new WebSocket(`ws://${location.host}/ws/signals`);
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                const grid = document.getElementById("grid");
                grid.innerHTML = "";

                for (const [symbol, metrics] of Object.entries(data)) {
                    if (!metrics.signals) continue;
                    
                    const card = document.createElement("div");
                    card.className = "card";
                    card.innerHTML = `
                        <h3>${symbol}</h3>
                        <div>Latest Digit: <span class="digit">${metrics.latest_digit}</span></div>
                        <p>Even/Odd: ${metrics.even_odd_ratio.even_pct}% / ${metrics.even_odd_ratio.odd_pct}%</p>
                        <p>Match Bias Digit: <strong>${metrics.most_frequent_digit.digit}</strong> (${metrics.most_frequent_digit.frequency_pct}%)</p>
                        <div>
                            <span class="badge signal-green">${metrics.signals.even_odd}</span>
                            <span class="badge signal-blue">${metrics.signals.over_under}</span>
                        </div>
                    `;
                    grid.appendChild(card);
                }
            };
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
