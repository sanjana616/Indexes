import json
import sys
import time
import sqlite3
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

import pandas as pd
import pytz
import yfinance as yf
from tvDatafeed import TvDatafeed, Interval
from ta.trend import (
    SMAIndicator, EMAIndicator, WMAIndicator, MACD, ADXIndicator,
    AroonIndicator, CCIIndicator, DPOIndicator, MassIndex,
    IchimokuIndicator, PSARIndicator, STCIndicator, TRIXIndicator,
    VortexIndicator,
)
from ta.volatility import (
    AverageTrueRange, BollingerBands, UlcerIndex,
    KeltnerChannel, DonchianChannel,
)
from ta.momentum import (
    RSIIndicator, StochasticOscillator, ROCIndicator, WilliamsRIndicator,
    AwesomeOscillatorIndicator, KAMAIndicator, PercentagePriceOscillator,
    TSIIndicator, UltimateOscillator,
)
from ta.volume import (
    OnBalanceVolumeIndicator, ChaikinMoneyFlowIndicator,
    AccDistIndexIndicator, MFIIndicator, ForceIndexIndicator,
    EaseOfMovementIndicator, VolumePriceTrendIndicator,
    NegativeVolumeIndexIndicator, VolumeWeightedAveragePrice,
)

# ==========================================================
# CONFIG
# ==========================================================
README_FILE  = "README.md"
SYMBOLS_FILE = "symbols.json"
DB_FILE      = "market_data.db"
IST          = pytz.timezone("Asia/Kolkata")

INDEX_MAP = {
    "NIFTY50":     ("NIFTY",      "NSE"),
    "BANKNIFTY":   ("BANKNIFTY",  "NSE"),
    "MIDCAPNIFTY": ("MIDCPNIFTY", "NSE"),
    "FINNIFTY":    ("FINNIFTY",   "NSE"),
    "SENSEX":      ("SENSEX",     "BSE"),
}

YF_MAP = {
    "NIFTY50":     "^NSEI",
    "BANKNIFTY":   "^NSEBANK",
    "MIDCAPNIFTY": "^NSEMDCP50",
    "FINNIFTY":    "NIFTY_FIN_SERVICE.NS",
    "SENSEX":      "^BSESN",
}

VOL_ETF_MAP = {
    "NIFTY50":     "NIFTYBEES.NS",
    "BANKNIFTY":   "BANKBEES.NS",
    "MIDCAPNIFTY": "MIDSELIETF.NS",
    "FINNIFTY":    "NIF100BEES.NS",
    "SENSEX":      "SENSEXETF.NS",
}

INDEXES = []

COLS = [
    "datetime", "stock_name",
    "open", "high", "low", "close", "volume",
    "sma_5", "sma_10", "sma_20", "sma_50", "sma_100", "sma_200",
    "ema_5", "ema_10", "ema_20", "ema_50", "ema_100", "ema_200",
    "wma_10", "wma_20",
    "macd", "macd_signal", "macd_diff",
    "adx", "adx_pos", "adx_neg",
    "aroon_up", "aroon_down", "aroon_indicator",
    "cci", "dpo", "mass_index",
    "ichimoku_a", "ichimoku_b", "ichimoku_base", "ichimoku_conv",
    "psar", "stc", "trix",
    "vortex_pos", "vortex_neg",
    "kc_upper", "kc_middle", "kc_lower",
    "dc_upper", "dc_middle", "dc_lower",
    "atr",
    "bb_upper", "bb_middle", "bb_lower", "bb_pband", "bb_wband",
    "ulcer_index",
    "rsi_7", "rsi_14", "rsi_21",
    "stoch_k", "stoch_d",
    "roc", "williams_r",
    "awesome_oscillator", "kama",
    "ppo", "tsi", "ultimate_oscillator",
    "obv", "cmf", "acc_dist", "mfi",
    "force_index", "eom", "vpt", "nvi", "vwap",
    "price_change_pct",
    "signal", "updated_at",
]

# ==========================================================
# LOGGING
# ==========================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_handler = RotatingFileHandler("data_fetch.log", maxBytes=5 * 1024 * 1024, backupCount=5)
_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_handler)
_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_console)


# ==========================================================
# MARKET HOURS
# ==========================================================
def is_market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    open_  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_ = now.replace(hour=15, minute=45, second=0, microsecond=0)
    return open_ <= now <= close_


# ==========================================================
# DATABASE
# ==========================================================
_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS indexes (
        datetime         TEXT,
        stock_name       TEXT,
        open             REAL, high            REAL, low             REAL,
        close            REAL, volume          REAL,
        sma_5            REAL, sma_10          REAL, sma_20          REAL,
        sma_50           REAL, sma_100         REAL, sma_200         REAL,
        ema_5            REAL, ema_10          REAL, ema_20          REAL,
        ema_50           REAL, ema_100         REAL, ema_200         REAL,
        wma_10           REAL, wma_20          REAL,
        macd             REAL, macd_signal     REAL, macd_diff       REAL,
        adx              REAL, adx_pos         REAL, adx_neg         REAL,
        aroon_up         REAL, aroon_down      REAL, aroon_indicator REAL,
        cci              REAL, dpo             REAL, mass_index      REAL,
        ichimoku_a       REAL, ichimoku_b      REAL, ichimoku_base   REAL, ichimoku_conv REAL,
        psar             REAL, stc             REAL, trix            REAL,
        vortex_pos       REAL, vortex_neg      REAL,
        kc_upper         REAL, kc_middle       REAL, kc_lower        REAL,
        dc_upper         REAL, dc_middle       REAL, dc_lower        REAL,
        atr              REAL,
        bb_upper         REAL, bb_middle       REAL, bb_lower        REAL,
        bb_pband         REAL, bb_wband        REAL,
        ulcer_index      REAL,
        rsi_7            REAL, rsi_14          REAL, rsi_21          REAL,
        stoch_k          REAL, stoch_d         REAL,
        roc              REAL, williams_r      REAL,
        awesome_oscillator REAL, kama          REAL,
        ppo              REAL, tsi             REAL, ultimate_oscillator REAL,
        obv              REAL, cmf             REAL, acc_dist        REAL,
        mfi              REAL, force_index     REAL, eom             REAL,
        vpt              REAL, nvi             REAL, vwap            REAL,
        price_change_pct REAL,
        signal           TEXT,
        updated_at       TEXT,
        PRIMARY KEY (datetime, stock_name)
    )
"""

_INSERT_SQL = """
    INSERT OR REPLACE INTO indexes
    ({cols})
    VALUES ({placeholders})
""".format(
    cols=", ".join(COLS),
    placeholders=", ".join(["?"] * len(COLS))
)


def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(_CREATE_SQL)
        conn.commit()


def _to_ist_str(dt_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dt_series)
    if dt.dt.tz is None:
        dt = dt.dt.tz_localize(IST)
    else:
        dt = dt.dt.tz_convert(IST)
    return dt.dt.strftime("%Y-%m-%d %H:%M")


def insert_data(symbol: str, df: pd.DataFrame):
    df = compute_indicators(df)
    df = df.copy()
    df["stock_name"]  = symbol
    df["datetime"]    = _to_ist_str(df["datetime"])
    df["volume"]      = df["volume"].fillna(0)
    df["updated_at"]  = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    # filter to Mon-Fri market hours 09:15-15:45
    dt = pd.to_datetime(df["datetime"])
    df = df[
        (dt.dt.weekday < 5) &
        (dt.dt.strftime("%H:%M") >= "09:15") &
        (dt.dt.strftime("%H:%M") <= "15:45")
    ]

    if df.empty:
        logger.warning("[%s] No candles within market hours", symbol)
        return

    for col in COLS:
        if col not in df.columns:
            df[col] = None

    with sqlite3.connect(DB_FILE) as conn:
        conn.executemany(_INSERT_SQL, df[COLS].values.tolist())
        conn.commit()
    logger.info("[%s] Inserted %d candles", symbol, len(df))


def latest_row(symbol: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM indexes WHERE stock_name=? ORDER BY datetime DESC LIMIT 10",
                conn, params=(symbol,)
            )
        if df.empty:
            return None
        with_vol = df[df["volume"] > 0]
        return with_vol.iloc[0] if not with_vol.empty else df.iloc[0]
    except Exception:
        logger.exception("Failed to read latest row for %s", symbol)
        return None


# ==========================================================
# FETCH — TRADINGVIEW
# ==========================================================
def fetch_tv(tv: TvDatafeed, tv_symbol: str, exchange: str, label: str) -> pd.DataFrame:
    for attempt in range(3):
        try:
            df = tv.get_hist(tv_symbol, exchange, interval=Interval.in_1_minute, n_bars=1875)
            if df is not None and not df.empty:
                df = df.reset_index()[["datetime", "open", "high", "low", "close", "volume"]].dropna()
                logger.info("[%s] TV fetched %d rows", label, len(df))
                return df
        except Exception:
            logger.warning("[%s] TV attempt %d failed", label, attempt + 1, exc_info=True)
            time.sleep(3)
    return pd.DataFrame()


# ==========================================================
# FETCH — YFINANCE FALLBACK
# ==========================================================
def _yf_download(ticker: str) -> pd.DataFrame:
    df = yf.download(ticker, period="5d", interval="1m", progress=False, auto_adjust=True)
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() for c in df.columns]
    else:
        df.columns = [c.lower() for c in df.columns]
    df = df.reset_index()
    df.rename(columns={df.columns[0]: "datetime"}, inplace=True)
    return df


def fetch_yf(label: str) -> pd.DataFrame:
    yf_ticker = YF_MAP.get(label)
    if not yf_ticker:
        logger.warning("[%s] No yfinance mapping, skipping fallback", label)
        return pd.DataFrame()
    try:
        df = _yf_download(yf_ticker)
        if df.empty:
            return pd.DataFrame()

        etf_ticker = VOL_ETF_MAP.get(label)
        if etf_ticker:
            etf_df = _yf_download(etf_ticker)
            if not etf_df.empty:
                etf_df["datetime"] = pd.to_datetime(etf_df["datetime"]).dt.floor("min")
                df["datetime"]     = pd.to_datetime(df["datetime"]).dt.floor("min")
                merged = df[["datetime"]].merge(
                    etf_df[["datetime", "volume"]].rename(columns={"volume": "etf_vol"}),
                    on="datetime", how="left"
                )
                df["volume"] = merged["etf_vol"].ffill().fillna(0).astype(int).values
                logger.info("[%s] ETF volume merged from %s", label, etf_ticker)

        df = df[["datetime", "open", "high", "low", "close", "volume"]].dropna(subset=["close"])
        logger.info("[%s] yfinance fetched %d rows", label, len(df))
        return df
    except Exception:
        logger.exception("[%s] yfinance fetch failed", label)
    return pd.DataFrame()


def fetch_candles(tv: TvDatafeed, tv_symbol: str, exchange: str, label: str) -> pd.DataFrame:
    df = fetch_tv(tv, tv_symbol, exchange, label)
    if df.empty:
        logger.info("[%s] Falling back to yfinance", label)
        df = fetch_yf(label)
    if df.empty:
        logger.warning("[%s] No data from any source", label)
    return df


# ==========================================================
# INDICATORS
# ==========================================================
def _safe(fn):
    try:
        return fn()
    except Exception:
        return None


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]

    # SMA
    df["sma_5"]   = _safe(lambda: SMAIndicator(c, 5).sma_indicator())
    df["sma_10"]  = _safe(lambda: SMAIndicator(c, 10).sma_indicator())
    df["sma_20"]  = _safe(lambda: SMAIndicator(c, 20).sma_indicator())
    df["sma_50"]  = _safe(lambda: SMAIndicator(c, 50).sma_indicator())
    df["sma_100"] = _safe(lambda: SMAIndicator(c, 100).sma_indicator())
    df["sma_200"] = _safe(lambda: SMAIndicator(c, 200).sma_indicator())

    # EMA
    df["ema_5"]   = _safe(lambda: EMAIndicator(c, 5).ema_indicator())
    df["ema_10"]  = _safe(lambda: EMAIndicator(c, 10).ema_indicator())
    df["ema_20"]  = _safe(lambda: EMAIndicator(c, 20).ema_indicator())
    df["ema_50"]  = _safe(lambda: EMAIndicator(c, 50).ema_indicator())
    df["ema_100"] = _safe(lambda: EMAIndicator(c, 100).ema_indicator())
    df["ema_200"] = _safe(lambda: EMAIndicator(c, 200).ema_indicator())

    # WMA
    df["wma_10"] = _safe(lambda: WMAIndicator(c, 10).wma())
    df["wma_20"] = _safe(lambda: WMAIndicator(c, 20).wma())

    # MACD
    _macd = MACD(c)
    df["macd"]        = _safe(lambda: _macd.macd())
    df["macd_signal"] = _safe(lambda: _macd.macd_signal())
    df["macd_diff"]   = _safe(lambda: _macd.macd_diff())

    # ADX
    _adx = ADXIndicator(h, l, c, 14)
    df["adx"]     = _safe(lambda: _adx.adx())
    df["adx_pos"] = _safe(lambda: _adx.adx_pos())
    df["adx_neg"] = _safe(lambda: _adx.adx_neg())

    # Aroon
    _aroon = AroonIndicator(h, l, 25)
    df["aroon_up"]        = _safe(lambda: _aroon.aroon_up())
    df["aroon_down"]      = _safe(lambda: _aroon.aroon_down())
    df["aroon_indicator"] = _safe(lambda: _aroon.aroon_indicator())

    # CCI, DPO, Mass Index
    df["cci"]        = _safe(lambda: CCIIndicator(h, l, c, 20).cci())
    df["dpo"]        = _safe(lambda: DPOIndicator(c, 20).dpo())
    df["mass_index"] = _safe(lambda: MassIndex(h, l, 9, 25).mass_index())

    # Ichimoku
    _ichi = IchimokuIndicator(h, l, 9, 26, 52)
    df["ichimoku_a"]    = _safe(lambda: _ichi.ichimoku_a())
    df["ichimoku_b"]    = _safe(lambda: _ichi.ichimoku_b())
    df["ichimoku_base"] = _safe(lambda: _ichi.ichimoku_base_line())
    df["ichimoku_conv"] = _safe(lambda: _ichi.ichimoku_conversion_line())

    # PSAR, STC, TRIX
    df["psar"] = _safe(lambda: PSARIndicator(h, l, c).psar())
    df["stc"]  = _safe(lambda: STCIndicator(c).stc())
    df["trix"] = _safe(lambda: TRIXIndicator(c, 15).trix())

    # Vortex
    _vortex = VortexIndicator(h, l, c, 14)
    df["vortex_pos"] = _safe(lambda: _vortex.vortex_indicator_pos())
    df["vortex_neg"] = _safe(lambda: _vortex.vortex_indicator_neg())

    # Keltner Channel
    _kc = KeltnerChannel(h, l, c, 20)
    df["kc_upper"]  = _safe(lambda: _kc.keltner_channel_hband())
    df["kc_middle"] = _safe(lambda: _kc.keltner_channel_mband())
    df["kc_lower"]  = _safe(lambda: _kc.keltner_channel_lband())

    # Donchian Channel
    _dc = DonchianChannel(h, l, c, 20)
    df["dc_upper"]  = _safe(lambda: _dc.donchian_channel_hband())
    df["dc_middle"] = _safe(lambda: _dc.donchian_channel_mband())
    df["dc_lower"]  = _safe(lambda: _dc.donchian_channel_lband())

    # ATR
    df["atr"] = _safe(lambda: AverageTrueRange(h, l, c, 14).average_true_range())

    # Bollinger Bands
    _bb = BollingerBands(c, 20, 2)
    df["bb_upper"]  = _safe(lambda: _bb.bollinger_hband())
    df["bb_middle"] = _safe(lambda: _bb.bollinger_mavg())
    df["bb_lower"]  = _safe(lambda: _bb.bollinger_lband())
    df["bb_pband"]  = _safe(lambda: _bb.bollinger_pband())
    df["bb_wband"]  = _safe(lambda: _bb.bollinger_wband())

    # Ulcer Index
    df["ulcer_index"] = _safe(lambda: UlcerIndex(c, 14).ulcer_index())

    # RSI
    df["rsi_7"]  = _safe(lambda: RSIIndicator(c, 7).rsi())
    df["rsi_14"] = _safe(lambda: RSIIndicator(c, 14).rsi())
    df["rsi_21"] = _safe(lambda: RSIIndicator(c, 21).rsi())

    # Stochastic
    _stoch = StochasticOscillator(h, l, c, 14, 3)
    df["stoch_k"] = _safe(lambda: _stoch.stoch())
    df["stoch_d"] = _safe(lambda: _stoch.stoch_signal())

    # ROC, Williams %R
    df["roc"]       = _safe(lambda: ROCIndicator(c, 12).roc())
    df["williams_r"] = _safe(lambda: WilliamsRIndicator(h, l, c, 14).williams_r())

    # Awesome Oscillator, KAMA
    df["awesome_oscillator"] = _safe(lambda: AwesomeOscillatorIndicator(h, l, 5, 34).awesome_oscillator())
    df["kama"]               = _safe(lambda: KAMAIndicator(c, 10, 2, 30).kama())

    # PPO, TSI, Ultimate Oscillator
    df["ppo"] = _safe(lambda: PercentagePriceOscillator(c, 26, 12, 9).ppo())
    df["tsi"] = _safe(lambda: TSIIndicator(c, 25, 13).tsi())
    df["ultimate_oscillator"] = _safe(lambda: UltimateOscillator(h, l, c, 7, 14, 28).ultimate_oscillator())

    # Volume indicators
    df["obv"]        = _safe(lambda: OnBalanceVolumeIndicator(c, v).on_balance_volume())
    df["cmf"]        = _safe(lambda: ChaikinMoneyFlowIndicator(h, l, c, v, 20).chaikin_money_flow())
    df["acc_dist"]   = _safe(lambda: AccDistIndexIndicator(h, l, c, v).acc_dist_index())
    df["mfi"]        = _safe(lambda: MFIIndicator(h, l, c, v, 14).money_flow_index())
    df["force_index"] = _safe(lambda: ForceIndexIndicator(c, v, 13).force_index())
    df["eom"]        = _safe(lambda: EaseOfMovementIndicator(h, l, v, 14).ease_of_movement())
    df["vpt"]        = _safe(lambda: VolumePriceTrendIndicator(c, v).volume_price_trend())
    df["nvi"]        = _safe(lambda: NegativeVolumeIndexIndicator(c, v).negative_volume_index())

    # VWAP
    df["vwap"] = _safe(lambda: VolumeWeightedAveragePrice(h, l, c, v).volume_weighted_average_price())

    # Price change %
    df["price_change_pct"] = _safe(lambda: c.pct_change() * 100)

    return df


# ==========================================================
# SIGNAL
# ==========================================================
def generate_signal(row) -> str:
    try:
        required = ["close", "ema_20", "rsi_14", "macd", "macd_signal", "adx"]
        if any(pd.isna(row[c]) for c in required):
            return "HOLD"
        if (row["close"] > row["ema_20"] and row["rsi_14"] > 55
                and row["macd"] > row["macd_signal"] and row["adx"] > 20):
            return "BUY"
        if (row["close"] < row["ema_20"] and row["rsi_14"] < 45
                and row["macd"] < row["macd_signal"] and row["adx"] > 20):
            return "SELL"
    except Exception:
        logger.exception("Signal generation failed")
    return "HOLD"


# ==========================================================
# README
# ==========================================================
def update_readme():
    ICONS = {"BUY": "🟢 BUY", "SELL": "🔴 SELL", "HOLD": "🟡 HOLD"}
    fmt   = lambda x: f"{x:.2f}" if pd.notna(x) else "-"

    with open(README_FILE, "w", encoding="utf-8") as f:
        f.write(f"Last updated: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}\n\n")

        # ── Summary Table ──────────────────────────────────────
        f.write("## 📊 Market Indexes — Summary\n\n")
        f.write("| Symbol | Time (IST) | Close | Volume | RSI(14) | EMA20 | MACD | ATR | ADX | Signal |\n")
        f.write("|--------|-----------|-------|--------|---------|-------|------|-----|-----|--------|\n")
        for sym in INDEXES:
            row = latest_row(sym)
            if row is None:
                continue
            signal = generate_signal(row)
            volume = int(row["volume"]) if pd.notna(row["volume"]) else 0
            f.write(
                f"| {sym} | {row['datetime']} | {fmt(row['close'])} | {volume:,} "
                f"| {fmt(row['rsi_14'])} | {fmt(row['ema_20'])} | {fmt(row['macd'])} "
                f"| {fmt(row['atr'])} | {fmt(row['adx'])} | {ICONS[signal]} |\n"
            )

        # ── Per Symbol Detail ──────────────────────────────────
        for sym in INDEXES:
            row = latest_row(sym)
            if row is None:
                continue
            signal = generate_signal(row)
            f.write(f"\n---\n\n### {sym} &nbsp; {ICONS[signal]}\n\n")
            f.write(f"> {row['datetime']} &nbsp;|&nbsp; "
                    f"O: {fmt(row['open'])} &nbsp; "
                    f"H: {fmt(row['high'])} &nbsp; "
                    f"L: {fmt(row['low'])} &nbsp; "
                    f"C: **{fmt(row['close'])}** &nbsp;|&nbsp; "
                    f"Vol: {int(row['volume']):,}\n\n")

            sections = [
                ("📈 Moving Averages", [
                    ("SMA 5",   "sma_5"),   ("SMA 10",  "sma_10"),  ("SMA 20",  "sma_20"),
                    ("SMA 50",  "sma_50"),  ("SMA 100", "sma_100"), ("SMA 200", "sma_200"),
                    ("EMA 5",   "ema_5"),   ("EMA 10",  "ema_10"),  ("EMA 20",  "ema_20"),
                    ("EMA 50",  "ema_50"),  ("EMA 100", "ema_100"), ("EMA 200", "ema_200"),
                    ("WMA 10",  "wma_10"),  ("WMA 20",  "wma_20"),
                ]),
                ("⚡ Momentum & Trend", [
                    ("MACD",        "macd"),        ("MACD Signal", "macd_signal"), ("MACD Diff",  "macd_diff"),
                    ("ADX",         "adx"),         ("ADX+",        "adx_pos"),     ("ADX-",       "adx_neg"),
                    ("RSI 7",       "rsi_7"),       ("RSI 14",      "rsi_14"),      ("RSI 21",     "rsi_21"),
                    ("Stoch %K",    "stoch_k"),     ("Stoch %D",    "stoch_d"),
                    ("ROC",         "roc"),         ("Williams %R", "williams_r"),  ("CCI",        "cci"),
                    ("DPO",         "dpo"),         ("AO",          "awesome_oscillator"),
                    ("KAMA",        "kama"),        ("PPO",         "ppo"),          ("TSI",        "tsi"),
                    ("Ult. Osc",    "ultimate_oscillator"),
                ]),
                ("🎯 Trend Indicators", [
                    ("Aroon Up",    "aroon_up"),    ("Aroon Down",  "aroon_down"),  ("Aroon Ind",  "aroon_indicator"),
                    ("Vortex+",     "vortex_pos"),  ("Vortex-",     "vortex_neg"),
                    ("Mass Index",  "mass_index"),  ("TRIX",        "trix"),        ("STC",        "stc"),
                    ("DPO",         "dpo"),         ("PSAR",        "psar"),
                    ("Ichi A",      "ichimoku_a"),  ("Ichi B",      "ichimoku_b"),
                    ("Ichi Base",   "ichimoku_base"),("Ichi Conv",  "ichimoku_conv"),
                ]),
                ("📊 Volatility & Channels", [
                    ("ATR",         "atr"),         ("Ulcer Idx",   "ulcer_index"),
                    ("BB Upper",    "bb_upper"),    ("BB Mid",      "bb_middle"),   ("BB Lower",   "bb_lower"),
                    ("BB %B",       "bb_pband"),    ("BB Width",    "bb_wband"),
                    ("KC Upper",    "kc_upper"),    ("KC Mid",      "kc_middle"),   ("KC Lower",   "kc_lower"),
                    ("DC Upper",    "dc_upper"),    ("DC Mid",      "dc_middle"),   ("DC Lower",   "dc_lower"),
                ]),
                ("💹 Volume Indicators", [
                    ("OBV",         "obv"),         ("CMF",         "cmf"),         ("Acc/Dist",   "acc_dist"),
                    ("MFI",         "mfi"),         ("Force Idx",   "force_index"), ("EOM",        "eom"),
                    ("VPT",         "vpt"),         ("NVI",         "nvi"),         ("VWAP",       "vwap"),
                    ("Chg %",       "price_change_pct"),
                ]),
            ]

            for section_name, indicators in sections:
                f.write(f"**{section_name}**\n\n")
                f.write("| Indicator | Value | Indicator | Value | Indicator | Value |\n")
                f.write("|-----------|-------|-----------|-------|-----------|-------|\n")
                # group into rows of 3
                for i in range(0, len(indicators), 3):
                    chunk = indicators[i:i+3]
                    while len(chunk) < 3:
                        chunk.append(("", ""))
                    cells = ""
                    for label, key in chunk:
                        val = fmt(row.get(key)) if key else ""
                        cells += f"| {label} | {val} "
                    f.write(cells + "|\n")
                f.write("\n")


# ==========================================================
# MAIN
# ==========================================================
def main():

    logger.info("Starting fetch cycle")
    tv = TvDatafeed()
    init_db()

    for symbol in INDEXES:
        try:
            tv_symbol, exchange = INDEX_MAP[symbol]
            df = fetch_candles(tv, tv_symbol, exchange, symbol)
            if not df.empty:
                insert_data(symbol, df)
        except Exception:
            logger.exception("[%s] Failed", symbol)
        time.sleep(2)

    update_readme()
    logger.info("Cycle complete. README updated.")


if __name__ == "__main__":
    try:
        with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        INDEXES = data.get("Indexes", [])
        if not INDEXES:
            raise ValueError("'Indexes' list is empty in symbols.json")
    except Exception:
        logger.exception("Startup error")
        raise SystemExit(1)

    main()
