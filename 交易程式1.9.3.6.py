# ==============================================================================
# 交易程式 1.9.3 - 當沖量化終端 (SQLite / 效能極速版 / 資金曲線)
# ==============================================================================
import json
import os
import math
import subprocess
import sys
import time as time_module
import warnings
import traceback
import importlib
import csv
import threading
import re
import glob
import sqlite3
from datetime import datetime, time, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from configparser import ConfigParser

REQUIRED = [
    ("pandas",           "pandas"),
    ("numpy",            "numpy"),
    ("colorama",         "colorama"),
    ("tabulate",         "tabulate"),
    ("openpyxl",         "openpyxl"),
    ("dateutil",         "python-dateutil"),
    ("matplotlib",       "matplotlib"),
    ("PyQt5",            "PyQt5"),
    ("scipy",            "scipy"),
    ("fastdtw",          "fastdtw")
]

def ensure_packages(pkgs):
    missing = []
    for mod, pkg in pkgs:
        try: importlib.import_module(mod)
        except ImportError: missing.append(pkg)
    if missing:
        # 🟢 使用 Windows 內建 API 彈出視窗，確保在任何情況下都能顯示
        import ctypes
        msg = f"系統偵測到缺少必要套件：\n{', '.join(missing)}\n\n按下「確定」後將開始自動安裝，安裝期間會顯示黑畫面，請耐心稍候..."
        ctypes.windll.user32.MessageBoxW(0, msg, "環境建置提示", 0x40)
        
        print("首次執行，正在安裝必要套件：", ", ".join(missing))
        for pkg in missing:
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
            
        # 🟢 安裝完成後彈出重新啟動提示，並安全退出
        ctypes.windll.user32.MessageBoxW(0, "✅ 已成功安裝所有必要套件！\n為了確保系統穩定，請「重新啟動」本程式。", "安裝完成", 0x40)
        sys.exit(0)

ensure_packages(REQUIRED)

import pandas as pd
import numpy as np
import colorama
import shioaji as sj
import touchprice as tp
import requests
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from bs4 import BeautifulSoup
from colorama import init, Fore, Style

def auto_install_esun_sdk():
    whl_filename = "esun_marketdata-2.2.0-cp37-abi3-win_amd64.whl" 
    try: importlib.import_module("esun_marketdata")
    except ImportError:
        whl_path = os.path.join(os.getcwd(), whl_filename)
        if os.path.exists(whl_path):
            try: subprocess.check_call([sys.executable, "-m", "pip", "install", whl_path])
            except Exception as e: sys.exit(f"❌ SDK 安裝失敗：{e}")
        else:
            sys.exit(f"❌ 找不到安裝檔：{whl_path}\n請確認 .whl 檔案是否放在同一個資料夾內。")

auto_install_esun_sdk()

import esun_marketdata
from esun_marketdata import EsunMarketdata
import shioaji_logic

from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QTextEdit, 
                             QInputDialog, QMessageBox, QDialog, QLineEdit, 
                             QComboBox, QFormLayout, QRadioButton, QScrollArea, 
                             QFrame, QButtonGroup, QDialogButtonBox, QFileDialog,
                             QGroupBox, QProgressBar, QSplitter, QListWidget, QCalendarWidget,
                             QAbstractItemView, QTableWidget, QTableWidgetItem, QHeaderView,
                             QDoubleSpinBox, QMenu)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject, pyqtSlot, QTimer
from PyQt5.QtGui import QFont, QColor, QTextCursor, QPalette

plt.rcParams['axes.unicode_minus'] = False
colorama.init(autoreset=True)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning, module="urllib3.connection")
np.seterr(divide='ignore', invalid='ignore')
pd.set_option('future.no_silent_downcasting', True)

RED = Fore.RED; GREEN = Fore.GREEN; YELLOW = Fore.YELLOW; BLUE = Fore.BLUE; RESET = Style.RESET_ALL
ESUN_LOGIN_PWD = None
ESUN_CERT_PWD = None

# ==================== 系統狀態與設定管理器 ====================

# 🟢 1.9.3.1 新增：智慧股票輸入解析器
def resolve_stock_code(input_str):
    """解析輸入，支援代號(2330)、名稱(台積電)、或混合(2330台積電 / 台積電 2330)"""
    load_twse_name_map()
    input_str = input_str.strip()
    if not input_str: return None
    
    # 建立反向查詢字典 (名稱 -> 代號)
    reverse_map = {}
    for mkt in ["TSE", "OTC"]:
        for code, name in STOCK_NAME_MAP.get(mkt, {}).items():
            reverse_map[name] = code
            
    # 1. 完美命中名稱或代號
    if input_str in reverse_map: return reverse_map[input_str]
    if input_str in STOCK_NAME_MAP.get("TSE", {}) or input_str in STOCK_NAME_MAP.get("OTC", {}): return input_str
    
    # 2. 萃取純數字與純文字
    digits = re.sub(r'\D', '', input_str)
    chars = re.sub(r'[\d\s\Wa-zA-Z]', '', input_str) # 盡可能只留中文字
    
    # 3. 判斷數字是否為有效代號
    if len(digits) >= 4 and (digits[:4] in STOCK_NAME_MAP.get("TSE", {}) or digits[:4] in STOCK_NAME_MAP.get("OTC", {})):
        return digits[:4]
        
    # 4. 模糊名稱搜尋
    if chars in reverse_map: return reverse_map[chars]
    for name, code in reverse_map.items():
        if chars and (chars in name or name in chars): return code
        
    return None


class TradingConfig:
    def __init__(self):
        self.capital_per_stock = 1000
        self.transaction_fee = 0.1425
        self.transaction_discount = 18.0
        self.trading_tax = 0.15
        self.below_50 = 500.0
        self.price_gap_50_to_100 = 1000.0
        self.price_gap_100_to_500 = 2000.0
        self.price_gap_500_to_1000 = 3000.0
        self.price_gap_above_1000 = 5000.0
        self.momentum_minutes = 1
        self.tg_bot_token = "8762583083:AAFm1B2-K6hzhAIvOoBrakJf3C1VPXtkE-4" 
        self.tg_chat_id = "" 
        self.tg_notify_enabled = True
        self.is_monitoring = False
        
        # 🟢 新增：停損再進場專屬設定區
        self.allow_reentry = False             # 是否開啟停損再進場
        self.max_reentry_times = 1             # 最多可再進場幾次
        self.reentry_lookback_candles = 3      # 停損後往前檢查幾根K棒
        
        # 🟢 1.9.4 開發者模式：進階策略參數
        self.similarity_threshold = 0.75      # DTW 相似度門檻
        self.pull_up_pct_threshold = 2.0      # 領漲股拉高漲幅門檻 (%)
        self.follow_up_pct_threshold = 1.5    # 跟漲股追蹤漲幅門檻 (%)
        self.rise_lower_bound = -1.0          # 當日總漲幅下限 (%)
        self.rise_upper_bound = 6.0           # 當日總漲幅上限 (%)
        self.volume_multiplier = 1.5          # 等待期均量門檻 (倍數)
        self.min_volume_threshold = 50        # 等待期絕對數量門檻 (張)
        self.pullback_tolerance = 0.5         # 突破前高容錯率 (%)

sys_config = TradingConfig()

class TradingState:
    def __init__(self):
        self.open_positions = {}
        self.triggered_limit_up = set()
        self.previous_stop_loss = set()
        self.in_memory_intraday = {}
        self.lock = threading.Lock()
        self.quit_flag = False
        self.api = None
        self.to = None
        self.trading = False
        self.stop_trading_flag = False

sys_config = TradingConfig()
sys_state = TradingState()

# ==================== SQLite 資料庫管理器 ====================
class DBManager:
    def __init__(self, db_name="quant_data.db"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.db_lock = threading.Lock() # 🟢 新增：SQLite 資料庫專用寫入鎖
        self._create_tables()
        self._init_docs() # 🟢 初始化教學文件

    def _create_tables(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, action TEXT, symbol TEXT,
                    shares INTEGER, price REAL, profit REAL, note TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS system_docs (
                    doc_id TEXT PRIMARY KEY,
                    content TEXT
                )
            """)
            # 🟢 新增：建立通用系統狀態表 (用來取代各種 JSON 檔案)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS system_state (
                    key_name TEXT PRIMARY KEY,
                    json_data TEXT
                )
            """)

    def _init_docs(self):
        doc_phone = """📱 <b>手機端量化遙控器 - 使用指南</b>

<b>【一、盤中監控篇】</b>
1. <b>登入與啟動</b>：初次使用請輸入 <code>登入 身分證 密碼 憑證密碼</code> 綁定帳戶。接著點擊「▶ 啟動盤中監控」設定參數後執行。
2. <b>盤中保護</b>：監控一旦啟動，為保護運算資源，系統將自動鎖定「參數設定」、「回測」、「更新K線」等重度功能。若重複點擊啟動，將自動引導您查看持倉。
3. <b>即時推播</b>：系統會在「領漲替換」、「無縫升級漲停」、「進場買進」、「停損/超時平倉」時主動發送推播給您。

<b>【二、盤後分析篇 (限13:30後使用)】</b>
1. <b>K線與大數據</b>：請先執行「更新 K 線數據」，若要進行進階分析，請至分析選單執行「採集大數據」。
2. <b>智能 DTW 掃描</b>：基於大數據運算族群連動的最適門檻，算完後會以表格回傳手機。
3. <b>自選與極大化</b>：可針對特定族群進行歷史回測，或使用雲端算力暴力破解最佳「等待/持有」時間。

<b>【三、實用工具】</b>
• <b>智能看圖</b>：無需任何指令，直接在聊天框輸入股票代號 (如: <code>2330</code>) 或族群名稱 (如: <code>散熱</code>)，系統將秒回當日走勢圖！
• <b>緊急平倉</b>：若遇突發狀況，點擊「🛑 緊急/手動平倉」並確認，系統將以市價拋售所有庫存。"""

        doc_pc = """💻 <b>電腦端主控台 - 協同運作指南</b>

<b>【一、核心運作觀念】</b>
1. <b>本機常駐</b>：您的電腦是整套量化系統的「大腦」，手機端的一切指令皆交由電腦運算。請確保程式保持開啟，且電腦「不可進入睡眠模式」。
2. <b>雙向同步</b>：手機與電腦端的持倉、設定與交易紀錄完全同步。無論從何處觸發平倉，兩邊皆會生效。

<b>【二、終端機監控 (黑畫面)】</b>
1. <b>安全攔截機制</b>：系統具備軍規級防護。若非您綁定的手機發送指令，系統會直接丟棄並在終端機印出 <code>[防護] 攔截到未授權訊息</code>。
2. <b>進度瀑布流</b>：當手機觸發「DTW 掃描」或「暴力破解」時，手機會顯示百分比，而電腦終端機會即時印出每一檔股票的運算細節與除錯日誌。

<b>【三、GUI 介面操作】</b>
• 介面左側面板功能與手機端完全一致。
• <b>族群管理</b>：支援直接雙擊股票名稱查看圖表。
• 若遇任何網路異常，可直接關閉視窗，系統將安全切斷 API 連線。"""

        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO system_docs (doc_id, content) VALUES (?, ?)", ('tut_phone', doc_phone))
            self.conn.execute("INSERT OR REPLACE INTO system_docs (doc_id, content) VALUES (?, ?)", ('tut_pc', doc_pc))

    def log_trade(self, action, symbol, shares, price, profit=0.0, note=""):
        with self.db_lock:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO trade_logs (timestamp, action, symbol, shares, price, profit, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), action, symbol, shares, price, profit, note)
            )

    def save_kline(self, table_name, data_dict):
        if not data_dict: return
        dfs = [pd.DataFrame(records).assign(symbol=sym) for sym, records in data_dict.items() if records]
        if dfs:
            df_all = pd.concat(dfs, ignore_index=True).astype(str)
            with self.db_lock:
                with self.conn:
                    df_all.to_sql(table_name, self.conn, if_exists='replace', index=False)

    def load_kline(self, table_name):
        try:
            with self.db_lock:    
                df = pd.read_sql(f"SELECT * FROM {table_name}", self.conn)
            cols_to_numeric = ['close', 'high', 'low', 'open', 'volume', 'rise', '2min_pct_increase', '漲停價', '昨日收盤價', 'highest']
            for c in cols_to_numeric:
                if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce')
            return {sym: group.to_dict('records') for sym, group in df.groupby('symbol')}
        except Exception: return {}

    # 🟢 新增：通用的 JSON-to-SQLite 讀寫介面
    def save_state(self, key_name, data):
        with self.db_lock:
            with self.conn:
                self.conn.execute(
                    "INSERT OR REPLACE INTO system_state (key_name, json_data) VALUES (?, ?)",
                    (key_name, json.dumps(data, ensure_ascii=False))
                )

    def load_state(self, key_name, default_value=None):
        if default_value is None: default_value = {} if key_name.endswith('dict') or key_name == 'settings' else []
        try:
            with self.db_lock:
                cursor = self.conn.execute("SELECT json_data FROM system_state WHERE key_name = ?", (key_name,))
                row = cursor.fetchone()
            return json.loads(row[0]) if row else default_value
        except Exception:
            return default_value

sys_db = DBManager()

# ==================== 極速直連通訊管理器 (1.9.4 終極防護教學版 + 報表匯出) ====================
import html
import requests
import threading
import time as time_module
import json
import re
import os
from datetime import datetime
import pandas as pd
from PyQt5.QtCore import Qt

class CloudBrainManager:
    def __init__(self): 
        self.is_running = False
        self.session = requests.Session()
        self.offset = None
        self.task_lock = threading.Lock()
        self.ui_states = {} 

    def _get_token(self):
        t = getattr(sys_config, 'tg_bot_token', '').strip()
        return t[3:] if t.lower().startswith('bot') else t

    def _get_chat_id(self):
        chat_id = getattr(sys_config, 'tg_chat_id', '').strip()
        return chat_id if chat_id else getattr(sys_config, 'tg_auth_key', '').strip()

    def send_message(self, text, force=False, reply_markup=None):
        token, chat_id = self._get_token(), self._get_chat_id()
        if not token or not chat_id: return None
        if not force and not getattr(sys_config, 'tg_notify_enabled', True): return None
        try: 
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
            if reply_markup: payload["reply_markup"] = reply_markup
            res = self.session.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload, timeout=10)
            if res.status_code == 200: return res.json().get("result", {}).get("message_id")
        except: pass
        return None

    def edit_message_text(self, message_id, text, reply_markup=None):
        token, chat_id = self._get_token(), self._get_chat_id()
        if not token or not chat_id or not message_id: return
        try:
            payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
            if reply_markup: payload["reply_markup"] = reply_markup
            self.session.post(f"https://api.telegram.org/bot{token}/editMessageText", json=payload, timeout=5)
        except: pass

    def send_photo(self, photo_bytes, caption=""):
        token, chat_id = self._get_token(), self._get_chat_id()
        if not token or not chat_id: return
        def _send():
            try:
                data = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
                files = {"photo": ("chart.png", photo_bytes, "image/png")}
                self.session.post(f"https://api.telegram.org/bot{token}/sendPhoto", data=data, files=files, timeout=20)
            except Exception as e: print(f"⚠️ 傳送圖片異常: {e}")
        threading.Thread(target=_send, daemon=True).start()

    def send_document(self, file_path, caption=""):
        token, chat_id = self._get_token(), self._get_chat_id()
        if not token or not chat_id: return
        def _send():
            try:
                with open(file_path, 'rb') as f:
                    files = {"document": f}
                    data = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
                    self.session.post(f"https://api.telegram.org/bot{token}/sendDocument", data=data, files=files, timeout=20)
            except Exception as e: print(f"⚠️ 傳送檔案異常: {e}")
        threading.Thread(target=_send, daemon=True).start()

    def send_chat_action(self, action="typing"):
        token, chat_id = self._get_token(), self._get_chat_id()
        if token and chat_id:
            try: self.session.post(f"https://api.telegram.org/bot{token}/sendChatAction", json={"chat_id": chat_id, "action": action}, timeout=5)
            except: pass

    def _set_bot_menu(self):
        token = self._get_token()
        cmds = [{"command": "menu", "description": "📱 開啟全端遙控中心"}]
        try: self.session.post(f"https://api.telegram.org/bot{token}/setMyCommands", json={"commands": cmds}, timeout=5)
        except: pass

    def get_bottom_keyboard(self):
        return {"keyboard": [
            [{"text": "▶ 啟動盤中監控"}, {"text": "📊 即時持倉監控"}],
            [{"text": "📊 盤後數據與分析"}, {"text": "🎯 自選進場模式"}],
            [{"text": "💰 極大化利潤"}, {"text": "📁 管理股票族群"}],
            [{"text": "🔄 更新 K 線數據"}, {"text": "📜 歷史交易紀錄"}],
            [{"text": "📈 畫圖查看走勢"}, {"text": "⚙️ 參數設定"}],
            [{"text": "🛑 緊急/手動平倉"}]
        ], "resize_keyboard": True}

    def get_analysis_menu(self):
        return {"inline_keyboard": [
            [{"text": "📥 下載/更新 相似度大數據", "callback_data": "cmd_fetch_data"}],
            [{"text": "🔬 啟動智能 DTW 門檻掃描", "callback_data": "cmd_opt_sim"}],
            [{"text": "📊 計算全部族群平均過高間隔", "callback_data": "cmd_avg_high"}],
            [{"text": "❌ 關閉面板", "callback_data": "cmd_close_menu"}]
        ]}

    def get_groups_keyboard(self):
        mat = load_matrix_dict_analysis()
        kb, row = [], []
        for g in mat.keys():
            row.append({"text": f"📁 {g}", "callback_data": f"grp_show_{g}"})
            if len(row) == 2:
                kb.append(row)
                row = []
        if row: kb.append(row)
        kb.append([{"text": "❌ 關閉面板", "callback_data": "cmd_close_menu"}])
        return {"inline_keyboard": kb}

    def get_slider_keyboard(self, state, prefix):
        groups = ["所有族群"] + list(load_matrix_dict_analysis().keys())
        g_idx = state.get('g_idx', 0) % len(groups)
        w, h = state.get('w', 5), state.get('h', 270)
        h_str = "尾盤(F)" if h == 270 else f"{h}分"
        
        kb = []
        if prefix == "sim": kb.append([{"text": "◀", "callback_data": f"{prefix}_g_prev"}, {"text": f"📁 族群: {groups[g_idx]}", "callback_data": "dummy"}, {"text": "▶", "callback_data": f"{prefix}_g_next"}])
        kb.append([{"text": "➖", "callback_data": f"{prefix}_w_dec"}, {"text": f"等待時間: {w}分", "callback_data": "dummy"}, {"text": "➕", "callback_data": f"{prefix}_w_inc"}])
        kb.append([{"text": "➖", "callback_data": f"{prefix}_h_dec"}, {"text": f"持有時間: {h_str}", "callback_data": "dummy"}, {"text": "➕", "callback_data": f"{prefix}_h_inc"}])
        btn_txt = "▶️ 執行回測" if prefix == "sim" else "🚀 啟動盤中監控"
        kb.append([{"text": btn_txt, "callback_data": f"{prefix}_execute"}, {"text": "❌ 取消", "callback_data": "cmd_close_menu"}])
        return {"inline_keyboard": kb}

    def get_max_builder_keyboard(self, state):
        groups = ["所有族群"] + list(load_matrix_dict_analysis().keys())
        g_idx = state.get('g_idx', 0) % len(groups)
        ws, we, hs, he = state.get('ws', 3), state.get('we', 5), state.get('hs', 10), state.get('he', 20)
        hss = "尾盤(F)" if hs == 270 else f"{hs}分"
        hes = "尾盤(F)" if he == 270 else f"{he}分"
        return {"inline_keyboard": [
            [{"text": "◀", "callback_data": "max_g_prev"}, {"text": f"📁 族群: {groups[g_idx]}", "callback_data": "dummy"}, {"text": "▶", "callback_data": "max_g_next"}],
            [{"text": "➖", "callback_data": "max_ws_dec"}, {"text": f"等(起): {ws}分", "callback_data": "dummy"}, {"text": "➕", "callback_data": "max_ws_inc"}],
            [{"text": "➖", "callback_data": "max_we_dec"}, {"text": f"等(終): {we}分", "callback_data": "dummy"}, {"text": "➕", "callback_data": "max_we_inc"}],
            [{"text": "➖", "callback_data": "max_hs_dec"}, {"text": f"持(起): {hss}", "callback_data": "dummy"}, {"text": "➕", "callback_data": "max_hs_inc"}],
            [{"text": "➖", "callback_data": "max_he_dec"}, {"text": f"持(終): {hes}", "callback_data": "dummy"}, {"text": "➕", "callback_data": "max_he_inc"}],
            [{"text": "▶️ 確認執行破解", "callback_data": "max_execute"}, {"text": "❌ 取消", "callback_data": "cmd_close_menu"}]
        ]}

    def start(self):
        token, chat_id = self._get_token(), self._get_chat_id()
        if not token or not chat_id: return print("\n⚠️ [遙控中心] 尚未設定 Token 或 Chat ID！")
        if self.is_running: return
        self.is_running = True
        
        def _init_and_poll():
            print("\n[遙控中心] 正在切斷雲端 Webhook...")
            try: 
                if self.session.get(f"https://api.telegram.org/bot{token}/deleteWebhook?drop_pending_updates=True", timeout=10).status_code == 200:
                    print("[遙控中心] ✅ 雲端綁定解除，遙控核心啟動。"); self._set_bot_menu()
            except: pass
            self._poll()
        threading.Thread(target=_init_and_poll, daemon=True).start()

    def _poll(self):
        token = self._get_token()
        print("[遙控中心] ⚡ 儀表板已開啟！等待指令中...")
        while self.is_running:
            try:
                url = f"https://api.telegram.org/bot{token}/getUpdates?timeout=10"
                if self.offset: url += f"&offset={self.offset}"
                res = self.session.get(url, timeout=15)
                if res.status_code == 200:
                    target_id = self._get_chat_id()
                    
                    for item in res.json().get("result", []):
                        self.offset = item["update_id"] + 1
                        
                        if "message" in item and "text" in item["message"]:
                            msg = item["message"]
                            sender_id = str(msg.get("chat", {}).get("id", ""))
                            text = msg["text"].strip()
                            
                            if text in ["/start", "開始"]:
                                if sender_id != target_id:
                                    welcome_msg = f"👋 <b>歡迎使用量化交易終端！</b>\n\n您的專屬綁定金鑰為：\n<code>{sender_id}</code>\n\n👉 請將這串數字複製，並貼入電腦端軟體的「⚙️ 參數設定 > 綁定授權碼」欄位中，儲存後即可開始連線。"
                                    self.session.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": sender_id, "text": welcome_msg, "parse_mode": "HTML"})
                                    print(f"\n[遙控中心] 📥 已配發專屬金鑰給新裝置: {sender_id}")
                                    continue
                                else:
                                    self._exec(text, message_id=msg.get("message_id"))
                                    continue
                                    
                            if sender_id == target_id:
                                self._exec(text, message_id=msg.get("message_id"))
                            else:
                                print(f"\n⚠️ [防護] 攔截到未授權訊息: 來源 ID '{sender_id}'")
                        
                        elif "callback_query" in item:
                            cb = item["callback_query"]
                            sender_id = str(cb.get("message", {}).get("chat", {}).get("id", ""))
                            data_str, msg_id = cb.get("data"), cb.get("message", {}).get("message_id")
                            try: self.session.post(f"https://api.telegram.org/bot{token}/answerCallbackQuery", json={"callback_query_id": cb["id"]}, timeout=5)
                            except: pass
                            if sender_id == target_id and data_str != "dummy":
                                self._handle_callback(data_str, msg_id, sender_id)
            except Exception as e: 
                time_module.sleep(1)
            time_module.sleep(0.1)

    def _update_slider_state(self, st, action):
        if action.endswith("_w_inc"): st['w'] = min(60, st.get('w',5) + 1)
        elif action.endswith("_w_dec"): st['w'] = max(1, st.get('w',5) - 1)
        elif action.endswith("_h_inc"):
            h = st.get('h', 270); st['h'] = 5 if h == 270 else (270 if h >= 120 else h + 5)
        elif action.endswith("_h_dec"):
            h = st.get('h', 270); st['h'] = 120 if h == 270 else (270 if h <= 5 else h - 5)
        elif action.endswith("_g_next"): st['g_idx'] = st.get('g_idx',0) + 1
        elif action.endswith("_g_prev"): st['g_idx'] = st.get('g_idx',0) - 1

    def _handle_callback(self, data_str, msg_id, chat_id):
        # 🟢 修正：防護鎖獨立，解決盤後按鈕失效的「黑洞 Bug」
        if data_str in ["cmd_update_kline", "cmd_opt_sim", "cmd_fetch_data", "cmd_avg_high", "max_execute", "sim_execute"]:
            if getattr(sys_state, 'trading', False):
                return self.send_message("⚠️ <b>拒絕存取：盤中監控正在執行中！</b>\n為確保交易零延遲與資料庫安全，盤中禁止執行耗時運算任務。", force=True)

        if data_str == "cmd_close_menu": return self.edit_message_text(msg_id, "✅ <b>面板已關閉。</b>")
        
        elif data_str == "cmd_export_csv":
            self.edit_message_text(msg_id, "⏳ <b>正在產生完整對帳單...</b>")
            try:
                df = pd.read_sql("SELECT * FROM trade_logs ORDER BY id DESC", sys_db.conn)
                file_name = f"Trade_Logs_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                df.to_csv(file_name, index=False, encoding='utf-8-sig') 
                self.send_document(file_name, f"📥 <b>您的歷史交易紀錄已匯出</b>\n結算時間：{datetime.now().strftime('%Y-%m-%d %H:%M')}")
                self.edit_message_text(msg_id, "✅ <b>對帳單已成功匯出！</b>\n請查看下方檔案。")
                time_module.sleep(5)
                if os.path.exists(file_name): os.remove(file_name) 
            except Exception as e: self.edit_message_text(msg_id, f"❌ 匯出失敗：{e}")
            
        elif data_str == "cmd_stop_trading":
            sys_state.stop_trading_flag = True
            self.edit_message_text(msg_id, "⏸️ <b>已發送終止指令！</b>\n系統將在本次掃描迴圈結束後，安全退出盤中監控模式。\n(注意：這不會平倉您目前的庫存)")

        elif data_str in ["tut_phone", "tut_pc"]:
            try:
                res = pd.read_sql(f"SELECT content FROM system_docs WHERE doc_id='{data_str}'", sys_db.conn)
                if not res.empty:
                    return self.edit_message_text(msg_id, res.iloc[0]['content'], {"inline_keyboard": [[{"text": "🔙 關閉指南", "callback_data": "cmd_close_menu"}]]})
            except Exception as e: print(e)
            return self.edit_message_text(msg_id, "⚠️ 讀取教學文件失敗。")

        elif data_str == "cmd_list_groups":
            self.edit_message_text(msg_id, "📁 <b>請選擇要查看的股票族群：</b>", self.get_groups_keyboard())
        elif data_str.startswith("grp_show_"):
            grp_name = data_str.replace("grp_show_", "")
            mat = load_matrix_dict_analysis()
            if grp_name in mat:
                syms = mat[grp_name]
                msg = f"📁 <b>【{grp_name}】</b> (共 {len(syms)} 檔)\n" + "━"*15 + "\n"
                load_twse_name_map() 
                for s in syms:
                    msg += f"• <code>{sn(str(s)).strip()}</code>\n"
                msg += f"\n💡 直接輸入代號即可查看走勢圖"
                kb = {"inline_keyboard": [[{"text": "🔙 返回清單", "callback_data": "cmd_list_groups"}]]}
                self.edit_message_text(msg_id, msg, kb)

        elif data_str.startswith("sim_") or data_str.startswith("trade_"):
            if chat_id not in self.ui_states: return
            st, prefix = self.ui_states[chat_id], data_str.split("_")[0]
            if data_str.endswith("_execute"):
                if prefix == "trade":
                    # 🟢 使用 is_monitoring 完美防止多執行緒撞車
                    if getattr(sys_state, 'is_monitoring', False):
                        return self.edit_message_text(msg_id, "✅ <b>盤中監控早已啟動並持續運作中！</b>\n請放心，系統正在為您盯盤。\n\n💡 欲查看目前持倉，請點擊主選單的「📊 即時持倉監控」。")
                    
                    w, h = st['w'], st['h']
                    h_val = None if h == 270 else h
                    self.edit_message_text(msg_id, f"🚀 <b>啟動盤中監控</b>\n參數：等待 {w}分 / 持有 {'尾盤(F)' if h==270 else str(h)+'分'}\n系統已在背景執行。")
                    threading.Thread(target=start_trading, args=('full', w, h_val), daemon=True).start()
                else:
                    groups = ["所有族群"] + list(load_matrix_dict_analysis().keys())
                    grp = groups[st.get('g_idx', 0) % len(groups)]
                    w, h = st['w'], st['h']
                    self.edit_message_text(msg_id, f"⏳ <b>啟動回測 ({grp})</b>\n等待 {w}分 / 持有 {'尾盤(F)' if h==270 else str(h)+'分'}...")
                    cmd_txt = f"內部回測 {grp} {w} {'F' if h==270 else h}"
                    threading.Thread(target=self._run_quick_backtest, args=(cmd_txt, msg_id), daemon=True).start()
                return
            
            self._update_slider_state(st, data_str)
            title = "🎯 <b>設定自選進場 (回測)</b>" if prefix == "sim" else "▶️ <b>設定盤中監控參數</b>"
            self.edit_message_text(msg_id, f"{title}\n請點擊加減號調整數值：", self.get_slider_keyboard(st, prefix))

        elif data_str.startswith("max_"):
            if chat_id not in self.ui_states: return
            st = self.ui_states[chat_id]
            if data_str == "max_ws_inc": st['ws'] += 1
            elif data_str == "max_ws_dec": st['ws'] = max(1, st['ws'] - 1)
            elif data_str == "max_we_inc": st['we'] += 1
            elif data_str == "max_we_dec": st['we'] = max(st['ws'], st['we'] - 1)
            elif data_str == "max_hs_inc": h=st['hs']; st['hs'] = 5 if h==270 else (270 if h>=120 else h+5)
            elif data_str == "max_hs_dec": h=st['hs']; st['hs'] = 120 if h==270 else (270 if h<=5 else h-5)
            elif data_str == "max_he_inc": h=st['he']; st['he'] = 5 if h==270 else (270 if h>=120 else h+5)
            elif data_str == "max_he_dec": h=st['he']; st['he'] = 120 if h==270 else (270 if h<=5 else h-5)
            elif data_str == "max_g_next": st['g_idx'] += 1
            elif data_str == "max_g_prev": st['g_idx'] -= 1
            elif data_str == "max_execute":
                self.edit_message_text(msg_id, "✅ 參數已鎖定，準備調度運算資源...")
                groups = ["所有族群"] + list(load_matrix_dict_analysis().keys())
                grp = groups[st['g_idx'] % len(groups)]
                cmd_txt = f"內部極大化 {grp} {st['ws']} {st['we']} {st['hs']} {st['he']}"
                threading.Thread(target=self._run_maximize, args=(cmd_txt, msg_id), daemon=True).start()
                return
            self.edit_message_text(msg_id, "🎛️ <b>設定破解參數</b>\n請點擊加減號調整數值：", self.get_max_builder_keyboard(st))

        elif data_str == "cmd_opt_sim":
            if hasattr(self, 'opt_thread') and self.opt_thread.isRunning():
                return self.send_message("⚠️ <b>掃描正在進行中</b>，請等待完成。", force=True)
            self.edit_message_text(msg_id, "⏳ <b>啟動智能 DTW 門檻掃描...</b>\n此運算較耗時，完成後將回傳報表給您。")
            self.opt_thread = OptimizeSimilarityThread(5, 270)
            dtw_logs = [] 
            def _opt_log(msg):
                try:
                    clean = re.sub(r'<[^>]+>', '', str(msg)).replace('&nbsp;', ' ').replace('&gt;', '>')
                    if clean.strip(): 
                        print(f"[DTW掃描] {clean}")
                        dtw_logs.append(clean) 
                except: pass
            def _on_dtw_finish(*args):
                print("[DTW掃描] ✅ 掃描完成，正在發送結果至手機...")
                res_text = "\n".join(dtw_logs)
                tg_msg = f"✅ <b>智能 DTW 門檻掃描結果</b>\n<pre>{html.escape(res_text)}</pre>"
                self.send_message(tg_msg, force=True)
            self.opt_thread.log_signal.connect(_opt_log, Qt.DirectConnection)
            self.opt_thread.finished_signal.connect(_on_dtw_finish, Qt.DirectConnection)
            self.opt_thread.start()

        elif data_str == "cmd_fetch_data":
            if hasattr(self, 'fetch_thread') and self.fetch_thread.isRunning():
                return self.send_message("⚠️ <b>採集正在進行中</b>，請等待完成。", force=True)
            self.edit_message_text(msg_id, "📥 <b>啟動大數據採集 (預設 5 天)...</b>\n系統正於背景處理，完成後將通知您。")
            self.fetch_thread = FetchSimilarityDataThread(5)
            def _fetch_log(msg):
                try:
                    clean = re.sub(r'<[^>]+>', '', str(msg)).replace('&nbsp;', ' ').replace('&gt;', '>')
                    if clean.strip(): print(f"[數據採集] {clean}")
                except: pass
            def _on_fetch_finish(*args):
                print("\n[數據採集] ✅ 大數據採集完成！已為 DTW 掃描準備好足夠的樣本。")
                self.send_message("✅ <b>大數據採集結束</b>\n已準備好足夠的分析樣本。", force=True)
            self.fetch_thread.log_signal.connect(_fetch_log, Qt.DirectConnection)
            self.fetch_thread.finished_signal.connect(_on_fetch_finish, Qt.DirectConnection)
            self.fetch_thread.start()

        elif data_str == "cmd_avg_high":
            self.edit_message_text(msg_id, "⏳ <b>正在計算平均過高間隔...</b>")
            def _run():
                print("\n[遙控中心] 啟動計算全部族群平均過高...")
                avgs = [avg for g in load_matrix_dict_analysis().keys() if (avg := calculate_average_over_high(g))]
                if avgs:
                    ans = sum(avgs)/len(avgs)
                    self.edit_message_text(msg_id, f"✅ <b>平均過高間隔：</b>\n<code>{ans:.2f}</code> 分鐘")
                    print(f"[遙控中心] ✅ 計算完成！全市場平均過高間隔為: {ans:.2f} 分鐘")
                else:
                    self.edit_message_text(msg_id, "⚠️ 無法計算，請確認數據是否充足。")
            threading.Thread(target=_run, daemon=True).start()

        elif data_str == "cmd_update_kline":
            self.edit_message_text(msg_id, "⏳ <b>啟動 K 線採集引擎...</b>\n請稍候。")
            def _run_kline():
                if not self.task_lock.acquire(blocking=False): return
                try:
                    last_t = 0
                    def _cb(pct, msg_txt):
                        nonlocal last_t
                        now = time_module.time()
                        if now - last_t > 2.0 or pct >= 100:
                            bar = "▓" * (pct // 10) + "░" * (10 - (pct // 10))
                            self.edit_message_text(msg_id, f"🔄 <b>資料更新中...</b>\n\n進度: <code>[{bar}] {pct}%</code>\n狀態: {msg_txt}")
                            last_t = now
                    update_kline_data(tg_progress_cb=_cb)
                    self.edit_message_text(msg_id, "✅ <b>資料更新完成</b>\n進度: <code>[▓▓▓▓▓▓▓▓▓▓] 100%</code>")
                finally: self.task_lock.release()
            threading.Thread(target=_run_kline, daemon=True).start()

        elif data_str == "confirm_close_all":
            self.edit_message_text(msg_id, "✅ <b>已授權，系統正在執行市價平倉...</b>")
            threading.Thread(target=exit_trade_live, daemon=True).start()

    def _exec(self, cmd, message_id=None):
        cmd = cmd.strip()
        mat = load_matrix_dict_analysis()
        
        if re.match(r'^[a-zA-Z0-9]{4,6}$', cmd) or cmd in mat:
            self.send_chat_action("upload_photo")
            def _send_chart():
                try:
                    load_twse_name_map()
                    buf = get_group_chart_bytes(cmd) if cmd in mat else get_stock_chart_bytes(cmd)
                    if buf: 
                        display_name = cmd if cmd in mat else sn(str(cmd)).strip()
                        self.send_photo(buf, caption=f"📊 <b>{display_name} 分析圖</b>")
                    else: 
                        self.send_message(f"⚠️ 無法生成圖表，請確認是否有當日數據。", force=True)
                except Exception as e: pass
            threading.Thread(target=_send_chart, daemon=True).start()
            return

        if cmd in ["/start", "/menu", "開始", "選單"]:
            my_id = self._get_chat_id()
            msg = f"🤖 <b>量化交易終端系統已連線</b>\n🔑 您的專屬金鑰：<code>{my_id}</code>\n\n👇 請選擇下方實體按鈕操作："
            self.send_message(msg, force=True, reply_markup=self.get_bottom_keyboard())
            tut_kb = {"inline_keyboard": [[{"text": "📱 手機端操作教學", "callback_data": "tut_phone"}, {"text": "💻 電腦端協作教學", "callback_data": "tut_pc"}]]}
            self.send_message("📖 <b>系統教學手冊</b>\n初次使用建議閱讀指南：", force=True, reply_markup=tut_kb)
            return
            
        if cmd == "▶ 啟動盤中監控":
            # 🟢 修正：使用 is_monitoring，完美防護
            if getattr(sys_state, 'is_monitoring', False):
                return self.send_message("✅ <b>盤中監控早已啟動並持續運作中！</b>\n💡 欲查看目前持倉，請點擊「📊 即時持倉監控」。", force=True)
            if not getattr(sys_config, 'esun_id', '') or not getattr(sys_config, 'esun_pwd', ''):
                msg = "⚠️ <b>尚未綁定登入憑證</b>\n請輸入以下格式進行安全綁定：\n\n<code>登入 身分證 登入密碼 憑證密碼</code>"
                return self.send_message(msg, force=True)
            
            chat_id = self._get_chat_id()
            self.ui_states[chat_id] = {'w': 5, 'h': 270}
            self.send_message("▶️ <b>設定盤中監控參數</b>\n請調整數值：", force=True, reply_markup=self.get_slider_keyboard(self.ui_states[chat_id], "trade"))
            
        elif cmd.startswith("登入"):
            parts = cmd.split()
            if len(parts) == 4:
                sys_config.esun_id, sys_config.esun_pwd, sys_config.esun_cert_pwd = parts[1], parts[2], parts[3]
                self.send_message("✅ <b>憑證驗證成功</b>\n請點擊「▶ 啟動盤中監控」開始監控。", force=True)
            else:
                self.send_message("⚠️ <b>格式錯誤</b>\n請輸入：<code>登入 身分證 密碼 憑證密碼</code>", force=True)
                
        elif cmd == "📊 即時持倉監控":
            with sys_state.lock:
                if not sys_state.open_positions: return self.send_message("📊 目前 <b>無任何持倉</b>。", force=True)
                msg = "📊 <b>持倉狀態</b>\n" + "━"*15 + "\n"
                for sym, p in sys_state.open_positions.items():
                    name_dict = globals().get('twse_name_map', {})
                    n = name_dict.get(str(sym), "")
                    display = f"{sym} {n}".strip()
                    msg += f"• <code>{html.escape(display)}</code>\n  數量: {p['shares']} 張 | 進場: {p['entry_price']:.2f}\n  停損: {p['stop_loss']:.2f}\n" + "━"*15 + "\n"
                self.send_message(msg, force=True)
                
        elif cmd == "📊 盤後數據與分析":
            self.send_message("📊 <b>盤後數據與分析</b>\n請選擇分析項目：", force=True, reply_markup=self.get_analysis_menu())
            
        elif cmd == "🎯 自選進場模式":
            chat_id = self._get_chat_id()
            self.ui_states[chat_id] = {'g_idx': 0, 'w': 5, 'h': 270}
            self.send_message("🎯 <b>設定自選進場</b>\n請調整數值：", force=True, reply_markup=self.get_slider_keyboard(self.ui_states[chat_id], "sim"))
            
        elif cmd == "💰 極大化利潤":
            chat_id = self._get_chat_id()
            self.ui_states[chat_id] = {'g_idx': 0, 'ws': 3, 'we': 5, 'hs': 10, 'he': 20}
            self.send_message("🎛️ <b>設定破解參數</b>\n請調整數值：", force=True, reply_markup=self.get_max_builder_keyboard(self.ui_states[chat_id]))
            
        elif cmd == "📁 管理股票族群":
            self.send_message("📁 <b>請選擇要展開的族群：</b>", force=True, reply_markup=self.get_groups_keyboard())
            
        elif cmd == "🔄 更新 K 線數據":
            if getattr(sys_state, 'trading', False):
                return self.send_message("⚠️ <b>拒絕存取：盤中監控正在執行中！</b>\n為避免 CPU 滿載與資料庫衝突，禁止於盤中更新資料。", force=True)
            kb = {"inline_keyboard": [[{"text": "✅ 確定更新", "callback_data": "cmd_update_kline"}], [{"text": "❌ 取消", "callback_data": "cmd_close_menu"}]]}
            self.send_message("⚠️ <b>即將更新 K 線數據</b>\n此操作需耗時數分鐘，確定執行？", force=True, reply_markup=kb)
            
        elif cmd == "📜 歷史交易紀錄":
            try:
                df = pd.read_sql("SELECT timestamp, action, symbol, profit FROM trade_logs ORDER BY id DESC LIMIT 15", sys_db.conn)
                if df.empty: return self.send_message("📜 尚無歷史交易紀錄。", force=True)
                msg = "📜 <b>最近 15 筆交易紀錄</b>\n" + "━"*15 + "\n"
                name_dict = globals().get('twse_name_map', {})
                for _, r in df.iterrows():
                    icon = "🔴" if r['profit'] > 0 else "🟢" if r['profit'] < 0 else "⚪"
                    s = r['symbol']
                    display = f"{s} {name_dict.get(str(s), '')}".strip()
                    msg += f"[{r['timestamp'][11:16]}] {r['action']} <code>{display}</code> | {icon} {r['profit']:.0f}\n"
                
                kb = {"inline_keyboard": [[{"text": "📥 下載完整 CSV 對帳單", "callback_data": "cmd_export_csv"}]]}
                self.send_message(msg, force=True, reply_markup=kb)
            except: self.send_message("❌ 無法讀取資料庫。", force=True)
            
        elif cmd == "📈 畫圖查看走勢":
            self.send_message("💡 <b>智能畫圖功能已啟用！</b>\n請直接在聊天框輸入「股票代號」或「族群名稱」！", force=True)
            
        elif cmd == "⚙️ 參數設定":
            msg = f"⚙️ <b>系統參數狀態</b>\n• 單筆資金: {sys_config.capital_per_stock} 萬\n• 手續費/折數/稅: {sys_config.transaction_fee}% / {sys_config.transaction_discount}折 / {sys_config.trading_tax}%\n• 發動觀察時間: {sys_config.momentum_minutes}分\n\n💡 欲修改請輸入：\n<code>設定 資金 500</code>"
            self.send_message(msg, force=True)
            
        elif cmd == "🛑 緊急/手動平倉":
            kb = {"inline_keyboard": [
                [{"text": "💥 確定全數市價平倉", "callback_data": "confirm_close_all"}], 
                [{"text": "⏸️ 退出盤中監控模式 (不平倉)", "callback_data": "cmd_stop_trading"}],
                [{"text": "❌ 取消", "callback_data": "cmd_close_menu"}]
            ]}
            self.send_message("⚠️ <b>【安全確認中心】</b>\n請選擇您要執行的緊急操作：", force=True, reply_markup=kb)

        elif cmd.startswith("設定"):
            if getattr(sys_state, 'trading', False):
                return self.send_message("⚠️ <b>拒絕存取：盤中監控執行中</b>\n為免干擾策略執行，盤中禁止修改任何核心參數！", force=True)
            parts = cmd.split()
            if len(parts) >= 3:
                k, v = parts[1], parts[2]
                try:
                    if k == "資金": sys_config.capital_per_stock = int(v)
                    elif k == "發動時間": sys_config.momentum_minutes = int(v)
                    save_settings(); self.send_message(f"✅ {k} 已更新為: {v}", force=True)
                except: pass

    def _run_maximize(self, cmd_text, msg_id):
        parts = cmd_text.split()
        grp, ws, we, hs, he = parts[1], int(parts[2]), int(parts[3]), int(parts[4]), int(parts[5])
        hold_opts = list(range(hs, he + 1))
        if not self.task_lock.acquire(blocking=False): return self.edit_message_text(msg_id, "⚠️ 系統忙碌中。")
        try:
            mat, (d_kline, i_kline), dispo = load_matrix_dict_analysis(), load_kline_data(), load_disposition_stocks()
            results_df = pd.DataFrame(columns=['等待時間', '持有時間', '總利潤'])
            total_steps, step, last_t = (we - ws + 1) * len(hold_opts), 0, 0
            for w in range(ws, we + 1):
                for h_val in hold_opts:
                    tp_sum = 0
                    if grp != "所有族群":
                        data = initialize_stock_data([s for s in mat.get(grp, []) if s not in dispo], d_kline, i_kline)
                        tp, _, _ = process_group_data(data, w, h_val, mat, verbose=False)
                        if tp is not None: tp_sum = tp
                    else:
                        for g, s in mat.items():
                            data = initialize_stock_data([x for x in s if x not in dispo], d_kline, i_kline)
                            tp, _, _ = process_group_data(data, w, h_val, mat, verbose=False)
                            if tp is not None: tp_sum += tp
                    results_df = pd.concat([results_df, pd.DataFrame([{'等待時間': w, '持有時間': h_val, '總利潤': float(tp_sum)}])], ignore_index=True)
                    step += 1
                    now = time_module.time()
                    pct = int((step / total_steps) * 100)
                    if now - last_t > 2.5 or pct == 100:
                        bar = "▓" * (pct // 10) + "░" * (10 - (pct // 10))
                        self.edit_message_text(msg_id, f"💻 <b>雲端算力破解中 ({grp})</b>\n\n進度: <code>[{bar}] {pct}%</code>\n測試: 等待 {w}分 / 持有 {h_val}分")
                        last_t = now
            if not results_df.empty:
                best = results_df.loc[results_df['總利潤'].idxmax()]
                res_msg = f"🏆 <b>最佳化完成 ({grp})</b>\n━━━━━━━━━━━━━━\n🥇 <b>最佳組合：</b>等 {best['等待時間']}分 / 持 {best['持有時間']}分\n💰 利潤：<b>{int(best['總利潤'])}</b> 元\n\n📊 <b>排行榜：</b>\n"
                for r, (_, row) in enumerate(results_df.sort_values(by='總利潤', ascending=False).head(3).iterrows(), 1):
                    res_msg += f"{r}. 等{row['等待時間']:>2} / 持{str(row['持有時間']):>2} ➔ {int(row['總利潤'])}元\n"
                self.edit_message_text(msg_id, res_msg)
            else: self.edit_message_text(msg_id, "⚠️ 無任何交易產生。")
        finally: self.task_lock.release()

    def _run_quick_backtest(self, cmd_text, msg_id=None):
        w, h, target_group = 5, None, None
        parts = cmd_text.replace("🧪", "").strip().split()
        if len(parts) >= 4 and parts[0] == "內部回測":
            target_group = parts[1]
            try: w = int(parts[2])
            except: pass
            try: h = None if parts[3].upper() == 'F' else int(parts[3])
            except: pass
        def _notify(txt, kb=None):
            if msg_id: self.edit_message_text(msg_id, txt, kb)
            else: self.send_message(txt, force=True, reply_markup=kb)
        if not msg_id: _notify(f"⏳ <b>啟動回測...</b>\n⚙️ 參數 ➔ 等待 {w} 分 / 持有 {'尾盤' if h is None else str(h)+'分'}")
        if not self.task_lock.acquire(blocking=False): return _notify("⚠️ 系統正在執行其他任務，請稍後再試。")
        try:
            mat, d_kline, i_kline = load_matrix_dict_analysis(), *load_kline_data()
            dispo = load_disposition_stocks()
            if not mat or not d_kline: return _notify("⚠️ <b>回測失敗：</b>請先在軟體內「更新 K 線數據」。")
            tp_sum, rate_list, all_trades = 0, [], []
            mat_to_run = {target_group: mat[target_group]} if target_group and target_group != "所有族群" and target_group in mat else mat
            total_groups = len(mat_to_run)
            for i, (g, s) in enumerate(mat_to_run.items()):
                data = initialize_stock_data([x for x in s if x not in dispo], d_kline, i_kline)
                tp, ap, t_hist, _ = process_group_data(data, w, h, mat, verbose=False)
                if tp is not None: tp_sum += tp; rate_list.append(ap); all_trades.extend(t_hist)
                pct = int(((i + 1) / total_groups) * 100)
                if msg_id and total_groups > 1 and (pct % 20 == 0 or pct == 100):
                    bar = "▓" * (pct // 10) + "░" * (10 - (pct // 10))
                    _notify(f"💻 <b>雲端回測運算中...</b>\n\n進度: <code>[{bar}] {pct}%</code>\n正在計算: 【{g}】")
            if rate_list:
                avg_rate = sum(rate_list) / len(rate_list)
                td = "".join([f"• <code>{html.escape(sn(t['symbol']))}</code> | {'🔴' if t['profit']>0 else '🟢'} {int(t['profit'])}元\n" for t in all_trades])
                if len(td) > 2500: td = td[:2500] + "\n... (資料過多已省略)"
                msg = f"📊 <b>全市場回測報告</b>\n━━━━━━━━━━━━━━\n⚙️ 參數：等待 {w} 分 / 持有 {'尾盤' if h is None else str(h)+'分'}\n💰 總利潤：<b>{int(tp_sum)}</b> 元\n📈 平均報酬：<b>{avg_rate:.2f}%</b>\n🤝 交易筆數：<b>{len(all_trades)}</b> 筆\n━━━━━━━━━━━━━━\n<b>【進場標的清單】</b>\n{td}\n"
                _notify(msg)
            else: _notify(f"📊 <b>今日回測報告</b>\n今日無任何符合進場條件的標的。")
        finally: self.task_lock.release()

tg_bot = CloudBrainManager()

# ==================== 核心邏輯區 ====================
def calculate_dtw_pearson(df_lead, df_follow, window_start, window_end):
    if isinstance(window_start, str):
        try: window_start = pd.to_datetime(window_start, format="%H:%M:%S").time()
        except: window_start = pd.to_datetime(window_start).time()
    if isinstance(window_end, str):
        try: window_end = pd.to_datetime(window_end, format="%H:%M:%S").time()
        except: window_end = pd.to_datetime(window_end).time()
    sub_l = df_lead[(df_lead['time'] >= window_start) & (df_lead['time'] <= window_end)].copy()
    sub_f = df_follow[(df_follow['time'] >= window_start) & (df_follow['time'] <= window_end)].copy()
    if len(sub_l) < 3 or len(sub_f) < 3: return 0.0
    merged = pd.merge(sub_l[['time', 'high', 'low', 'close']], sub_f[['time', 'high', 'low', 'close']], on='time', suffixes=('_l', '_f'))
    if len(merged) < 3: return 0.0
    tp_l = (merged['high_l'] + merged['low_l'] + merged['close_l']) / 3
    tp_f = (merged['high_f'] + merged['low_f'] + merged['close_f']) / 3
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        correlation = tp_l.corr(tp_f)
    return 0.0 if pd.isna(correlation) else correlation

def load_target_symbols():
    try:
        data = load_nb_matrix_dict()
        groups = data.get("consolidated_symbols", data)
        symbols = {s for stocks in groups.values() for s in stocks}
        return list(symbols), groups
    except Exception: return [], {}

def get_stop_loss_config(price):
    if price < 10: return sys_config.below_50, 0.01
    elif price < 50: return sys_config.below_50, 0.05
    elif price < 100: return sys_config.price_gap_50_to_100, 0.1
    elif price < 500: return sys_config.price_gap_100_to_500, 0.5
    elif price < 1000: return sys_config.price_gap_500_to_1000, 1
    else: return sys_config.price_gap_above_1000, 5

def show_exit_menu(): print("💡 提示：在 PyQt5 介面中，請直接點選左側面板的【🛑 緊急/手動平倉】按鈕")

def load_nb_matrix_dict():
    if os.path.exists('nb_matrix_dict.json'):
        d = json.load(open('nb_matrix_dict.json', 'r', encoding='utf-8'))
        sys_db.save_state('nb_matrix_dict', d)
        os.remove('nb_matrix_dict.json')
    return sys_db.load_state('nb_matrix_dict', default_value={})

def save_nb_matrix_dict(d):
    sys_db.save_state('nb_matrix_dict', d)

def consolidate_and_save_stock_symbols():
    m = load_matrix_dict_analysis()
    if m: save_nb_matrix_dict({"consolidated_symbols": m})

# 🟢 補回：讀取族群字典的工具函數
def load_group_symbols():
    return load_nb_matrix_dict()

# 🟢 補回：Matplotlib 族群平均勢畫圖核心
def view_kline_data(json_path, symbol_to_group):
    plt.close('all')
    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'SimHei']
    plt.rcParams['axes.unicode_minus'] = False  
    plt.rcParams['figure.max_open_warning'] = 0

    if not os.path.exists(json_path): raise FileNotFoundError(f"找不到檔案：{json_path}")
    with open(json_path, 'r', encoding='utf-8') as f: raw_data = json.load(f)
    
    stock_data = {}
    for symbol, records in raw_data.items():
        df = pd.DataFrame(records)
        if 'time' in df.columns and 'close' in df.columns and 'date' in df.columns:
            df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'], format="%Y-%m-%d %H:%M:%S")
            stock_data[symbol] = df.sort_values(by='datetime')
            
    group_to_stocks = {}
    for symbol, group in symbol_to_group.items():
        if symbol in stock_data: group_to_stocks.setdefault(group, []).append(symbol)
        
    for group, symbols in group_to_stocks.items():
        if not symbols: continue
        fig, ax = plt.subplots(figsize=(14, 7))
        all_z_scores = []
        for symbol in symbols:
            df = stock_data[symbol]
            close = df['close']
            close_z = (close - close.mean()) / close.std() if close.std() != 0 else close - close.mean()
            z_df = pd.DataFrame({'datetime': df['datetime'], symbol: close_z}).set_index('datetime')
            all_z_scores.append(z_df)
            
            # 🟢 修正 2：強制使用 get_stock_name 加上代號，確保圖表顯示 "代號 名稱"
            full_name = f"{symbol} {get_stock_name(symbol)}"
            ax.plot(df['datetime'], close_z, label=full_name, alpha=0.3, linewidth=1.5) 
        
        if all_z_scores:
            group_avg_z = pd.concat(all_z_scores, axis=1).mean(axis=1) 
            ax.plot(group_avg_z.index, group_avg_z.values, color='#E74C3C', linewidth=4, label="族群平均勢 (核心主軸)", zorder=5) 
        
        ax.set_title(f"【{group}】族群連動分析 (Z-Score 標準化)", fontsize=16, fontweight='bold')
        ax.set_xlabel("時間", fontsize=12); ax.set_ylabel("標準化收盤價 (Z-score)", fontsize=12)
        ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0.)
        ax.grid(True, linestyle='--', alpha=0.6); plt.tight_layout()
    plt.show(block=False)

# 🟢 新增：TradingView 風格覆盤圖表 (支援半透明損益區塊 + 觸發事件標註)
def plot_tradingview_chart(symbol, trades_history, events_log, stock_df):
    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    plt.rcParams['figure.max_open_warning'] = 0
    
    df = stock_df.copy()
    if 'datetime' not in df.columns:
        if 'date' in df.columns and 'time' in df.columns:
            df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str), format='mixed')
        else:
            return print("資料缺少 datetime 欄位，無法畫圖")
            
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 畫出走勢線
    ax.plot(df['datetime'], df['close'], color='#2C3E50', linewidth=1.5, label='收盤價')
    
    # 🟢 1. 標註所有「觸發事件」 (即使沒進場也會標示)
    symbol_events = [e for e in events_log if e.get('symbol') == symbol]
    for e in symbol_events:
        try:
            e_time = pd.to_datetime(f"{df['date'].iloc[0]} {e['time']}", format='mixed')
            e_price = e['price']
            event_text = e['event']
            
            # 使用簡單的文字與箭頭標註 (無 Emoji)
            ax.annotate(f"[{event_text}]", xy=(e_time, e_price), xytext=(0, 20), textcoords="offset points",
                        arrowprops=dict(arrowstyle="->", color='purple', alpha=0.5), 
                        fontsize=9, fontweight='bold', color='purple', bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="purple", alpha=0.8))
        except Exception as e: pass

    # 🟢 2. 畫出進出場的半透明損益區塊
    symbol_trades = [t for t in trades_history if t.get('symbol') == symbol]
    for idx, trade in enumerate(symbol_trades):
        try:
            t_entry = pd.to_datetime(f"{df['date'].iloc[0]} {trade['entry_time']}", format='mixed')
            t_exit = pd.to_datetime(f"{df['date'].iloc[0]} {trade['exit_time']}", format='mixed')
            p_entry = trade['entry_price']
            p_exit = trade['exit_price']
            p_stop = trade['stop_loss']
            profit = trade.get('profit', 0)
            
            fill_color = '#E74C3C' if profit > 0 else '#2ECC71' 
            ax.axvspan(t_entry, t_exit, color=fill_color, alpha=0.15)
            
            ax.hlines(y=p_entry, xmin=t_entry, xmax=t_exit, color='blue', linestyle='-', linewidth=2, label='進場價' if idx==0 else "")
            ax.hlines(y=p_exit, xmin=t_entry, xmax=t_exit, color='orange', linestyle='--', linewidth=2, label='出場價' if idx==0 else "")
            ax.hlines(y=p_stop, xmin=t_entry, xmax=t_exit, color='red', linestyle=':', linewidth=1.5, label='停損線' if idx==0 else "")
            
            ax.annotate(f"空 {p_entry:.2f}", xy=(t_entry, p_entry), xytext=(0, -15), textcoords="offset points",
                        arrowprops=dict(arrowstyle="->", color='blue'), fontsize=10, fontweight='bold', color='blue')
            ax.annotate(f"補 {p_exit:.2f}\n({int(profit)}元)", xy=(t_exit, p_exit), xytext=(0, -30), textcoords="offset points",
                        arrowprops=dict(arrowstyle="->", color='orange'), fontsize=10, fontweight='bold', color='orange')
        except Exception as e: pass

    stock_name = sn(symbol)
    total_pnl = sum(t.get('profit', 0) for t in symbol_trades)
    title_color = 'red' if total_pnl > 0 else ('green' if total_pnl < 0 else 'black')
    
    # 標題會顯示是否有交易
    title_text = f"【{stock_name}】策略覆盤 (總損益: {int(total_pnl)} 元)" if symbol_trades else f"【{stock_name}】策略觸發軌跡 (未進場)"
    ax.set_title(title_text, fontsize=18, fontweight='bold', color=title_color)
    ax.set_xlabel("交易時間", fontsize=12)
    ax.set_ylabel("價格", fontsize=12)
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0.)
    plt.tight_layout()
    plt.show(block=False)

# ==================== PyQt5 訊號與重導向 ====================
class EmittingStream(QObject):
    textWritten = pyqtSignal(str)
    def write(self, text): 
        if "Response Code: 0 | Event Code: 0" in text or "Session up" in text: return
        self.textWritten.emit(text)
    def flush(self): pass

class SignalDispatcher(QObject):
    portfolio_updated = pyqtSignal(list)
    progress_updated = pyqtSignal(int, str) 
    progress_visible = pyqtSignal(bool)     
    plot_equity_curve = pyqtSignal(object) # 🟢 資金曲線繪圖訊號

ui_dispatcher = SignalDispatcher()
cached_portfolio_data = []

STOCK_NAME_MAP = {}
def load_twse_name_map(json_path="twse_stocks_by_market.json"):
    global STOCK_NAME_MAP
    if STOCK_NAME_MAP: return
    try:
        if os.path.exists(json_path):
            STOCK_NAME_MAP = json.load(open(json_path, "r", encoding="utf-8")); return
        def fetch_isin(mode):
            r = requests.get(f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}", headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            # 🟢 修正 1：將 big5 升級為 cp950，能解析更多繁體罕見字
            r.encoding = "cp950" 
            return {tds[0].text.strip()[:4]: tds[0].text.strip().split("\u3000", 1)[1] if "\u3000" in tds[0].text.strip() else tds[0].text.strip()[4:] for tr in BeautifulSoup(r.text, "lxml").select("table tr")[1:] if (tds := tr.find_all("td")) and tds[0].text.strip()[:4].isdigit()}
        STOCK_NAME_MAP = {"TSE": fetch_isin("2"), "OTC": fetch_isin("4")}
        json.dump(STOCK_NAME_MAP, open(json_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except: STOCK_NAME_MAP = {}

def get_stock_name(code):
    return next((STOCK_NAME_MAP[m][code] for m in ["TSE", "OTC"] if code in STOCK_NAME_MAP.get(m, {})), "")

def sn(sym): 
    # 🟢 修正：過濾 Telegram HTML 敏感字元與亂碼，防止推播失敗
    clean_name = get_stock_name(sym).replace('\uFFFD', '').replace('<', '').replace('>', '').replace('&', '及')
    return f"{sym} {clean_name}"

def init_esun_client():
    global ESUN_LOGIN_PWD, ESUN_CERT_PWD
    if not os.path.exists('config.ini'): sys.exit(f"{RED}❌ 找不到玉山 API 設定檔{RESET}")
    try:
        config = ConfigParser(); config.read('config.ini', encoding='utf-8-sig') 
        import unittest.mock as mock
        with mock.patch('getpass.getpass', side_effect=[ESUN_LOGIN_PWD, ESUN_CERT_PWD]):
            sdk = EsunMarketdata(config); sdk.login() 
        return sdk.rest_client
    except Exception as e: sys.exit(f"{RED}玉山 API 連線失敗：{e}{RESET}")

# 🟢 新增：全域 API 速率限制器 (確保跨線程/跨次數點擊不會超過 Shioaji 限制)
class APIRateLimiter:
    def __init__(self, max_calls=550, period=60):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def wait_if_needed(self):
        with self.lock:
            now = time_module.time()
            self.calls = [t for t in self.calls if now - t < self.period]
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    print(f"⚠️ API 呼叫已達 {self.max_calls} 次，自動暫停 {sleep_time:.1f} 秒...")
                    time_module.sleep(sleep_time)
                self.calls = []
            self.calls.append(time_module.time())

global_rate_limiter = APIRateLimiter(max_calls=550, period=60)
historical_rate_limiter = APIRateLimiter(max_calls=550, period=60)

# 🟢 新增：透過公開網路爬取「歷史任意一日」的處置股清單
import requests
def fetch_historical_disposition_stocks(target_date_str):
    try:
        date_obj = datetime.strptime(target_date_str, '%Y-%m-%d')
        twse_date = date_obj.strftime('%Y%m%d')
        tpex_date = f"{date_obj.year - 1911}/{date_obj.strftime('%m/%d')}"
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        dispo_stocks = set()

        try: # 上市 (TWSE)
            r1 = requests.get(f"https://www.twse.com.tw/exchangeReport/TWT44U?response=json&date={twse_date}", headers=headers, timeout=5)
            d1 = r1.json()
            if d1.get('stat') == 'OK':
                for row in d1.get('data', []): dispo_stocks.add(row[0])
        except Exception: pass
            
        try: # 上櫃 (TPEx)
            r2 = requests.get(f"https://www.tpex.org.tw/web/bulletin/disposal_information/disposal_information_result.php?l=zh-tw&d={tpex_date}&o=json", headers=headers, timeout=5)
            d2 = r2.json()
            if d2.get('iTotalRecords', 0) > 0:
                for row in d2.get('aaData', []): dispo_stocks.add(row[1])
        except Exception: pass
            
        return list(dispo_stocks)
    except Exception: return []

def safe_esun_api_call(api_func, max_retries=3, **kwargs):
    for attempt in range(max_retries + 1):
        try: 
            res = api_func(**kwargs)
            if isinstance(res, dict) and (res.get('statusCode') == 429 or 'Rate limit' in str(res)): raise Exception("429 Rate limit")
            return res
        except Exception as e:
            if any(x in str(e) for x in ["429", "Too Many Requests", "Rate limit", "502", "503", "504"]):
                if attempt < max_retries: time_module.sleep(2 ** attempt) 
                else: return None
            else: return None
    return None

def _reconnect_shioaji_if_needed():
    print(f"{YELLOW}⚠️ 偵測到 Shioaji 異常，重連中...{RESET}")
    try:
        sys_state.api.login(api_key=shioaji_logic.TEST_API_KEY, secret_key=shioaji_logic.TEST_API_SECRET)
        sys_state.api.activate_ca(ca_path=shioaji_logic.CA_CERT_PATH, ca_passwd=shioaji_logic.CA_PASSWORD)
        time_module.sleep(2); sys_state.to = tp.TouchOrderExecutor(sys_state.api)
        print(f"{GREEN}✅ Shioaji 重連成功！{RESET}")
    except Exception as e: print(f"{RED}❌ 重連失敗: {e}{RESET}")

def safe_place_order(api_instance, contract, order, max_retries=1):
    for attempt in range(max_retries + 1):
        try: return api_instance.place_order(contract, order)
        except Exception as e:
            if attempt < max_retries: _reconnect_shioaji_if_needed()
            else: raise e

def safe_add_touch_condition(to_instance, tcond, max_retries=1):
    for attempt in range(max_retries + 1):
        try: return to_instance.add_condition(tcond)
        except Exception:
            if attempt < max_retries: _reconnect_shioaji_if_needed()

def safe_delete_touch_condition(to_instance, cond, max_retries=1):
    for attempt in range(max_retries + 1):
        try: return to_instance.delete_condition(cond)
        except Exception:
            if attempt < max_retries: _reconnect_shioaji_if_needed()

def calculate_2min_pct_increase_and_highest(new_candle, existing_candles):
    # 🟢 修正：根據 sys_config.momentum_minutes (界面設定) 動態計算動能
    new_candle['2min_pct_increase'], new_candle['highest'] = 0.0, new_candle.get('high', 0)
    if not existing_candles: return new_candle
    
    # 根據用戶設定的「觀察時間」抓取 K 棒
    window = getattr(sys_config, 'momentum_minutes', 2) 
    relevant_candles = (existing_candles[-window:] + [new_candle])
    
    rise_values = [float(c.get('rise', 0.0)) for c in relevant_candles if c.get('rise') is not None]
    if len(rise_values) >= 2:
        # 計算區間內的最大漲幅差
        diff = max(rise_values) - min(rise_values)
        new_candle['2min_pct_increase'] = round(diff if rise_values[-1] >= rise_values[0] else -diff, 2)
        
    new_candle['highest'] = max(max(c.get('highest', 0) for c in existing_candles), new_candle.get('high', 0))
    return new_candle

def calculate_limit_up_price(close_price):
    lu = close_price * 1.10
    return (lu // (0.01 if lu < 10 else 0.05 if lu < 50 else 0.1 if lu < 100 else 0.5 if lu < 500 else 1 if lu < 1000 else 5)) * (0.01 if lu < 10 else 0.05 if lu < 50 else 0.1 if lu < 100 else 0.5 if lu < 500 else 1 if lu < 1000 else 5)

def truncate_to_two_decimals(v): return math.floor(v * 100) / 100 if isinstance(v, float) else v

# 🟢 專門用於「歷史時光機」的分 K 抓取引擎
# 🟢 完美修復版：專門用來抓取「歷史」資料的引擎 (套用完美的時區對齊機制)
def fetch_intraday_data(client, symbol, trading_day, yesterday_close_price, start_time=None, end_time=None):
    historical_rate_limiter.wait_if_needed()
    try:
        _from = datetime.strptime(f"{trading_day} {start_time or '09:00'}", "%Y-%m-%d %H:%M")
        to_dt = datetime.strptime(f"{trading_day} {end_time or '13:30'}", "%Y-%m-%d %H:%M")

        # 🟢 關鍵修復：將 intraday 改為 historical，並透過 from 與 to 指定歷史日期
        # 這樣才能真正向券商要到過去（如 3/12）的分K數據，而不是被強制塞給今天的數據
        api_params = {
            "symbol": symbol, 
            "timeframe": "1", 
            "from": trading_day, 
            "to": trading_day
        }
        candles_rsp = safe_esun_api_call(client.stock.historical.candles, **api_params)

        if not candles_rsp or 'data' not in candles_rsp: return pd.DataFrame()
        df = pd.DataFrame(candles_rsp['data'])
        if df.empty or 'volume' not in df.columns: return pd.DataFrame()

        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)

        # 🟢 套用與實戰相同的時區對齊機制，徹底解決 0 量與空資料過濾問題
        df['datetime'] = pd.to_datetime(df['date'], utc=True).dt.tz_convert('Asia/Taipei').dt.tz_localize(None).dt.floor('min')
        df.set_index('datetime', inplace=True)
        df = df[~df.index.duplicated(keep='last')]

        orig = df.reset_index()[['datetime', 'volume']].rename(columns={'volume': 'orig_volume'})
        df = df.reindex(pd.date_range(start=_from, end=to_dt, freq='1min')).reset_index().rename(columns={'index': 'datetime'})
        df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')
        df['time'] = df['datetime'].dt.strftime('%H:%M:%S')
        df = pd.merge(df, orig, how='left', on='datetime')

        for col in ['open', 'high', 'low', 'close']:
            vals, last_v = df[col].to_numpy(), yesterday_close_price
            for i in range(len(vals)):
                v, c = df.at[i, 'volume'], df.at[i, 'close']
                if v > 0 and not pd.isna(c): last_v = c
                if pd.isna(vals[i]) or v == 0: vals[i] = last_v
            df[col] = vals

        df['volume'] = df['orig_volume'].fillna(0)
        df['symbol'] = symbol
        df['昨日收盤價'] = yesterday_close_price
        df['漲停價'] = truncate_to_two_decimals(calculate_limit_up_price(yesterday_close_price))
        df[['symbol', '昨日收盤價', '漲停價']] = df[['symbol', '昨日收盤價', '漲停價']].ffill().bfill()
        df['rise'] = (df['close'] - df['昨日收盤價']) / df['昨日收盤價'] * 100
        df['highest'] = df['high'].cummax().fillna(yesterday_close_price)

        return df[['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', '昨日收盤價', '漲停價', 'rise', 'highest']]

    except Exception as e:
        print(f"抓取 {symbol} 歷史分K失敗: {e}")
        return pd.DataFrame()

def fetch_realtime_intraday_data(client, symbol, trading_day, yesterday_close_price, start_time=None, end_time=None):
    global_rate_limiter.wait_if_needed() # 確保使用 550 極速
    try:
        _from = datetime.strptime(f"{trading_day} {start_time or '09:00'}", "%Y-%m-%d %H:%M")
        to_dt = datetime.strptime(f"{trading_day} {end_time or '13:30'}", "%Y-%m-%d %H:%M")
        
        # 嚴格依照 1.9.3.2 寫法：不傳入 date 參數，讓它純粹抓當日盤中
        candles_rsp = safe_esun_api_call(client.stock.intraday.candles, symbol=symbol, timeframe='1')
        
        if not candles_rsp or 'data' not in candles_rsp: return pd.DataFrame()
        df = pd.DataFrame(candles_rsp['data'])
        if df.empty or 'volume' not in df.columns: return pd.DataFrame()
        
        # 解決 0 量問題：退回最原始的時間對齊法，杜絕 UTC 偏移
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['datetime'] = pd.to_datetime(df['date'], errors='coerce').dt.tz_localize(None).dt.floor('min')
        df.set_index('datetime', inplace=True)
        
        orig = df.reset_index()[['datetime', 'volume']].rename(columns={'volume': 'orig_volume'})
        df = df.reindex(pd.date_range(start=_from, end=to_dt, freq='1min')).reset_index().rename(columns={'index': 'datetime'})
        df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')
        df['time'] = df['datetime'].dt.strftime('%H:%M:%S')
        df = pd.merge(df, orig, how='left', on='datetime')
        
        for col in ['open', 'high', 'low', 'close']:
            vals, last_v = df[col].to_numpy(), yesterday_close_price
            for i in range(len(vals)):
                v, c = df.at[i, 'volume'], df.at[i, 'close']
                if v > 0 and not pd.isna(c): last_v = c
                if pd.isna(vals[i]) or v == 0: vals[i] = last_v
            df[col] = vals
            
        df['volume'] = df['orig_volume'].fillna(0)
        df['symbol'] = symbol
        df['昨日收盤價'] = yesterday_close_price
        df['漲停價'] = truncate_to_two_decimals(calculate_limit_up_price(yesterday_close_price))
        df[['symbol', '昨日收盤價', '漲停價']] = df[['symbol', '昨日收盤價', '漲停價']].ffill().bfill()
        df['rise'] = (df['close'] - df['昨日收盤價']) / df['昨日收盤價'] * 100
        df['highest'] = df['high'].cummax().fillna(yesterday_close_price)
        
        return df[['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', '昨日收盤價', '漲停價', 'rise', 'highest']]
        
    except Exception as e:
        print(f"抓取 {symbol} 即時分K失敗: {e}")
        return pd.DataFrame()

def fetch_daily_kline_data(client, symbol, days=2, end_date=None):
    historical_rate_limiter.wait_if_needed()
    if end_date is None: end_date = get_recent_trading_day()
    try:
        data = safe_esun_api_call(client.stock.historical.candles, **{"symbol": symbol, "from": (end_date - timedelta(days=days)).strftime('%Y-%m-%d'), "to": end_date.strftime('%Y-%m-%d')})
        if data and 'data' in data and data['data']: return pd.DataFrame(data['data'])
    except Exception: pass
    return pd.DataFrame()

def get_valid_trading_day(client, symbols_to_analyze):
    import random
    import scipy.stats as st
    base_date = get_recent_trading_day()
    
    while True:
        target_date_str = base_date.strftime('%Y-%m-%d')
        ui_dispatcher.progress_updated.emit(5, f"🔍 抽樣檢驗 {target_date_str} 是否為假日...")
        
        sample_size = min(10, len(symbols_to_analyze))
        if sample_size == 0: return base_date
            
        sampled_stocks = random.sample(symbols_to_analyze, sample_size)
        volumes = []
        
        for sym in sampled_stocks:
            try:
                df = fetch_daily_kline_data(client, sym, days=5, end_date=base_date)
                if not df.empty and 'date' in df.columns:
                    target_row = df[df['date'] == target_date_str]
                    if not target_row.empty:
                        vol = pd.to_numeric(target_row.iloc[0]['volume'], errors='coerce')
                        volumes.append(vol if not pd.isna(vol) else 0)
                    else:
                        volumes.append(0) # 沒這天的資料代表沒開盤
                else:
                    volumes.append(0)
            except:
                volumes.append(0)
                
        # 🟢 使用 T 分配信賴區間法判斷是否開盤
        is_holiday = True
        if len(volumes) >= 2:
            mean_vol = np.mean(volumes)
            std_vol = np.std(volumes, ddof=1)
            
            if std_vol == 0:
                upper_bound = mean_vol
            else:
                n = len(volumes)
                t_crit = st.t.ppf(0.975, n-1) # 95% 信賴水準
                upper_bound = mean_vol + t_crit * (std_vol / np.sqrt(n))
                
            # 如果 95% 信賴區間上限大於 10 張，判定為有效開盤日
            if upper_bound > 10: 
                is_holiday = False
        else:
            if sum(volumes) > 0: is_holiday = False
            
        if not is_holiday:
            print(f"✅ {target_date_str} 統計檢驗通過，為有效開盤日！")
            return base_date
        
        print(f"⚠️ T分配判定 {target_date_str} 為國定假日(預期成交量趨近0)，往前搜尋...")
        ui_dispatcher.progress_updated.emit(5, f"⚠️ {target_date_str} 為假日，往前推一天...")
        
        base_date -= timedelta(days=1)
        while base_date.weekday() in [5, 6]:
            base_date -= timedelta(days=1)

def get_recent_trading_day():
    today, now_time = datetime.now().date(), datetime.now().time()
    def last_fri(d):
        while d.weekday() != 4: d -= timedelta(days=1)
        return d
    w = today.weekday()
    if w in [5, 6]: return last_fri(today)
    if w == 0 and now_time < time(13, 30): return last_fri(today)
    if w > 0 and now_time < time(13, 30): return today - timedelta(days=1)
    return today

# ==================== 資料存取介面 (已全面升級為 SQLite) ====================

def save_settings():
    # 🟢 將設定檔寫入 SQLite
    sys_db.save_state('settings', {k: getattr(sys_config, k) for k in vars(sys_config)})

def load_settings():
    # 🟢 從 SQLite 讀取設定檔，若有舊的 settings.json 順便遷移
    if os.path.exists('settings.json'):
        s = json.load(open('settings.json', 'r', encoding='utf-8'))
        sys_db.save_state('settings', s)
        try: os.remove('settings.json')
        except: pass
    else:
        s = sys_db.load_state('settings', default_value={})
    
    for k, v in s.items():
        if hasattr(sys_config, k): setattr(sys_config, k, v)

def load_matrix_dict_analysis(): 
    if os.path.exists('matrix_dict_analysis.json'):
        d = json.load(open('matrix_dict_analysis.json', 'r', encoding='utf-8'))
        sys_db.save_state('matrix_dict_analysis', d)
        try: os.remove('matrix_dict_analysis.json')
        except: pass
    return sys_db.load_state('matrix_dict_analysis', default_value={})

def save_matrix_dict(d): 
    sys_db.save_state('matrix_dict_analysis', d)
    
def save_auto_intraday_data(data):
    # 🟢 修正：實戰過程的分K，強制存入實戰專用表 (live)
    sys_db.save_kline('intraday_kline_live', data)
    # 同步保留一份文字快取 (用於緊急恢復)
    threading.Thread(target=lambda: json.dump(sys_state.in_memory_intraday, open('auto_intraday.json', 'w', encoding='utf-8'), indent=4, ensure_ascii=False) if os.path.exists('auto_intraday.json') else None, daemon=True).start()

def load_disposition_stocks():
    if os.path.exists('Disposition.json'):
        try:
            d = json.load(open('Disposition.json', 'r', encoding='utf-8'))
            sys_db.save_state('disposition_stocks', d)
            os.remove('Disposition.json')
        except: pass
    return sys_db.load_state('disposition_stocks', default_value=[])

def fetch_disposition_stocks(client, matrix_dict):
    dispo = [s for g, stocks in matrix_dict.items() for s in stocks if (res := safe_esun_api_call(client.stock.intraday.ticker, symbol=s)) and res.get('isDisposition', False)]
    # 🟢 修正：改用 SQLite 存儲處置股名單
    sys_db.save_state('disposition_stocks', dispo)

def save_disposition_stocks(d):
    sys_db.save_state('disposition_stocks', d)

def load_kline_data():
    daily = sys_db.load_kline('daily_kline_history')
    intra = sys_db.load_kline('intraday_kline_history')
    return daily, intra

def ensure_continuous_time_series(df):
    df['date'], df['time'] = pd.to_datetime(df['date']), pd.to_datetime(df['time'], format='%H:%M:%S').dt.time
    df.set_index(['date', 'time'], inplace=True)
    df = df.reindex(pd.MultiIndex.from_product([df.index.get_level_values('date').unique(), pd.date_range('09:00', '13:30', freq='1min').time], names=['date', 'time']))
    
    # 🟢 修正：防呆機制，動態檢查 DataFrame 內擁有哪些欄位才進行補齊，防止 KeyError
    fill_cols = [c for c in ['symbol', '昨日收盤價', '漲停價'] if c in df.columns]
    if fill_cols:
        df[fill_cols] = df[fill_cols].ffill().bfill()
        
    if 'high' not in df.columns: df['high'] = df['close']
    
    # 確保 '昨日收盤價' 存在，否則預設為 0
    yc = df['昨日收盤價'] if '昨日收盤價' in df.columns else 0
    df['close'] = df['close'].ffill().fillna(yc)
    
    for c in ['open', 'high', 'low']: df[c] = df[c].ffill().fillna(df['close'])
    df['volume'], df['2min_pct_increase'] = df['volume'].fillna(0), df['2min_pct_increase'].fillna(0.0) if '2min_pct_increase' in df.columns else 0.0
    return df.reset_index()

def initialize_stock_data(symbols, daily, intra):
    # 這裡的 daily 和 intra 參數會由 load_kline_data() 傳入
    # 已由 load_kline_data 確保傳進來的是 'daily_kline_history' 與 'intraday_kline_history'
    return {s: ensure_continuous_time_series(pd.DataFrame(intra[s])).drop(columns=['average'], errors='ignore') 
            for s in symbols if s in intra and not pd.DataFrame(intra[s]).empty}

def purge_disposition_from_nb(disposition_list, nb_path='nb_matrix_dict.json'):
    if not os.path.exists(nb_path): return
    try: nb_dict = json.load(open(nb_path, 'r', encoding='utf-8'))
    except: return
    if 'consolidated_symbols' not in nb_dict or not isinstance(nb_dict['consolidated_symbols'], dict): return
    changed = False
    for grp, syms in nb_dict['consolidated_symbols'].items():
        filtered = [s for s in dict.fromkeys(syms) if s not in disposition_list]
        if len(filtered) != len(syms): nb_dict['consolidated_symbols'][grp] = filtered; changed = True
    if changed: json.dump(nb_dict, open(nb_path, 'w', encoding='utf-8'), ensure_ascii=False, indent=4)

def load_symbols_to_analyze():
    return [s for g in load_matrix_dict_analysis().values() for s in g if s not in load_disposition_stocks()]

def exit_trade(selected_stock_df, shares, entry_price, sell_cost, entry_fee, tax, message_log, current_time, hold_time, entry_time, use_f_exit=False):
    current_time_str = current_time if isinstance(current_time, str) else current_time.strftime('%H:%M:%S')
    selected_stock_df['time'] = pd.to_datetime(selected_stock_df['time'], format='%H:%M:%S').dt.time
    
    if use_f_exit:
        end_price_series = selected_stock_df[selected_stock_df['time'] == datetime.strptime('13:30', '%H:%M').time()]['close']
        if not end_price_series.empty: end_price = end_price_series.values[0]
        else: return None, None
    else:
        entry_index_series = selected_stock_df[selected_stock_df['time'] == (datetime.strptime(entry_time, '%H:%M:%S').time() if isinstance(entry_time, str) else entry_time)].index
        if not entry_index_series.empty and entry_index_series[0] + hold_time < len(selected_stock_df): end_price = selected_stock_df.iloc[entry_index_series[0] + hold_time]['close']
        else: return None, None

    buy_cost = shares * end_price * 1000
    exit_fee = int(buy_cost * (sys_config.transaction_fee * 0.01) * (sys_config.transaction_discount * 0.01))
    profit = sell_cost - buy_cost - entry_fee - exit_fee - tax
    return_rate = (profit * 100) / (buy_cost - exit_fee) if (buy_cost - exit_fee) != 0 else 0.0
    message_log.append((current_time_str, f"{RED}出場！利潤：{int(profit)} 元，報酬率：{return_rate:.2f}%{RESET}"))
    return profit, return_rate

# ------------------ Shioaji API & 平倉邏輯 ------------------
sys_state.api = sj.Shioaji(simulation=True)
try:
    print(f"{YELLOW}⏳ 正在初始化 Shioaji API 並自動登入預設帳戶...{RESET}")
    sys_state.api.login(api_key=shioaji_logic.TEST_API_KEY, secret_key=shioaji_logic.TEST_API_SECRET)
    sys_state.api.activate_ca(ca_path=shioaji_logic.CA_CERT_PATH, ca_passwd=shioaji_logic.CA_PASSWORD)
    print(f"{GREEN}✅ Shioaji 登入成功！合約資料已就緒。{RESET}")
except Exception as e: print(f"{RED}⚠️ Shioaji 初始登入失敗: {e}{RESET}")

try: sys_state.to = tp.TouchOrderExecutor(sys_state.api)
except Exception: print(f"{RED}⚠️ 觸價單模組初始化失敗，請稍後在介面中重新登入。{RESET}"); sys_state.to = None
# 🟢 新增工具函數：負責向 Shioaji 查詢真實的成交均價
def get_actual_fill_price(api_instance, code, action=sj.constant.Action.Buy, fallback_price=0.0):
    """從 Shioaji API 獲取最新的實際成交均價"""
    try:
        api_instance.update_status()
        trades = api_instance.list_trades()
        # 倒序尋找該股票最近一筆符合方向的交易
        for t in reversed(trades):
            if t.contract.code == code and getattr(t.order, 'action', '') == action:
                deals = getattr(t.status, 'deals', [])
                if deals:
                    total_qty = sum(d.quantity for d in deals)
                    if total_qty > 0:
                        return sum(d.price * d.quantity for d in deals) / total_qty
        return fallback_price
    except Exception as e:
        print(f"獲取實際成交價失敗 ({code}): {e}")
        return fallback_price


# 🟢 替換 2：尾盤強制平倉
def exit_trade_live():
    with sys_state.lock: conditions_dict = dict(sys_state.to.conditions)
    exit_data = {code: sum(int(getattr(c.order, 'quantity', 0)) for c in conds) for code, conds in conditions_dict.items() if sum(int(getattr(c.order, 'quantity', 0)) for c in conds) > 0}
    
    load_twse_name_map() # 確保上市櫃對照表有載入
    now_time = datetime.now().time()
    is_late_market = now_time >= time(13, 25) # 判斷是否進入尾盤試算時間
    
    for stock_code, shares in exit_data.items():
        try:
            # 🟢 修正：動態判斷上市 (TSE) 或上櫃 (OTC) 合約，解決上櫃股無法平倉的報錯
            if str(stock_code) in STOCK_NAME_MAP.get("TSE", {}):
                contract = getattr(sys_state.api.Contracts.Stocks.TSE, f"TSE{stock_code}")
            else:
                contract = getattr(sys_state.api.Contracts.Stocks.OTC, f"OTC{stock_code}")

            # 🟢 修正：動態獲取當日真實漲停價
            with sys_state.lock:
                real_limit_up = contract.limit_up
                if sys_state.in_memory_intraday and stock_code in sys_state.in_memory_intraday:
                    if len(sys_state.in_memory_intraday[stock_code]) > 0:
                        real_limit_up = sys_state.in_memory_intraday[stock_code][-1].get('漲停價', real_limit_up)

            # 🟢 修正：尾盤改用 LMT + ROD，非尾盤維持 LMT + IOC
            order_type = sj.constant.OrderType.ROD if is_late_market else sj.constant.OrderType.IOC
            
            safe_place_order(sys_state.api, contract, sys_state.api.Order(action=sj.constant.Action.Buy, price=real_limit_up, quantity=shares, price_type=sj.constant.StockPriceType.LMT, order_type=order_type, order_lot=sj.constant.StockOrderLot.Common, account=sys_state.api.stock_account))
            
            # 🟢 修正：移除競態條件
            with sys_state.lock:
                if stock_code in sys_state.previous_stop_loss:
                    sys_state.previous_stop_loss.remove(stock_code)
                pos = sys_state.open_positions.pop(stock_code, {})
            
            # 🟢 修正：背景執行緒延遲結算
            def finalize_trade(code, qty, position_data, used_limit_up):
                if is_late_market:
                    print(f"⏳ {code} 進入尾盤結算模式，等待 13:30 撮合...")
                    target_time = datetime.now().replace(hour=13, minute=30, second=5, microsecond=0)
                    while datetime.now() < target_time:
                        time_module.sleep(1)
                else:
                    time_module.sleep(0.5) # 非尾盤只需等待 0.5 秒
                    
                actual_price = get_actual_fill_price(sys_state.api, code, action=sj.constant.Action.Buy, fallback_price=used_limit_up)
                
                profit = 0.0
                if position_data:
                    buy_cost = qty * actual_price * 1000
                    exit_fee = int(buy_cost * (sys_config.transaction_fee*0.01) * (sys_config.transaction_discount*0.01))
                    profit = position_data.get('sell_cost', 0) - buy_cost - position_data.get('entry_fee', 0) - exit_fee - position_data.get('tax', 0)

                sys_db.log_trade("平倉", code, qty, actual_price, profit, "13:26 強制平倉")
                tg_bot.send_message(f"⏱ <b>尾盤強制平倉</b>\n標的: <code>{sn(code)}</code>\n實際買回: {actual_price:.2f}\n實現損益: {int(profit)} 元", force=True)
                print(f"{RED}✅ {code} {qty}張 已平倉，成交價 {actual_price:.2f}{RESET}")

            threading.Thread(target=finalize_trade, args=(stock_code, shares, pos, real_limit_up), daemon=True).start()

        except Exception as e: print(f"平倉 {stock_code} 錯誤: {e}")
        
    with sys_state.lock:
        for conds in conditions_dict.values():
            for c in conds: safe_delete_touch_condition(sys_state.to, c)

# 🟢 替換 3：單一/超時平倉
def close_one_stock(code: str):
    with sys_state.lock:
        conds = sys_state.to.conditions.get(code, [])
        qty = sum(getattr(c.order, 'quantity', 0) for c in conds)
    if qty == 0: return print(f"⚠️ {code} 無委託或持倉")
    
    load_twse_name_map() # 確保上市櫃對照表有載入
    now_time = datetime.now().time()
    is_late_market = now_time >= time(13, 25) # 判斷是否進入尾盤
    
    try:
        # 🟢 修正：動態判斷上市 (TSE) 或上櫃 (OTC) 合約
        if str(code) in STOCK_NAME_MAP.get("TSE", {}):
            contract = getattr(sys_state.api.Contracts.Stocks.TSE, f"TSE{code}")
        else:
            contract = getattr(sys_state.api.Contracts.Stocks.OTC, f"OTC{code}")
            
        with sys_state.lock:
            real_limit_up = contract.limit_up
            if sys_state.in_memory_intraday and code in sys_state.in_memory_intraday:
                if len(sys_state.in_memory_intraday[code]) > 0:
                    real_limit_up = sys_state.in_memory_intraday[code][-1].get('漲停價', real_limit_up)

        # 尾盤改用 ROD
        order_type = sj.constant.OrderType.ROD if is_late_market else sj.constant.OrderType.IOC
        safe_place_order(sys_state.api, contract, sys_state.api.Order(action=sj.constant.Action.Buy, price=real_limit_up, quantity=qty, price_type=sj.constant.StockPriceType.LMT, order_type=order_type, order_lot=sj.constant.StockOrderLot.Common, account=sys_state.api.stock_account))
        
        # 🟢 修正：移除競態條件
        with sys_state.lock:
            if code in sys_state.previous_stop_loss:
                sys_state.previous_stop_loss.remove(code)
            pos = sys_state.open_positions.pop(code, {})
            
        def finalize_single_trade(sym, quantity, position_data, used_limit_up):
            if is_late_market:
                print(f"⏳ {sym} 進入尾盤結算模式，等待 13:30 撮合...")
                target_time = datetime.now().replace(hour=13, minute=30, second=5, microsecond=0)
                while datetime.now() < target_time:
                    time_module.sleep(1)
            else:
                time_module.sleep(0.5)
                
            actual_price = get_actual_fill_price(sys_state.api, sym, action=sj.constant.Action.Buy, fallback_price=used_limit_up)
            
            profit = 0.0
            if position_data:
                buy_cost = quantity * actual_price * 1000
                exit_fee = int(buy_cost * (sys_config.transaction_fee*0.01) * (sys_config.transaction_discount*0.01))
                profit = position_data.get('sell_cost', 0) - buy_cost - position_data.get('entry_fee', 0) - exit_fee - position_data.get('tax', 0)

            sys_db.log_trade("平倉", sym, quantity, actual_price, profit, "單一/超時平倉")
            print(f"{GREEN}✅ 已平倉 {sym} 共 {quantity} 張，成交價 {actual_price:.2f}{RESET}")
            tg_bot.send_message(f"⚪ <b>自動平倉執行 (超時/手動)</b>\n標的: <code>{sn(sym)}</code>\n實際買回: {actual_price:.2f}\n實現損益: {int(profit)} 元", force=True)
            
        threading.Thread(target=finalize_single_trade, args=(code, qty, pos, real_limit_up), daemon=True).start()

    except Exception as e: print(f"平倉 {code} 錯誤: {e}")
    
    with sys_state.lock:
        for c in conds: safe_delete_touch_condition(sys_state.to, c)
        sys_state.to.conditions.pop(code, None)


# 🟢 替換 4：自動停損監控邏輯
def monitor_stop_loss_orders(group_positions):
    with sys_state.lock:
        current_codes = set(sys_state.to.conditions.keys()) if isinstance(sys_state.to.conditions, dict) else set()
        if not current_codes and not isinstance(sys_state.to.conditions, dict):
            for cond in sys_state.to.conditions:
                try: current_codes.add(cond.order_contract.code)
                except: pass
                
        # 偵測到條件單消失 (代表停損被觸發)
        if removed_codes := sys_state.previous_stop_loss - current_codes:
            for code in removed_codes:
                print(f"{Fore.RED}🔴 停損單已觸發！{sn(code)} 已送出市價買回委託，正在更新持倉狀態...{Style.RESET_ALL}")
                
                if code in sys_state.open_positions:
                    pos = sys_state.open_positions.pop(code)
                    stop_price = pos.get('stop_loss', 0)
                    
                    actual_price = get_actual_fill_price(sys_state.api, code, action=sj.constant.Action.Buy, fallback_price=stop_price)
                    
                    buy_cost = pos.get('shares', 0) * actual_price * 1000
                    exit_fee = int(buy_cost * (sys_config.transaction_fee*0.01) * (sys_config.transaction_discount*0.01))
                    profit = pos.get('sell_cost', 0) - buy_cost - pos.get('entry_fee', 0) - exit_fee - pos.get('tax', 0)
                    sys_db.log_trade("買回(停損)", code, pos.get('shares', 0), actual_price, profit, "觸價單停損出場")
                    
                    tg_bot.send_message(f"🔴 <b>停損出場觸發</b>\n標的: <code>{sn(code)}</code>\n設定停損: {stop_price:.2f}\n實際買回: {actual_price:.2f}\n實現損益: {int(profit)} 元", force=True)
                else:
                    sys_db.log_trade("買回(停損)", code, 0, 0.0, 0.0, "觸價單停損出場")
                    tg_bot.send_message(f"🔴 <b>停損出場觸發</b>\n標的: <code>{sn(code)}</code>\n已自動送出市價回補委託！", force=True)

            if sys_config.allow_reentry:
                nb = load_nb_matrix_dict().get("consolidated_symbols", {})
                for code in removed_codes:
                    for group, symbols in nb.items():
                        if code in symbols and group in group_positions and group_positions[group] == "已進場": 
                            group_positions[group] = False
                            print(f"⚠️ {group} 族群停損出場：股票 {code}，開放重新進場。")
                            
        sys_state.previous_stop_loss = current_codes.copy()

def update_variable(file_path, var_name, new_value, is_raw=False):
    lines = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            # 確保精準匹配變數名稱，才進行替換
            if line.lstrip().startswith(var_name + "=") or line.lstrip().startswith(var_name + " ="):
                lines.append(f'{var_name} = r"{new_value}"\n' if is_raw else f'{var_name} = "{new_value}"\n')
            else:
                lines.append(line)
    with open(file_path, "w", encoding="utf-8") as f: f.writelines(lines)
    importlib.reload(shioaji_logic)

def initialize_triggered_limit_up(auto_intraday_data: dict):
    for sym, kbars in auto_intraday_data.items():
        for i in range(1, len(kbars)):
            if kbars[i]["high"] == kbars[i]["漲停價"] and kbars[i-1]["high"] < kbars[i]["漲停價"]:
                sys_state.triggered_limit_up.add(sym); break

def calculate_average_over_high(group_name=None, progress_callback=None):
    daily_kline_data, intraday_kline_data = load_kline_data()
    matrix_dict_analysis = load_matrix_dict_analysis()
    if group_name is None: group_name = input("請輸入要分析的族群名稱：")
    if group_name not in matrix_dict_analysis: return None
    symbols_to_analyze = [s for s in matrix_dict_analysis[group_name] if s not in load_disposition_stocks()]
    if not symbols_to_analyze: return None

    print(f"開始分析族群 {group_name} 中的股票...")
    group_over_high_averages, total_symbols = [], len(symbols_to_analyze) 
    
    for i, symbol in enumerate(symbols_to_analyze):
        if progress_callback: progress_callback(int((i / total_symbols) * 100), f"正在分析: {symbol}")
        if symbol not in daily_kline_data or symbol not in intraday_kline_data: continue
        
        df = pd.DataFrame(intraday_kline_data[symbol])
        if df.empty or 'time' not in df.columns: continue
        
        # 🟢 修正：強制轉換時間格式
        df['time_dt'] = pd.to_datetime(df['time'].astype(str), format='mixed')
        df['time_only'] = df['time_dt'].dt.time
        
        # 🟢 關鍵修復：取得欄位索引位置，避免 itertuples 命名混淆
        cols = df.columns.tolist()
        pct_idx = cols.index('2min_pct_increase') + 1 if '2min_pct_increase' in cols else -1
        high_idx = cols.index('high') + 1
        highest_idx = cols.index('highest') + 1
        time_idx = cols.index('time_only') + 1

        c1, c2, peak_h, c2_time, intervals = False, False, None, None, []
        
        for row in df.itertuples():
            curr_time = row[time_idx]
            curr_high = row[high_idx]
            curr_highest = row[highest_idx]
            pct_inc = row[pct_idx] if pct_idx != -1 else 0
            
            if peak_h is None: peak_h = curr_high; continue
            
            # C1: 啟動條件 (放寬至 1.5%)
            if not c1 and pct_inc >= 1.5: 
                c1, c2 = True, False
                peak_h = curr_high
            
            # C2: 回檔判定 (從波段高點滑落)
            if c1 and not c2:
                if curr_high > peak_h:
                    peak_h = curr_high
                elif curr_high < peak_h:
                    c2_time, c2 = curr_time, True
            
            # 過高判定: 盤中最高價突破剛才的波段高點
            elif c2 and curr_highest > peak_h:
                if c2_time: 
                    t1 = datetime.combine(date.today(), c2_time)
                    t2 = datetime.combine(date.today(), curr_time)
                    diff = (t2 - t1).total_seconds() / 60
                    if 1 < diff < 60: intervals.append(diff)
                c1 = c2 = False; c2_time = None; peak_h = curr_highest

        if intervals:
            import numpy as np
            # 過濾離群值
            q1, q3 = np.percentile(intervals, 25), np.percentile(intervals, 75)
            iqr = q3 - q1
            filtered = [inv for inv in intervals if q1 - 1.5 * iqr <= inv <= q3 + 1.5 * iqr]
            if not filtered: filtered = intervals
            group_over_high_averages.append(sum(filtered) / len(filtered))

    if group_over_high_averages:
        avg = sum(group_over_high_averages) / len(group_over_high_averages)
        print(f"{group_name} 平均過高間隔：{avg:.2f} 分鐘")
        return avg
    else:
        print(f"{group_name} 沒有足夠的過高間隔數據。")
        return None

# 🟢 1.9.1 升級：背景繪圖引擎 (免彈窗直接產出圖片 Byte 數據)
import io
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

# 🟢 修正版：Telegram 單股走勢圖生成引擎 (含 KeyError 防護與資料校驗)
def get_stock_chart_bytes(code):
    try:
        # 1. 取得資料，優先取實戰表，次之取歷史表
        raw_data = sys_db.load_kline('intraday_kline_live')
        if not raw_data: 
            raw_data = sys_db.load_kline('intraday_kline_history')
        
        # 🟢 關鍵修正：使用 .get() 避免 KeyError。如果代號不存在，回傳空清單
        stock_records = raw_data.get(str(code), [])
        
        # 2. 檢查資料是否有效
        if not stock_records:
            print(f"⚠️ [Telegram] 資料庫中找不到代號 {code} 的數據。")
            return None
            
        df = pd.DataFrame(stock_records)
        if df.empty or 'time' not in df.columns or 'close' not in df.columns:
            return None
            
        # 3. 資料預處理
        # 確保日期與時間欄位正確合併，並強制轉型為數值以防畫圖出錯
        df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str), errors='coerce')
        df = df.dropna(subset=['datetime']).sort_values(by='datetime')
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 4. 繪圖設定 (Headless 模式，適用於伺服器端生成)
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        
        fig = Figure(figsize=(10, 6))
        canvas = FigureCanvas(fig)
        gs = fig.add_gridspec(2, 1, height_ratios=[3, 1])
        
        # --- 上圖：價格走勢 ---
        ax_price = fig.add_subplot(gs[0])
        ax_price.plot(df['datetime'], df['close'], color='#2980B9', linewidth=2, label='收盤價')
        
        # 標註最高與最低點
        if len(df) > 0:
            max_idx = df['close'].idxmax()
            min_idx = df['close'].idxmin()
            max_p, min_p = df.loc[max_idx, 'close'], df.loc[min_idx, 'close']
            max_t, min_t = df.loc[max_idx, 'datetime'], df.loc[min_idx, 'datetime']
            
            ax_price.plot(max_t, max_p, 'r^', markersize=8) 
            ax_price.annotate(f'最高 {max_p}', (max_t, max_p), textcoords="offset points", 
                             xytext=(0,10), ha='center', color='red', fontweight='bold')
            ax_price.plot(min_t, min_p, 'gv', markersize=8) 
            ax_price.annotate(f'最低 {min_p}', (min_t, min_p), textcoords="offset points", 
                             xytext=(0,-15), ha='center', color='green', fontweight='bold')

        # 取得股票名稱
        load_twse_name_map()
        title_text = sn(str(code)).strip()
        
        ax_price.set_title(f"{title_text} - 當日價量走勢圖", fontsize=16, fontweight='bold')
        ax_price.grid(True, linestyle='--', alpha=0.6)
        
        # --- 下圖：紅綠成交量 ---
        ax_vol = fig.add_subplot(gs[1], sharex=ax_price)
        # 修正：確保 open 與 close 都有資料才計算顏色
        if 'open' in df.columns:
            colors = ['#E74C3C' if c >= o else '#2ECC40' for c, o in zip(df['close'], df['open'])]
        else:
            colors = '#2980B9'
            
        ax_vol.bar(df['datetime'], df['volume'], color=colors, width=0.0005) 
        ax_vol.grid(True, linestyle='--', alpha=0.6)
        ax_vol.set_ylabel("成交量")
        
        fig.tight_layout()
        
        # 5. 轉為 Bytes 輸出
        buf = io.BytesIO()
        fig.savefig(buf, format='png')
        buf.seek(0)
        return buf

    except Exception as e:
        import traceback
        print(f"❌ [Telegram] 生成圖表發生異常錯誤: {e}")
        print(traceback.format_exc())
        return None

def get_group_chart_bytes(group_name):
    groups = load_matrix_dict_analysis()
    if group_name not in groups: return None
    symbols = groups[group_name]
    raw_data = sys_db.load_kline('intraday_kline_live')
    if not raw_data: raw_data = sys_db.load_kline('intraday_kline_history')
    
    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    
    fig = Figure(figsize=(10, 5))
    canvas = FigureCanvas(fig)
    ax = fig.add_subplot(111)
    
    all_z = []
    for sym in symbols:
        if sym not in raw_data: continue
        df = pd.DataFrame(raw_data[sym])
        if df.empty or 'close' not in df.columns: continue
        df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        df = df.sort_values(by='datetime')
        close = df['close']
        z = (close - close.mean()) / close.std() if close.std() != 0 else close - close.mean()
        all_z.append(pd.Series(z.values, index=df['datetime'], name=sym))
        ax.plot(df['datetime'], z, alpha=0.3, linewidth=1)
        
    if all_z:
        avg_z = pd.concat(all_z, axis=1).mean(axis=1)
        ax.plot(avg_z.index, avg_z.values, color='#E74C3C', linewidth=3, label="族群平均勢")
        
    ax.set_title(f"【{group_name}】族群連動分析 (Z-Score)", fontsize=16, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.6)
    ax.legend(loc='upper left')
    
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    return buf

# 🟢 動態進度條 K 線引擎
# 🟢 升級：支援指定歷史日期與多執行緒加速的 K 線引擎
def update_kline_data(tg_progress_cb=None, target_date_str=None):
    def report_progress(p, msg):
        ui_dispatcher.progress_updated.emit(p, msg)
        if tg_progress_cb: tg_progress_cb(p, msg)

    client = init_esun_client()
    matrix_dict_analysis = load_matrix_dict_analysis()
    if not matrix_dict_analysis: return print("沒有任何族群資料，請先管理族群。")
    load_twse_name_map()

    ui_dispatcher.progress_visible.emit(True)
    
    # 判斷要採集的日期
    if target_date_str:
        valid_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
    else:
        # 如果沒傳入，用原本邏輯找最近交易日
        all_syms = [s for g in matrix_dict_analysis.values() for s in g]
        valid_date = get_valid_trading_day(client, all_syms)
        
    trading_day = valid_date.strftime('%Y-%m-%d')
    print(f"📅 【歷史採集模式】開始採集 {trading_day} 的資料...")

    # 🟢 新增：將當前採集的日期寫入狀態，供回測引擎「鎖定」使用
    sys_db.save_state('last_fetched_date', trading_day)

    report_progress(2, f"取得 {trading_day} 處置股清單...")
    hist_dispo = fetch_historical_disposition_stocks(trading_day)
    if hist_dispo:
        sys_db.save_state('disposition_stocks', hist_dispo)
    
    # 這裡保留所有要分析的標的
    symbols_to_analyze = [sym for group in matrix_dict_analysis.values() for sym in group]
    total_syms = len(symbols_to_analyze)

    # 1. 抓取日K (為了昨收價)
    existing_daily_kline_data = sys_db.load_kline('daily_kline_history')
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_sym = {executor.submit(fetch_daily_kline_data, client, sym, 5, valid_date): sym for sym in symbols_to_analyze}
        for i, future in enumerate(as_completed(future_to_sym)):
            sym = future_to_sym[future]
            df = future.result()
            if not df.empty: existing_daily_kline_data[sym] = df.to_dict(orient='records')
    sys_db.save_kline('daily_kline_history', existing_daily_kline_data)

    # 2. 抓取一分K
    intraday_kline_data = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_sym = {}
        for sym in symbols_to_analyze:
            daily_data = existing_daily_kline_data.get(sym, [])
            if not daily_data: continue
            sorted_daily_data = sorted(daily_data, key=lambda x: x['date'], reverse=True)
            # 取得昨收價邏輯 (對標 85克 精準計算)
            yesterday_close_price = sorted_daily_data[1].get('close', 0) if len(sorted_daily_data) > 1 else sorted_daily_data[0].get('close', 0)
            future_to_sym[executor.submit(fetch_intraday_data, client, sym, trading_day, yesterday_close_price, "09:00", "13:30")] = sym

        for i, future in enumerate(as_completed(future_to_sym)):
            sym = future_to_sym[future]
            report_progress(30 + int((i/total_syms)*70), f"歷史一分K: {sn(sym)}")
            intraday_df = future.result()
            if intraday_df.empty: continue
            
            # 計算動能並寫入
            records = intraday_df.to_dict(orient='records')
            updated_records = []
            for j in range(len(records)):
                # 這裡調用你修改後的 1分鐘/2分鐘 動能函數
                updated_records.append(calculate_2min_pct_increase_and_highest(records[j], records[:j]))
            intraday_kline_data[sym] = updated_records

    # 🟢 重要修正：不要直接 DELETE ALL，改為根據日期和代號覆蓋，這樣新增股票才不會殺掉舊股票
    try:
        with sys_db.db_lock:
            with sys_db.conn:
                # 只刪除本次採集日期的資料，保護其他日期的歷史數據
                sys_db.conn.execute("DELETE FROM intraday_kline_history WHERE date = ?", (trading_day,))
    except Exception as e: print(f"清理舊數據失敗: {e}")

    # 儲存到資料庫
    sys_db.save_kline('intraday_kline_history', intraday_kline_data)
    
    # 🟢 重要修正：強制清空記憶體快取，強迫畫圖功能讀取最新的 SQLite 資料
    if hasattr(sys_state, 'in_memory_intraday'):
        sys_state.in_memory_intraday.clear()

    consolidate_and_save_stock_symbols()
    report_progress(100, f"{trading_day} 資料採集完畢")
    ui_dispatcher.progress_visible.emit(False)
    print("✅ 資料採集與記憶體同步完成！")

# 🟢 效能大躍進：消滅 iterrows()
# 🟢 替換 1：回測核心引擎
def process_group_data(stock_data_collection, wait_minutes, hold_minutes, matrix_dict_analysis, verbose=True, progress_callback=None):
    load_twse_name_map()
    in_position, has_exited, current_position, stop_loss_triggered, hold_time, reentry_count = False, False, None, False, 0, 0
    trades_history = [] 
    events_log = [] 

    message_log: list[tuple[str, str]] = []
    tracking_stocks: set[str] = set()
    leader, leader_peak_rise, leader_rise_before_decline = None, None, None
    in_waiting_period, waiting_time, pull_up_entry, limit_up_entry, first_c1_time = False, 0, False, False, None

    merged_df = None
    req_cols = ['time', 'rise', 'high', '漲停價', 'close', '2min_pct_increase', 'volume']
    for sym, df in stock_data_collection.items():
        if not all(c in df.columns for c in req_cols): continue
        
        for col in ['rise', 'high', 'close', 'volume', '漲停價', '2min_pct_increase']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        tmp = df[req_cols].rename(columns={c: f"{c}_{sym}" if c != 'time' else c for c in req_cols})
        merged_df = tmp if merged_df is None else pd.merge(merged_df, tmp, on='time', how='outer')

    if merged_df is None or merged_df.empty: return None, None, [], []
    merged_df.sort_values('time', inplace=True, ignore_index=True)
    total_profit = total_profit_rate = total_trades = 0
    total_rows = len(merged_df)
    
    merged_records = merged_df.to_dict('records')

    for i, row in enumerate(merged_records):
        if progress_callback and i % 5 == 0: progress_callback(int(((i + 1) / total_rows) * 100), f"回測中: {row['time'].strftime('%H:%M')}")
        current_time = row['time']
        current_time_str = current_time.strftime('%H:%M:%S') if not isinstance(current_time, str) else current_time

        FIRST3_AVG_VOL = {}
        for sym, df in stock_data_collection.items():
            valid_times = [t for t in ['09:00:00', '09:01:00', '09:02:00'] if t <= current_time_str]
            if not valid_times: FIRST3_AVG_VOL[sym] = 0
            else:
                first3 = df[df['time'].astype(str).isin(valid_times)]
                FIRST3_AVG_VOL[sym] = first3['volume'].mean() if not first3.empty else 0

        if in_position and not has_exited:
            hold_time += 1
            if current_time_str == '13:30:00' or (current_position.get('actual_hold_minutes') and hold_time >= current_position['actual_hold_minutes']):
                profit, rate = exit_trade(stock_data_collection[current_position['symbol']], current_position['shares'], current_position['entry_price'], current_position['sell_cost'], current_position['entry_fee'], current_position['tax'], message_log, current_time, hold_time, current_position['entry_time'], use_f_exit=(current_time_str == '13:30:00'))
                if profit is not None: 
                    buy_total = current_position['sell_cost'] - profit - current_position['entry_fee'] - current_position['tax']
                    estimated_exit_price = buy_total / (current_position['shares'] * 1000)
                    total_trades += 1; total_profit += profit; total_profit_rate += rate
                    trades_history.append({'symbol': current_position['symbol'], 'entry_time': current_position['entry_time'], 'entry_price': current_position['entry_price'], 'exit_time': current_time_str, 'exit_price': estimated_exit_price, 'profit': profit, 'stop_loss': current_position['stop_loss_threshold'], 'reason': '時間平倉'})
                in_position, has_exited, current_position = False, True, None
                continue

            sel_df = stock_data_collection[current_position['symbol']]
            now_row = sel_df[sel_df['time'] == current_time]
            if not now_row.empty:
                h_now, thresh = truncate_to_two_decimals(now_row.iloc[0]['high']), truncate_to_two_decimals(current_position['stop_loss_threshold'])
                if h_now >= thresh:
                    exit_cost = current_position['shares'] * thresh * 1000
                    exit_fee = int(exit_cost * (sys_config.transaction_fee*0.01) * (sys_config.transaction_discount*0.01))
                    profit = current_position['sell_cost'] - exit_cost - current_position['entry_fee'] - exit_fee - current_position['tax']
                    rate = (profit * 100) / (current_position['sell_cost'] - current_position['entry_fee'] - exit_fee)
                    message_log.append((current_time_str, f"{Fore.RED}停損觸發，利潤 {int(profit)} 元 ({rate:.2f}%){Style.RESET_ALL}"))
                    total_trades += 1; total_profit += profit; total_profit_rate += rate
                    trades_history.append({'symbol': current_position['symbol'], 'entry_time': current_position['entry_time'], 'entry_price': current_position['entry_price'], 'exit_time': current_time_str, 'exit_price': thresh, 'profit': profit, 'stop_loss': current_position['stop_loss_threshold'], 'reason': '停損觸發'})
                    
                    in_position, current_position = False, None
                    
                    reentry_success = False
                    if sys_config.allow_reentry and reentry_count < sys_config.max_reentry_times:
                        lookback_start = (datetime.combine(date.today(), current_time) - timedelta(minutes=sys_config.reentry_lookback_candles)).time()
                        
                        for r_sym in stock_data_collection.keys():
                            df_sym = stock_data_collection[r_sym]
                            sub_df = df_sym[(df_sym['time'] >= lookback_start) & (df_sym['time'] <= current_time)]
                            
                            for _, r_row in sub_df.iterrows():
                                h, lup, pct, vol = r_row.get('high'), r_row.get('漲停價'), r_row.get('2min_pct_increase'), r_row.get('volume')
                                avgv = FIRST3_AVG_VOL.get(r_sym, 0)
                                is_limit_up = (h == lup) if (h is not None and pd.notna(h) and lup is not None and pd.notna(lup)) else False
                                
                                vol_check = (vol > sys_config.volume_multiplier * avgv) if avgv > 0 else (vol >= sys_config.min_volume_threshold)
                                is_pull_up = (pct is not None and pd.notna(pct) and pct >= sys_config.pull_up_pct_threshold and vol is not None and pd.notna(vol) and vol_check and vol >= sys_config.min_volume_threshold)
                                
                                if is_limit_up or is_pull_up:
                                    reentry_success = True
                                    reentry_count += 1
                                    has_exited = False 
                                    
                                    tracking_stocks.clear(); tracking_stocks.add(r_sym)
                                    leader, in_waiting_period, waiting_time = r_sym, True, 0
                                    first_c1_time = datetime.combine(date.today(), current_time)
                                    
                                    cond_str = "漲停" if is_limit_up else "拉高"
                                    limit_up_entry, pull_up_entry = is_limit_up, not is_limit_up
                                    leader_rise_before_decline = r_row['highest']
                                    
                                    if verbose: message_log.append((current_time_str, f"{Fore.MAGENTA}🔄 停損後再進場觸發！回溯發現 {sn(r_sym)} {cond_str}，啟動第 {reentry_count} 次監控{Style.RESET_ALL}"))
                                    events_log.append({'time': current_time_str, 'symbol': r_sym, 'event': f'停損再進場({cond_str})', 'price': r_row['close']})
                                    break
                            if reentry_success: break
                            
                    if not reentry_success:
                        stop_loss_triggered = True
                        if sys_config.allow_reentry and reentry_count < sys_config.max_reentry_times:
                            has_exited = False 
                            reentry_count += 1
                            tracking_stocks.clear()
                            pull_up_entry = limit_up_entry = in_waiting_period = False
                            if verbose: message_log.append((current_time_str, f"{Fore.MAGENTA}🔄 停損結算。回溯無觸發，系統進入獵人模式等待下一次轉折 (剩餘次數: {sys_config.max_reentry_times - reentry_count}){Style.RESET_ALL}"))
                            continue
                        else:
                            has_exited = True
                            break 
                    continue 

            continue 

        trigger_list = []
        for sym in stock_data_collection.keys():
            pct, vol, high, lup = row.get(f'2min_pct_increase_{sym}'), row.get(f'volume_{sym}'), row.get(f'high_{sym}'), row.get(f'漲停價_{sym}')
            avgv = FIRST3_AVG_VOL.get(sym, 0)
            hit_limit = False
            if high is not None and pd.notna(high) and lup is not None and pd.notna(lup) and high == lup:
                if current_time_str == '09:00:00': hit_limit = True
                else:
                    prev_time = (datetime.combine(date.today(), current_time) - timedelta(minutes=1)).time()
                    prev_high = stock_data_collection[sym].loc[stock_data_collection[sym]['time'] == prev_time, 'high']
                    if prev_high.empty or prev_high.iloc[0] < lup: hit_limit = True
            if hit_limit: trigger_list.append({'symbol': sym, 'condition': 'limit_up'}); continue
            
            vol_check = (vol > sys_config.volume_multiplier * avgv) if avgv > 0 else (vol >= sys_config.min_volume_threshold)
            if pct is not None and pd.notna(pct) and pct >= sys_config.pull_up_pct_threshold and vol is not None and pd.notna(vol) and vol_check and vol >= sys_config.min_volume_threshold: 
                trigger_list.append({'symbol': sym, 'condition': 'pull_up'})

        for item in trigger_list:
            sym, cond = item['symbol'], item['condition']
            if cond == 'limit_up':
                tracking_stocks.add(sym)
                leader, in_waiting_period, waiting_time = sym, True, 0
                pull_up_entry, limit_up_entry = False, True
                first_c1_time = datetime.combine(date.today(), current_time) 
                if verbose: message_log.append((current_time_str, f"{Fore.RED}🚀 {sn(sym)} 漲停觸發，強制中斷拉高，升級為漲停進場模式！{Style.RESET_ALL}"))
                events_log.append({'time': current_time_str, 'symbol': sym, 'event': '漲停觸發', 'price': row.get(f'close_{sym}')})
            else:
                if not pull_up_entry and not limit_up_entry: 
                    pull_up_entry, limit_up_entry = True, False
                    tracking_stocks.clear(); first_c1_time = datetime.combine(date.today(), current_time)
                tracking_stocks.add(sym)
                if verbose: message_log.append((current_time_str, f"{YELLOW}{sn(sym)} 拉高觸發，加入追蹤{RESET}"))
                events_log.append({'time': current_time_str, 'symbol': sym, 'event': '拉高觸發', 'price': row.get(f'close_{sym}')}) 

        if pull_up_entry or limit_up_entry:
            for sym in stock_data_collection.keys():
                if sym not in tracking_stocks and (pct := row.get(f'2min_pct_increase_{sym}')) is not None and pd.notna(pct) and pct >= sys_config.follow_up_pct_threshold: 
                    tracking_stocks.add(sym)
                    events_log.append({'time': current_time_str, 'symbol': sym, 'event': '跟漲追蹤', 'price': row.get(f'close_{sym}')}) 

        if tracking_stocks:
            max_sym, max_rise = None, None
            for sym in tracking_stocks:
                if (r := row.get(f'rise_{sym}')) is not None and pd.notna(r) and (max_rise is None or r > max_rise): max_rise, max_sym = r, sym
            
            if leader is None:
                leader = max_sym
                leader_peak_rise = max_rise
            elif max_sym:
                if leader_peak_rise is None: leader_peak_rise = max_rise
                if max_sym == leader:
                    if max_rise > leader_peak_rise: leader_peak_rise = max_rise
                else:
                    if max_rise > leader_peak_rise:
                        if verbose: message_log.append((current_time_str, f"{Fore.CYAN}✨ 領漲替換：{sn(leader)} ➔ {sn(max_sym)}{Style.RESET_ALL}"))
                        events_log.append({'time': current_time_str, 'symbol': max_sym, 'event': '成為新領漲', 'price': row.get(f'close_{max_sym}')}) 
                        leader, leader_peak_rise, leader_rise_before_decline, in_waiting_period, waiting_time = max_sym, max_rise, None, False, 0
                        first_c1_time = datetime.combine(date.today(), current_time)
            
            if leader:
                h_now = row.get(f'high_{leader}')
                prev_time = (datetime.combine(date.today(), current_time) - timedelta(minutes=1)).time()
                prev_row = stock_data_collection[leader][stock_data_collection[leader]['time'] == prev_time]
                if not prev_row.empty and pd.notna(h_now) and h_now <= prev_row.iloc[0]['high'] and not in_waiting_period:
                    leader_highest = stock_data_collection[leader].loc[stock_data_collection[leader]['time'] == current_time, 'highest'].iloc[0]
                    in_waiting_period, waiting_time, leader_rise_before_decline = True, 0, leader_highest
                    if verbose: message_log.append((current_time_str, f"領漲 {sn(leader)} 反轉，確立天花板 {leader_highest}，開始等待"))
                    events_log.append({'time': current_time_str, 'symbol': leader, 'event': '確立天花板', 'price': row.get(f'close_{leader}')}) 

        if in_waiting_period:
            window_start_t = max((datetime.combine(date.today(), first_c1_time.time()) - timedelta(minutes=2)).time(), time(9,0))
            to_remove = [sym for sym in tracking_stocks if sym != leader and calculate_dtw_pearson(stock_data_collection[leader], stock_data_collection[sym], window_start_t, current_time) < sys_config.similarity_threshold]
            for sym in to_remove:
                tracking_stocks.remove(sym)
                if verbose: message_log.append((current_time_str, f"{Fore.RED}[滾動剔除] {sn(sym)} 相似度 < {sys_config.similarity_threshold}{Style.RESET_ALL}"))

            if leader and leader_rise_before_decline is not None and (h_now := row.get(f"high_{leader}")) is not None and pd.notna(h_now) and h_now > leader_rise_before_decline:
                if verbose: message_log.append((current_time_str, f"{Fore.YELLOW}🔥 領漲 {sn(leader)} 突破前高 {leader_rise_before_decline}，漲勢延續，中斷等待！{Style.RESET_ALL}"))
                events_log.append({'time': current_time_str, 'symbol': leader, 'event': '突破前高', 'price': h_now}) 
                leader_highest = stock_data_collection[leader].loc[stock_data_collection[leader]['time'] == current_time, 'highest'].iloc[0]
                leader_rise_before_decline, in_waiting_period, waiting_time = leader_highest, False, 0
                continue  

            if waiting_time >= wait_minutes:
                in_waiting_period, waiting_time = False, 0
                eligible = []
                for sym in set(tracking_stocks):
                    if sym == leader: continue
                    df = stock_data_collection[sym]
                    later = df[(df['time'] >= first_c1_time.time()) & (df['time'] <= current_time)]
                    if later.empty: continue
                    
                    # 🟢 同步優化：與實戰相同的進場量能檢查
                    avgv = FIRST3_AVG_VOL.get(sym, 0)
                    vol_cond = (later['volume'] >= sys_config.volume_multiplier * avgv) if avgv > 0 else (later['volume'] >= sys_config.min_volume_threshold)
                    if not (vol_cond & (later['volume'] >= sys_config.min_volume_threshold)).any(): continue
                    
                    # 🟢 致命修正 2 (對應盤中)：完美的防誘空容錯率邏輯對齊
                    if len(later) >= 2 and later.iloc[-1]['rise'] > later.iloc[:-1]['rise'].max() + sys_config.pullback_tolerance: continue
                    
                    rise_now, price_now = row.get(f'rise_{sym}'), row.get(f'close_{sym}')
                    if rise_now is None or pd.isna(rise_now) or not (sys_config.rise_lower_bound <= rise_now <= sys_config.rise_upper_bound) or price_now is None or pd.isna(price_now) or price_now > sys_config.capital_per_stock*1.5: continue
                    eligible.append({'symbol': sym, 'rise': rise_now, 'row': stock_data_collection[sym].loc[stock_data_collection[sym]['time'] == current_time].iloc[0]})

                if not eligible: pull_up_entry = limit_up_entry = False; tracking_stocks.clear()
                else:
                    eligible.sort(key=lambda x: x['rise'], reverse=True)
                    chosen = eligible[len(eligible) // 2]
                    rowch, entry_p = chosen['row'], chosen['row']['close']
                    shares = round((sys_config.capital_per_stock*10000)/(entry_p*1000))
                    sell_cost = shares * entry_p * 1000
                    entry_fee, tax = int(sell_cost * (sys_config.transaction_fee*0.01) * (sys_config.transaction_discount*0.01)), int(sell_cost * (sys_config.trading_tax*0.01))
                    gap, tick = get_stop_loss_config(entry_p)
                    highest_on_entry = rowch['highest'] or entry_p
                    stop_thr = entry_p + gap/1000 if (highest_on_entry-entry_p)*1000 < gap else highest_on_entry + tick
                    
                    limit_up = row.get(f'漲停價_{chosen["symbol"]}')
                    if limit_up and pd.notna(limit_up):
                        tick_for_limit = 0.01 if limit_up < 10 else 0.05 if limit_up < 50 else 0.1 if limit_up < 100 else 0.5 if limit_up < 500 else 1 if limit_up < 1000 else 5
                        if stop_thr > limit_up - 2 * tick_for_limit: 
                            stop_thr = limit_up - 2 * tick_for_limit

                    actual_hold_minutes = hold_minutes
                    if actual_hold_minutes is not None and (datetime.combine(date.today(), current_time) + timedelta(minutes=actual_hold_minutes)).time() >= time(13, 26): actual_hold_minutes = None
                    
                    current_position = {'symbol': chosen['symbol'], 'shares': shares, 'entry_price': entry_p, 'sell_cost': sell_cost, 'entry_fee': entry_fee, 'tax': tax, 'entry_time': current_time_str, 'stop_loss_threshold': stop_thr, 'actual_hold_minutes': actual_hold_minutes}
                    in_position, has_exited, hold_time = True, False, 0
                    pull_up_entry = limit_up_entry = False; tracking_stocks.clear()
                    if verbose: message_log.append((current_time_str, f"{Fore.GREEN}進場！{sn(chosen['symbol'])} {shares}張 價 {entry_p:.2f} 停損 {stop_thr:.2f}{Style.RESET_ALL}"))
            else:
                waiting_time += 1
                if verbose: message_log.append((current_time_str, f"⏳ 領漲 {sn(leader)} 反轉，等待第 {waiting_time} 分鐘"))

    message_log.sort(key=lambda x: x[0])
    for t, msg in message_log: print(f"[{t}] {msg}")

    if total_trades:
        avg_rate = total_profit_rate / total_trades
        c = RED if total_profit > 0 else ("" if total_profit <= 0 else "")
        print(f"{c}🏁 族群測試完成，利潤：{int(total_profit)} 元 (報酬率：{avg_rate:.2f}%){RESET}\n")
        return total_profit, avg_rate, trades_history, events_log 
    else:
        print(f"🏁 族群測試完成，無任何交易觸發。\n")
        return None, None, [], events_log

# =================================================================================
# 🟢 替換 2：盤中邏輯核心
def process_live_trading_logic(symbols_to_analyze, current_time_str, wait_minutes, hold_minutes, message_log, in_position, has_exited, current_position, hold_time, already_entered_stocks, stop_loss_triggered, final_check_active, final_check_count, in_waiting_period, waiting_time, leader, tracking_stocks, previous_rise_values, leader_peak_rise, leader_rise_before_decline, first_condition_one_time, can_trade, group_positions, nb_matrix_path="nb_matrix_dict.json"):
    monitor_stop_loss_orders(group_positions)
    if sys_state.quit_flag: threading.Thread(target=show_exit_menu, daemon=True).start(); sys_state.quit_flag = False
    try: trading_time = datetime.strptime(current_time_str, "%H:%M").time()
    except ValueError: return
    trading_txt = trading_time.strftime("%H:%M:%S")

    if not os.path.exists(nb_matrix_path): return
    with open(nb_matrix_path, "r", encoding="utf-8") as f: nb_dict = json.load(f)
    consolidated_symbols = nb_dict.get("consolidated_symbols", {})
    if not isinstance(consolidated_symbols, dict): return

    if sys_state.in_memory_intraday:
        with sys_state.lock: auto_intraday_data = sys_state.in_memory_intraday.copy()
    else:
        auto_intraday_data = sys_db.load_kline('intraday_kline_live')
        if not auto_intraday_data: return

    stock_df = {}
    for sym in symbols_to_analyze:
        df = pd.DataFrame(auto_intraday_data.get(sym, [])).copy()
        if not df.empty:
            for col in ['rise', 'high', 'close', 'volume', '漲停價', '2min_pct_increase']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df["time"] = pd.to_datetime(df["time"].astype(str), format='mixed').dt.time
            df.sort_values("time", inplace=True); df.reset_index(drop=True, inplace=True)
        stock_df[sym] = df

    FIRST3_AVG_VOL: dict[str, float] = {}
    for sym, df in stock_df.items():
        if df.empty or "time" not in df.columns: FIRST3_AVG_VOL[sym] = 0; continue
        valid_times = [datetime.strptime(t, "%H:%M:%S").time() for t in ["09:00:00", "09:01:00", "09:02:00"] if datetime.strptime(t, "%H:%M:%S").time() <= trading_time]
        if not valid_times: FIRST3_AVG_VOL[sym] = 0
        else:
            first3 = df[df["time"].isin(valid_times)]
            FIRST3_AVG_VOL[sym] = first3["volume"].mean() if not first3.empty else 0

    if not hasattr(sys_state, 'reentry_counts'): sys_state.reentry_counts = {}
    
    for grp, gstat in list(group_positions.items()):
        if gstat == "已進場" or (isinstance(gstat, dict) and gstat.get("status") == "已進場"):
            still_open = any(sym in sys_state.open_positions for sym in consolidated_symbols.get(grp, []))
            if not still_open:
                group_positions[grp] = "停損結算中" 
                reentry_count = sys_state.reentry_counts.get(grp, 0)
                
                reentry_success = False
                if sys_config.allow_reentry and reentry_count < sys_config.max_reentry_times:
                    lookback_start = (datetime.combine(date.today(), trading_time) - timedelta(minutes=sys_config.reentry_lookback_candles)).time()
                    
                    for r_sym in consolidated_symbols.get(grp, []):
                        if r_sym not in stock_df or stock_df[r_sym].empty: continue
                        df_sym = stock_df[r_sym]
                        sub_df = df_sym[(df_sym['time'] >= lookback_start) & (df_sym['time'] <= trading_time)]
                        
                        for _, r_row in sub_df.iterrows():
                            h, lup, pct, vol = r_row.get('high'), r_row.get('漲停價'), r_row.get('2min_pct_increase'), r_row.get('volume')
                            avgv = FIRST3_AVG_VOL.get(r_sym, 0)
                            is_limit_up = (h == lup) if (h is not None and pd.notna(h) and lup is not None and pd.notna(lup)) else False
                            
                            vol_check = (vol > sys_config.volume_multiplier * avgv) if avgv > 0 else (vol >= sys_config.min_volume_threshold)
                            is_pull_up = (pct is not None and pd.notna(pct) and pct >= sys_config.pull_up_pct_threshold and vol is not None and pd.notna(vol) and vol_check and vol >= sys_config.min_volume_threshold)
                            
                            if is_limit_up or is_pull_up:
                                reentry_success = True
                                sys_state.reentry_counts[grp] = reentry_count + 1
                                cond_str = "漲停" if is_limit_up else "拉高"
                                
                                group_positions[grp] = {
                                    "status": "觀察中", 
                                    "trigger": f"{cond_str}進場", 
                                    "start_time": datetime.combine(date.today(), trading_time), 
                                    "tracking": {r_sym: {"join_time": datetime.combine(date.today(), trading_time), "base_vol": vol, "base_rise": r_row['rise']}}, 
                                    "leader": r_sym,
                                    "leader_peak": r_row['rise'],
                                    "leader_reversal_rise": r_row.get('highest', h),
                                    "wait_counter": 0,
                                    "wait_start": datetime.combine(date.today(), trading_time)
                                }
                                msg = f"🔄 停損後再進場觸發！回溯發現 {sn(r_sym)} {cond_str}，啟動第 {reentry_count+1} 次監控"
                                print(f"{Fore.MAGENTA}{msg}{Style.RESET_ALL}"); message_log.append((trading_txt, msg))
                                tg_bot.send_message(f"🔄 <b>停損再進場啟動</b>\n族群: {grp}\n標的: <code>{sn(r_sym)}</code>\n條件: {cond_str}\n系統已重新鎖定目標！", force=True)
                                break
                        if reentry_success: break
                
                if not reentry_success:
                    if sys_config.allow_reentry and reentry_count < sys_config.max_reentry_times:
                        sys_state.reentry_counts[grp] = reentry_count + 1
                        group_positions.pop(grp, None) 
                        msg = f"🔄 停損結算。回溯無觸發，系統進入獵人模式等待下一次轉折 (剩餘次數: {sys_config.max_reentry_times - (reentry_count+1)})"
                        print(f"{Fore.MAGENTA}{msg}{Style.RESET_ALL}"); message_log.append((trading_txt, msg))
                        tg_bot.send_message(f"🔄 <b>系統進入獵人模式</b>\n族群: {grp}\n系統保持空手監控，等待下一次進場時機！", force=True)
                    else:
                        group_positions[grp] = False 

    trigger_list = []
    if trading_time >= time(13, 0): pass
    else:
        for grp, syms in consolidated_symbols.items():
            if grp in group_positions:
                gstat = group_positions[grp]
                if gstat == "已進場" or (isinstance(gstat, dict) and gstat.get("status") == "已進場"):
                    continue

            for sym in syms:
                if sym not in symbols_to_analyze: continue
                df = stock_df[sym]
                if df.empty: continue
                row_now = df[df["time"] == trading_time]
                if row_now.empty: continue
                row_now = row_now.iloc[0]

                hit_limit, pull_up = False, False
                if pd.notna(row_now["high"]) and pd.notna(row_now["漲停價"]) and row_now["high"] == row_now["漲停價"]:
                    if trading_time == time(9, 0): hit_limit = True
                    else:
                        prev_t = (datetime.combine(date.today(), trading_time) - timedelta(minutes=1)).time()
                        prev = df[df["time"] == prev_t]
                        if prev.empty or (not prev.empty and prev.iloc[0]["high"] < row_now["漲停價"]):
                            hit_limit = True
                            
                            for g2, gstat in group_positions.items():
                                if isinstance(gstat, dict) and gstat.get("trigger") == "拉高進場" and sym in consolidated_symbols.get(g2, []):
                                    gstat["trigger"], gstat["wait_start"], gstat["wait_counter"], gstat["leader"] = "漲停進場", datetime.combine(date.today(), trading_time), 0, sym
                                    msg = f"🚀 {sn(sym)} 衝上漲停，{g2} 族群從拉高無縫升級為漲停進場！"
                                    print(msg); message_log.append((trading_txt, msg))
                                    tg_bot.send_message(f"🚀 <b>漲停進場</b>\n標的: <code>{sn(sym)}</code>\n所屬族群: {g2}\n系統已鎖定目標等待進場！", force = True)
                                    hit_limit = False

                avgv = FIRST3_AVG_VOL.get(sym, 0)
                vol_check = (row_now["volume"] > sys_config.volume_multiplier * avgv) if avgv > 0 else (row_now["volume"] >= sys_config.min_volume_threshold)
                if pd.notna(row_now["2min_pct_increase"]) and row_now["2min_pct_increase"] >= sys_config.pull_up_pct_threshold and vol_check and row_now["volume"] >= sys_config.min_volume_threshold: 
                    pull_up = True
                    
                if hit_limit or pull_up: trigger_list.append({"symbol": sym, "group": grp, "condition": "limit_up" if hit_limit else "pull_up"})

    trigger_list.sort(key=lambda x: 0 if x["condition"] == "limit_up" else 1)
    for item in trigger_list:
        grp, cond_txt = item["group"], "漲停進場" if item["condition"] == "limit_up" else "拉高進場"
        sym = item["symbol"]
        
        if grp not in group_positions or not group_positions[grp]:
            group_positions[grp] = {
                "status": "觀察中", 
                "trigger": cond_txt, 
                "start_time": datetime.combine(date.today(), trading_time), 
                "tracking": {sym: {"join_time": datetime.combine(date.today(), trading_time), "base_vol": stock_df[sym].loc[stock_df[sym]["time"] == trading_time, "volume"].iloc[0], "base_rise": stock_df[sym].loc[stock_df[sym]["time"] == trading_time, "rise"].iloc[0]}}, 
                "leader": sym if cond_txt == "漲停進場" else None
            }
            if cond_txt == "漲停進場": 
                group_positions[grp]["wait_start"], group_positions[grp]["wait_counter"] = datetime.combine(date.today(), trading_time), 0
                tg_bot.send_message(f"🔥 <b>條件觸發 (漲停進場)</b>\n領漲股: <code>{sn(sym)}</code>\n已鎖定族群【{grp}】準備進場！", force=True)
            else:
                msg = f"🔥 {sn(sym)} 觸發拉高條件，已鎖定族群【{grp}】觀察中"
                print(f"{YELLOW}{msg}{RESET}"); message_log.append((trading_txt, msg))
                tg_bot.send_message(f"🔥 <b>條件觸發 (拉高進場)</b>\n領漲股: <code>{sn(sym)}</code>\n已鎖定族群【{grp}】觀察中！", force=True)
                
        elif isinstance(group_positions[grp], dict) and group_positions[grp]["status"] == "觀察中" and group_positions[grp]["trigger"] == "拉高進場" and cond_txt == "漲停進場":
            gstat = group_positions[grp]
            gstat["trigger"] = "漲停進場"
            gstat["leader"] = sym
            gstat["start_time"] = datetime.combine(date.today(), trading_time)
            gstat["wait_start"] = datetime.combine(date.today(), trading_time)
            gstat["wait_counter"] = 0
            
            if "tracking" not in gstat: gstat["tracking"] = {}
            gstat["tracking"][sym] = {"join_time": datetime.combine(date.today(), trading_time), "base_vol": stock_df[sym].loc[stock_df[sym]["time"] == trading_time, "volume"].iloc[0], "base_rise": stock_df[sym].loc[stock_df[sym]["time"] == trading_time, "rise"].iloc[0]}
            
            msg = f"🚀 {sn(sym)} 漲停觸發，強制中斷拉高，無縫升級為漲停進場模式！"
            print(f"{Fore.RED}{msg}{Style.RESET_ALL}"); message_log.append((trading_txt, msg))
            tg_bot.send_message(f"🚀 <b>強制升級 (漲停進場)</b>\n領漲股: <code>{sn(sym)}</code>\n族群【{grp}】已從拉高切換為漲停模式！", force=True)

    for grp, gstat in group_positions.items():
        if not (isinstance(gstat, dict) and gstat["status"] == "觀察中"): continue
        track = gstat.setdefault("tracking", {})
        for sym in consolidated_symbols[grp]:
            df = stock_df[sym]
            if df.empty: continue
            row_now = df[df["time"] == trading_time]
            if not row_now.empty and pd.notna(row_now.iloc[0]["2min_pct_increase"]) and row_now.iloc[0]["2min_pct_increase"] >= sys_config.follow_up_pct_threshold and sym not in track:
                track[sym] = {"join_time": datetime.combine(date.today(), trading_time), "base_vol": row_now.iloc[0]["volume"], "base_rise": row_now.iloc[0]["rise"]}

    for grp, gstat in group_positions.items():
        if not (isinstance(gstat, dict) and gstat["status"] == "觀察中"): continue
        track = gstat.get("tracking", {})
        if not track: continue

        max_sym, max_rise = None, None
        for sym in track:
            row_now = stock_df[sym][stock_df[sym]["time"] == trading_time]
            if not row_now.empty and pd.notna(rise_now := row_now.iloc[0]["rise"]) and (max_rise is None or rise_now > max_rise): max_rise, max_sym = rise_now, sym

        if gstat.get("leader") is None: 
            gstat["leader"] = max_sym
            gstat["leader_peak"] = max_rise
        elif max_sym:
            if "leader_peak" not in gstat:
                gstat["leader_peak"] = max_rise
                
            if max_sym == gstat["leader"]:
                if max_rise > gstat["leader_peak"]:
                    gstat["leader_peak"] = max_rise
            else:
                if max_rise > gstat["leader_peak"]:
                    print(f"{Fore.CYAN}✨ 領漲替換：{sn(gstat['leader'])} ➔ {sn(max_sym)}{Style.RESET_ALL}")
                    tg_bot.send_message(f"✨ <b>領漲股替換</b>\n由 <code>{sn(gstat['leader'])}</code> 切換為 <code>{sn(max_sym)}</code>\n系統已重新計算 DTW 相似度。", force=True)
                    gstat["leader"], gstat["leader_peak"], gstat["leader_reversal_rise"], gstat["status"], gstat["wait_counter"], gstat["start_time"] = max_sym, max_rise, max_rise, "觀察中", 0, datetime.combine(date.today(), trading_time)
                    gstat.pop("wait_start", None)
                
        lead_sym = gstat["leader"]
        if not lead_sym: continue
        df_lead = stock_df[lead_sym]
        idx_now = df_lead[df_lead["time"] == trading_time].index
        if idx_now.empty: continue
        
        if "wait_start" not in gstat and idx_now[0] - 1 >= 0 and df_lead.loc[idx_now[0], "high"] <= df_lead.loc[idx_now[0] - 1, "high"]:
            gstat["wait_start"], gstat["wait_counter"], gstat["leader_reversal_rise"] = datetime.combine(date.today(), trading_time), 0, df_lead.loc[idx_now[0], "highest"]
            msg = f"{gstat.get('trigger')} {grp} 領漲 {sn(lead_sym)} 反轉，確立天花板 {df_lead.loc[idx_now[0], 'highest']}，開始等待"
            print(msg); message_log.append((trading_txt, msg))
            tg_bot.send_message(f"⏳ <b>領漲股反轉 (開始等待)</b>\n族群: {grp}\n領漲: <code>{sn(lead_sym)}</code>\n天花板: {df_lead.loc[idx_now[0], 'highest']}\n準備計算洗盤時間...", force=True)

    for grp, gstat in group_positions.items():
        if not (isinstance(gstat, dict) and gstat["status"] == "觀察中" and "wait_start" in gstat): continue

        lead = gstat.get("leader")
        if lead and gstat.get("leader_reversal_rise") is not None:
            row_now = stock_df.get(lead, pd.DataFrame())[stock_df.get(lead, pd.DataFrame())["time"] == trading_time]
            if not row_now.empty and pd.notna(row_now.iloc[0]["high"]) and row_now.iloc[0]["high"] > gstat["leader_reversal_rise"]:
                msg = f"🔥 領漲 {sn(lead)} 突破前高 {gstat['leader_reversal_rise']}，漲勢延續，中斷等待！"
                print(msg); message_log.append((trading_txt, msg))
                tg_bot.send_message(f"🚀 <b>突破前高 (中斷等待)</b>\n領漲股: <code>{sn(lead)}</code>\n突破前高: {gstat['leader_reversal_rise']}\n漲勢延續，已重置倒數計時！", force=True)
                gstat["leader_reversal_rise"], gstat["status"], gstat["wait_counter"] = row_now.iloc[0]["highest"], "觀察中", 0
                gstat.pop("wait_start", None)
                continue

        gstat["wait_counter"] += 1
        
        if leader_sym := gstat.get("leader"):
            window_start_live = max(time(9,0), (gstat["start_time"] - timedelta(minutes=2)).time())
            to_remove_live = [s_sym for s_sym in list(gstat.get("tracking", {}).keys()) if s_sym != leader_sym and calculate_dtw_pearson(stock_df[leader_sym], stock_df[s_sym], window_start_live, trading_time) < sys_config.similarity_threshold]
            for s_sym in to_remove_live: 
                gstat["tracking"].pop(s_sym, None)
                print(f"{RED}[滾動剔除] {sn(s_sym)} 相似度 < {sys_config.similarity_threshold}{RESET}")

    # 🟢 修正 1：移除 `- 1`，確保實戰盤中的等待時間與回測一模一樣！
    groups_ready = [grp for grp, gstat in group_positions.items() if isinstance(gstat, dict) and gstat["status"] == "觀察中" and "wait_start" in gstat and (datetime.combine(date.today(), trading_time) - gstat["wait_start"]).total_seconds() / 60 >= wait_minutes]

    for grp in groups_ready:
        gstat = group_positions[grp]
        filtered_track = gstat.get("tracking", {}).copy()
        leader_sym = gstat.get("leader")
        
        if not filtered_track: group_positions[grp] = False; continue

        eligible = []
        for sym, info in filtered_track.items():
            if sym == leader_sym: continue
            df = stock_df[sym]
            sub = df[(df["time"] >= gstat["start_time"].time()) & (df["time"] <= trading_time)]
            if sub.empty: continue
            
            # 🟢 同步優化：進場量能檢查，支援開盤前幾分鐘無均量的狀況
            avgv = FIRST3_AVG_VOL.get(sym, 0)
            vol_cond = (sub['volume'] >= sys_config.volume_multiplier * avgv) if avgv > 0 else (sub['volume'] >= sys_config.min_volume_threshold)
            if not (vol_cond & (sub['volume'] >= sys_config.min_volume_threshold)).any(): continue
            
            # 🟢 致命修正 2：修復盤中完全失效的「防誘空容錯率」，讓它跟回測 100% 一致！
            if len(sub) >= 2 and sub.iloc[-1]['rise'] > sub.iloc[:-1]['rise'].max() + sys_config.pullback_tolerance: continue
            
            row_now = df[df["time"] == trading_time]
            if row_now.empty or not (sys_config.rise_lower_bound <= row_now.iloc[0]["rise"] <= sys_config.rise_upper_bound) or row_now.iloc[0]["close"] > sys_config.capital_per_stock * 1.5: continue
            try:
                contract = sys_state.api.Contracts.Stocks.TSE.get(sym) or sys_state.api.Contracts.Stocks.OTC.get(sym)
                if not contract or not (getattr(contract, 'day_trade', None) in ["Yes", sj.constant.DayTrade.Yes] or getattr(getattr(contract, 'day_trade', None), 'value', None) == "Yes"): continue
            except: continue
            eligible.append({"symbol": sym, "rise": row_now.iloc[0]["rise"], "row": row_now.iloc[0]})

        if not eligible: group_positions[grp] = False; continue

        eligible.sort(key=lambda x: x["rise"], reverse=True)
        chosen = eligible[len(eligible) // 2]
        row, entry_px = chosen["row"], chosen["row"]["close"]
        shares = round((sys_config.capital_per_stock * 10000) / (entry_px * 1000))
        sell_amt = shares * entry_px * 1000
        fee, tax = int(sell_amt * (sys_config.transaction_fee * 0.01) * (sys_config.transaction_discount * 0.01)), int(sell_amt * (sys_config.trading_tax * 0.01))
        gap, tick = get_stop_loss_config(entry_px)
        highest_on_entry = row["highest"] or entry_px
        stop_thr = entry_px + gap / 1000 if (highest_on_entry - entry_px) * 1000 < gap else highest_on_entry + tick

        limit_up = row.get(f'漲停價')
        if limit_up and pd.notna(limit_up):
            tick_for_limit = 0.01 if limit_up < 10 else 0.05 if limit_up < 50 else 0.1 if limit_up < 100 else 0.5 if limit_up < 500 else 1 if limit_up < 1000 else 5
            if stop_thr > limit_up - 2 * tick_for_limit: stop_thr = limit_up - 2 * tick_for_limit

        planned_exit = datetime.combine(date.today(), trading_time) + timedelta(minutes=hold_minutes) if hold_minutes is not None and (datetime.combine(date.today(), trading_time) + timedelta(minutes=hold_minutes)).time() < time(13, 26) else None

        with sys_state.lock: sys_state.open_positions[chosen['symbol']] = {'entry_price': entry_px, 'shares': shares, 'sell_cost': sell_amt, 'entry_fee': fee, 'stop_loss': stop_thr, 'planned_exit': planned_exit}
        
        stock_code_str = chosen["symbol"]
        contract = getattr(sys_state.api.Contracts.Stocks.TSE, "TSE" + stock_code_str) if stock_code_str in STOCK_NAME_MAP.get("TSE", {}) else getattr(sys_state.api.Contracts.Stocks.OTC, "OTC" + stock_code_str)
        
        order = sys_state.api.Order(price=0, quantity=shares, action=sj.constant.Action.Sell, price_type=sj.constant.StockPriceType.MKT, order_type=sj.constant.OrderType.IOC, order_lot=sj.constant.StockOrderLot.Common, daytrade_short=True, account=sys_state.api.stock_account)
        safe_place_order(sys_state.api, contract, order)
        tcond = tp.TouchOrderCond(tp.TouchCmd(code=f"{stock_code_str}", close=tp.Price(price=stop_thr, trend="Equal")), tp.OrderCmd(code=f"{stock_code_str}", order=sj.Order(price=0, quantity=shares, action="Buy", order_type="ROD", price_type="MKT")))
        
        with sys_state.lock:
            if stock_code_str not in sys_state.to.contracts: sys_state.to.contracts[stock_code_str] = contract
            safe_add_touch_condition(sys_state.to, tcond)
            group_positions[grp] = "已進場"

        sys_db.log_trade("賣出", stock_code_str, shares, entry_px, 0.0, "拉高進場")
        msg = f"{GREEN}進場！{sn(stock_code_str)} {shares}張 成交價 {entry_px:.2f} 停損價 {stop_thr:.2f}{RESET}"
        tg_bot.send_message(f"🟢 <b>自動進場賣空</b>\n標的: <code>{sn(stock_code_str)}</code>\n數量: {shares} 張\n成交: {entry_px:.2f}\n停損: {stop_thr:.2f}", force=True)
        print(msg); message_log.append((trading_txt, msg))

    message_log.sort(key=lambda x: x[0])
    for t, m in message_log: print(f"[{t}] {m}")
    message_log.clear()

# =================================================================================
# 🟢 替換 3：盤中主控制迴圈
# 🟢 替換 1：盤中主控制迴圈 (修復 K 棒空缺與 rise 斷層)
def start_trading(mode='full', wait_minutes=None, hold_minutes=None):
    sys_state.is_monitoring = True
    sys_state.stop_trading_flag = False

    now = datetime.now()
    if now.weekday() in [5, 6]:
        days_ahead = 7 - now.weekday()
        next_monday = (now + timedelta(days=days_ahead)).replace(hour=8, minute=30, second=0, microsecond=0)
        sleep_seconds = (next_monday - now).total_seconds()
        msg = f"🏖️ 目前為週末休市時間！系統將自動進入深度休眠。\n⏰ 預計喚醒時間：{next_monday.strftime('%Y-%m-%d %H:%M:%S')}"
        print(f"{YELLOW}{msg}{RESET}")
        try: tg_bot.send_message(f"🏖️ <b>週末休市中</b>\n系統已進入深度休眠，將於下週一 08:30 自動喚醒並執行盤前準備！", force=True)
        except Exception: pass
        time_module.sleep(sleep_seconds)
        start_trading(mode, wait_minutes, hold_minutes) 
        return

    load_twse_name_map()
    client = init_esun_client()
    matrix_dict_analysis = load_matrix_dict_analysis()
    fetch_disposition_stocks(client, matrix_dict_analysis)   
    purge_disposition_from_nb(load_disposition_stocks())           
    symbols_to_analyze = load_symbols_to_analyze()
    
    group_symbols = load_group_symbols()
    if not group_symbols or not group_symbols.get('consolidated_symbols', {}): return print("沒有加載到族群資料。")
    group_positions = {group: False for group in group_symbols['consolidated_symbols'].keys()}

    now = datetime.now()
    pre_market_start = now.replace(hour=8, minute=30, second=0, microsecond=0)
    market_start     = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_exit      = now.replace(hour=13, minute=26, second=0, microsecond=0)
    market_end       = now.replace(hour=13, minute=30, second=0, microsecond=0)

    if now < pre_market_start:
        sys_state.trading = False
        wait_sec = (pre_market_start - now).total_seconds()
        print(f"目前為 {now.strftime('%H:%M:%S')}，將休眠至 08:30...")
        time_module.sleep(wait_sec)
        start_trading(mode, wait_minutes, hold_minutes) 
        return

    elif now >= market_end:
        sys_state.trading = False
        tomorrow = now + timedelta(days=1)
        if tomorrow.weekday() == 5: tomorrow = tomorrow + timedelta(days=2)
        tomorrow_pre_market = tomorrow.replace(hour=8, minute=30, second=0, microsecond=0)
        print(f"今日已收盤。系統將休眠至下次開盤日 {tomorrow_pre_market.strftime('%m/%d %H:%M')}...")
        time_module.sleep((tomorrow_pre_market - now).total_seconds())
        start_trading(mode, wait_minutes, hold_minutes) 
        return

    elif pre_market_start <= now < market_start:
        sys_state.trading = False
        print(f"進入盤前時間，開始準備【當日實戰】日K資料...")
        # 🟢 修正：讀取與存入實戰專用表 (live)
        existing_auto_daily_data = sys_db.load_kline('daily_kline_live')
        auto_daily_data, data_is_same, initial_api_count = {}, True, 0
        for symbol in symbols_to_analyze[:20]:
            if initial_api_count >= 55: time_module.sleep(60); initial_api_count = 0
            daily_kline_df = fetch_daily_kline_data(client, symbol, days=2)
            initial_api_count += 1
            if daily_kline_df.empty: continue
            daily_kline_data = daily_kline_df.to_dict(orient='records')
            auto_daily_data[symbol] = daily_kline_data
            if existing_auto_daily_data.get(symbol) != daily_kline_data: data_is_same = False; existing_auto_daily_data[symbol] = daily_kline_data

        if not data_is_same:
            for symbol in symbols_to_analyze[20:]:
                if initial_api_count >= 55: time_module.sleep(60); initial_api_count = 0
                daily_kline_df = fetch_daily_kline_data(client, symbol, days=2)
                initial_api_count += 1
                if daily_kline_df.empty: continue
                daily_kline_data = daily_kline_df.to_dict(orient='records')
                if existing_auto_daily_data.get(symbol) != daily_kline_data: existing_auto_daily_data[symbol] = daily_kline_data

        # 🟢 修正：儲存至實戰日K表
        sys_db.save_kline('daily_kline_live', existing_auto_daily_data)
        print(f"{YELLOW}實戰盤前參考數據更新完成。{RESET}")
        now = datetime.now()
        if (market_start - now).total_seconds() > 0: time_module.sleep((market_start - now).total_seconds())
        print("開盤！自動切換到盤中監控模式…")
        start_trading(mode='post', wait_minutes=wait_minutes, hold_minutes=hold_minutes)
        return

    elif market_start <= now < market_end:
        sys_state.trading = True
        print(f"盤中監控時間，直接載入【當日實戰】參考資料。")
        # 🟢 修正：強制讀取實戰日K表
        existing_auto_daily_data = sys_db.load_kline('daily_kline_live')
        trading_day = (datetime.now() - timedelta(minutes=1)).strftime('%Y-%m-%d')
        yesterday_close_prices = {}
        for symbol in symbols_to_analyze:
            daily_data = existing_auto_daily_data.get(symbol, [])
            if not daily_data: yesterday_close_prices[symbol] = 0
            else:
                sorted_daily_data = sorted(daily_data, key=lambda x: x['date'], reverse=True)
                if len(sorted_daily_data) > 1:
                    now2 = datetime.now()
                    yesterday_close_prices[symbol] = sorted_daily_data[0].get('close', 0) if (0 <= now2.weekday() <= 4 and 8 <= now2.hour < 15) else sorted_daily_data[1].get('close', 0)
                else: yesterday_close_prices[symbol] = sorted_daily_data[0].get('close', 0)

        print("🔁 [實戰] 開始補齊今日 09:00 到目前為止的一分K資料...")
        full_intraday_end = (now - timedelta(minutes=1)).strftime('%H:%M') if now < now.replace(hour=13, minute=30, second=0, microsecond=0) else "13:30"
        auto_intraday_data = {}
        
        # 🟢 修復：移除不必要的 sleep 與 api_count，全速 5 工人併發
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_symbol = {}
            for symbol in symbols_to_analyze:
                if (yc := yesterday_close_prices.get(symbol, 0)) == 0: continue
                # 🟢 改用剛才修好的 fetch_realtime_intraday_data
                future_to_symbol[executor.submit(fetch_realtime_intraday_data, client, symbol, trading_day, yc, "09:00", full_intraday_end)] = symbol
                
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                df = future.result()
                if df.empty: continue
                updated_records = []
                records = df.to_dict(orient='records')
                for i, candle in enumerate(records): 
                    updated_records.append(calculate_2min_pct_increase_and_highest(candle, records[:i]))
                auto_intraday_data[symbol] = pd.DataFrame(updated_records).to_dict(orient='records')

        print(f"{GREEN}✅ [實戰] 補齊完成{RESET}")
        save_auto_intraday_data(auto_intraday_data)
        initialize_triggered_limit_up(auto_intraday_data)

        has_exited, current_position, hold_time, message_log, already_entered_stocks = False, None, 0, [], []
        stop_loss_triggered, final_check_active, final_check_count, in_waiting_period, waiting_time = False, False, 0, False, 0
        leader, tracking_stocks, previous_rise_values, leader_peak_rise, leader_rise_before_decline, first_condition_one_time = None, set(), {}, None, None, None
        can_trade, exit_live_done = True, False

        while True:
            if sys_state.stop_trading_flag:
                print(f"\n{YELLOW}🛑 接收到手動終止指令，已退出盤中監控模式！{RESET}")
                sys_state.trading = False; sys_state.is_monitoring = False; sys_state.stop_trading_flag = False
                return
            now_loop = datetime.now()
            if now_loop >= market_exit and not exit_live_done:
                print(f"🔍 13:26 觸發：檢查所有庫存平倉。"); exit_trade_live(); exit_live_done = True
            if now_loop >= market_end:
                print(f"\n⏰ 13:30 今日盤中監控結束。"); break
                
            with sys_state.lock:
                for sym, pos_info in list(sys_state.open_positions.items()):
                    if (planned_exit := pos_info.get('planned_exit')) and now_loop >= planned_exit:
                        print(f"{RED}⏰ {sn(sym)} 已達設定持有時間，執行自動平倉！{RESET}")
                        pos_info['planned_exit'] = None; threading.Thread(target=close_one_stock, args=(sym,), daemon=True).start()

            # 🟢 修復：絕對精準的「等待下一個 00 秒」機制
            now = datetime.now()
            # 算出下一分鐘的 00 秒是何時
            next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
            sleep_seconds = (next_minute - now).total_seconds()
            
            # 等待直到下一分鐘 00 秒
            if sleep_seconds > 0:
                time_module.sleep(sleep_seconds)
                
            # 醒來後，現在已經是「新的一分鐘」了。我們要抓的是「剛結束的那一分鐘」
            # 例如現在是 11:00:00，我們要向 API 要求 10:59 的資料
            actual_fetch_time = next_minute - timedelta(minutes=1)
            fetch_time_str = actual_fetch_time.strftime('%H:%M') if actual_fetch_time.time() <= market_end.time() else "13:30"
            print(f"⏱ [即時] 開始取得 {fetch_time_str} 的一分K資料...")
            t_start_fetch = time_module.perf_counter()

            updated_intraday_data = {}
            # 🟢 修復：改為 max_workers=5，並拔除所有手動 sleep，讓 global_rate_limiter 自動幫你以 550 次/分 狂飆
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_symbol = {}
                for symbol in symbols_to_analyze:
                    if (yc := yesterday_close_prices.get(symbol, 0)) == 0: continue
                    future_to_symbol[executor.submit(fetch_realtime_intraday_data, client, symbol, trading_day, yc, fetch_time_str, fetch_time_str)] = symbol
                    
                for fut in as_completed(future_to_symbol):
                    sym = future_to_symbol[fut]
                    df = fut.result()
                    
                    if df.empty:
                        prev_data = auto_intraday_data.get(sym, [])
                        if prev_data:
                            last_c = prev_data[-1].copy(); last_c['time'] = fetch_time_str + ":00"; last_c['volume'] = 0.0; raw_candle = last_c
                        else:
                            yc = yesterday_close_prices.get(sym, 0)
                            raw_candle = {'symbol': sym, 'date': trading_day, 'time': fetch_time_str, 'open': yc, 'high': yc, 'low': yc, 'close': yc, 'volume': 0.0, '昨日收盤價': yc, '漲停價': truncate_to_two_decimals(calculate_limit_up_price(yc)), 'rise': 0.0}
                    else:
                        raw_candle = df.to_dict(orient='records')[0]
                        if float(raw_candle.get('volume', 0)) == 0.0:
                            prev_data = auto_intraday_data.get(sym, [])
                            if prev_data:
                                last_c = prev_data[-1]; raw_candle['open'] = raw_candle['high'] = raw_candle['low'] = raw_candle['close'] = last_c['close']; raw_candle['rise'] = last_c.get('rise', 0.0)
                            else:
                                yc = yesterday_close_prices.get(sym, 0); raw_candle['open'] = raw_candle['high'] = raw_candle['low'] = raw_candle['close'] = yc; raw_candle['rise'] = 0.0
                    
                    candle = calculate_2min_pct_increase_and_highest(raw_candle, auto_intraday_data.get(sym, []))
                    if '漲停價' in candle: candle['漲停價'] = truncate_to_two_decimals(candle['漲停價'])
                    updated_intraday_data.setdefault(sym, []).append(candle)
            
            fetch_duration = time_module.perf_counter() - t_start_fetch
            print(f"✅ [即時] {fetch_time_str} 一分K取得完成，耗時 {fetch_duration:.2f} 秒")
            for sym, lst in updated_intraday_data.items():
                auto_intraday_data.setdefault(sym, []).extend(lst)
                auto_intraday_data[sym] = auto_intraday_data[sym][-1000:]
            # 🟢 關鍵修正：先同步到記憶體，再執行判斷，最後存入資料庫
            with sys_state.lock:
                sys_state.in_memory_intraday = auto_intraday_data.copy()
            
            # 這樣 process_live_trading_logic 就會直接從記憶體拿資料，速度最快
            process_live_trading_logic(symbols_to_analyze, fetch_time_str, wait_minutes, hold_minutes, message_log, False, has_exited, current_position, hold_time, already_entered_stocks, stop_loss_triggered, final_check_active, final_check_count, in_waiting_period, waiting_time, leader, tracking_stocks, previous_rise_values, leader_peak_rise, leader_rise_before_decline, first_condition_one_time, can_trade, group_positions)
            
            # 非同步儲存，不影響判斷效能
            
            save_auto_intraday_data(auto_intraday_data)       
            with sys_state.lock:
                ui_data = []
                for sym, pos_info in sys_state.open_positions.items():
                    current_price = pd.DataFrame(auto_intraday_data[sym]).iloc[-1]['close'] if sym in auto_intraday_data and not pd.DataFrame(auto_intraday_data[sym]).empty else pos_info['entry_price']
                    buy_cost = pos_info['shares'] * current_price * 1000
                    ui_data.append({"symbol": sym, "entry_price": pos_info['entry_price'], "current_price": current_price, "profit": pos_info.get('sell_cost', 0) - buy_cost - pos_info.get('entry_fee', 0), "stop_loss": pos_info.get('stop_loss', '未設定')})
            try: ui_dispatcher.portfolio_updated.emit(ui_data)
            except Exception: pass

        sys_state.trading = False; tomorrow = datetime.now() + timedelta(days=1)
        if tomorrow.weekday() == 5: tomorrow = tomorrow + timedelta(days=2)
        tomorrow_pre_market = tomorrow.replace(hour=8, minute=30, second=0, microsecond=0)
        print(f"今日交易已完成。系統將自動休眠至開盤日 {tomorrow_pre_market.strftime('%m/%d %H:%M')}..."); time_module.sleep((tomorrow_pre_market - datetime.now()).total_seconds())
        start_trading(mode, wait_minutes, hold_minutes); return

# ==================== PyQt5 專業圖形介面 (GUI) ====================
class BaseDialog(QDialog):
    def __init__(self, title, size=(400, 300)):
        super().__init__()
        self.setWindowTitle(title); self.resize(*size)
        self.setWindowFlags(Qt.Window | Qt.WindowTitleHint | Qt.WindowSystemMenuHint | Qt.WindowMinimizeButtonHint | Qt.WindowCloseButtonHint)
        
        # 🟢 修正：強制指定清單元件為「白底黑字」，並確保選取狀態為「黑底白字」
        self.setStyleSheet("""
            QDialog { background-color: #1E1E1E; color: white; }
            QLabel { font-size: 14px; font-weight: bold; color: #E0E0E0; }
            QLineEdit, QComboBox { background-color: #2C2C2C; color: white; border: 1px solid #555; padding: 5px; border-radius: 4px;}
            QLineEdit::placeholder { color: #FFFFFF; font-style: normal; } 
            QPushButton { background-color: #34495E; color: white; font-size: 14px; border-radius: 4px; padding: 6px 15px; border: 1px solid #2C3E50;}
            QPushButton:hover { background-color: #4B6584; }
            
            /* 修正：針對下拉選單的清單檢視器進行強制設定 */
            QComboBox QAbstractItemView { 
                background-color: white; 
                color: black;                      /* 強制非選取項為黑字 */
                selection-background-color: black; /* 滑鼠指著時變黑底 */
                selection-color: white;            /* 滑鼠指著時變白字 */
                outline: none;
                border: 1px solid #555;
            }

            /* 額外補強：確保清單內的文字顏色不會被外層 QDialog 覆蓋 */
            QComboBox QListView {
                background-color: white;
                color: black;
            }
        """)

class EsunLoginDialog(BaseDialog):
    def __init__(self):
        super().__init__("玉山 API 安全登入", (350, 180))
        layout = QFormLayout(self)
        self.e_pwd = QLineEdit(); self.e_pwd.setEchoMode(QLineEdit.Password); self.e_pwd.setPlaceholderText("輸入網路登入密碼")
        self.e_cert = QLineEdit(); self.e_cert.setEchoMode(QLineEdit.Password); self.e_cert.setPlaceholderText("輸入憑證 (.p12) 密碼")
        layout.addRow("玉山登入密碼:", self.e_pwd); layout.addRow("數位憑證密碼:", self.e_cert)
        btn = QPushButton("🔓 登入並繼續"); btn.setStyleSheet("background-color: #27AE60; margin-top: 10px;"); btn.clicked.connect(self.accept)
        layout.addRow(btn)
    def get_passwords(self): return self.e_pwd.text(), self.e_cert.text()

def ensure_esun_passwords():
    global ESUN_LOGIN_PWD, ESUN_CERT_PWD
    if ESUN_LOGIN_PWD and ESUN_CERT_PWD: return True
    d = EsunLoginDialog()
    if d.exec_() == QDialog.Accepted: ESUN_LOGIN_PWD, ESUN_CERT_PWD = d.get_passwords(); return True
    return False

class CorrelationAnalysisThread(QThread):
    finished_signal = pyqtSignal(list)
    def __init__(self, mode, wait_mins): super().__init__(); self.mode = mode; self.wait_mins = wait_mins
    def run(self):
        result_data = []
        try:
            _, history_data = load_kline_data()
            groups = load_matrix_dict_analysis()
            dispo = load_disposition_stocks() 
            for grp_name, stocks in groups.items():
                stock_dfs = {}
                for s in [x for x in stocks if x not in dispo]:
                    if s in history_data and history_data[s]:
                        df = pd.DataFrame(history_data[s])
                        if not df.empty and 'time' in df.columns: df['time'] = pd.to_datetime(df['time'], format="%H:%M:%S").dt.time; stock_dfs[s] = df
                if len(stock_dfs) < 2: continue
                if self.mode == "macro":
                    leader = None; max_rise = -999
                    for s, df in stock_dfs.items():
                        if (s_max := df['rise'].max()) > max_rise: max_rise = s_max; leader = s
                    if not leader: continue
                    w_start, w_end = time(9,0), time(13,30)
                    for s in stock_dfs.keys():
                        if s == leader: continue
                        sim = calculate_dtw_pearson(stock_dfs[leader], stock_dfs[s], w_start, w_end)
                        result_data.append({'group': grp_name, 'leader': sn(leader), 'follower': sn(s), 'window': '09:00~13:30 (全天)', 'similarity': sim})
                elif self.mode == "micro":
                    leader, start_time, wait_counter, in_waiting, leader_peak_rise = None, None, 0, False, -999
                    intercept_w_start, intercept_w_end = None, None
                    tracking_stocks = set(stock_dfs.keys())
                    time_range = [ (datetime.combine(date.today(), time(9,0)) + timedelta(minutes=i)).time() for i in range(271) ]
                    for current_t in time_range:
                        cur_max_sym, cur_max_rise = None, -999
                        for s in tracking_stocks:
                            row = stock_dfs[s][stock_dfs[s]['time'] == current_t]
                            if not row.empty and (r := row.iloc[0]['rise']) > cur_max_rise: cur_max_rise, cur_max_sym = r, s
                        if not cur_max_sym: continue
                        if leader != cur_max_sym: leader, start_time, leader_peak_rise, in_waiting, wait_counter = cur_max_sym, current_t, cur_max_rise, False, 0
                        else:
                            if cur_max_rise < leader_peak_rise and not in_waiting: in_waiting, wait_counter = True, 0
                            elif cur_max_rise > leader_peak_rise: leader_peak_rise, in_waiting = cur_max_rise, False 
                        if in_waiting:
                            wait_counter += 1
                            if wait_counter >= self.wait_mins:
                                intercept_w_end, intercept_w_start = current_t, max(time(9,0), (datetime.combine(date.today(), start_time) - timedelta(minutes=2)).time())
                                break
                    if leader and intercept_w_start and intercept_w_end:
                        window_str = f"{intercept_w_start.strftime('%H:%M')}~{intercept_w_end.strftime('%H:%M')}"
                        for s in tracking_stocks:
                            if s == leader: continue
                            sim = calculate_dtw_pearson(stock_dfs[leader], stock_dfs[s], intercept_w_start, intercept_w_end)
                            result_data.append({'group': grp_name, 'leader': sn(leader), 'follower': sn(s), 'window': window_str, 'similarity': sim})
        except Exception as e: print(f"分析失敗: {e}")
        self.finished_signal.emit(result_data)

class SimilarityOptimizationDialog(BaseDialog):
    def __init__(self):
        super().__init__("🏆 智能 DTW 最適門檻最佳化", (950, 850))
        layout = QVBoxLayout(self)
        ctrl_layout = QHBoxLayout()
        
        g1 = QGroupBox("📥 大數據庫採集"); g1.setStyleSheet("QGroupBox{color:#9C27B0; font-weight:bold; border:1px solid #4A148C; padding:15px;}")
        l1 = QFormLayout(g1)
        self.days_in = QLineEdit("5"); self.days_in.setStyleSheet("background-color:#1E1E1E; border:1px solid #444; padding:5px;")
        l1.addRow("採集天數:", self.days_in)
        self.btn_f = QPushButton("執行數據採集"); self.btn_f.setStyleSheet("background-color:#34495E; font-weight:bold;"); self.btn_f.clicked.connect(self.start_f)
        l1.addRow(self.btn_f); ctrl_layout.addWidget(g1)

        g2 = QGroupBox("🏆 最適門檻掃描"); g2.setStyleSheet("QGroupBox{color:#F1C40F; font-weight:bold; border:1px solid #B7950B; padding:15px;}")
        l2 = QFormLayout(g2)
        self.wait_in, self.hold_in = QLineEdit("5"), QLineEdit("F")
        for inp in [self.wait_in, self.hold_in]: inp.setStyleSheet("background-color:#1E1E1E; border:1px solid #444; padding:5px;")
        l2.addRow("等待(分):", self.wait_in); l2.addRow("持有(分/F):", self.hold_in)
        self.btn_o = QPushButton("開始智能掃描"); self.btn_o.setStyleSheet("background-color:#D35400; font-weight:bold;"); self.btn_o.clicked.connect(self.start_o)
        l2.addRow(self.btn_o); ctrl_layout.addWidget(g2)

        layout.addLayout(ctrl_layout)
        self.console = QTextEdit(); self.console.setReadOnly(True); self.console.setStyleSheet("background-color:#000; color:#0F0; font-family:'Consolas','MingLiU',monospace; font-size:15px; padding:10px;")
        layout.addWidget(self.console)
        self.p_bar = QProgressBar(); self.p_bar.setStyleSheet("QProgressBar { border: 1px solid #555; background-color: #1E1E1E; color: white; text-align: center; height: 22px; font-weight: bold; } QProgressBar::chunk{background-color:#27AE60;}")
        layout.addWidget(self.p_bar)
        self.p_bar.hide()

    def log(self, m): self.console.append(m); self.console.verticalScrollBar().setValue(self.console.verticalScrollBar().maximum())
    def start_f(self):
        try: d = int(self.days_in.text())
        except: return QMessageBox.critical(self, "錯誤", "請輸入天數數字")
        self.console.clear(); self.btn_f.setEnabled(False)
        self.p_bar.setValue(0); self.p_bar.show()
        self.thread = FetchSimilarityDataThread(d)
        self.thread.log_signal.connect(self.log); self.thread.progress_signal.connect(self.p_bar.setValue)
        self.thread.finished_signal.connect(lambda s, m: (self.btn_f.setEnabled(True), self.log(m), self.p_bar.hide()))
        self.thread.start()
    def start_o(self):
        try: w = int(self.wait_in.text()); h = 270 if self.hold_in.text().upper() == 'F' else int(self.hold_in.text())
        except: return QMessageBox.critical(self, "錯誤", "時間參數錯誤")
        self.console.clear(); self.btn_o.setEnabled(False)
        self.p_bar.setRange(0, 0); self.p_bar.show()
        self.thread = OptimizeSimilarityThread(w, h)
        self.thread.log_signal.connect(self.log); self.thread.finished_signal.connect(lambda: (self.btn_o.setEnabled(True), self.p_bar.hide(), self.p_bar.setRange(0, 100)))
        self.thread.start()

class FetchSimilarityDataThread(QThread):
    log_signal = pyqtSignal(str); progress_signal = pyqtSignal(int); finished_signal = pyqtSignal(bool, str)
    def __init__(self, days_to_fetch): super().__init__(); self.days_to_fetch = days_to_fetch
    def run(self):
        db_folder = "回測大數據庫"
        os.makedirs(db_folder, exist_ok=True)
        for f in glob.glob(os.path.join(db_folder, "*.json")):
            try: os.remove(f)
            except: pass
        self.log_signal.emit("<span style='color:#9C27B0;'>⏳ 正在登入 Shioaji API (交易日採集模式)...</span>")
        if not getattr(sys_state.api, 'positions', None): sys_state.api.login(api_key=shioaji_logic.TEST_API_KEY, secret_key=shioaji_logic.TEST_API_SECRET)
        try: dispo_stocks = set(load_disposition_stocks())
        except: dispo_stocks = set()
        all_symbols, _ = load_target_symbols()
        symbols = [s for s in all_symbols if s not in dispo_stocks]
        if not symbols: return self.finished_signal.emit(False, "找不到符合的股票清單。")
        self.log_signal.emit(f"📋 準備採集 {len(symbols)} 檔股票之全指標交易日資料庫...")
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=self.days_to_fetch * 3 + 15) 
        try:
            kbars_2330 = sys_state.api.kbars(sys_state.api.Contracts.Stocks.TSE.TSE2330, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
            df_2330 = pd.DataFrame({**kbars_2330})
            df_2330['ts'] = pd.to_datetime(df_2330.ts)
            shift_mask = (df_2330['ts'].dt.hour != 13) | (df_2330['ts'].dt.minute != 30)
            df_2330.loc[shift_mask, 'ts'] = df_2330.loc[shift_mask, 'ts'] - pd.Timedelta(minutes=1)
            trading_days = sorted(df_2330['ts'].dt.date.unique())
        except Exception as e: return self.finished_signal.emit(False, f"取得交易日曆失敗: {e}")
        if len(trading_days) > self.days_to_fetch + 1: trading_days = trading_days[-(self.days_to_fetch + 1):]
        self.log_signal.emit(f"📅 成功鎖定最近 {self.days_to_fetch} 個真實開盤日！")
        daily_database = {}
        total_syms = len(symbols)
        for idx, sym in enumerate(symbols):
            contract = sys_state.api.Contracts.Stocks.get(sym)
            if not contract: continue
            try:
                self.log_signal.emit(f"⏳ 正在採集 [{idx+1}/{total_syms}] {sn(sym)} ...")
                kbars = sys_state.api.kbars(contract, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
                df = pd.DataFrame({**kbars})
                if df.empty: continue
                df['ts'] = pd.to_datetime(df.ts)
                shift_mask = (df['ts'].dt.hour != 13) | (df['ts'].dt.minute != 30)
                df.loc[shift_mask, 'ts'] = df.loc[shift_mask, 'ts'] - pd.Timedelta(minutes=1)
                df['date_only'] = df['ts'].dt.date
                for i in range(1, len(trading_days)):
                    t_day, p_day = trading_days[i], trading_days[i-1]
                    prev_df = df[df['date_only'] == p_day]
                    if prev_df.empty: continue
                    ref_p = prev_df.iloc[-1]['Close'] 
                    curr_df = df[df['date_only'] == t_day].copy().set_index('ts')
                    curr_df = curr_df.reindex(pd.date_range(start=datetime.combine(t_day, time(9, 0)), end=datetime.combine(t_day, time(13, 30)), freq='1min'))
                    curr_df['Close'] = curr_df['Close'].ffill().fillna(ref_p)
                    for c in ['High', 'Open', 'Low']: curr_df[c] = curr_df[c].fillna(curr_df['Close'])
                    curr_df['Volume'] = curr_df['Volume'].fillna(0)
                    curr_df['rise'] = ((curr_df['Close'] - ref_p) / ref_p * 100).round(2)
                    curr_df['highest'] = curr_df['High'].cummax()
                    try: m_mins = sys_config.momentum_minutes
                    except: m_mins = 1
                    rolling_rise = curr_df['rise'].rolling(window=m_mins + 1, min_periods=1)
                    curr_df['2min_pct'] = np.where(curr_df['rise'] >= curr_df['rise'].shift(m_mins).fillna(0), rolling_rise.max() - rolling_rise.min(), rolling_rise.min() - rolling_rise.max()).round(2)
                    curr_df.reset_index(inplace=True)
                    recs = []
                    for _, row in curr_df.iterrows():
                        recs.append({"symbol": sym, "date": t_day.strftime("%Y-%m-%d"), "time": row['index'].strftime("%H:%M:%S"), "open": row['Open'], "high": row['High'], "low": row['Low'], "close": row['Close'], "volume": row['Volume'], "昨日收盤價": ref_p, "漲停價": round(ref_p * 1.1, 2), "rise": row['rise'], "highest": row['highest'], "2min_pct_increase": row['2min_pct']})
                    d_key = t_day.strftime("%Y%m%d")
                    if d_key not in daily_database: daily_database[d_key] = {}
                    daily_database[d_key][sym] = recs
            except Exception as e: self.log_signal.emit(f"  ❌ {sym} 錯誤: {e}")
            self.progress_signal.emit(int((idx+1)/total_syms * 100))

        for d_key, data in daily_database.items():
            with open(os.path.join(db_folder, f"intraday_kline_data_{d_key}.json"), "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=4)
        self.finished_signal.emit(True, f"🎉 成功採集 {len(daily_database)} 個實體交易日大數據！")

class OptimizeSimilarityThread(QThread):
    log_signal = pyqtSignal(str); finished_signal = pyqtSignal()
    def __init__(self, wait_mins, hold_mins): super().__init__(); self.wait_mins = wait_mins; self.hold_mins = hold_mins
    def run(self):
        try:
            cap_val = sys_config.capital_per_stock
            f_rate, d_rate, t_rate = sys_config.transaction_fee*0.01, sys_config.transaction_discount*0.01, sys_config.trading_tax*0.01
            try: dispo = set(load_disposition_stocks())
            except: dispo = set()
            _, groups = load_target_symbols() 

            files = sorted(glob.glob("回測大數據庫/intraday_kline_data_*.json"))
            if not files: return self.log_signal.emit("❌ 找不到大數據庫！請先執行「資料採集」。")

            self.log_signal.emit(f"<br><span style='color:#FFDC00;'>{'=' * 62}</span>")
            self.log_signal.emit(f"🚀 啟動邏輯同步模擬 (分析 {len(files)} 天數據)")
            self.log_signal.emit(f"<span style='color:#FFDC00;'>{'-' * 62}</span>")
            self.log_signal.emit(f"<b>{'      門檻 |     交易數 |       勝率 |     平均淨利 |   穩健分數'.replace(' ', '&nbsp;')}</b>")
            self.log_signal.emit(f"<span style='color:#FFDC00;'>{'-' * 62}</span>")

            thresholds = np.arange(0.60, 0.95, 0.05)
            stats = {th: {'signals': 0, 'pnl_sum': 0.0, 'wins': 0} for th in thresholds}
            best_th, max_score = 0.75, -999.0

            for file in files:
                history_data = json.load(open(file, "r", encoding="utf-8"))
                for th in thresholds:
                    for grp_name, stocks in groups.items():
                        valid_s = [s for s in stocks if s not in dispo and s in history_data]
                        if len(valid_s) < 2: continue
                        stock_dfs = {}
                        for sym in valid_s:
                            df = pd.DataFrame(history_data[sym])
                            if df['volume'].sum() == 0: continue 
                            df['time'] = pd.to_datetime(df['time'], format="%H:%M:%S").dt.time
                            stock_dfs[sym] = df
                        if not stock_dfs: continue

                        leader, tracking = None, set()
                        in_wait, wait_cnt, start_t, leader_peak_rise = False, 0, None, None
                        pull_up, limit_up = False, False
                        first3_vol = {s: df.iloc[0:3]['volume'].mean() for s, df in stock_dfs.items()}
                        is_busy, exit_at = False, -1
                        day_times = stock_dfs[list(stock_dfs.keys())[0]]['time'].tolist()

                        # 🟢 效能優化：把所有的 df 轉為 dict list 提升 10 倍速度
                        stock_records = {s: df.to_dict('records') for s, df in stock_dfs.items()}

                        for m in range(271):
                            if is_busy:
                                if m >= exit_at: is_busy = False
                                continue
                            cur_t = day_times[m]
                            row_data = {s: stock_records[s][m] for s in stock_dfs.keys()}
                            
                            trigger_list = []
                            for sym in stock_dfs.keys():
                                row, avgv = row_data[sym], first3_vol[sym]
                                if row['high'] == row['漲停價']:
                                    if m == 0 or stock_records[sym][m-1]['high'] < row['漲停價']: trigger_list.append((sym, 'limit'))
                                elif row['2min_pct_increase'] >= 2.0 and avgv > 0 and row['volume'] > 1.3 * avgv: trigger_list.append((sym, 'pull'))

                            for sym, cond in trigger_list:
                                if cond == 'limit':
                                    tracking.add(sym); leader, in_wait, wait_cnt = sym, True, 0
                                    if not (pull_up or limit_up): start_t = cur_t
                                    pull_up, limit_up = False, True
                                else:
                                    if not pull_up and not limit_up: pull_up, limit_up, start_t = True, False, cur_t; tracking.clear()
                                    tracking.add(sym)

                            if pull_up or limit_up:
                                for sym in stock_dfs.keys():
                                    if sym not in tracking and row_data[sym]['2min_pct_increase'] >= 1.5: tracking.add(sym)

                            if tracking:
                                max_sym, max_r = None, -999
                                for sym in tracking:
                                    if row_data[sym]['rise'] > max_r: max_r, max_sym = row_data[sym]['rise'], sym
                                if leader != max_sym: leader, start_t, in_wait, wait_cnt, leader_peak_rise = max_sym, cur_t, False, 0, max_r 
                                if leader and row_data[leader]['high'] <= stock_records[leader][max(0, m-1)]['high'] and not in_wait: in_wait, wait_cnt, leader_peak_rise = True, 0, max_r 

                            if in_wait:
                                w_start = max(time(9,0), (datetime.combine(date.today(), start_t) - timedelta(minutes=2)).time())
                                to_remove = [sym for sym in list(tracking) if sym != leader and calculate_dtw_pearson(stock_dfs[leader], stock_dfs[sym], w_start, cur_t) < th]
                                for sym in to_remove: tracking.remove(sym)

                                if wait_cnt >= self.wait_mins:
                                    eligible = []
                                    for sym in tracking:
                                        if sym == leader: continue
                                        df_wait = stock_dfs[sym][(stock_dfs[sym]['time'] >= start_t) & (stock_dfs[sym]['time'] <= cur_t)]
                                        if not (df_wait['volume'] >= 1.5 * first3_vol[sym]).any() or (len(df_wait) >= 2 and df_wait.iloc[-1]['rise'] > df_wait.iloc[:-1]['rise'].max() + 0.5): continue
                                        if not (-1 <= row_data[sym]['rise'] <= 6) or row_data[sym]['close'] > cap_val * 1.5: continue
                                        eligible.append({'sym': sym, 'rise': row_data[sym]['rise'], 'row': row_data[sym]})

                                    if eligible:
                                        eligible.sort(key=lambda x: x['rise'], reverse=True)
                                        target = eligible[len(eligible)//2] 
                                        p_ent = target['row']['close']
                                        shrs = round((cap_val * 10000) / (p_ent * 1000))
                                        sell_total = shrs * p_ent * 1000
                                        ent_fee, tax = int(sell_total * f_rate * d_rate), int(sell_total * t_rate)
                                        gap, tick = get_stop_loss_config(p_ent)
                                        hi_on_e = target['row']['highest'] or p_ent
                                        stop_p = hi_on_e + tick if (hi_on_e - p_ent)*1000 >= gap else p_ent + gap/1000
                                        
                                        m_end = min(m + self.hold_mins, 270)
                                        for m_exit in range(m + 1, m_end + 1):
                                            r_ex = stock_records[target['sym']][m_exit]
                                            if r_ex['high'] >= stop_p or m_exit == m_end:
                                                p_exit = stop_p if r_ex['high'] >= stop_p else r_ex['close']
                                                buy_total = shrs * p_exit * 1000
                                                profit = sell_total - buy_total - ent_fee - int(buy_total * f_rate * d_rate) - tax
                                                stats[th]['signals'] += 1; stats[th]['pnl_sum'] += (profit * 100) / (buy_total - int(buy_total * f_rate * d_rate))
                                                if profit > 0: stats[th]['wins'] += 1
                                                is_busy, exit_at = True, m_exit; break
                                    
                                    pull_up = limit_up = False; leader, tracking, in_wait, wait_cnt = None, set(), False, 0
                                else:
                                    if leader and leader_peak_rise is not None and row_data[leader]['rise'] > leader_peak_rise: leader_peak_rise, in_wait, wait_cnt = row_data[leader]['rise'], False, 0
                                    else: wait_cnt += 1

            for th in thresholds:
                n = stats[th]['signals']
                avg_p, wr = (stats[th]['pnl_sum'] / n) if n > 0 else 0, (stats[th]['wins'] / n * 100) if n > 0 else 0
                score = avg_p * np.log10(n + 1) if avg_p > 0 else avg_p
                self.log_signal.emit(f"<span style='color:{'#2ECC40' if avg_p > 0 else '#FF4136'}; font-family:Consolas, \"MingLiU\", monospace;'>{f'{th:>10.2f} | {n:>10} | {wr:>8.1f} % | {avg_p:>8.1f} % | {score:>10.2f}'.replace(' ', '&nbsp;')}</span>")
                if score > max_score and n > 0: max_score, best_th = score, th
            
            self.log_signal.emit(f"<span style='color:#FFDC00;'>{'-' * 62}</span>")
            self.log_signal.emit(f"<span style='color:#2ECC40; font-weight:bold;'>👉 同步後建議門檻：DTW &gt;= {best_th:.2f}</span>")
            self.log_signal.emit(f"<span style='color:#2ECC40;'>💡 說明：穩健分數越高，代表在該門檻下獲利越穩定。</span>")
            self.log_signal.emit(f"<span style='color:#2ECC40;'>💡 建議：可依據個人風險承受度，微調 ±0.05 的門檻值。</span>")
            self.log_signal.emit(f"<span style='color:#FFDC00;'>{'=' * 62}</span><br>")
        except Exception as e: self.log_signal.emit(f"<span style='color:#FF4136;'>❌ 系統錯誤：<br>{traceback.format_exc().replace(chr(10), '<br>')}</span>")
        finally: self.finished_signal.emit()

class LoginDialog(BaseDialog):
    def __init__(self):
        super().__init__("雙券商 API 帳戶設定", (550, 500))
        layout = QVBoxLayout(self)

        g_esun = QGroupBox("玉山證券 (看盤數據)"); g_esun.setStyleSheet("QGroupBox{color: #F1C40F; font-weight: bold; border: 1px solid #B7950B; padding-top: 15px;}")
        l_esun = QFormLayout(g_esun)
        self.e_esun_key, self.e_esun_sec, self.e_esun_acc, self.e_esun_cert = QLineEdit(), QLineEdit(), QLineEdit(), QLineEdit()
        if os.path.exists('config.ini'):
            c = ConfigParser(); c.read('config.ini', encoding='utf-8-sig')
            self.e_esun_key.setText(c.get('Api', 'Key', fallback='')); self.e_esun_sec.setText(c.get('Api', 'Secret', fallback=''))
            self.e_esun_acc.setText(c.get('User', 'Account', fallback='')); self.e_esun_cert.setText(c.get('Cert', 'Path', fallback=''))
        l_esun.addRow("esun_api_key:", self.e_esun_key); l_esun.addRow("esun_api_Secret:", self.e_esun_sec); l_esun.addRow("esun_user_Account:", self.e_esun_acc)
        cl = QHBoxLayout(); cl.addWidget(self.e_esun_cert)
        b1 = QPushButton("📁 瀏覽..."); b1.setStyleSheet("background-color: #34495E;"); b1.clicked.connect(lambda: self.e_esun_cert.setText(os.path.basename(QFileDialog.getOpenFileName(self, "", "", "*.p12 *.pfx")[0]))); cl.addWidget(b1); l_esun.addRow("esun_Cert_path:", cl)
        layout.addWidget(g_esun)

        g_shio = QGroupBox("永豐金證券 Shioaji (實際下單)"); g_shio.setStyleSheet("QGroupBox{color: #3498DB; font-weight: bold; border: 1px solid #2980B9; padding-top: 15px;}")
        l_shio = QFormLayout(g_shio)
        self.e_api, self.e_sec, self.e_ca, self.e_pw = QLineEdit(shioaji_logic.TEST_API_KEY), QLineEdit(shioaji_logic.TEST_API_SECRET), QLineEdit(shioaji_logic.CA_CERT_PATH), QLineEdit(shioaji_logic.CA_PASSWORD)
        l_shio.addRow("TEST_API_KEY:", self.e_api); l_shio.addRow("TEST_API_SECRET:", self.e_sec)
        cl2 = QHBoxLayout(); cl2.addWidget(self.e_ca); b2 = QPushButton("📁 瀏覽..."); b2.setStyleSheet("background-color: #34495E;"); b2.clicked.connect(lambda: self.e_ca.setText(QFileDialog.getOpenFileName(self, "", "", "*.p12 *.pfx")[0])); cl2.addWidget(b2); l_shio.addRow("CA_CERT_PATH:", cl2)
        l_shio.addRow("CA_PASSWORD:", self.e_pw); layout.addWidget(g_shio)

        btn = QPushButton("💾 儲存並套用所有設定"); btn.setStyleSheet("background-color: #27AE60; font-weight: bold;"); btn.clicked.connect(self.save); layout.addWidget(btn)

    def save(self):
        update_variable("shioaji_logic.py", "TEST_API_KEY", self.e_api.text()); update_variable("shioaji_logic.py", "TEST_API_SECRET", self.e_sec.text())
        update_variable("shioaji_logic.py", "CA_CERT_PATH", self.e_ca.text(), is_raw=True); update_variable("shioaji_logic.py", "CA_PASSWORD", self.e_pw.text())
        c = ConfigParser(); c.read('config.ini', encoding='utf-8-sig') if os.path.exists('config.ini') else None
        for sec in ['Core', 'Api', 'Cert', 'User']: 
            if not c.has_section(sec): c.add_section(sec)
        c.set('Core', 'Entry', 'https://esuntradingapi-simulation.esunsec.com.tw/api/v1'); c.set('Core', 'Environment', 'SIMULATION')
        c.set('Api', 'Key', self.e_esun_key.text().strip()); c.set('Api', 'Secret', self.e_esun_sec.text().strip()); c.set('User', 'Account', self.e_esun_acc.text().strip()); c.set('Cert', 'Path', self.e_esun_cert.text().strip())
        with open('config.ini', 'w', encoding='utf-8') as f: c.write(f)
        QMessageBox.information(self, "成功", "雙券商設定已儲存！"); self.accept()

class TradeDialog(BaseDialog):
    def __init__(self):
        super().__init__("啟動盤中監控", (350, 250))
        layout = QFormLayout(self)
        self.w_wait = QLineEdit("5"); self.w_hold = QLineEdit("F")
        layout.addRow("等待時間 (分鐘):", self.w_wait); layout.addRow("持有時間 (分鐘, F=尾盤):", self.w_hold)
        
        btn = QPushButton("▶ 啟動監控")
        btn.setStyleSheet("QPushButton { background-color: #C0392B; } QPushButton:hover { background-color: #E74C3C; }")
        btn.clicked.connect(self.run_trade); layout.addRow(btn)
        
        btn_login = QPushButton("🔑 登入/修改帳戶")
        btn_login.setStyleSheet("QPushButton { background-color: #2980B9; } QPushButton:hover { background-color: #3498DB; }")
        btn_login.clicked.connect(self.open_login_dialog); layout.addRow(btn_login)

    def open_login_dialog(self):
        if not hasattr(self, 'dlg_login') or not self.dlg_login.isVisible():
            self.dlg_login = LoginDialog(); self.dlg_login.show()
        else:
            self.dlg_login.raise_(); self.dlg_login.activateWindow()

    def run_trade(self):
        if getattr(sys_state, 'is_monitoring', False):
            return QMessageBox.warning(self, "提示", "⚠️ 盤中監控已經在背景執行（或待命中），請勿重複啟動！")
        
        try: w = int(self.w_wait.text())
        except: return QMessageBox.critical(self, "錯誤", "等待時間需為整數")
        try: h = None if self.w_hold.text().strip().upper() == 'F' else int(self.w_hold.text().strip().upper()); 
        except: return QMessageBox.critical(self, "錯誤", "持有時間格式錯誤")
        if h is not None and h < 1: return QMessageBox.critical(self, "錯誤", "持有時間最少需為 1 分鐘")
        if ensure_esun_passwords(): self.accept(); threading.Thread(target=start_trading, args=('full', w, h), daemon=True).start()

class CorrelationResultDialog(BaseDialog):
    def __init__(self, result_data, parent=None):
        super().__init__("🧬 族群連動分析結果", (850, 600))
        self.result_data = result_data 
        layout = QVBoxLayout(self)
        self.table = QTableWidget(); self.table.setColumnCount(6); self.table.setHorizontalHeaderLabels(["族群", "領漲股", "跟漲股", "時間窗", "DTW相似度", "結果"])
        self.table.setStyleSheet("QTableWidget { background-color: #1e1e1e; color: white; gridline-color: #444; } QTableWidget::item { color: white; } QHeaderView::section { background-color: #2C3E50; color: white; font-weight: bold; }")
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch); self.table.setRowCount(len(result_data))
        for i, r in enumerate(result_data):
            self.table.setItem(i, 0, QTableWidgetItem(str(r['group']))); self.table.setItem(i, 1, QTableWidgetItem(str(r['leader']))); self.table.setItem(i, 2, QTableWidgetItem(str(r['follower']))); self.table.setItem(i, 3, QTableWidgetItem(str(r['window'])))
            sim_item = QTableWidgetItem(f"{r['similarity']:.3f}"); sim_item.setForeground(QColor("#2ECC40" if r['similarity'] >= 0.75 else "#FF4136"))
            self.table.setItem(i, 4, sim_item); self.table.setItem(i, 5, QTableWidgetItem("✅ 合格" if r['similarity'] >= 0.75 else "❌ 剔除"))
        layout.addWidget(self.table)
        btn_export = QPushButton("📥 匯出 CSV"); btn_export.setStyleSheet("background-color: #27AE60;"); btn_export.clicked.connect(self.export_to_csv); layout.addWidget(btn_export)

    def export_to_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "儲存", "族群連動分析結果.csv", "CSV 檔案 (*.csv)")
        if path:
            try:
                with open(path, 'w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.writer(f); writer.writerow(["族群", "領漲股", "跟漲股", "時間窗", "相似度", "結果"])
                    for r in self.result_data: writer.writerow([r['group'], r['leader'], r['follower'], r['window'], f"{r['similarity']:.3f}", "合格" if r['similarity'] >= 0.75 else "剔除"])
                QMessageBox.information(self, "成功", f"儲存至：\n{path}")
            except Exception as e: QMessageBox.critical(self, "失敗", f"發生錯誤：\n{e}")

class CorrelationConfigDialog(BaseDialog):
    def __init__(self, main_window):
        super().__init__("設定連動分析參數", (400, 200))
        self.main_window = main_window
        layout = QVBoxLayout(self)
        
        self.mode_combo = QComboBox()
        # 🟢 關鍵修正：套用與「自選進場模式」一模一樣的視圖，強制 CSS 生效！
        self.mode_combo.setView(QListView()) 
        self.mode_combo.addItems(["[A] 宏觀連動 (09:00~13:30)", "[B] 微觀模擬"])
        
        self.wait_spin = QLineEdit("5")
        
        form_layout = QFormLayout()
        form_layout.addRow("分析模式：", self.mode_combo)
        form_layout.addRow("微觀等待 (分鐘)：", self.wait_spin)
        layout.addLayout(form_layout)
        
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_box.accepted.connect(self.start_analysis)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

    def start_analysis(self):
        self.accept() # 關閉目前視窗
        mode = "macro" if self.mode_combo.currentIndex() == 0 else "micro"
        w_mins = int(self.wait_spin.text()) if self.wait_spin.text().isdigit() else 5
        # 呼叫主視窗去跑背景分析
        self.main_window.start_correlation_thread(mode, w_mins)

class AnalysisMenuDialog(BaseDialog):
    def __init__(self, main_window):
        super().__init__("盤後數據與分析中心", (320, 240))
        self.main_window = main_window
        layout = QVBoxLayout(self)
        b1 = QPushButton("🏆 最適相似度門檻分析")
        b1.setStyleSheet("QPushButton { background-color: #D35400; } QPushButton:hover { background-color: #E67E22; }")
        b1.clicked.connect(self.click_opt_sim); layout.addWidget(b1)
        
        b2 = QPushButton("📈 計算平均過高")
        b2.setStyleSheet("QPushButton { background-color: #34495E; } QPushButton:hover { background-color: #4B6584; }")
        b2.clicked.connect(self.click_avg_high); layout.addWidget(b2)
        
        b3 = QPushButton("🧬 族群連動分析掃描")
        b3.setStyleSheet("QPushButton { background-color: #8E44AD; } QPushButton:hover { background-color: #9B59B6; }")
        b3.clicked.connect(self.click_correlation); layout.addWidget(b3)

    # 🟢 加入 *args 吸收 PyQt5 預設傳遞的 checked 布林值，徹底防止當機
    def click_opt_sim(self, *args):
        self.accept()
        self.main_window.open_modeless(SimilarityOptimizationDialog, 'dlg_opt_sim')

    def click_avg_high(self, *args):
        self.accept()
        self.main_window.open_modeless(AverageHighDialog, 'dlg_avg_high')

    def click_correlation(self, *args):
        self.accept()
        if not hasattr(self.main_window, 'dlg_corr_cfg') or not self.main_window.dlg_corr_cfg.isVisible():
            self.main_window.dlg_corr_cfg = CorrelationConfigDialog(self.main_window)
            self.main_window.dlg_corr_cfg.show()
        else:
            self.main_window.dlg_corr_cfg.raise_()
            self.main_window.dlg_corr_cfg.activateWindow()

from PyQt5.QtWidgets import QListView
class SimulateDialog(BaseDialog):
    def __init__(self):
        super().__init__("自選進場模式 (回測)", (400, 250))
        layout = QFormLayout(self)
        self.w_grp = QComboBox(); self.w_grp.setView(QListView())
        self.w_grp.addItem("所有族群"); self.w_grp.addItems(list(load_matrix_dict_analysis().keys()))
        self.w_wait = QLineEdit("5"); self.w_hold = QLineEdit("F")
        layout.addRow("分析族群:", self.w_grp); layout.addRow("等待 (分鐘):", self.w_wait); layout.addRow("持有 (分鐘/F):", self.w_hold)
        btn = QPushButton("▶ 開始分析"); btn.setStyleSheet("background-color: #E67E22;"); btn.clicked.connect(self.run_sim); layout.addRow(btn)

    def run_sim(self):
        grp = self.w_grp.currentText()
        try: w = int(self.w_wait.text())
        except: return QMessageBox.critical(self, "錯誤", "等待時間需為整數")
        try: h = None if self.w_hold.text().upper() == 'F' else int(self.w_hold.text().upper())
        except: return QMessageBox.critical(self, "錯誤", "持有時間格式錯誤")
        self.accept()

        def _logic():
            ui_dispatcher.progress_visible.emit(True)
            mat, (d_kline, i_kline), dispo = load_matrix_dict_analysis(), load_kline_data(), load_disposition_stocks()
            all_trades = []
            all_events = [] 

            # 🟢 核心除蟲：鎖定最後一次採集的日期，過濾掉資料庫中其他日期的歷史數據
            target_date = sys_db.load_state('last_fetched_date')
            if target_date:
                print(f"\n🎯 系統已自動鎖定回測日期：{target_date}")
                for sym in list(i_kline.keys()):
                    i_kline[sym] = [r for r in i_kline[sym] if r.get('date') == target_date]
            else:
                print("\n⚠️ 找不到最後採集日期標籤，可能會混雜多日數據！")

            if grp != "所有族群":
                if grp not in mat: return print(f"❌ 找不到族群: {grp}")
                tp, ap, t_hist, e_log = process_group_data(initialize_stock_data([s for s in mat[grp] if s not in dispo], d_kline, i_kline), w, h, mat, verbose=True, progress_callback=lambda p, msg: ui_dispatcher.progress_updated.emit(p, msg))
                if t_hist: all_trades.extend(t_hist)
                if e_log: all_events.extend(e_log) 
            else:
                print("\n🌐 啟動全市場族群掃描...")
                tp_sum, rate_list, total = 0, [], len(mat)
                for i, (g, s) in enumerate(mat.items()):
                    print(f"\n【開始分析族群：{g}】")
                    data = initialize_stock_data([x for x in s if x not in dispo], d_kline, i_kline)
                    tp, ap, t_hist, e_log = process_group_data(data, w, h, mat, verbose=True, progress_callback=lambda p, msg: ui_dispatcher.progress_updated.emit(int((i/total)*100 + (p/total)), f"[{g}] {msg}"))
                    if tp is not None: tp_sum += tp; rate_list.append(ap); all_trades.extend(t_hist)
                    if e_log: all_events.extend(e_log) 

                if rate_list:
                    avg_rate = sum(rate_list)/len(rate_list)
                    c = RED if tp_sum > 0 else (GREEN if tp_sum < 0 else "")
                    print(f"\n{c}================================")
                    print(f"{c}💰 當日總利潤：{int(tp_sum)} 元")
                    print(f"{c}📈 平均報酬率：{avg_rate:.2f}%")
                    print(f"{c}================================{RESET}")
                else:
                    print("\n================================")
                    print("⚠️ 全市場掃描完畢，當日無任何交易產生。")
                    print("================================")

            if all_trades or all_events:
                ui_dispatcher.plot_equity_curve.emit((all_trades, all_events, i_kline))
            ui_dispatcher.progress_visible.emit(False)
        threading.Thread(target=_logic, daemon=True).start()

class MaximizeDialog(BaseDialog):
    def __init__(self):
        super().__init__("極大化利潤模式 (矩陣併發版)", (400, 350))
        layout = QFormLayout(self)
        self.e_grp = QComboBox(); self.e_grp.setView(QListView())
        self.e_grp.addItem("所有族群"); self.e_grp.addItems(list(load_matrix_dict_analysis().keys()))
        self.e_ws, self.e_we, self.e_hs, self.e_he = QLineEdit("3"), QLineEdit("5"), QLineEdit("10"), QLineEdit("20")
        layout.addRow("族群:", self.e_grp); layout.addRow("等(起):", self.e_ws); layout.addRow("等(終):", self.e_we); layout.addRow("持(起/F):", self.e_hs); layout.addRow("持(終/F):", self.e_he)
        btn = QPushButton("🚀 啟動矩陣併發回測"); btn.setStyleSheet("background-color: #D35400; color: white; font-weight: bold; padding: 8px;"); btn.clicked.connect(self.run_max); layout.addRow(btn)

    def run_max(self):
        grp = self.e_grp.currentText()
        try: ws, we = int(self.e_ws.text()), int(self.e_we.text())
        except: return QMessageBox.critical(self, "錯誤", "等待時間必須是整數")
        hs_str, he_str = self.e_hs.text().strip().upper(), self.e_he.text().strip().upper()
        hold_options = []
        if hs_str == 'F' and he_str == 'F': hold_options = [None]
        elif hs_str != 'F' and he_str != 'F':
            try: hold_options = list(range(int(hs_str), int(he_str) + 1))
            except: return QMessageBox.critical(self, "錯誤", "至少為 1 分鐘")
        else:
            try: hold_options = [int(hs_str), None] if hs_str != 'F' else [None, int(he_str)]
            except: return QMessageBox.critical(self, "錯誤", "必須是整數或 F")

        self.accept()

        def _logic():
            ui_dispatcher.progress_visible.emit(True)
            ui_dispatcher.progress_updated.emit(5, "📦 正在預熱與清洗族群歷史資料...")
            
            mat = load_matrix_dict_analysis()
            d_kline, i_kline = load_kline_data()
            dispo = load_disposition_stocks()
            
            # 🟢 1. 資料預處理快取 (Pre-computation)
            # 把重複又耗時的資料合併動作移到最外面，做一次就好！
            group_data_cache = {}
            target_groups = [grp] if grp != "所有族群" else list(mat.keys())
            for g_name in target_groups:
                symbols = [s for s in mat.get(g_name, []) if s not in dispo]
                if symbols:
                    group_data_cache[g_name] = initialize_stock_data(symbols, d_kline, i_kline)
            
            # 🟢 2. 建立參數矩陣 (例如 3x11 = 33 組)
            param_matrix = [(w, h) for w in range(ws, we + 1) for h in hold_options]
            total_combinations = len(param_matrix)
            
            print(f"\n🔥 資料快取完成！啟動併發引擎，同時處理 {total_combinations} 組參數矩陣...\n")
            ui_dispatcher.progress_updated.emit(10, f"啟動 {total_combinations} 組矩陣運算...")
            
            results = []
            completed_tasks = 0
            progress_lock = threading.Lock() # 安全鎖：確保多執行緒不會把進度條算錯

            # 🟢 定義「單一參數組合」的運算邏輯
            def run_combination(w, h_val):
                tp_sum, rate_list = 0, []
                for g_name, data in group_data_cache.items():
                    # 注意：這裡刻意將 progress_callback 設為 None，避免內外進度條互相打架
                    tp, ap, _, _ = process_group_data(data, w, h_val, mat, verbose=False, progress_callback=None)
                    if tp is not None:
                        tp_sum += tp
                        rate_list.append(ap)
                
                avg_rate = sum(rate_list) / len(rate_list) if rate_list else 0
                
                # 運算完畢，安全地推進整體進度條
                nonlocal completed_tasks
                with progress_lock:
                    completed_tasks += 1
                    # 進度從 10% 走到 100%
                    pct = 10 + int((completed_tasks / total_combinations) * 90)
                    ui_dispatcher.progress_updated.emit(pct, f"矩陣平行運算中... ({completed_tasks} / {total_combinations})")
                    
                return {'等待時間': w, '持有時間': 'F' if h_val is None else h_val, '總利潤': float(tp_sum), '平均報酬率': float(avg_rate)}

            # 🟢 3. 啟動多執行緒平行運算
            # max_workers 會自動根據你的參數數量和 CPU 核心數來調配火力
            with ThreadPoolExecutor(max_workers=min(32, total_combinations)) as executor:
                # 一口氣將 33 組任務全部發派給工人
                futures = [executor.submit(run_combination, w, h) for w, h in param_matrix]
                for future in as_completed(futures):
                    results.append(future.result())

            # 🟢 4. 排序並產出最終排行榜
            results_df = pd.DataFrame(results)
            if not results_df.empty:
                best = results_df.loc[results_df['總利潤'].idxmax()]
                c_best = RED if best['總利潤'] > 0 else (GREEN if best['總利潤'] < 0 else "")
                print(f"\n{c_best}🏆 最佳組合：等待 {best['等待時間']} 分 / 持有 {best['持有時間']} 分 / 利潤：{int(best['總利潤'])} 元{RESET}")
                
                print("\n📊 參數矩陣排行榜 (前 10 名)：")
                for rank, (idx, row) in enumerate(results_df.sort_values(by='總利潤', ascending=False).head(10).iterrows(), 1):
                    rc = RED if row['總利潤'] > 0 else (GREEN if row['總利潤'] < 0 else "")
                    print(f"   第 {rank:>2} 名: 等待 {row['等待時間']:>2} 分, 持有 {str(row['持有時間']):>2} 分 ➔ {rc}利潤: {int(row['總利潤']):>6} 元 ({row['平均報酬率']:.2f}%){RESET}")
                print("\n")

            ui_dispatcher.progress_visible.emit(False) 
            print("✅ 極大化利潤矩陣運算完畢！")

        threading.Thread(target=_logic, daemon=True).start()

class AverageHighDialog(BaseDialog):
    def __init__(self):
        super().__init__("計算平均過高", (350, 200))
        layout = QVBoxLayout(self)
        b1 = QPushButton("單一族群分析"); b1.setStyleSheet("background-color: #2980B9;"); b1.clicked.connect(self.run_single); layout.addWidget(b1)
        b2 = QPushButton("全部族群分析"); b2.setStyleSheet("background-color: #16A085;"); b2.clicked.connect(self.run_all); layout.addWidget(b2)
        
    def run_single(self):
        grp, ok = QInputDialog.getItem(self, "選擇", "選擇族群:", list(load_matrix_dict_analysis().keys()), 0, False)
        if ok and grp: 
            self.accept()
            # 🟢 使用獨立內部函式取代 lambda，確保跨執行緒的安全
            def _task():
                ui_dispatcher.progress_visible.emit(True)
                calculate_average_over_high(grp, progress_callback=lambda p, msg: ui_dispatcher.progress_updated.emit(p, msg))
                ui_dispatcher.progress_visible.emit(False)
            threading.Thread(target=_task, daemon=True).start()
            
    def run_all(self):
        self.accept()
        def _logic():
            ui_dispatcher.progress_visible.emit(True)
            groups, avgs, total = load_matrix_dict_analysis(), [], len(load_matrix_dict_analysis())
            for i, g in enumerate(groups.keys()):
                if avg := calculate_average_over_high(g, progress_callback=lambda p, msg: ui_dispatcher.progress_updated.emit(int((i/total)*100 + (p/total)), f"[{g}] {msg}")): avgs.append(avg)
            if avgs: print(f"\n全部族群的平均過高間隔：{sum(avgs)/len(avgs):.2f} 分鐘")
            ui_dispatcher.progress_visible.emit(False)
        threading.Thread(target=_logic, daemon=True).start()

class SettingsDialog(BaseDialog):
    def __init__(self):
        super().__init__("系統參數設定", (500, 900)) # 🟢 稍微拉高視窗以容納新設定
        self.setStyleSheet("""
            QDialog, QWidget, QScrollArea { background-color: #F5F5F5; color: black; }
            QLabel { font-size: 14px; font-weight: bold; color: black; }
            QLineEdit, QComboBox, QDoubleSpinBox, QSpinBox { background-color: white; color: black; border: 1px solid #999; padding: 4px; border-radius: 4px;}
            QPushButton { font-size: 14px; border-radius: 5px; color: white; background-color: #27AE60; padding: 6px;}
            QGroupBox { font-size: 14px; font-weight: bold; border: 2px solid #bdc3c7; border-radius: 6px; margin-top: 10px; padding-top: 15px;}
            QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 5px; color: #2c3e50; }
        """)
        layout = QVBoxLayout(self)
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        w = QWidget(); main_form = QVBoxLayout(w)

        # --- 基本交易設定 ---
        grp_basic = QGroupBox("💰 基本交易與停損設定")
        form_b = QFormLayout()
        self.e_cap = QLineEdit(str(sys_config.capital_per_stock)); form_b.addRow("投入資本額 (萬元):", self.e_cap)
        self.e_fee = QLineEdit(str(sys_config.transaction_fee)); form_b.addRow("手續費 (%):", self.e_fee)
        self.e_dis = QLineEdit(str(sys_config.transaction_discount)); form_b.addRow("手續費折數 (%):", self.e_dis)
        self.e_tax = QLineEdit(str(sys_config.trading_tax)); form_b.addRow("證交稅 (%):", self.e_tax)
        form_b.addRow(QLabel("【停損價差階距】"))
        self.e_50, self.e_100, self.e_500, self.e_1000, self.e_above = QLineEdit(str(sys_config.below_50)), QLineEdit(str(sys_config.price_gap_50_to_100)), QLineEdit(str(sys_config.price_gap_100_to_500)), QLineEdit(str(sys_config.price_gap_500_to_1000)), QLineEdit(str(sys_config.price_gap_above_1000))
        form_b.addRow("50元以下:", self.e_50); form_b.addRow("50~100元:", self.e_100); form_b.addRow("100~500元:", self.e_500); form_b.addRow("500~1000元:", self.e_1000); form_b.addRow("1000元以上:", self.e_above)
        grp_basic.setLayout(form_b); main_form.addWidget(grp_basic)

        # --- 🟢 新增：停損再進場專屬分區 ---
        from PyQt5.QtWidgets import QCheckBox, QSpinBox
        grp_reentry = QGroupBox("🔄 停損再進場設定")
        form_r = QFormLayout()
        self.reentry_cb = QCheckBox("開啟停損再進場功能")
        self.reentry_cb.setChecked(sys_config.allow_reentry)
        form_r.addRow(self.reentry_cb)
        
        self.max_reentry_spin = QSpinBox()
        self.max_reentry_spin.setRange(1, 5)
        self.max_reentry_spin.setValue(sys_config.max_reentry_times)
        form_r.addRow("最多允許再進場次數:", self.max_reentry_spin)
        
        self.lookback_spin = QSpinBox()
        self.lookback_spin.setRange(1, 20)
        self.lookback_spin.setValue(sys_config.reentry_lookback_candles)
        form_r.addRow("停損後往前檢查 K 棒數:", self.lookback_spin)
        
        grp_reentry.setLayout(form_r); main_form.addWidget(grp_reentry)

        # --- 開發者模式分區 ---
        grp_dev = QGroupBox("🛠️ 開發者模式 (進階策略微調)")
        form_d = QFormLayout()
        form_d.addRow(QLabel("▶ 【整體動能與關聯】"))
        self.momentum_combo = QComboBox(); self.momentum_combo.setView(QListView()); self.momentum_combo.addItems(["1 分鐘", "2 分鐘", "3 分鐘", "4 分鐘", "5 分鐘"]); self.momentum_combo.setCurrentIndex(sys_config.momentum_minutes - 1)
        form_d.addRow("發動動能觀察時間:", self.momentum_combo)
        self.sim_thresh_spin = QDoubleSpinBox(); self.sim_thresh_spin.setRange(-1.0, 1.0); self.sim_thresh_spin.setSingleStep(0.05); self.sim_thresh_spin.setValue(sys_config.similarity_threshold)
        form_d.addRow("DTW 相似度門檻 (-1.0~1.0):", self.sim_thresh_spin)

        form_d.addRow(QLabel("▶ 【觸發條件門檻】"))
        self.pull_up_spin = QDoubleSpinBox(); self.pull_up_spin.setRange(0.5, 10.0); self.pull_up_spin.setSingleStep(0.5); self.pull_up_spin.setValue(sys_config.pull_up_pct_threshold)
        form_d.addRow("領漲股拉高漲幅 (%):", self.pull_up_spin)
        self.follow_up_spin = QDoubleSpinBox(); self.follow_up_spin.setRange(0.5, 10.0); self.follow_up_spin.setSingleStep(0.5); self.follow_up_spin.setValue(sys_config.follow_up_pct_threshold)
        form_d.addRow("跟漲股追蹤漲幅 (%):", self.follow_up_spin)

        form_d.addRow(QLabel("▶ 【進場防護濾網】"))
        self.rise_lower_spin = QDoubleSpinBox(); self.rise_lower_spin.setRange(-10.0, 9.0); self.rise_lower_spin.setSingleStep(0.5); self.rise_lower_spin.setValue(sys_config.rise_lower_bound)
        self.rise_upper_spin = QDoubleSpinBox(); self.rise_upper_spin.setRange(-9.0, 10.0); self.rise_upper_spin.setSingleStep(0.5); self.rise_upper_spin.setValue(sys_config.rise_upper_bound)
        form_d.addRow("標的當日總漲幅 下限 (%):", self.rise_lower_spin)
        form_d.addRow("標的當日總漲幅 上限 (%):", self.rise_upper_spin)

        self.vol_mult_spin = QDoubleSpinBox(); self.vol_mult_spin.setRange(0.1, 10.0); self.vol_mult_spin.setSingleStep(0.1); self.vol_mult_spin.setValue(sys_config.volume_multiplier)
        self.vol_min_spin = QSpinBox(); self.vol_min_spin.setRange(0, 10000); self.vol_min_spin.setSingleStep(10); self.vol_min_spin.setValue(sys_config.min_volume_threshold)
        form_d.addRow("爆量：需大於開盤均量 (倍):", self.vol_mult_spin)
        form_d.addRow("爆量：且絕對數量大於 (張):", self.vol_min_spin)

        self.pullback_spin = QDoubleSpinBox(); self.pullback_spin.setRange(0.0, 5.0); self.pullback_spin.setSingleStep(0.1); self.pullback_spin.setValue(sys_config.pullback_tolerance)
        form_d.addRow("防誘空：突破前高容錯率 (%):", self.pullback_spin)
        grp_dev.setLayout(form_d); main_form.addWidget(grp_dev)

        # --- 授權 ---
        grp_tg = QGroupBox("⚡ Telegram 終端授權")
        form_t = QFormLayout()
        self.e_tg_chat_id = QLineEdit(getattr(sys_config, 'tg_chat_id', ''))
        self.e_tg_chat_id.setPlaceholderText("請輸入您的專屬授權碼 (純數字)")
        form_t.addRow("綁定授權碼:", self.e_tg_chat_id)
        grp_tg.setLayout(form_t); main_form.addWidget(grp_tg)

        scroll.setWidget(w); layout.addWidget(scroll)
        btn = QPushButton("💾 儲存並連線"); btn.clicked.connect(self.save); layout.addWidget(btn)

    def save(self):
        try:
            sys_config.capital_per_stock, sys_config.transaction_fee, sys_config.transaction_discount, sys_config.trading_tax = int(self.e_cap.text()), float(self.e_fee.text()), float(self.e_dis.text()), float(self.e_tax.text())
            sys_config.below_50, sys_config.price_gap_50_to_100, sys_config.price_gap_100_to_500, sys_config.price_gap_500_to_1000, sys_config.price_gap_above_1000 = float(self.e_50.text()), float(self.e_100.text()), float(self.e_500.text()), float(self.e_1000.text()), float(self.e_above.text())
            
            # 🟢 儲存停損再進場設定
            sys_config.allow_reentry = self.reentry_cb.isChecked()
            sys_config.max_reentry_times = self.max_reentry_spin.value()
            sys_config.reentry_lookback_candles = self.lookback_spin.value()
            
            sys_config.momentum_minutes = self.momentum_combo.currentIndex() + 1
            sys_config.similarity_threshold = self.sim_thresh_spin.value()
            sys_config.pull_up_pct_threshold = self.pull_up_spin.value()
            sys_config.follow_up_pct_threshold = self.follow_up_spin.value()
            sys_config.rise_lower_bound = self.rise_lower_spin.value()
            sys_config.rise_upper_bound = self.rise_upper_spin.value()
            sys_config.volume_multiplier = self.vol_mult_spin.value()
            sys_config.min_volume_threshold = self.vol_min_spin.value()
            sys_config.pullback_tolerance = self.pullback_spin.value()

            sys_config.tg_chat_id = self.e_tg_chat_id.text().strip()
            save_settings()
            tg_bot.start()
            print(f"✅ 授權儲存！目前綁定客戶: {'已綁定' if sys_config.tg_chat_id else '未綁定'}"); self.accept()
        except: QMessageBox.critical(self, "錯誤", "數字格式不正確")

class GroupManagerDialog(BaseDialog):
    def __init__(self):
        super().__init__("管理股票族群", (750, 550)) # 稍微加高以容納搜尋列
        layout = QVBoxLayout(self)
        
        # 🟢 新增：頂部萬用搜尋列
        search_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("🔍 搜尋股票 (支援代號、名稱或混合輸入，例如: 2330, 台積電, 2330台積電)")
        self.search_input.setStyleSheet("QLineEdit { padding: 8px; font-size: 14px; border-radius: 4px; border: 1px solid #555; background-color: #2C2C2C; color: white; }")
        self.search_input.returnPressed.connect(self.search_stock) # 按 Enter 直接搜
        
        btn_search = QPushButton("搜尋並定位")
        btn_search.setStyleSheet("QPushButton { background-color: #E67E22; padding: 8px 15px; font-weight: bold; } QPushButton:hover { background-color: #D35400; }")
        btn_search.clicked.connect(self.search_stock)
        
        search_layout.addWidget(self.search_input)
        search_layout.addWidget(btn_search)
        layout.addLayout(search_layout)

        # 分隔線
        line = QFrame(); line.setFrameShape(QFrame.HLine); line.setFrameShadow(QFrame.Sunken); layout.addWidget(line)

        # 以下是原本的版面結構
        splitter = QSplitter(Qt.Horizontal)
        
        left_widget = QWidget(); left_layout = QVBoxLayout(left_widget)
        left_layout.addWidget(QLabel("📁 族群分類 (💡 右鍵可修改名稱/刪除)"))
        self.group_list = QListWidget(); self.group_list.setStyleSheet("QListWidget { font-size: 15px; background-color: #2C2C2C; color: white; border: 1px solid #555; } QListWidget::item:selected { background-color: #2980B9; color: white; }")
        
        self.group_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.group_list.customContextMenuRequested.connect(self.show_group_context_menu)
        self.group_list.itemSelectionChanged.connect(self.on_group_selected); left_layout.addWidget(self.group_list)
        
        btn_layout_l = QHBoxLayout()
        btn_add_g = QPushButton("➕ 新增族群"); btn_add_g.setStyleSheet("QPushButton { background-color: #27AE60; }"); btn_add_g.clicked.connect(self.add_grp)
        btn_layout_l.addWidget(btn_add_g); left_layout.addLayout(btn_layout_l)
        
        right_widget = QWidget(); right_layout = QVBoxLayout(right_widget)
        self.lbl_current_group = QLabel("📌 請選擇左側族群 (💡 雙擊看走勢 / 右鍵刪除或看走勢)"); self.lbl_current_group.setStyleSheet("color: #F1C40F;")
        right_layout.addWidget(self.lbl_current_group)
        self.stock_list = QListWidget(); self.stock_list.setStyleSheet("QListWidget { font-size: 15px; background-color: #1E1E1E; color: white; border: 1px solid #555; } QListWidget::item:selected { background-color: #8E44AD; }")
        self.stock_list.setSelectionMode(QAbstractItemView.ExtendedSelection); self.stock_list.itemDoubleClicked.connect(self.plot_single_stock)
        
        self.stock_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.stock_list.customContextMenuRequested.connect(self.show_stock_context_menu)
        right_layout.addWidget(self.stock_list)
        
        btn_layout_r = QHBoxLayout()
        btn_add_s = QPushButton("➕ 新增個股"); btn_add_s.setStyleSheet("QPushButton { background-color: #2980B9; }"); btn_add_s.clicked.connect(self.add_stk)
        btn_layout_r.addWidget(btn_add_s); right_layout.addLayout(btn_layout_r)
        
        splitter.addWidget(left_widget); splitter.addWidget(right_widget); splitter.setSizes([250, 500])
        layout.addWidget(splitter); self.refresh_groups()

    # 🟢 萬能搜索引擎邏輯
    def search_stock(self):
        query = self.search_input.text().strip()
        if not query: return
        
        # 嘗試從使用者的各種輸入格式中，萃取出 4 碼以上的數字 (股票代號)
        import re
        code_match = re.search(r'\d{4,}', query)
        target_code = code_match.group() if code_match else None
        
        # 如果找不到數字，就把整個字串當作股票名稱來搜
        target_name = query if not target_code else None

        g_dict = load_matrix_dict_analysis()
        found_groups = []
        found_code = None

        # 全面掃描資料庫
        for grp_name, symbols in g_dict.items():
            for sym in symbols:
                sym_name = get_stock_name(sym)
                # 比對邏輯：代號吻合，或是名稱包含搜尋字串
                if (target_code and target_code == sym) or (target_name and target_name in sym_name):
                    found_groups.append(grp_name)
                    found_code = sym # 記下真實的代號

        if not found_groups:
            return QMessageBox.information(self, "搜尋結果", f"找不到與「{query}」相關的個股。\n請確認該股票是否已加入任何族群中。")

        # 情境 A：該股票只存在於「一個族群」
        if len(found_groups) == 1:
            target_group = found_groups[0]
            # 1. 讓左側列表跳到該族群
            items = self.group_list.findItems(target_group, Qt.MatchExactly)
            if items:
                self.group_list.setCurrentItem(items[0])
                self.on_group_selected() # 觸發右側更新
                
            # 2. 讓右側列表跳到該個股，並高亮選取
            target_str_start = f"{found_code} "
            for i in range(self.stock_list.count()):
                item = self.stock_list.item(i)
                if item.text().startswith(target_str_start):
                    self.stock_list.setCurrentItem(item)
                    self.stock_list.scrollToItem(item, QAbstractItemView.PositionAtCenter)
                    break
        
        # 情境 B：該股票存在於「多個族群」
        else:
            msg = f"🔍 股票【{found_code} {get_stock_name(found_code)}】目前存在於以下 {len(found_groups)} 個族群中：\n\n"
            for g in found_groups:
                msg += f"👉 {g}\n"
            msg += "\n(因為存在於多個族群，請手動點擊左側族群查看)"
            QMessageBox.information(self, "多重族群提示", msg)

    # 🟢 族群右鍵選單
    def show_group_context_menu(self, pos):
        item = self.group_list.itemAt(pos)
        if not item: return
        menu = QMenu()
        menu.setStyleSheet("QMenu { background-color: #2C2C2C; color: white; } QMenu::item:selected { background-color: #2980B9; }")
        rename_action = menu.addAction("✏️ 修改名稱")
        delete_action = menu.addAction("🗑️ 刪除該族群")
        action = menu.exec_(self.group_list.mapToGlobal(pos))
        if action == rename_action:
            self.rename_grp(item)
        elif action == delete_action:
            self.del_grp(item)

    # 🟢 個股右鍵選單
    def show_stock_context_menu(self, pos):
        item = self.stock_list.itemAt(pos)
        if not item: return
        menu = QMenu()
        menu.setStyleSheet("QMenu { background-color: #2C2C2C; color: white; } QMenu::item:selected { background-color: #2980B9; }")
        view_action = menu.addAction("📈 看該個股走勢")
        delete_action = menu.addAction("🗑️ 刪除該個股")
        action = menu.exec_(self.stock_list.mapToGlobal(pos))
        if action == view_action:
            self.plot_single_stock(item)
        elif action == delete_action:
            self.del_stk(item)

    def refresh_groups(self):
        self.group_list.clear(); groups = load_matrix_dict_analysis(); self.group_list.addItems(list(groups.keys()))
        if self.group_list.count() > 0: self.group_list.setCurrentRow(0)

    def on_group_selected(self):
        self.stock_list.clear()
        if not (selected := self.group_list.currentItem()): return self.lbl_current_group.setText("📌 請選擇左側族群 (💡 點兩下個股可查看當日價量走勢)")
        grp_name = selected.text()
        self.lbl_current_group.setText(f"📌 {grp_name} (💡 點兩下看走勢)")
        groups = load_matrix_dict_analysis(); load_twse_name_map()
        for code in groups.get(grp_name, []): self.stock_list.addItem(sn(code))

    def add_grp(self):
        grp, ok = QInputDialog.getText(self, "新增", "輸入新族群名稱:")
        if ok and grp:
            g = load_matrix_dict_analysis()
            if grp not in g: 
                g[grp] = []; save_matrix_dict(g); self.refresh_groups()
                if items := self.group_list.findItems(grp, Qt.MatchExactly): self.group_list.setCurrentItem(items[0])

    def rename_grp(self, item):
        old_name = item.text()
        new_name, ok = QInputDialog.getText(self, "修改名稱", f"將【{old_name}】修改為：", QLineEdit.Normal, old_name)
        if ok and new_name and new_name != old_name:
            g = load_matrix_dict_analysis()
            if new_name not in g:
                g[new_name] = g.pop(old_name)
                save_matrix_dict(g)
                self.refresh_groups()
                if items := self.group_list.findItems(new_name, Qt.MatchExactly): self.group_list.setCurrentItem(items[0])
            else:
                QMessageBox.warning(self, "錯誤", "該族群名稱已存在！")

    def del_grp(self, item=None):
        if not item: item = self.group_list.currentItem()
        if not item: return
        grp = item.text()
        if QMessageBox.question(self, '⚠️ 確認刪除', f"確定要刪除整個【{grp}】族群嗎？\n該族群內的所有個股將一併被移除！", QMessageBox.Yes | QMessageBox.No, QMessageBox.No) == QMessageBox.Yes:
            g = load_matrix_dict_analysis()
            if grp in g: del g[grp]; save_matrix_dict(g); self.refresh_groups()

    def add_stk(self):
        if not (selected := self.group_list.currentItem()): return QMessageBox.warning(self, "提示", "請先選擇左側的一個族群！")
        grp = selected.text()
        raw_input, ok = QInputDialog.getText(self, "新增個股", "輸入股票名稱或代號 (多檔用半形逗號分隔，支援混合輸入如: 台積電 或 2330):")
        if ok and raw_input:
            g = load_matrix_dict_analysis()
            added = False
            for raw_str in raw_input.split(','):
                stock_code = resolve_stock_code(raw_str)
                if stock_code:
                    if stock_code not in g[grp]:
                        g[grp].append(stock_code)
                        added = True
            if added:
                save_matrix_dict(g); self.on_group_selected()

    def del_stk(self, item=None):
        selected_grp = self.group_list.currentItem()
        if not selected_grp: return QMessageBox.warning(self, "提示", "請先選擇左側族群！")
        
        selected_stocks = [item] if item else self.stock_list.selectedItems()
        if not selected_stocks: return QMessageBox.warning(self, "提示", "請先在右側選取要刪除的個股！")
        
        stock_names = [i.text() for i in selected_stocks]
        if QMessageBox.question(self, '⚠️ 確認刪除', f"確定要從【{selected_grp.text()}】刪除以下個股嗎？\n\n" + "\n".join(stock_names), QMessageBox.Yes | QMessageBox.No, QMessageBox.No) != QMessageBox.Yes: return
        
        g, grp = load_matrix_dict_analysis(), selected_grp.text()
        for i in selected_stocks:
            code = i.text().split(' ')[0]
            if code in g[grp]: g[grp].remove(code)
        save_matrix_dict(g); self.on_group_selected()

    def plot_single_stock(self, item):
        # 🟢 完美修正版：恢復紅綠色風格 + 解決新股票資料對齊問題
        code = item.text().split(' ')[0]
        name = item.text().split(' ')[1] if len(item.text().split(' ')) > 1 else ""
        try:
            # 1. 直接從資料庫抓取最新的 500 筆數據 (確保剛更新的數據能被抓到)
            sql = f"SELECT * FROM intraday_kline_history WHERE symbol = '{code}' ORDER BY date DESC, time ASC LIMIT 500"
            df = pd.read_sql(sql, sys_db.conn)
            
            if df.empty:
                df = pd.read_sql(f"SELECT * FROM intraday_kline_live WHERE symbol = '{code}'", sys_db.conn)

            if df.empty:
                return QMessageBox.warning(self, "資料缺失", f"資料庫中找不到 {code} 的分K數據，請先執行「歷史數據更新」。")

            # 2. 數據清洗：確保日期與時間合併成正確的索引
            df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
            df = df.sort_values(by='datetime').drop_duplicates(subset=['datetime'])
            
            # 強制轉型數值
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # 3. 恢復原本漂亮的畫圖風格 (含高低點標註與紅綠成交量)
            plt.close('all')
            plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'SimHei']
            plt.rcParams['axes.unicode_minus'] = False
            
            fig = plt.figure(figsize=(12, 8))
            gs = gridspec.GridSpec(2, 1, height_ratios=[3, 1]) 

            # 上圖：價格走勢
            ax_price = fig.add_subplot(gs[0])
            ax_price.plot(df['datetime'], df['close'], color='#2980B9', linewidth=2, label='收盤價', alpha=0.8)
            
            # 恢復高低點標註
            max_idx, min_idx = df['close'].idxmax(), df['close'].idxmin()
            ax_price.plot(df.loc[max_idx, 'datetime'], df.loc[max_idx, 'close'], 'r^', markersize=10) 
            ax_price.annotate(f'最高 {df.loc[max_idx, "close"]}', (df.loc[max_idx, "datetime"], df.loc[max_idx, "close"]), xytext=(0,10), textcoords="offset points", ha='center', color='red', fontweight='bold')
            ax_price.plot(df.loc[min_idx, 'datetime'], df.loc[min_idx, 'close'], 'gv', markersize=10) 
            ax_price.annotate(f'最低 {df.loc[min_idx, "close"]}', (df.loc[min_idx, "datetime"], df.loc[min_idx, "close"]), xytext=(0,-15), textcoords="offset points", ha='center', color='green', fontweight='bold')

            ax_price.set_title(f"{code} {name} - 分K走勢圖", fontsize=16, fontweight='bold')
            ax_price.set_ylabel("價格", fontsize=12); ax_price.grid(True, linestyle='--', alpha=0.4); ax_price.legend()

            # 下圖：紅綠成交量
            ax_vol = fig.add_subplot(gs[1], sharex=ax_price)
            # 紅漲綠跌邏輯
            colors = ['#E74C3C' if c >= o else '#2ECC40' for c, o in zip(df['close'], df['open'])]
            ax_vol.bar(df['datetime'], df['volume'], color=colors, width=0.0005) 
            ax_vol.set_ylabel("成交量", fontsize=12); ax_vol.grid(True, linestyle='--', alpha=0.4)

            plt.tight_layout()
            plt.show(block=False)   
        except Exception as e: 
            import traceback
            print(traceback.format_exc())
            QMessageBox.critical(self, "畫圖錯誤", f"無法顯示圖表: {e}")

class DispositionDialog(BaseDialog):
    def __init__(self):
        super().__init__("處置股清單", (300, 400))
        layout = QVBoxLayout(self)
        self.text = QTextEdit(); self.text.setReadOnly(True); self.text.setStyleSheet("font-family: Consolas; font-size: 14px;")
        layout.addWidget(self.text)
        try:
            data = load_disposition_stocks()
            stocks = data if isinstance(data, list) else data.get("stock_codes", [])
            if stocks:
                load_twse_name_map()
                for i, code in enumerate(stocks, 1): self.text.append(f"{i}. {sn(code)}")
            else: self.text.append("目前無處置股。")
        except: self.text.append("無法讀取處置股檔案。")

class TradeLogViewerDialog(BaseDialog):
    def __init__(self):
        super().__init__("📜 歷史交易紀錄與日誌", (900, 600))
        self.layout = QVBoxLayout(self)
        self.load_data()

    def load_data(self):
        # 如果已經有表格存在，先清空它 (確保重新整理時不會疊加)
        for i in reversed(range(self.layout.count())): 
            widget = self.layout.itemAt(i).widget()
            if widget is not None: widget.setParent(None)

        try:
            # 🟢 關鍵：把資料庫的 id 也撈出來，作為刪除依據
            df = pd.read_sql("SELECT id, timestamp, action, symbol, shares, price, profit, note FROM trade_logs ORDER BY id DESC", sys_db.conn)
            
            self.table = QTableWidget(len(df), 7)
            self.table.setHorizontalHeaderLabels(["時間", "動作", "商品", "股數", "價格", "損益", "備註"])
            self.table.setStyleSheet("QTableWidget { background-color: #1E1E1E; color: white; gridline-color: #444; selection-background-color: #E67E22; } QHeaderView::section { background-color: #2C3E50; color: white; font-weight: bold; padding: 5px; }")
            self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            self.table.setEditTriggers(QTableWidget.NoEditTriggers)
            self.table.setSelectionBehavior(QTableWidget.SelectRows) # 點擊時整行反白
            
            for r, row in df.iterrows():
                # 第 0 欄：時間 (偷偷把 id 藏在這個 Item 裡面)
                time_item = QTableWidgetItem(str(row['timestamp']))
                time_item.setData(Qt.UserRole, int(row['id'])) # 🟢 隱藏存儲 ID
                self.table.setItem(r, 0, time_item)
                
                # 動作顏色判斷
                act_item = QTableWidgetItem(str(row['action']))
                if row['action'] == '買進': act_item.setForeground(QColor("#FF4136"))
                elif row['action'] == '平倉': act_item.setForeground(QColor("#2ECC40"))
                self.table.setItem(r, 1, act_item)
                
                self.table.setItem(r, 2, QTableWidgetItem(sn(str(row['symbol']))))
                self.table.setItem(r, 3, QTableWidgetItem(str(row['shares'])))
                self.table.setItem(r, 4, QTableWidgetItem(f"{row['price']:.2f}"))
                
                prof = float(row['profit'])
                p_item = QTableWidgetItem(f"{prof:.0f}" if prof != 0 else "-")
                if prof > 0: p_item.setForeground(QColor("#FF4136"))
                elif prof < 0: p_item.setForeground(QColor("#2ECC40"))
                self.table.setItem(r, 5, p_item)
                
                self.table.setItem(r, 6, QTableWidgetItem(str(row['note'])))
                
            # 🟢 啟用右鍵選單功能
            self.table.setContextMenuPolicy(Qt.CustomContextMenu)
            self.table.customContextMenuRequested.connect(self.show_context_menu)
            
            self.layout.addWidget(self.table)
            
        except Exception as e:
            self.layout.addWidget(QLabel(f"無法讀取資料庫：{e}"))

    # 🟢 處理右鍵選單的邏輯
    def show_context_menu(self, pos):
        item = self.table.itemAt(pos)
        if item is None: return # 如果沒點在格子上就沒事
        
        row = item.row()
        symbol_name = self.table.item(row, 2).text()
        db_id = self.table.item(row, 0).data(Qt.UserRole) # 把剛才藏進去的 ID 拿出來
        
        from PyQt5.QtWidgets import QMenu
        menu = QMenu(self)
        menu.setStyleSheet("""
            QMenu { background-color: #2C3E50; color: white; border: 1px solid #34495E; font-size: 14px; }
            QMenu::item { padding: 6px 20px; }
            QMenu::item:selected { background-color: #E74C3C; } /* 滑過去變紅色警告色 */
        """)
        
        del_action = menu.addAction(f"🗑️ 刪除這筆紀錄 ({symbol_name})")
        action = menu.exec_(self.table.viewport().mapToGlobal(pos))
        
        if action == del_action:
            self.delete_record(row, db_id, symbol_name)

    # 🟢 執行刪除與雙重確認
    def delete_record(self, row, db_id, symbol_name):
        # 設定訊息對話框為白字
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle('⚠️ 刪除確認')
        msg_box.setText(f"確定要永久刪除 【{symbol_name}】 的這筆交易紀錄嗎？")
        msg_box.setStyleSheet("""
            QMessageBox QLabel { color: #FFFFFF; font-size: 14px; font-weight: bold; }
            QMessageBox QPushButton { background-color: #34495E; color: white; padding: 6px 15px; border-radius: 4px; }
            QMessageBox QPushButton:hover { background-color: #E74C3C; }
            QMessageBox { background-color: #121212; }
        """)
        msg_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        msg_box.setDefaultButton(QMessageBox.No)
        
        if msg_box.exec_() == QMessageBox.Yes:
            try:
                # 1. 從資料庫中刪除
                with sys_db.db_lock:
                    with sys_db.conn:
                        sys_db.conn.execute("DELETE FROM trade_logs WHERE id = ?", (db_id,))
                
                # 2. 直接從畫面上移除該列 (不用重新讀取資料庫，畫面更滑順)
                self.table.removeRow(row)
                
            except Exception as e:
                QMessageBox.warning(self, "錯誤", f"刪除失敗: {e}")

def trigger_matplotlib_chart():
    try:
        # 🟢 智慧判斷：如果正在「盤中實戰」，才優先畫 live 實戰表；否則一律畫最新更新的 history 歷史表
        if getattr(sys_state, 'is_monitoring', False):
            data = sys_db.load_kline('intraday_kline_live')
            if not data: data = sys_db.load_kline('intraday_kline_history')
        else:
            data = sys_db.load_kline('intraday_kline_history')
            if not data: data = sys_db.load_kline('intraday_kline_live')

        if not data: return print("⚠️ 沒有可用的 K 線資料可供畫圖，請先執行【更新 K 線數據】！")

        current_groups = load_matrix_dict_analysis()
        valid_symbol_to_group = {}
        for group_name, symbols in current_groups.items():
            for sym in symbols:
                if sym in data:  # 確保這檔股票真的有被抓進資料庫
                    valid_symbol_to_group[sym] = group_name
        
        if not valid_symbol_to_group:
            return print("⚠️ 目前族群內的股票，在資料庫中皆無 K 線數據，請先更新數據！")

        json.dump(data, open('temp_kline.json','w'))
        view_kline_data('temp_kline.json', valid_symbol_to_group)
        os.remove('temp_kline.json')
    except Exception as e: print(f"畫圖發生錯誤: {e}")

class EmergencyDialog(BaseDialog):
    def __init__(self):
        super().__init__("緊急平倉中心", (350, 250)) # 視窗稍微拉高
        layout = QVBoxLayout(self)
        b1 = QPushButton("💥 一鍵全部平倉 (市價)")
        b1.setStyleSheet("QPushButton { background-color: #E74C3C; font-weight: bold; padding: 10px; } QPushButton:hover { background-color: #C0392B; }")
        b1.clicked.connect(lambda: [self.accept(), threading.Thread(target=exit_trade_live, daemon=True).start()])
        
        b2 = QPushButton("🎯 指定單一股票平倉")
        b2.setStyleSheet("QPushButton { background-color: #2980B9; padding: 10px; } QPushButton:hover { background-color: #3498DB; }")
        b2.clicked.connect(self.single_close)
        
        # 🟢 新增：電腦端的終止盤中監控按鈕
        b3 = QPushButton("⏸️ 退出盤中監控模式 (不平倉)")
        b3.setStyleSheet("QPushButton { background-color: #F39C12; padding: 10px; color: black; font-weight: bold;} QPushButton:hover { background-color: #D68910; }")
        b3.clicked.connect(self.stop_live)
        
        b4 = QPushButton("❌ 強制關閉程式 (不平倉)")
        b4.setStyleSheet("QPushButton { background-color: #7F8C8D; padding: 10px; } QPushButton:hover { background-color: #95A5A6; }")
        b4.clicked.connect(lambda: os._exit(0))
        for b in [b1, b2, b3, b4]: layout.addWidget(b)

    def single_close(self):
        code, ok = QInputDialog.getText(self, "單一平倉", "請輸入股票代號:")
        if ok and code: self.accept(); threading.Thread(target=close_one_stock, args=(code,), daemon=True).start()

    def stop_live(self):
        self.accept()
        sys_state.stop_trading_flag = True
        QMessageBox.information(self, "提示", "已發送終止指令！\n系統將在目前的掃描週期結束後自動退出監控模式。")

class PortfolioMonitorDialog(BaseDialog):
    def __init__(self):
        super().__init__("📊 即時持倉監控面板", (650, 300))
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, 5); self.table.setHorizontalHeaderLabels(["代號", "進場價", "現價", "未實現損益", "停損價"])
        self.table.verticalHeader().setVisible(False); self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setStyleSheet("QTableWidget { background-color: #1E1E1E; } QHeaderView::section { background-color: #2C3E50; }")
        layout.addWidget(self.table)
        ui_dispatcher.portfolio_updated.connect(self.update_table)
        if cached_portfolio_data: self.update_table(cached_portfolio_data)

    @pyqtSlot(list)
    def update_table(self, data_list):
        self.table.setRowCount(len(data_list))
        for r, d in enumerate(data_list):
            self.table.setItem(r, 0, QTableWidgetItem(sn(str(d['symbol'])))); self.table.setItem(r, 1, QTableWidgetItem(f"{d['entry_price']:.2f}")); self.table.setItem(r, 2, QTableWidgetItem(f"{d['current_price']:.2f}"))
            pi = QTableWidgetItem(f"{int(d['profit'])} 元"); pi.setForeground(QColor("#FF4136" if d['profit'] > 0 else "#2ECC40"))
            self.table.setItem(r, 3, pi); self.table.setItem(r, 4, QTableWidgetItem(f"{d['stop_loss']:.2f}" if isinstance(d['stop_loss'], float) else str(d['stop_loss'])))

# ==================== 主視窗 (MainWindow) ====================
class QuantMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("交易程式 1.9.3 - 當沖量化終端 (架構大躍進)")
        self.resize(1100, 700)
        self.setStyleSheet("background-color: #121212;")

        central = QWidget(); self.setCentralWidget(central); layout = QHBoxLayout(central)
        sidebar = QFrame(); sidebar.setFixedWidth(230); sidebar.setStyleSheet("background-color: #1E1E1E; border-radius: 10px;")
        vbox = QVBoxLayout(sidebar); vbox.setSpacing(15)
        title = QLabel("量化終端"); title.setStyleSheet("color: #FFFFFF; font-size: 20px; font-weight: bold;"); title.setAlignment(Qt.AlignCenter); vbox.addWidget(title)

        def make_btn(text, callback, color="#2C3E50"):
            btn = QPushButton(text); hover_color = "#4B6584" if color.upper() == "#34495E" else "#34495E"
            btn.setStyleSheet(f"QPushButton {{ background-color: {color}; color: white; padding: 12px; border-radius: 6px; font-weight: bold;}} QPushButton:hover {{ background-color: {hover_color}; }}")
            btn.clicked.connect(callback); return btn

        vbox.addWidget(make_btn("▶ 啟動盤中監控", lambda: self.open_modeless(TradeDialog, 'dlg_trade')))
        vbox.addWidget(make_btn("📊 即時持倉監控", lambda: self.open_modeless(PortfolioMonitorDialog, 'dlg_port'), "#8E44AD"))
        vbox.addWidget(QLabel("── 回測分析 ──", styleSheet="color: #888888; font-size: 12px; margin-top: 10px;"))
        vbox.addWidget(make_btn("📊 盤後數據與分析", self.open_analysis_menu, "#34495E")) 
        vbox.addWidget(make_btn("🎯 自選進場模式", lambda: self.open_modeless(SimulateDialog, 'dlg_sim'), "#34495E"))
        vbox.addWidget(make_btn("💰 極大化利潤", lambda: self.open_modeless(MaximizeDialog, 'dlg_max'), "#34495E"))
        vbox.addWidget(QLabel("── 系統管理 ──", styleSheet="color: #888888; font-size: 12px; margin-top: 10px;"))
        vbox.addWidget(make_btn("📁 管理股票族群", lambda: self.open_modeless(GroupManagerDialog, 'dlg_group')))
        
        # 🟢 終極升級：輕量化「近期 7 交易日」選單 (包含下拉選項反光與加高)
        def open_kline_calendar():
            from PyQt5.QtCore import Qt
            from PyQt5.QtWidgets import QStyledItemDelegate # 🟢 匯入代理器
            from datetime import date, timedelta
            
            # --- 1. 自動計算最近的 7 個「有效交易日」 ---
            valid_days = []
            curr_date = date.today()
            holidays = {"2026-02-28", "2026-04-03", "2026-04-06"} # 可自行擴充假日

            # 🟢 修正：如果現在時間已經超過 14:00 (保險起見設為下午2點，此時玉山API的分K通常已結算完畢)
            # 就可以把「今天」也納入可選擇的歷史回測日期中！
            if datetime.now().time() >= time(14, 0):
                days_to_check = 0  # 從「今天」開始檢查
            else:
                days_to_check = 1  # 盤中還沒結算，只能從「昨天」開始檢查

            while len(valid_days) < 7:
                check_date = curr_date - timedelta(days=days_to_check)
                date_str = check_date.strftime('%Y-%m-%d')
                if check_date.weekday() < 5 and date_str not in holidays:
                    valid_days.append(date_str)
                days_to_check += 1

            # --- 2. 建立極簡小巧的選擇視窗 ---
            dlg = QDialog(self)
            dlg.setWindowTitle("⏳ 選擇近期歷史數據")
            dlg.resize(320, 180)
            
            # 🎨 加入選項的反光效果與無邊框設定
            dlg.setStyleSheet("""
                QDialog { background-color: #121212; }
                QLabel { color: white; font-size: 15px; font-weight: bold; }
                QComboBox {
                    background-color: #2C3E50; color: white;
                    border: 1px solid #34495E; border-radius: 6px;
                    padding: 8px 15px; font-size: 16px; font-weight: bold;
                }
                QComboBox:hover { border: 1px solid #E67E22; }
                QComboBox::drop-down { border: 0px; }
                
                /* 🟢 下拉清單的整體背景與去邊框 */
                QComboBox QAbstractItemView {
                    background-color: #2C3E50; 
                    color: white;
                    outline: none; /* 去除點擊時醜醜的虛線框 */
                    border: 1px solid #34495E;
                }
                
                /* 🟢 下拉選項的各別設定 (高度加高、反光設定) */
                QComboBox QAbstractItemView::item {
                    min-height: 35px; /* 讓格子變大，更好點擊 */
                    padding-left: 10px;
                }
                QComboBox QAbstractItemView::item:selected,
                QComboBox QAbstractItemView::item:hover {
                    background-color: #E67E22; /* 滑鼠指到時變成亮橘色 */
                    color: white;
                }
                
                QPushButton {
                    background-color: #E67E22; color: white; font-weight: bold;
                    font-size: 16px; padding: 12px; border-radius: 8px;
                }
                QPushButton:hover { background-color: #D35400; }
            """)
            
            layout = QVBoxLayout(dlg)
            layout.setContentsMargins(25, 25, 25, 25)
            layout.setSpacing(15)
            
            layout.addWidget(QLabel("📅 請選擇要採集的交易日：\n(受限於券商 API，僅支援近 7 日)"))
            
            combo_days = QComboBox()
            combo_days.addItems(valid_days)
            # 🟢 關鍵：裝上 QStyledItemDelegate，它才會乖乖聽從 QSS 的 item:hover 指令
            combo_days.setItemDelegate(QStyledItemDelegate()) 
            layout.addWidget(combo_days)
            
            btn_ok = QPushButton("🚀 立即採集")
            btn_ok.setCursor(Qt.PointingHandCursor)
            layout.addWidget(btn_ok)
            
            # --- 3. 觸發採集邏輯 ---
            def on_confirm():
                sel_str = combo_days.currentText()
                dlg.accept()
                print(f"✅ 成功鎖定日期：{sel_str}，準備執行極速採集任務...")
                
                global ESUN_LOGIN_PWD, ESUN_CERT_PWD
                if not ESUN_LOGIN_PWD: ESUN_LOGIN_PWD = ""; ESUN_CERT_PWD = ""
                threading.Thread(target=update_kline_data, args=(None, sel_str), daemon=True).start()
                
            btn_ok.clicked.connect(on_confirm)
            dlg.exec_()

        # 重新綁定側邊欄按鈕
        vbox.addWidget(make_btn("🔄 更新 K 線數據", open_kline_calendar))
        vbox.addWidget(make_btn("📜 歷史交易紀錄", lambda: self.open_modeless(TradeLogViewerDialog, 'dlg_log')))
        vbox.addWidget(make_btn("📈 畫圖查看走勢", trigger_matplotlib_chart, "#27AE60"))
        vbox.addWidget(make_btn("⚙️ 參數設定", lambda: self.open_modeless(SettingsDialog, 'dlg_set')))
        vbox.addStretch()
        vbox.addWidget(make_btn("🛑 緊急/手動平倉", lambda: self.open_modeless(EmergencyDialog, 'dlg_emg'), "#C0392B"))

        right_vbox = QVBoxLayout(); right_vbox.setContentsMargins(0, 0, 0, 0)
        
        # 🟢 新增：控制台標題與清除畫面按鈕的橫向佈局
        console_header = QHBoxLayout()
        console_title = QLabel("💻 系統終端機輸出")
        console_title.setStyleSheet("color: #F1C40F; font-weight: bold; font-size: 14px; margin-left: 5px;")
        
        btn_clear = QPushButton("🗑️ 清除畫面")
        btn_clear.setStyleSheet("background-color: #444; color: white; border-radius: 4px; padding: 4px 12px; font-weight: bold;")
        btn_clear.setCursor(Qt.PointingHandCursor)
        # 綁定清除動作
        btn_clear.clicked.connect(lambda: self.console.clear())
        
        console_header.addWidget(console_title)
        console_header.addStretch()
        console_header.addWidget(btn_clear)
        right_vbox.addLayout(console_header)

        self.console = QTextEdit(); self.console.setReadOnly(True); self.console.setStyleSheet("background-color: #000000; color: #FFFFFF; font-family: Consolas; font-size: 14px; border: 1px solid #333333; padding: 10px;")
        self.progress_bar = QProgressBar(); self.progress_bar.setRange(0, 100); self.progress_bar.setValue(0); self.progress_bar.hide(); self.progress_bar.setStyleSheet("QProgressBar { border: 1px solid #555; background-color: #1E1E1E; color: white; text-align: center; height: 22px; font-weight: bold; } QProgressBar::chunk { background-color: #2980B9; }")

        right_vbox.addWidget(self.console, stretch=1); right_vbox.addWidget(self.progress_bar)
        layout.addWidget(sidebar); layout.addLayout(right_vbox, stretch=1)

        self.stream = EmittingStream(); self.stream.textWritten.connect(self.normal_output); sys.stdout = self.stream; sys.stderr = self.stream
        ui_dispatcher.progress_updated.connect(lambda p, msg: (self.progress_bar.setValue(p), self.progress_bar.setFormat(f"{msg}  %p%" if msg else "%p%")))
        ui_dispatcher.progress_visible.connect(self.progress_bar.setVisible)
        ui_dispatcher.plot_equity_curve.connect(self.plot_equity) # 🟢 註冊資金曲線畫圖訊號

    def open_modeless(self, dialog_class, attr_name):
        d = getattr(self, attr_name, None)
        if d is None or not d.isVisible(): d = dialog_class(); setattr(self, attr_name, d); d.show()
        else: d.raise_(); d.activateWindow()
        
    def open_analysis_menu(self):
        # 🟢 徹底改為安全的非阻斷模式 (Modeless) 顯示選單
        if not hasattr(self, 'dlg_analysis_menu') or not self.dlg_analysis_menu.isVisible():
            self.dlg_analysis_menu = AnalysisMenuDialog(self)
            self.dlg_analysis_menu.show()
        else:
            self.dlg_analysis_menu.raise_()
            self.dlg_analysis_menu.activateWindow()

    @pyqtSlot(object)
    def plot_equity(self, data_tuple):
        # 🟢 解包收到的資料 (現在多傳了 intraday_data 進來，保證資料絕對正確)
        if len(data_tuple) == 3:
            all_trades, all_events, intraday_data = data_tuple
        else:
            all_trades, all_events = data_tuple
            intraday_data = sys_db.load_kline('intraday_kline_history')
            
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        
        # --- 畫總資金曲線 (只有在有交易時才畫) ---
        if all_trades:
            df_trades = pd.DataFrame(all_trades).sort_values('exit_time')
            start_row = pd.DataFrame([{'exit_time': '盤前起始', 'profit': 0.0, 'symbol': ''}])
            df_trades = pd.concat([start_row, df_trades], ignore_index=True)
            df_trades['cumulative_profit'] = df_trades['profit'].cumsum()
            
            plt.figure(figsize=(10, 5))
            plt.plot(df_trades['exit_time'], df_trades['cumulative_profit'], marker='o', linestyle='-', color='#E74C3C', linewidth=2)
            plt.fill_between(df_trades['exit_time'], df_trades['cumulative_profit'], 0, alpha=0.1, color='#E74C3C')
            
            for i, row in df_trades.iterrows():
                if i > 0:
                    sym_name = sn(str(row['symbol']))
                    profit_val = int(row['profit'])
                    profit_str = f"+{profit_val}" if profit_val > 0 else f"{profit_val}"
                    label_text = f"{sym_name}\n({profit_str})"
                    plt.annotate(label_text, (row['exit_time'], row['cumulative_profit']),
                                textcoords="offset points", xytext=(0,10), ha='center', fontsize=9, color='#1A5276', fontweight='bold')
            
            plt.axhline(0, color='gray', linestyle='--', linewidth=1)
            plt.title("回測資金成長曲線 (Equity Curve)", fontsize=16, fontweight='bold')
            plt.xlabel("平倉時間", fontsize=12)
            plt.ylabel("累積淨利潤 (元)", fontsize=12)
            plt.xticks(rotation=45)
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.tight_layout()
            plt.show(block=False)

        # --- 🟢 畫每檔股票的專屬覆盤圖 (只要有事件或交易就畫) ---
        # 找出所有「有交易」或「有觸發事件」的股票代號
        triggered_symbols = list(set([t['symbol'] for t in all_trades] + [e['symbol'] for e in all_events]))
        
        # 直接使用傳進來的 intraday_data，不需要再 load_kline
        for sym in triggered_symbols:
            if sym in intraday_data:
                df = pd.DataFrame(intraday_data[sym])
                if not df.empty:
                    plot_tradingview_chart(sym, all_trades, all_events, df)
            
    @pyqtSlot(str)
    def normal_output(self, text):
        h = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace(' ', '&nbsp;').replace('\n', '<br>')
        for p, c in [(r'\x1b\[(?:31|91)m|\033\[(?:31|91)m', '#FF4136'), (r'\x1b\[(?:32|92)m|\033\[(?:32|92)m', '#2ECC40'), (r'\x1b\[(?:33|93)m|\033\[(?:33|93)m', '#FFDC00'), (r'\x1b\[(?:34|94)m|\033\[(?:34|94)m', '#0074D9')]: h = re.sub(p, f'<span style="color: {c}; font-weight: bold;">', h)
        h = re.sub(r'\x1b\[0m|\x1b\[39m|\033\[0m', '</span>', h)
        self.console.moveCursor(QTextCursor.End); self.console.insertHtml(h); self.console.moveCursor(QTextCursor.End)

    def start_correlation_thread(self, mode, w_mins):
        # 🟢 接收來自設定視窗的參數，並安全地在背景啟動連動分析
        print(f"\x1b[35m🧬 啟動族群連動分析 ({'微觀實戰模擬' if mode == 'micro' else '全天宏觀連動'}, 等待: {w_mins}分)...\x1b[0m")
        self.corr_thread = CorrelationAnalysisThread(mode, w_mins)
        self.corr_thread.finished_signal.connect(lambda r: (print(f"\x1b[32m✅ 分析完成，共產出 {len(r)} 筆。\x1b[0m"), setattr(self, 'corr_dialog', CorrelationResultDialog(r, self)), self.corr_dialog.show()))
        self.corr_thread.start()

# ==================== 程式進入點 ====================
def main():
    try:
        load_settings()
        
        # ⚠️ 注意：原本在這邊的 tg_bot.start() 已經被移除了！
        
        app = QApplication(sys.argv)
        app.setAttribute(Qt.AA_DisableWindowContextHelpButton)
        app.setStyle("Fusion")
        
        # 🟢 關鍵 1：先建立主視窗，讓黑色終端機準備好接收文字
        window = QuantMainWindow()
        
        print("=" * 60)
        print("✅ 系統核心模組載入完成 (1.9.3 SQLite 效能極速版)")
        print("✅ 安全鎖、SQLite 資料庫、非同步I/O 已全面啟動")
        print("👉 請點擊介面左側按鈕開始操作。")
        print("=" * 60)

        # 🟢 關鍵 2：等黑色終端機都準備好了，才啟動 Telegram 引擎
        tg_bot.start()

        window.show()
        sys.exit(app.exec_())
        
    except Exception as e:
        # 這裡把紅色字體拿掉，避免終端機還沒準備好時印出亂碼
        print(f"\n❌ 程式發生未預期的致命錯誤：{e}")
        print("💡 提示：請檢查 config.ini 設定、網路連線，或是否有檔案被其他程式佔用。\n")

if __name__ == "__main__":
    main()