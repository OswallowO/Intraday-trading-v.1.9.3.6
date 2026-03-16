# Release Notes: v1.9.3.6 (Major Update)

本次更新 (自 v1.9.1 升級) 涵蓋了底層資料庫架構的全面重構，以及核心交易演算法的深度校準。本次升級的最高指導原則為「回測與實戰邏輯的 100% 絕對鏡像」，消除了過去版本中因非同步延遲、資料型態異常與防護機制分歧所導致的盤中實戰落差。

## 核心架構升級 (Core Architecture Upgrades)

* **資料庫引擎重構 (SQLite Migration)**
    淘汰了早期的 JSON/CSV 檔案存取依賴，全面導入 SQLite 時序資料庫 (`quant_data.db`)。區分歷史回測 (`intraday_kline_history`) 與盤中實戰 (`intraday_kline_live`) 雙表架構，大幅降低 I/O 延遲，提升高頻讀寫時的穩定性與記憶體管理效率。
* **非同步併發數據採集 (Asynchronous Fetching)**
    盤中 K 線採集模組全面改寫，導入 `ThreadPoolExecutor(max_workers=5)` 進行多執行緒併發處理。結合自定義的 `APIRateLimiter` (限制 550 calls/min)，在不觸發 API 節流限制的前提下，將全市場數據更新延遲壓縮至極致。
* **記憶體快照攔截機制 (In-Memory Snapshot Interception)**
    針對實戰監控迴圈，新增 `sys_state.in_memory_intraday` 記憶體鎖定讀取機制。系統在邏輯判斷前會優先讀取記憶體快照，徹底解決過去因等待資料庫寫入而導致的「數據落後一分鐘」盲區。

## 交易邏輯與演算法對齊 (Algorithmic Synchronization)

* **回測與實戰引擎 100% 鏡像對齊**
    針對 `process_group_data` (回測引擎) 與 `process_live_trading_logic` (實戰引擎) 進行位元組級別的邏輯同步，確保盤中所見即回測所得，消除任何偷跑或漏單現象。
* **嚴格洗盤計時重構 (Strict Wait-Time Enforcement)**
    修正了盤中實戰引擎提早觸發的瑕疵。將條件驗證嚴格對齊至 `wait_minutes` 閾值，並修正了「突破前高」與「倒數計時」的先後驗證順序，確保領漲股在創新高時能精準中斷計時，防止過早做空強勢族群。
* **防誘空容錯演算法修復 (Pullback Tolerance Fix)**
    修復了盤中實戰引擎裡 DataFrame `.all()` 評估失效的問題。全面實裝絕對高點容錯檢測 (`sub.iloc[-1]['rise'] > sub.iloc[:-1]['rise'].max() + pullback_tolerance`)，大幅增強系統在大震盪盤整期間的抗雜訊能力。
* **漲停板狀態機解鎖 (Limit-Up State Machine Optimization)**
    移除了盤中實戰專用的 `triggered_limit_up` 靜態黑名單。改為採用與回測一致的相對狀態檢測（對比上一分鐘高點與漲停價），使系統具備捕捉「打開後二次漲停鎖死」的進場能力，同時完美兼容強制升級機制。

## 數據完整性與防禦性機制 (Data Integrity & Defensive Mechanisms)

* **強制型態安全防禦 (Strict Type Casting)**
    解決了 SQLite 偶發將數值欄位儲存為 `TEXT` 型態，導致數值比較 (如 `rise >= 1.4`) 靜默失效的致命錯誤。在進入核心邏輯矩陣前，強制對所有指標欄位執行 `pd.to_numeric` 轉型，確保閾值觸發百分之百精確。
* **早盤動能基準修正 (Opening Volume Baseline)**
    優化了 09:00 至 09:02 期間 `FIRST3_AVG_VOL` (開盤均量) 尚未建立時的量能檢測邏輯。引入絕對成交量門檻 (`min_volume_threshold`) 作為備用檢測標準，確保第一分鐘的主力爆發訊號不會因除以零或缺失值而被略過。
* **動態漲停停損保護 (Dynamic Limit-Up Stop-Loss Protection)**
    在回測引擎中補齊了與實戰相同的「漲停保護」邏輯。系統在計算停損價差時，會自動計算 Tick 階距，強制確保空單停損點不會設定在超過漲停價的位置，避免死鎖。
