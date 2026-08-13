"""資料庫基礎設施層：只負責連線管理，不包含任何 SQL。

分層設計（對應 Laravel）：
- 本層（基礎設施 infrastructure）：MySQL 連線建立/復用/斷線偵測
- Repository 層（src/repositories.py）：所有 SQL 語句
- 服務層（src/crawler.py）：業務編排，依賴 Repository 層

執行緒安全：每個執行緒各自持有自己的連線（thread-local）。共用的
`connect()` 連線僅供單執行緒的初始化/收尾操作使用。

交易的開始與結束（commit）交由服務層決定；本類別只提供機制。
斷線/死結等使交易失效的錯誤一律向上拋出（ConnectionLost / 原例外），
由服務層重跑完整冪等區塊 —— 本層絕不重跑單一 SQL（避免交易殘缺）。
"""

import logging
import threading

import pymysql
from pymysql.cursors import DictCursor

from .config import DB_CONFIG

log = logging.getLogger("db")


class ConnectionLost(Exception):
    """連線被伺服器斷開（2006/2013/InterfaceError）。

    與死結（1213）/鎖等待逾時（1205）同級的**服務層重跑訊號**：斷線時
    舊連線上未提交的交易已隨連線一起回滾，本層只負責捨棄舊連線，
    **不在此重跑單一 SQL**（SOL review P1：重跑會讓交易殘缺 —— 例如
    parts 寫在舊連線 A、receipt 遇斷線後建立連線 B 只重跑 receipt，
    A 的未提交 parts 回滾、B 卻能提交 terminal receipt）。由服務層
    （crawl_group）重跑完整、冪等的 parts+receipt 區塊。
    """


class Database:
    """MySQL 連線管理員。

    - 每執行緒一條連線（惰性建立），避免執行緒間共用連線的競態
    - 連線被伺服器斷開（2006/2013/InterfaceError）時丟棄壞連線並拋
      ConnectionLost，由服務層重跑完整交易（絕不在本層重跑單一 SQL）
    """

    def __init__(self):
        self._local = threading.local()
        self.conn = None

    def connect(self):
        """初始化（回傳自身以便鏈式呼叫）。

        SOL review P3：**不建立** self.conn 主連線 —— 所有實際 SQL
        都經由 _thread_conn() 惰性建立（含主執行緒的第一個查詢），
        舊碼這裡建立的一條 self.conn 從未被任何查詢使用，只是每個
        程序多掛一條閒置 DB 連線。資料庫可用性由第一次查詢驗證
        （supervisor 另有 SELECT 1 健康檢查）。
        """
        return self

    def _new_conn(self):
        """建立一條新的 MySQL 連線，並設定合理的交易隔離層級。"""
        conn = pymysql.connect(
            **DB_CONFIG,
            cursorclass=DictCursor,
            autocommit=False,
            charset="utf8mb4",
        )
        # READ COMMITTED：避免 InnoDB 在共用的唯一索引上產生 gap lock
        # （多個 worker 會同時對同一個 model_id 底下插入車型/零件組）
        with conn.cursor() as cur:
            cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        return conn

    def close(self):
        """關閉主連線與所有執行緒連線，並重置 thread-local 狀態。"""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
        try:
            for c in list(self._local.__dict__.values()):
                if hasattr(c, "close"):
                    try:
                        c.close()
                    except Exception:
                        pass
        except Exception:
            pass
        self._local = threading.local()

    def _thread_conn(self):
        """取得目前執行緒的連線（若尚未建立則惰性建立）。"""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new_conn()
            self._local.conn = conn
        return conn

    def _execute(self, sql, params=None):
        """執行單一 SQL（可帶參數）。回傳 cursor。

        Repository 層透過此方法執行所有語句；本方法不 commit，
        由服務層決定交易邊界。

        死結 1213 / 鎖等待逾時 1205 → **rollback 後直接重拋**；
        斷線（2006/2013/InterfaceError）→ 捨棄舊連線後拋
        ConnectionLost —— 兩者都**不在此重跑**（SOL review P1）：
        deadlock 可能已回滾整個 transaction、斷線則帶走整個未提交
        交易，只重跑最後一條 SQL 會讓交易殘缺；由服務層重跑完整、
        冪等的 group transaction（見 crawl_group 的重試）。
        """
        try:
            conn = self._thread_conn()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur
        except pymysql.err.OperationalError as e:
            code = e.args[0] if e.args else None
            if code in (1213, 1205):
                log.warning("db contention (%s); rolling back, service layer retries", code)
                self.rollback()
                raise
            if code in (2006, 2013):
                self._raise_connection_lost(code)
            self.rollback()
            raise
        except pymysql.err.InterfaceError:
            self._raise_connection_lost("InterfaceError")
        except pymysql.MySQLError:
            # DataError / IntegrityError 等一般 SQL 錯誤也可能發生在
            # 多語句 transaction 中。若只把例外往上拋，thread-local
            # connection 會保留前面尚未提交的寫入，下一個工作可能誤提交。
            self.rollback()
            raise

    def _raise_connection_lost(self, code):
        """斷線：捨棄目前執行緒的連線，並向服務層拋出 ConnectionLost。

        絕不「重連後重跑單一 SQL」：舊連線上未提交的交易已隨斷線
        回滾，新連線重跑單一語句會讓交易殘缺（見 ConnectionLost
        的說明）。重連只是清掉壞連線，讓服務層重跑完整區塊時
        從乾淨的交易開始。
        """
        log.warning("db connection lost (%s); discarding connection", code)
        self._discard_thread_conn()
        raise ConnectionLost(code)

    def _discard_thread_conn(self):
        """丟棄壞連線；下一次 SQL 由 _thread_conn 惰性重連。

        不在錯誤處理中同步建立連線：若 MySQL 暫時仍不可達，重連的
        OperationalError 會遮蔽原本的 ConnectionLost，服務層便無法
        辨識「整個 transaction 必須重跑」。
        """
        try:
            self._local.conn.close()
        except Exception:
            pass
        try:
            del self._local.conn
        except AttributeError:
            pass

    def rollback(self):
        """回滾目前執行緒的交易（deadlock 後清除殘留狀態，服務層重跑）。"""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self.conn
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

    def commit(self):
        """提交目前執行緒的交易。

        commit 失敗時記錄錯誤、重置該執行緒連線，並**重新丟出例外**。
        呼叫端必須把這個失敗傳上去（例如不要標記 done），否則會形成
        「資料 rollback 但狀態卻標記完成」的靜默缺漏。

        斷線（2006/2013/InterfaceError）在 commit 階段也拋
        ConnectionLost（SOL review P2）：舊碼只重建連線後拋原
        OperationalError，而 crawl_group 只對 ConnectionLost /
        1205 / 1213 重跑完整 parts+receipt 區塊 —— commit 階段的
        斷線因此沒有被完整交易重試涵蓋。斷線後無法確認交易是否
        落庫（伺服器可能已提交才斷線），重跑完整冪等區塊是最安全的
        恢復方式。
        """
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self.conn
        if not conn:
            raise RuntimeError("no connection to commit")
        try:
            conn.commit()
        except pymysql.err.OperationalError as e:
            code = e.args[0] if e.args else None
            self._reset_conn_after_failure(conn, e)
            if code in (2006, 2013):
                raise ConnectionLost(code) from None
            raise
        except pymysql.err.InterfaceError as e:
            self._reset_conn_after_failure(conn, e)
            raise ConnectionLost("InterfaceError") from None
        except Exception as e:
            self._reset_conn_after_failure(conn, e)
            raise

    def _reset_conn_after_failure(self, conn, e):
        """commit 失敗後丟棄壞連線；下次 SQL 才惰性重連。"""
        log.error("commit failed: %s; discarding thread connection", e)
        try:
            conn.close()
        except Exception:
            pass
        if getattr(self._local, "conn", None) is conn:
            try:
                del self._local.conn
            except AttributeError:
                pass
        elif self.conn is conn:
            self.conn = None

    def query_one(self, sql, params=None):
        """執行 SELECT 並回傳第一列（無結果則回傳 None）。

        主要供監督迴圈等需要直接查詢的場合使用。
        """
        cur = self._execute(sql, params)
        row = cur.fetchone()
        return row

    def _executemany(self, sql, rows):
        """批次執行同一語句的多組參數（一次往返，效能最佳）。

        死結 1213 / 鎖等待逾時 1205 → rollback 後直接重拋；斷線
        （2006/2013/InterfaceError）→ 捨棄舊連線後拋 ConnectionLost。
        與 _execute 一致：**都不在此重跑單一 SQL**（SOL review P1，
        由服務層重跑完整冪等交易）。
        """
        try:
            conn = self._thread_conn()
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
                return
        except pymysql.err.OperationalError as e:
            code = e.args[0] if e.args else None
            if code in (1213, 1205):
                log.warning("db contention (%s); rolling back, service layer retries", code)
                self.rollback()
                raise
            if code in (2006, 2013):
                self._raise_connection_lost(code)
            self.rollback()
            raise
        except pymysql.err.InterfaceError:
            self._raise_connection_lost("InterfaceError")
        except pymysql.MySQLError:
            self.rollback()
            raise
