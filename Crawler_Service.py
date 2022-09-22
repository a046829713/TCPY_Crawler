import SQL_operate
import time
import Debug_tool
import Datatransformer
import logging
import threading
from tcoreapi_mq import QuoteAPI
import zmq
import DataProvider
from datetime import datetime
from datetime import timedelta

class Crawler_Service():
    def __init__(self) -> None:
        self.transformer = Datatransformer.Datatransformer()
        self.provider = DataProvider.DataProvider()
        self.db = SQL_operate.DB_operate()
        self.g_QuoteZMQ, self.g_QuoteSession = self.provider.login()
        self.sessions = {self.g_QuoteSession: "onuse"}
        self.lock = threading.Lock()
        # 總線程數
        self.sem_thredinglock = threading.Semaphore(1)

    def get_data(self):
        """ 
        用來取得歷史分鐘資料
        """
        self.dbtablename = 'taiwanstockfutures'
        Symbolsinfo = self.db.get_db_data(f"select * from {self.dbtablename}")

        for symbolname, symbolcode, statsucode in Symbolsinfo:
            # 查詢指定合约訊息 TC 官方的代碼都會這樣顯示
            # for symbolname, symbolcode in [('台積電', 'TC.F.TWF.CDF.HOT')]:
            # 先將總線程鎖住
            self.sem_thredinglock.acquire()

            if statsucode != 0:
                self.sem_thredinglock.release()
                continue

            Debug_tool.debug.record_msg(
                f"主要商品:{symbolname},線程名稱:{threading.current_thread().name}")
            symbolcode = symbolcode.upper()
            threading.Thread(target=self.get_historydata, args=(
                self.g_QuoteSession, symbolname, symbolcode, '1K'), name=f"threading-{symbolname}").start()

    def changestatuscode(self, Symbolsinfo: list):
        """用來覆寫DB內的狀態碼

        Args:
            Symbolsinfo (list): [()]

        """
        # 每次更新DB內的代碼
        for symbolname, symbolcode, statsucode in Symbolsinfo:
            self.db.change_db_data(
                f"update {self.dbtablename} set statuscode = 0 where futuresname = '{symbolname}';")

    def get_daydata(self):
        """ 
        用來取得歷史日線資料
        """
        print("重新登錄")
        self.dbtablename = 'taiwanstockfuturesdaily'
        self.g_QuoteZMQ, self.g_QuoteSession = self.provider.login()
        self.sessions.update({self.g_QuoteSession: "onuse"})

        Symbolsinfo = self.db.get_db_data(f"select * from {self.dbtablename}")

        # 更新狀態碼
        self.changestatuscode(Symbolsinfo)

        threads = []
        for symbolname, symbolcode, statsucode in Symbolsinfo:
            if statsucode != 0:
                continue

            Debug_tool.debug.record_msg(
                f"主要商品:{symbolname},線程名稱:{threading.current_thread().name}")
            symbolcode = symbolcode.upper()

            threads.append(threading.Thread(target=self.get_historydata, args=(
                self.g_QuoteSession, symbolname, symbolcode, 'DK'), name=f"threading-{symbolname}-{symbolcode}.DK"))

        for i in range(len(threads)):
            self.sem_thredinglock.acquire()
            threads[i].start()

        for i in range(len(threads)):
            threads[i].join()

    def reconnect(self, g_QuoteSession: str, symbolname, symbolcode):
        """斷線重新連接機制

        Args:
            g_QuoteSession (_type_): _description_
            symbolname (_type_): _description_
            symbolcode (_type_): _description_
        """
        try:
            with self.lock:
                Debug_tool.debug.record_msg(
                    f"重新連線,商品名稱:{symbolname},商品代碼:{symbolcode}")
                self.db.change_db_data(
                    f"update {self.dbtablename} set statuscode = 3 where futuresname = '{symbolname}';")
                # 增加一個list 去判斷與最後的session是否相通

                # 登出後重新進入
                if self.sessions[g_QuoteSession] == 'onuse':
                    self.sessions[g_QuoteSession] = "nouse"
                    Debug_tool.debug.record_msg(
                        f"字典{self.sessions},線程名稱:{threading.current_thread().name}")
                    self.g_QuoteZMQ, self.g_QuoteSession = self.provider.login()
                    self.sessions.update({self.g_QuoteSession: "onuse"})
                    Debug_tool.debug.record_msg(
                        f"字典{self.sessions},線程名稱:{threading.current_thread().name}")

                Debug_tool.debug.record_msg(
                    f"錯誤資料結束,線程名稱:{threading.current_thread().name}")
        except:
            Debug_tool.debug.print_info()

        self.sem_thredinglock.release()

    def get_historydata(self, g_QuoteSession, symbolname: str, symbolcode: str, datatype: str):
        """_summary_

        Args:
            g_QuoteSession (_type_): _description_
            symbolname (str): FH滬深
            symbolcode (str): TC.F.TWF.OCF.HOT
            datatype (str): 資料週期 1.歷史ticks (TICKS) 2.歷史1K (1K) 3.歷史日K (DK)

        """
        try:
            if datatype == 'DK':
                StrTim = str(self.db.get_db_data(f"select * from `{symbolcode + '.DK'}`;")[-1][0] + timedelta(days= 1))
                StrTim = StrTim.replace("-","") + "00"
                EndTim = str((datetime.today() + timedelta(days= -1)).date())
                EndTim = EndTim.replace("-","") + "00"
            else:   
                # 起始時間
                StrTim = '2010070100'
                # 結束時間
                EndTim = '2022082000'


            # 資料頁數
            QryInd = '0'

            SubHis = self.g_QuoteZMQ.SubHistory(g_QuoteSession, symbolcode, datatype, StrTim, EndTim)

            print("訂閱成功:",SubHis)
            if isinstance(SubHis, zmq.error.Again):
                self.reconnect(g_QuoteSession, symbolname, symbolcode)
                return

            Debug_tool.debug.record_msg(
                f"取得資料完成,線程名稱:{threading.current_thread().name}")

            i = 0
            while True:  # 等待訂閱回補
                i = i+1
                time.sleep(5)
                HisData = self.g_QuoteZMQ.GetHistory(
                    g_QuoteSession, symbolcode, datatype, StrTim, EndTim, QryInd, "1")

                print("取得資料",HisData)
                if isinstance(HisData, zmq.error.Again):
                    self.reconnect(g_QuoteSession, symbolname, symbolcode)
                    return

                if (len(HisData['HisData']) != 0):
                    Debug_tool.debug.record_msg(
                        f"回補成功,線程名稱:{threading.current_thread().name}")
                    break

                if i > 20:
                    Debug_tool.debug.record_msg(
                        f"檔案儲存失敗,商品名稱:{symbolname},商品代碼:{symbolcode}")
                    self.db.change_db_data(
                        f"update {self.dbtablename} set statuscode = 2 where futuresname = '{symbolname}';")
                    self.sem_thredinglock.release()
                    return

            while True:  # 獲取訂閱成功的全部歷史資料並另存
                # 近來前要先檢查
                if self.sessions[g_QuoteSession] == "nouse":
                    Debug_tool.debug.record_msg(
                        f"線程已經取代，線程名稱:{threading.current_thread().name}")
                    self.db.change_db_data(
                        f"update {self.dbtablename} set statuscode = 3 where futuresname = '{symbolname}';")
                    Debug_tool.debug.record_msg(
                        f"線程釋放，線程名稱:{threading.current_thread().name}")
                    self.sem_thredinglock.release()
                    return

                HisData = self.g_QuoteZMQ.GetHistory(
                    g_QuoteSession, symbolcode, datatype, StrTim, EndTim, QryInd, '2')

                if isinstance(HisData, zmq.error.Again):
                    self.reconnect(g_QuoteSession, symbolname, symbolcode)
                    return

                # 改成所需要的資料庫時間
                if self.transformer.change_time(HisData['HisData'], datatype) is not None:
                    chagnedata, QryInd = self.transformer.change_time(HisData['HisData'], datatype)
                else:
                    Debug_tool.debug.record_msg(
                        f"檔案儲存成功,商品名稱:{symbolname},商品代碼:{symbolcode}", logging.info)
                    self.db.change_db_data(
                        f"update {self.dbtablename} set statuscode = 1 where futuresname = '{symbolname}';")
                    self.sem_thredinglock.release()
                    return

                if datatype == 'DK':
                    writecode = symbolcode + ".DK"
                elif datatype == '1K':
                    writecode = symbolcode

                if self.db.wirte_data(writecode, chagnedata) is None:
                    Debug_tool.debug.record_msg(f"資料寫入成功,線程名稱:{threading.current_thread().name}")
                else:
                    self.sem_thredinglock.release()
                    return

                time.sleep(1)
        except Exception as e:
            Debug_tool.debug.print_info()
            self.sem_thredinglock.release()
