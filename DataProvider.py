from tcoreapi_mq import QuoteAPI
import zmq
import json
import re


class DataProvider():
    def __init__(self) -> None:
        self.g_QuoteZMQ = None
        self.g_QuoteSession = ""

    def login(self):
        """
        登入(與 TOUCHANCE zmq 連線用，不可改)
        q_data= {'Reply': 'LOGIN', 'Success': 'OK', 'SessionKey': '663d10659b9eb08626a3e7837b6d5481', 'SubPort': '58458'}
        """
        self.g_QuoteZMQ = QuoteAPI("ZMQ", "8076c9867a372d2a9a814ae710c256e2")
        q_data = self.g_QuoteZMQ.Connect("51237")

        if q_data["Success"] != "OK":
            print("[quote]connection failed")
            return

        self.g_QuoteSession = q_data["SessionKey"]
        return self.g_QuoteZMQ, self.g_QuoteSession

    def getallsymbol(self):
        """ 取得所有合約資料 """
        # 查詢指定合约訊息
        # quoteSymbol = "TC.F.TWF.CDF.HOT"
        #print("查詢指定合約：",g_QuoteZMQ.QueryInstrumentInfo(g_QuoteSession, quoteSymbol))
        # 查詢指定類型合約列表
        # 期貨：Fut
        # 期權：Opt
        # 證券：Sto # Fut2(這邊代碼不確定為何有兩個)
        return self.g_QuoteZMQ.QueryAllInstrumentInfo(self.g_QuoteSession, "Fut")

    # 實時行情回補

    def OnRealTimeQuote(self, symbol):
        print("商品：", symbol["Symbol"], "成交價:", symbol["TradingPrice"], "開:",
              symbol["OpeningPrice"], "高:", symbol["HighPrice"], "低:", symbol["LowPrice"])

    # 行情消息接收
    def quote_sub_th(self, obj: QuoteAPI, sub_port, g_QuoteSession, filter=""):
        socket_sub = obj.context.socket(zmq.SUB)
        # socket_sub.RCVTIMEO=7000   #ZMQ超時設定
        socket_sub.connect("tcp://127.0.0.1:%s" % sub_port)
        socket_sub.setsockopt_string(zmq.SUBSCRIBE, filter)
        while(True):
            message = (socket_sub.recv()[:-1]).decode("utf-8")
            index = re.search(":", message).span()[1]  # filter
            message = message[index:]
            message = json.loads(message)
            # for message in messages:
            if(message["DataType"] == "REALTIME"):
                self.OnRealTimeQuote(message["Quote"])
            elif(message["DataType"] == "GREEKS"):
                OnGreeks(message["Quote"])
            elif(message["DataType"] == "TICKS" or message["DataType"] == "1K" or message["DataType"] == "DK"):
                # print("@@@@@@@@@@@@@@@@@@@@@@@",message)
                strQryIndex = ""
                while(True):
                    s_history = obj.GetHistory(
                        g_QuoteSession, message["Symbol"], message["DataType"], message["StartTime"], message["EndTime"], strQryIndex)
                    historyData = s_history["HisData"]
                    if len(historyData) == 0:
                        break

                    last = ""
                    for data in historyData:
                        last = data
                        #print("歷史行情：Time:%s, Volume:%s, QryIndex:%s" % (data["Time"], data["Volume"], data["QryIndex"]))

                    strQryIndex = last["QryIndex"]

        return
