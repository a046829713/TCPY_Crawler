import json
import router
import sqlalchemy
from sqlalchemy import text
import Debug_tool


class DB_operate():

    def wirte_data(self, quoteSymbol: str, out_list: list):
        """
        將商品資料寫入資料庫
        [{'Date': '2022/07/01', 'Time': '09:25:00', 'Open': '470', 'High': '470', 'Low': '470', 'Close': '470', 'Volume': '10'}]

        """
        try:
            self.userconn = router.Router().mysql_financialdata_conn
            with self.userconn as conn:
                sql_sentence = f'INSERT INTO `{quoteSymbol}` (Date, Time, Open, High, Low, Close, Volume) VALUES (:Date, :Time, :Open, :High, :Low, :Close, :Volume)'
                conn.execute(
                    text(sql_sentence),
                    out_list
                )
        except Exception as e:
            Debug_tool.debug.print_info()
            return e

    def get_db_data(self, text_msg: str) -> list:
        """
            專門用於select from
        """
        try:
            self.userconn = router.Router().mysql_financialdata_conn
            with self.userconn as conn:

                result = conn.execute(
                    text(text_msg)
                )
                # 資料範例{'Date': '2022/07/01', 'Time': '09:25:00', 'Open': '470', 'High': '470', 'Low': '470', 'Close': '470', 'Volume': '10'}

                return list(result)
        except:
            Debug_tool.debug.print_info()

    def change_db_data(self, text_msg: str) -> None:
        """ 用於下其他指令

        Args:
            text_msg (str): SQL_Query

        Returns:
            None
        """
        try:
            self.userconn = router.Router().mysql_financialdata_conn
            with self.userconn as conn:
                conn.execute(text(text_msg))
        except:
            Debug_tool.debug.print_info()

    def create_table(self, text_msg: str) -> None:
        """ 創建日線資料表

        Args:
            text_msg (str): symbolcode TC.F.TWF.IAF.HOT
        """

        try:
            self.userconn = router.Router().mysql_financialdata_conn
            with self.userconn as conn:
                conn.execute(text(f"CREATE TABLE `financialdata`.`{text_msg}`(`Date` date NOT NULL,`Time` time NOT NULL,`Open` FLOAT NOT NULL,`High` FLOAT NOT NULL,`Low` FLOAT NOT NULL,`Close` FLOAT NOT NULL,`Volume` FLOAT  NOT NULL,PRIMARY KEY(`Date`, `Time`));"))
        except:
            Debug_tool.debug.print_info()


if __name__ == '__main__':
    out_list = []

    with open('test.txt', 'r', encoding='UTF-8-sig') as file:
        data = json.loads(file.read().replace(u'\\u3000', u''))
        infodatas = data['Instruments']['Node'][1]['Node']
        for industrydata in infodatas:
            for each_data in industrydata['Node']:
                out_dict = {}
                out_dict['futuresname'] = each_data['CHT']
                out_dict['futurescode'] = each_data['Contracts'][0] +".HOT"
                out_dict['statuscode'] = 0
                out_list.append(out_dict)

    userconn = router.Router().mysql_financialdata_conn
    with userconn as conn:
        conn.execute(
            text("INSERT INTO taiwanstockfuturesdaily (futuresname, futurescode, statuscode) VALUES (:futuresname, :futurescode,:statuscode)"),
            out_list
        )




    # 創建日線資料
    # app = DB_operate()
    # with open('test.txt', 'r', encoding='UTF-8-sig') as file:
    #     data = json.loads(file.read().replace(u'\\u3000', u''))
    #     infodatas = data['Instruments']['Node'][1]['Node']
    #     for industrydata in infodatas:
    #         for each_data in industrydata['Node']:
    #             app.create_table(each_data['Contracts'][0] + ".HOT.DK")
