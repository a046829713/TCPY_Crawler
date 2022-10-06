import time
from typing import List
import pytz
from pytz import timezone
from datetime import datetime
import Debug_tool
import copy


class Datatransformer():
    def __init__(self) -> None:
        pass

    def change_time(self, datas: List, datatype: str):
        """改變時間格式

        Args:
            datas (List): [
            {'Date': '20220701', 'Time': '24200', 'UpTick': '0', 'UpVolume': '0', 'DownTick': '24', 'DownVolume': '66', 'UnchVolume': '0', 'Open': '462', 'High': '462', 'Low': '462', 'Close': '462', 'Volume': '66', 'OI': '0', 'QryIndex': '117'}
            {'Date': '20220701', 'Time': '24200', 'UpTick': '0', 'UpVolume': '0', 'DownTick': '24', 'DownVolume': '66', 'UnchVolume': '0', 'Open': '462', 'High': '462', 'Low': '462', 'Close': '462', 'Volume': '66', 'OI': '0', 'QryIndex': '117'}
            ]
            datatype (str): "DK" or "1K" or "TICKS"

        Returns:
            _type_: _description_
        """

        try:
            out_list = []
            utc = pytz.utc
            tw = timezone('Asia/Taipei')
            QryInd = datas[-1]['QryIndex']
            new_datas = copy.deepcopy(datas)

            
            for data in new_datas:
                if datatype=="1K":
                    if len(data['Time']) != 6:
                        data['Time'] = (6 - len(data['Time'])) * "0" + data['Time']
                    utctime = utc.localize(datetime.strptime(
                        data['Date'] + ' ' + data['Time'], "%Y%m%d %H%M%S"))
                    newtime = utctime.astimezone(tw).replace(tzinfo=None)
                elif datatype =="DK":
                    # 分鐘與日線不同 雖然還是UTC時間 但不經過轉換結果相同 (沒有時間)
                    newtime = datetime.strptime(data['Date'] + ' ' + "00:00:00", "%Y%m%d %H:%M:%S")

                newtime = datetime.strftime(newtime, "%Y-%m-%d %H:%M:%S")
                data['Date'] = newtime.split(' ')[0]
                data['Time'] = newtime.split(' ')[1]

                for key in ['UpTick', 'UpVolume', 'DownTick', 'DownVolume', 'UnchVolume', 'OI', 'QryIndex']:
                    data.pop(key, None)

                out_list.append(data)

            return out_list, QryInd
        except:
            Debug_tool.debug.print_info()
