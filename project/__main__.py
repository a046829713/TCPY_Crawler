import Crawler_Service


def main():
    service = Crawler_Service.Crawler_Service()
    # service.get_data() # 取得股票期貨分K資料
    service.get_daydata() # 取得股票期貨日K資料


if __name__ == '__main__':
    main()
