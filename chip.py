import requests
from bs4 import BeautifulSoup as BS

class Chip:
    def __init__(self):
        self.url = None
        self.headers = {'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8',
                        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"}
        
    def set_url(self, url):
        self.url = url
    def post_test(self):
        url = "https://www.tdcc.com.tw/smWeb/QryStockAjax.do"
        data_dict = {
            "scaDates":"20210401",
            "scaDate":"20210401",
            "SqlMethod":"StockNo",
            "StockNo":"2330",
            "StockName":"",
            "REQ_OPR": "SELECT",
            "clkStockNo": "2330",
            "clkStockName":""
        }
        self.set_url(url)
        s = requests.Session()
        s.headers.update(self.headers)
        result = s.post(self.url, data=data_dict)
        if result.status_code == 200:
            soup = BS(result.text, "html5lib")

if __name__ == "__main__":
    chip = Chip()
    chip.post_test()
