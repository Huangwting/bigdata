import random
import time

import requests
import json
import pandas as pd


def Agent():
    # pc端的user-agent
    agent = [
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50', \
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50', \
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)', \
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)', 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)', \
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1', \
        'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1', \
        'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11', \
        'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11', \
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11', \
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)', \
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)', \
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)', \
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)', \
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)', \
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)', \
        'Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) wOSBrowser/233.70 Safari/534.6 TouchPad/1.0', \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1", \
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6", \
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1", \
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5", \
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5", \
    ]
    return random.choice(agent)

headers = {
    'User-Agent': Agent(),
    'Cookie': 'home_gray=1; cna=tcomHsg9ZXwCAbKt5GeuAbKj; xlly_s=1; _uab_collina=170718842672484512872772; search_gray=1; AMAPID=b13e2e8f586718eb4b4c7fd28cabbe59; passport_login=MTQyMDg4NjcxOCxhbWFwbGRjcjduaEgseHl5aWU2MzJiZGhkcm1xN2pocWhnempoNGI2enhvaDMsMTcwNzIyMzQzOSxaR001Wm1VM01ERTRNVFZoWXpWa1pHUmhaamswTlRSaU4ySmlNR0ZsT0RBPQ%3D%3D; dev_help=zLc8B5eS%2BwH%2FoZ123S6NR2ZiMGEwMjIwMmQ0NWEwNGZiMzBjMGJiNTgyNGVhNDgyMjU2YjJkNzFmMDNhMTM4YWI3MGQwMDhhMGE4YzYwZmLuX9Ax%2FcdNiUVydDQLzHDcXQk15UPhRfI0LnkHBKbWGGpOqwdltsSPMfcN7xtAKLIj04kP%2FLAk%2F9vCfdYFj5ypFC4fj9nPlcugV%2BAxSeQnQ9Ona9V%2BM32rADLR87DqokM%3D; Hm_lvt_c8ac07c199b1c09a848aaab761f9f909=1707188425,1707223255,1707237854; Hm_lpvt_c8ac07c199b1c09a848aaab761f9f909=1707237863; isg=BHl5FNKjOKqmzeQ9q5S3gIbuiOVThm04hYSZiJuu9aAfIpm049Z9COdwoCbUgQVw; tfstk=eUoylcitA3KrB76szrqUuQMLAoq8Ald1EDNQKvD3F7VoOeFnuXG2eWMHwXoUiXPIFuj5Yykn_J_IPQIEgfDZFD98qJcEijj7ZphIT6cx_wgSODuAYAHKVLEBeuH8vkA61hZEeYEpMxUW-hMpGmr61Ct6cXuaGqdsPFYRUMj7k9QclPAidSm4dknzZCDzgMjNB8zu_hFVxMm4UxPiEdIhxmyzzgof9-jbLD3PtwzuH-P63KPN8yD85bScywQLPse41LvlJwUuH-P63K7dJzTY359kE'
}

# 数据保存至csv文件里（使用pandas中的to_csv保存）
def save_data(data):
    data_frame = pd.DataFrame(data,  columns=['小区名称', '经纬度'])

    data_frame.to_csv('tianjin_addr.csv', header=False, index=False, mode='a', encoding='utf_8')

all_data = []
u_key = '6da236aebd69b17f979901bd776c1e56'
addr_arr = []
with open('tianjinaddr.txt', 'r', encoding='utf-8') as f:
    str = f.read()
    addr_arr = str.replace('\n', '').split(',')

for item in addr_arr:
    # time.sleep(15)
    if item != '' :
        url = 'https://restapi.amap.com/v3/geocode/geo?address={}&city=022&key={}'.format(item, u_key)
        print(url)
        resp = requests.get(url, headers=headers)
        jdata = json.loads(resp.content)
        if jdata['geocodes'] is None or len(jdata['geocodes']) == 0:
            print("none")
        else:
            row_data = [item, jdata['geocodes'][0]['location']]
            print(row_data)
            out_data = [row_data]
            all_data.append(row_data)
            save_data(out_data)

print(all_data)
# save_data(all_data)
