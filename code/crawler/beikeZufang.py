import requests
import time
from multiprocessing import Pool
import pandas as pd
from bs4 import BeautifulSoup
import random

page_start = 999
page_size = 1000
file_name = 'data/beike_tianjin_zufang_' + str(page_start) + '.csv'

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

# 获取房源的基本url
# 参数page
def get_home_url(page):
    url = 'http://tj.zu.ke.com/zufang/pg{}/'.format(page)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.3987.132 Safari/537.36',
        'Cookie': 'SECKEY_ABVK=+cFyWJD6ci7kXeStf7yLua+rA3FMkg7pieT2djiJnRU%3D; BMAP_SECKEY=Gjb8NigY7W1EGkuXYAAybtlHfS89XQ5hzHAEk85Q1ZjvwIcbNX7yEgBQaYFY5nZx9TCSOx7GaZuiRaCE7Dz5nP5oWjQl0ItKmC9KWag7uMSGnBwaLGGoyzHzxuz1O1KFGEEpTuza1JprdIP5WkOL8MEogVz0jIyCbl_wC1wBCxSnsh-Jgq--C88HSBSdpRFT; lianjia_uuid=08224de4-bc4e-4333-83ce-b4cc95d1f5b6; crosSdkDT2019DeviceId=5c3a14--chkhq4-m0fmweob5ku44vn-mkbtp7lv3; ftkrc_=aba92241-ae8f-4e21-91c9-2ebf3e54bccc; lfrc_=170d8959-feef-42fb-9c92-333ec158402a; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2218d4ec7933ee30-0a4a972c2354e2-26001951-921600-18d4ec7933f139f%22%2C%22%24device_id%22%3A%2218d4ec7933ee30-0a4a972c2354e2-26001951-921600-18d4ec7933f139f%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_utm_source%22%3A%22baidu%22%2C%22%24latest_utm_medium%22%3A%22pinzhuan%22%2C%22%24latest_utm_campaign%22%3A%22wydalian%22%2C%22%24latest_utm_content%22%3A%22biaotimiaoshu%22%2C%22%24latest_utm_term%22%3A%22biaoti%22%7D%7D; select_city=120000; GUARANTEE_POPUP_SHOW=true; digv_extends=%7B%22utmTrackId%22%3A%22%22%7D; _ga=GA1.2.476941347.1706440547; _gid=GA1.2.1743374011.1706440547; ke_uuid=06a583619ed7c9aaccc89ac9af0c887e; GUARANTEE_BANNER_SHOW=true; lianjia_ssid=a305f2a9-7148-4799-be48-894b7497b7d5; srcid=eyJ0Ijoie1wiZGF0YVwiOlwiZjk0NGI5NGU0ZjVlOWJiYzAxZDE4MzhlOTE1ZDBlMjVjZjZmZWY4YWVjMDUzMGUyMmVmOWUwZGFkZjdmMmQzYjhkMjkxZTZjYTM1ZTBhN2E5NzhkYjMxZDgwNDBkYWYyZTdhMWIxMzhiOTBjNmIxMmE5M2ExZDJiYzBiNjNjMzRjMzA1NWE2MTlkMzYzNmNmNjNjOWI0MmMwZTY2NGRkM2UzMjI0ZTg1MjdiNzY4MmQ3N2ZmMDQxN2NkZWU0MjY0OGU1OTA5ODc2ODI5YzRlZDIyNDhmNTViYmNhZGQxODc4ODhjZTM4MTFiM2UyNGU1MWRhNjJkYjg2NGViNzg5N1wiLFwia2V5X2lkXCI6XCIxXCIsXCJzaWduXCI6XCI5OTcxZmVlYlwifSIsInIiOiJodHRwczovL3RqLnp1LmtlLmNvbS96dWZhbmcvVEoxODU1ODUzNDE2NjYwNTk4Nzg0Lmh0bWwiLCJvcyI6IndlYiIsInYiOiIwLjEifQ=='
    }
    resp = requests.get(url, headers=headers)
    param_arr = []
    html = resp.content.decode('utf-8', 'ignore')
    my_page = BeautifulSoup(html, 'lxml')

    con_li = my_page.find('div', class_='content__list')
    for item in con_li.find_all('div', class_='content__list--item'):
        item_arr = []

        detail_url = item.find('a', class_='content__list--item--aside')['href']
        item_arr.append(detail_url)

        dis_a_arr = item.find('div', class_='content__list--item--main').find('p', 'content__list--item--des').find_all('a')
        district = dis_a_arr[0].get_text()
        item_arr.append(district)

        road = dis_a_arr[1].get_text()
        item_arr.append(road)

        sub_district = dis_a_arr[2].get_text()
        item_arr.append(sub_district)

        param_arr.append(item_arr)

    return param_arr


# 获取房源详细数据信息
def get_home_detail_infos(detail_url, district, road, sub_district):
    url_pre = 'http://tj.zu.ke.com/'
    url_req = url_pre + detail_url
    headers = {
        'User-Agent': Agent(),
        'Cookie': 'SECKEY_ABVK=+cFyWJD6ci7kXeStf7yLua+rA3FMkg7pieT2djiJnRU%3D; BMAP_SECKEY=Gjb8NigY7W1EGkuXYAAybtlHfS89XQ5hzHAEk85Q1ZjvwIcbNX7yEgBQaYFY5nZx9TCSOx7GaZuiRaCE7Dz5nP5oWjQl0ItKmC9KWag7uMSGnBwaLGGoyzHzxuz1O1KFGEEpTuza1JprdIP5WkOL8MEogVz0jIyCbl_wC1wBCxSnsh-Jgq--C88HSBSdpRFT; lianjia_uuid=08224de4-bc4e-4333-83ce-b4cc95d1f5b6; crosSdkDT2019DeviceId=5c3a14--chkhq4-m0fmweob5ku44vn-mkbtp7lv3; ftkrc_=aba92241-ae8f-4e21-91c9-2ebf3e54bccc; lfrc_=170d8959-feef-42fb-9c92-333ec158402a; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2218d4ec7933ee30-0a4a972c2354e2-26001951-921600-18d4ec7933f139f%22%2C%22%24device_id%22%3A%2218d4ec7933ee30-0a4a972c2354e2-26001951-921600-18d4ec7933f139f%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_utm_source%22%3A%22baidu%22%2C%22%24latest_utm_medium%22%3A%22pinzhuan%22%2C%22%24latest_utm_campaign%22%3A%22wydalian%22%2C%22%24latest_utm_content%22%3A%22biaotimiaoshu%22%2C%22%24latest_utm_term%22%3A%22biaoti%22%7D%7D; select_city=120000; GUARANTEE_POPUP_SHOW=true; digv_extends=%7B%22utmTrackId%22%3A%22%22%7D; _ga=GA1.2.476941347.1706440547; _gid=GA1.2.1743374011.1706440547; ke_uuid=06a583619ed7c9aaccc89ac9af0c887e; GUARANTEE_BANNER_SHOW=true; lianjia_ssid=a305f2a9-7148-4799-be48-894b7497b7d5; srcid=eyJ0Ijoie1wiZGF0YVwiOlwiZjk0NGI5NGU0ZjVlOWJiYzAxZDE4MzhlOTE1ZDBlMjVjZjZmZWY4YWVjMDUzMGUyMmVmOWUwZGFkZjdmMmQzYjhkMjkxZTZjYTM1ZTBhN2E5NzhkYjMxZDgwNDBkYWYyZTdhMWIxMzhiOTBjNmIxMmE5M2ExZDJiYzBiNjNjMzRjMzA1NWE2MTlkMzYzNmNmNjNjOWI0MmMwZTY2NGRkM2UzMjI0ZTg1MjdiNzY4MmQ3N2ZmMDQxN2NkZWU0MjY0OGU1OTA5ODc2ODI5YzRlZDIyNDhmNTViYmNhZGQxODc4ODhjZTM4MTFiM2UyNGU1MWRhNjJkYjg2NGViNzg5N1wiLFwia2V5X2lkXCI6XCIxXCIsXCJzaWduXCI6XCI5OTcxZmVlYlwifSIsInIiOiJodHRwczovL3RqLnp1LmtlLmNvbS96dWZhbmcvVEoxODU1ODUzNDE2NjYwNTk4Nzg0Lmh0bWwiLCJvcyI6IndlYiIsInYiOiIwLjEifQ=='
    }

    detail_resp = requests.get(url_req, headers=headers)
    detail_html = detail_resp.content.decode('utf-8', 'ignore')
    detail_bf = BeautifulSoup(detail_html, 'lxml')

    all_data = []

    # 二手房，租房，新房这几个东西（新房 不急，放最后）
    # 字段：小区地点，小区名称、类型（lofter 普通房子什么的）、房型、朝向、楼层、面积、编码、总价、单价、周边、词条（那些介绍的优势 采光好
    # 租房也是包括 租金什么的
    columns = ['小区行政区', '小区街道', '小区名称', '面积(平米)', '户型', '装修', '朝向', '电梯', '用水', '用电',
               '燃气', '采暖', '付款方式', '租金 (元/月)', '押金(元)', '服务费(元)', '中介费 (元)']

    # 行政区
    all_data.append(district)

    # 街道
    all_data.append(road)

    # 小区
    all_data.append(sub_district)

    # 顶部右侧信息
    content_aside = detail_bf.find('div', class_='content__aside fr')
    content_aside_li = content_aside.find('ul', class_='content__aside__list').find_all('li')
    content_aside_tags = content_aside.find('p', class_='content__aside--tags').find_all('i')
    content_aside_li_arr = content_aside_li[1].get_text().split(' ')

    # detail
    detail_info = detail_bf.find('div', class_='content__detail').find('div', class_='content__article fl')
    detail_info1_li = detail_info.find('div', class_='content__article__info').find('ul').find_all('li')

    # 费用
    detail_cost = detail_bf.find('div', class_='content__article__info3 cost_box').find('div', class_='cost_content')
    detail_cost_li = detail_cost.find('div', class_='table_content').find('ul').find_all('li')

    # 面积(平米)
    area = content_aside_li_arr[1].replace('㎡', '')
    all_data.append(area)

    # 户型
    room = content_aside_li_arr[0].split('：')[1].replace(' ', '')
    all_data.append(room)

    # 装修
    renovation = ''
    if len(content_aside_li_arr) >= 3:
        renovation = content_aside_li_arr[2].replace(' ', '')
    all_data.append(renovation)

    # 朝向
    toward = detail_info1_li[2].get_text().split('：')[1]
    all_data.append(toward)

    # 电梯
    elevator = detail_info1_li[8].get_text().split('：')[1].replace(' ', '')
    all_data.append(elevator)

    # 用水
    water = detail_info1_li[11].get_text().split('：')[1].replace(' ', '')
    all_data.append(water)

    # 用电
    electricity = detail_info1_li[13].get_text().split('：')[1].replace(' ', '')
    all_data.append(electricity)

    # 燃气
    gas = detail_info1_li[14].get_text().split('：')[1].replace(' ', '')
    all_data.append(gas)

    # 采暖
    heating = detail_info1_li[16].get_text().split('：')[1].replace(' ', '')
    all_data.append(heating)

    # 付款方式
    pay_type = detail_cost_li[0].get_text().replace(' ', '')
    all_data.append(pay_type)

    # 租金 (元/月)
    pay_cost = detail_cost_li[1].get_text().replace(' ', '')
    all_data.append(pay_cost)

    # 押金(元)
    deposit = detail_cost_li[2].get_text().replace(' ', '')
    all_data.append(deposit)

    # 服务费(元)
    s_charge = detail_cost_li[3].get_text().replace(' ', '')
    all_data.append(s_charge)

    # 中介费 (元)
    commission = detail_cost_li[4].get_text().replace(' ', '')
    all_data.append(commission)

    # 标签
    labs = []
    if len(content_aside_tags) > 0 :
        for i in content_aside_tags :
            labs.append(i.get_text())
    all_data.append(labs)

    return all_data


# 数据保存至csv文件里（使用pandas中的to_csv保存）
def save_data(data):
    data_frame = pd.DataFrame(data,
                              columns=['小区行政区', '小区街道', '小区名称', '面积(平米)', '户型', '装修', '朝向', '电梯', '用水', '用电', '燃气', '采暖', '付款方式', '租金 (元/月)', '押金(元)', '服务费(元)', '中介费 (元)', '标签'])

    # 二手房，租房，新房这几个东西（新房 不急，放最后）
    # 字段：小区地点，小区名称、类型（lofter 普通房子什么的）、房型、朝向、楼层、面积、编码、总价、单价、周边、词条（那些介绍的优势 采光好
    # 租房也是包括 租金什么的

    data_frame.to_csv(file_name, header=True, index=False, mode='a', encoding='utf_8_sig')


def main(page):
    print('开始爬取第{}页的数据！'.format(page))
    # choice_time = random.choice(range(0,5))
    # print(choice_time)

    params = get_home_url(page)
    for item in params:
        print('开始爬去详细网页为{}的房屋详细信息资料！'.format(item[0]))
        all_data = get_home_detail_infos(detail_url=item[0], district=item[1], road=item[2], sub_district=item[3])
        data = []
        data.append(all_data)
        save_data(data)
        time.sleep(30)


if __name__ == "__main__":

    page = range(page_start, page_start + page_size)
    print('爬虫开始')
    try:
        pool = Pool(processes=25)
        pool.map(main, page)
        # proxies = proxy.get_proxy_random()
        # pool.apply_async(main,args=(page,proxies,))
        pool.close()
        pool.join()
    except Exception as e:
        print('Exception:' + e)
    finally:
        print(f'爬虫结束：供{0}', page)
