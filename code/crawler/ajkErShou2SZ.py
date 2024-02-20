import requests
from bs4 import BeautifulSoup
import threading
import time
import csv
import random
from crawlerUrlManager import CrawlerUrlManager


page_start = 1
page_size = 100
under_line = '_'
city_name = 'shenzhen'
file_name = 'data/ajk_' + city_name + '_ershoufang_total_' + str(page_start) + '.csv'

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

def craw_anjuke(craw_url, proxy):
    if craw_url is None:
        print(threading.current_thread().getName() + ' craw_url is None')
        return

    # 构造url的request headers，伪装成正常用户
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
        'cache-control': 'no-cache',
        'cookie': 'aQQ_ajkguid=B92A5535-A853-AE39-3FE0-CC7FF20AFF15; ajk-appVersion=; seo_source_type=0; id58=CroD4GW19awFqa9hXkCeAg==; cmctid=18; xxzlclientid=8f58ce17-0576-4940-a7fb-1706423853900; xxzlxxid=pfmx7MUmM8VixI13t01IeuJpaE7nlFptUB/mPoV1q++WvE+/s4Sh5ZOJ9po0Q5tMZzcr; isp=true; 58tj_uuid=03c1025a-bdc6-452d-a2c3-cc3cfdd59d61; als=0; _ga=GA1.2.828932174.1706469611; xxzlbbid=pfmbM3wxMDMyNXwxLjUuMHwxNzA2NTQxOTgwNDk0fGJBajQzbmVWVjNJRmgwRlQvV05weSsxQVpLQ3RCWVRTL3ZPMjNVdU9BQUU9fDk4YTFhNWYwMmE5MTA4ZDkzY2M2MjRlMDE2MzhkYjJhXzE3MDY1NDE5NzY5NzdfZTIzOTk4N2U1MzhjNDRjN2ExZDQxNzdkMDcyYjM1YzZfMzA2NjgzNzkyNXxkMzM1YzdmZTNkMGViNzRmZmNlODlmNjQwNTIzOTI5Ml8xNzA2NTQxOTc3MDkwXzI1NA==; _gid=GA1.2.1242764250.1706838776; new_uv=12; _ga_DYBJHZFBX2=GS1.2.1706868681.9.0.1706868681.0.0.0; twe=2; sessid=DB91762E-4D4F-7733-1393-9290FB9F9058; fzq_h=8f483bdc49646bb8921ddcbd955ae502_1706927800656_b5d9e6c2aa7d4915a3b8c87f378218c9_1911188441; fzq_js_anjuke_ershoufang_pc=e9b0fe3621c7c798f27809a69af7a5b8_1706927801821_24; xxzl_cid=f286055eadc049b8bf8dd7729782d419; xxzl_deviceid=cg2S64JHJ5nTZK9fuklwMpIIpnWrTu1lTth5PwBu726ME039VVCVrULrWmUjVqOt; obtain_by=2; ctid=14',
        'host': 'tianjin.anjuke.com',
        'pragma': 'no-cache',
        'referer': 'https://{}.anjuke.com/sale/p1/'.format(city_name),
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': "Windows",
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': Agent()
    }

    with open(file_name, 'a', newline='', encoding="utf-8") as f:
        # 有代理用代理，没代理直接爬
        r = requests.get(craw_url, headers=headers, timeout=3)
        # 如果正常返回结果，开始解析
        if r.status_code == 200:
            content = r.text
            # print(content)
            soup = BeautifulSoup(content, 'html.parser')
            content_div_nodes = soup.find_all('div', class_='property-content')
            for content_div_node in content_div_nodes:
                # 获取房产标题内容
                content_title_name = content_div_node.find('h3', class_='property-content-title-name')
                title_name = content_title_name.get_text()
                # 获取房子户型
                content_layout = content_div_node.find('p',
                                                       class_='property-content-info-text property-content-info-attribute')
                layout_datas = content_layout.find_all('span')
                datas_shi = layout_datas[0].get_text() + layout_datas[1].get_text()
                datas_ting = layout_datas[2].get_text() + layout_datas[3].get_text()
                datas_wei = layout_datas[4].get_text() + layout_datas[5].get_text()
                # 获取房子的面积、朝向、楼层和建筑年份
                square_num = ''
                square_unit = ''
                orientations = ''
                floor_level = ''
                build_year = ''
                content_extra_info_datas = content_div_node.find_all(
                    lambda content_div_node: content_div_node.name == 'p' and content_div_node.get('class') == [
                        'property-content-info-text'])
                for i in range(len(content_extra_info_datas)):
                    if i == 0:
                        square = content_extra_info_datas[0].get_text().strip()
                        square_num = square[0:len(square) - 1]
                        square_unit = square[len(square) - 1:]
                    if i == 1:
                        orientations = content_extra_info_datas[1].get_text().strip()
                    if i == 2:
                        floor_level = content_extra_info_datas[2].get_text().strip()
                    if i == 3:
                        build_year = content_extra_info_datas[3].get_text().strip()
                # 获取房子的小区名称、位置信息（区-镇-道路）
                content_info_comm = content_div_node.find('div',
                                                          class_='property-content-info property-content-info-comm')
                # 获取小区名称
                housing_estate = content_info_comm.find('p',
                                                        class_='property-content-info-comm-name').get_text().strip()
                # 获取小区地址信息
                content_info_address = content_info_comm.find('p',
                                                              class_='property-content-info-comm-address').find_all(
                    'span')
                district = content_info_address[0].get_text().strip()
                town = content_info_address[1].get_text().strip()
                road = content_info_address[2].get_text().strip()
                # 获取房子的更多tag信息，比如朝向、是否满五唯一、房子新旧、是否近地铁等
                content_info_tag = content_div_node.find_all('span', class_='property-content-info-tag')
                tagstr = ''
                for i in range(len(content_info_tag)):
                    tagstr = tagstr + content_info_tag[i].get_text().strip() + ','
                # 获取房子价格信息
                price_info_datas = content_div_node.find('div', class_='property-price')
                total_price = price_info_datas.find('span', class_='property-price-total-num').get_text().strip()
                total_price_unit = price_info_datas.find('span', class_='property-price-total-text').get_text().strip()
                avarage_price = price_info_datas.find('p', class_='property-price-average').get_text().strip()
                avarage_price_num = avarage_price[0:len(avarage_price) - 3]
                avarage_price_unit = avarage_price[len(avarage_price) - 3:]
                # 输出到文件
                writer = csv.writer(f)
                writer.writerow([title_name,
                                 datas_shi,
                                 datas_ting,
                                 datas_wei,
                                 square_num,
                                 square_unit,
                                 orientations,
                                 floor_level,
                build_year, housing_estate, district, town, road, tagstr, total_price, total_price_unit,
                avarage_price_num, avarage_price_unit])

                print([title_name,
                    datas_shi,
                    datas_ting,
                    datas_wei,
                    square_num,
                    square_unit,
                    orientations,
                    floor_level,
                    build_year,
                    housing_estate,
                    district,
                    town,
                    road,
                    tagstr,
                    total_price,
                    total_price_unit,
                    avarage_price_num,
                    avarage_price_unit])
                # f.write("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s\n" % (
                # title_name, datas_shi, datas_ting, datas_wei, square_num, square_unit, orientations, floor_level,
                # build_year, housing_estate, district, town, road, tagstr, total_price, total_price_unit,
                # avarage_price_num, avarage_price_unit))
            print(f'{threading.current_thread().getName()} crawl over!;Crawler Url is:{craw_url}')
        else:
            print(
                f'{threading.current_thread().getName()} crawl fail!status code={r.status_code};Crawler Url is:{craw_url}')


if __name__ == '__main__':

    # 先将标题写入结果数据文件
    with open(file_name, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(
        ['待售房屋', '室', '厅', '卫', '面积', '面积单位', '朝向', '楼层', '建筑年份', '小区名称', '区', '镇', '道路', '标签', '总价', '总价单位', '均价',
        '均价单位']
        )

    # 假设爬取crawler_pages页，生成待爬取的url，放入url池管理起来
    crawlerUrlManager = CrawlerUrlManager()
    # 要爬取的页数，默认为100，可调整
    crawler_pages = page_start
    for i in range(crawler_pages, crawler_pages + page_size):
        url = 'https://{region}.anjuke.com/sale/p{page}/'
        craw_url = url.format(region=city_name, page=i)
        crawlerUrlManager.add_new_url(craw_url)

    try:
        print('没有获取到代理IP，开始使用自身IP爬取页面数据...')
        while crawlerUrlManager.has_new_url():
            crawler_thread = threading.Thread(target=craw_anjuke, args=(crawlerUrlManager.get_url(), None))
            crawler_thread.start()

            crawler_thread.join()
            time.sleep(25)  # 为避免同一个ip频繁爬取被反爬封禁，一线程爬取完后，等待10秒再爬取下一个页面

    except Exception as e:
        print('Crawler Excepiton:' + e)
    finally:
        print(f'已爬取的url数量：{crawlerUrlManager.get_old_url_size()}')
        print(f'未爬取的url数量：{+crawlerUrlManager.get_new_url_size()}')
        if crawlerUrlManager.get_new_url_size() > 0:
            print('未爬取的url如下：')
            for new_url in crawlerUrlManager.get_url():
                print(f'{new_url}')



