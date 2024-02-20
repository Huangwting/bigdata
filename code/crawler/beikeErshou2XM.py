import requests
import time
from multiprocessing import Pool
from lxml import etree
import pandas as pd
import random
from PIL import Image
import os


page_start = 1
page_size = 100
under_line = '_'
city_param = 'xm'
city_name = 'xiamen'
file_name = 'data/beike_' + city_name + '_ershoufang_' + str(page_start) + '.csv'

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
    'Cookie': 'lianjia_uuid=2d84f21a-41a6-4869-9d54-7618d4ef21ec; expires=Wed, 25-Jan-34 06:54:33 GMT; Max-Age=315360000; domain=.lianjia.com; path=/'
}

def saveImages(url, title):
    city_img_dir = 'imgs/' + city_param + '/'
    if not os.path.exists(city_img_dir):  # 判断当前路径中是否存在imgs文件夹，如果不存在则创建
        os.mkdir(city_img_dir)  # 创建一个名为imgs的文件夹
    response = requests.get(url, headers=headers)
    img = response.content  # 获取图片字节数据
    with open(city_img_dir + title +'.jpg', 'wb') as f:
        f.write(img)

# 获取房源的基本url
# 参数page
def get_home_url(page):
    url = 'http://{}.ke.com/ershoufang/pg{}/'.format(city_param, page)
    text = requests.get(url, headers=headers).text
    html = etree.HTML(text)
    detail_url = html.xpath('//ul[@class="sellListContent"]//li[@class="clear"]/a/@href')
    img_url = html.xpath('//ul[@class="sellListContent"]//li[@class="clear"]/a/img[@class="lj-lazy"]/@data-original')
    for idx, item in enumerate(img_url):
        saveImages(item, city_param + under_line + 'p' + str(page) + under_line + str(idx))
    return detail_url


# 获取房源详细数据信息
def get_home_detail_infos(detail_url, page, idx):
    # headers = {
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.3987.132 Safari/537.36',
    #     'Cookie': 'lianjia_uuid=2d84f21a-41a6-4869-9d54-7618d4ef21ec; expires=Wed, 25-Jan-34 06:54:33 GMT; Max-Age=315360000; domain=.lianjia.com; path=/'
    # }

    url_arr = detail_url.split('/')
    url_arr_len = len(url_arr)

    detail_text = requests.get(detail_url, headers=headers).text
    html = etree.HTML(detail_text)

    all_data = []
    id_txt = url_arr[url_arr_len - 1].replace('.html', '')
    all_data.append(id_txt)

    # 解析获取相关数据
    # 所在地址
    home_location = html.xpath(
        '//div[@data-component="overviewIntro"]//div[@class="content"]//div[@class="areaName"]/span[@class="info"]/a/text()')
    all_data.append(home_location)
    # 小区名称
    local_name = \
    html.xpath('//div[@data-component="overviewIntro"]//div[@class="content"]//div[@class="communityName"]/a/text()')[0]
    all_data.append(local_name)
    # 总价格
    total_price = html.xpath(
        '//div[@data-component="overviewIntro"]//div[@class="content"]//div[@class="price "]/span[@class="total"]/text()')[
        0]
    all_data.append(total_price)
    # 单价
    unit_price = html.xpath('//div[@data-component="overviewIntro"]//div[@class="content"]//div[@class="price "]//div[@class="unitPrice"]/span/text()')[0]
    all_data.append(unit_price)

    # 面积
    area = html.xpath('//div[@data-component="overviewIntro"]//div[@class="houseInfo"]//div[@class="area"]//div[@class="mainInfo"]/text()')[0]
    area_txt = str(area).replace('平米', '')
    all_data.append(area_txt)

    # 户型
    room = html.xpath('//div[@data-component="overviewIntro"]//div[@class="houseInfo"]//div[@class="room"]//div[@class="mainInfo"]/text()')[0]
    all_data.append(room)

    # 户型结构
    house_info_type_txt = html.xpath('//div[@data-component="overviewIntro"]//div[@class="houseInfo"]//div[@class="type"]//div[@class="subInfo"]/text()')[0]
    house_info_type_txt_arr = str(house_info_type_txt).split('/')
    house_type = house_info_type_txt_arr[0]
    all_data.append(house_type)

    # 朝向
    toward = html.xpath('//div[@data-component="overviewIntro"]//div[@class="houseInfo"]//div[@class="type"]//div[@class="mainInfo"]/text()')[0]
    all_data.append(toward)

    # 装修
    renovation = house_info_type_txt_arr[1]
    all_data.append(renovation)

    # 小区均价
    xiaoqu_price = html.xpath(
        '//div[@class="xiaoquCard"]//div[@class="xiaoqu_main fl"]//span[@class="xiaoqu_main_info price_red"]/text()')[
        0].replace(' ', '')
    all_data.append(xiaoqu_price)
    # 小区建造时间
    xiaoqu_built_time = \
    html.xpath('//div[@class="xiaoquCard"]//div[@class="xiaoqu_main fl"]//span[@class="xiaoqu_main_info"]/text()')[
        0].replace(' ', '').replace('\n', '')
    all_data.append(xiaoqu_built_time)
    # 小区建筑类型
    xiaoqu_built_style = \
    html.xpath('//div[@class="xiaoquCard"]//div[@class="xiaoqu_main fl"]//span[@class="xiaoqu_main_info"]/text()')[
        1].replace(' ', '').replace('\n', '')
    all_data.append(xiaoqu_built_style)
    # 小区楼层总数
    xiaoqu_total_ceng = \
    html.xpath('//div[@class="xiaoquCard"]//div[@class="xiaoqu_main fl"]//span[@class="xiaoqu_main_info"]/text()')[
        2].replace(' ', '').replace('\n', '')
    all_data.append(xiaoqu_total_ceng)

    # img
    # 读取图片
    city_img_dir = 'imgs/' + city_param + '/'
    title = city_param + under_line + 'p' + str(page) + under_line + str(idx)
    img_path = city_img_dir + title + '.jpg'
    # img = Image.open(img_path)
    # # 将图片转换为灰度图，并调整大小
    # gray_img = img.convert('L').resize((200, 200))
    all_data.append(img_path)
    return all_data


# 数据保存至csv文件里（使用pandas中的to_csv保存）
def save_data(data):
    data_frame = pd.DataFrame(data,
                              columns=['id', '小区位置', '小区名称', '房屋总价（万元）', '房屋单价（元/平米）', '面积(平米)', '户型', '户型结构', '朝向', '装修', '小区均价', '小区建造时间', '小区房屋类型', '小区层数', '房间图'])

    data_frame.to_csv(file_name, header=False, index=False, mode='a', encoding='utf_8_sig')


def main(page):

    print('开始爬取第{}页的数据！'.format(page))
    urls = get_home_url(page)
    for idx, url in enumerate(urls):
        print('开始第{}个房间，详细网页为{}的房屋详细信息资料！'.format(idx, url))
        all_data = get_home_detail_infos(detail_url=url, page=page, idx=idx)
        data = []
        data.append(all_data)
        save_data(data)
        time.sleep(5)


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
