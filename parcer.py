import pandas as pd
import requests
import bs4
import logging
import collections
import csv
from lxml import html
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('wb')

ParseResult = collections.namedtuple(
    'ParseResult',
    (
        'goods_name',
        'brand_name',
        'url',
        'search_num',
        'Vcode',
        'Price'
    )
)

HEADERS = (
    'Good',
    'Brand',
    'Url',
    'Number',
    'Vcode',
    'Price'
)


class CLient:
    def __init__(self):
        self.session = requests.session()
        self.session.headers = {
            'User-Ahent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/94.0.4606.81 Safari/537.36 OPR/80.0.4170.61',
            'Accept-Language': 'ru',
        }
        self.result = []

    def load_page(self, url):
        url = url
        res = self.session.get(url=url)
        res.raise_for_status()
        return res.text

    def pars_page(self, text: str):
        i = 1
        soup = bs4.BeautifulSoup(text, 'lxml')
        container = soup.select('div.product-card__wrapper')
        for block in container:
            self.parce_block(block=block,  i=i)
            i += 1

    def parce_block(self, block, i):
        logger.info(block)
        logger.info('*' * 100)

        url_block = block.select_one('a.product-card__main.j-open-full-product-card')
        if not url_block:
            logging.error('no url_block')
            return

        url = url_block.get('href')
        if not url:
            logging.error('no href')
            return

        name_block = block.select_one('div.product-card__brand-name')
        if not name_block:
            logger.error(f'no name block on {url}')
            return

        brand_name = name_block.select_one('strong.brand-name')
        if not brand_name:
            logger.error(f'no brand name on {url}')
            return
        brand_name = brand_name.text
        brand_name = brand_name.replace('/', '').strip()

        goods_name = name_block.select_one('span.goods-name')
        if not goods_name:
            logger.error(f'no goods name on {url}')
            return
        goods_name = goods_name.text
        goods_name = goods_name.replace('/', '').strip()

        # logger.info('%s, %s, %s, %s', 'https://www.wildberries.ru'+url, brand_name, goods_name, i)

        urlcode = url.split('/')

        price = block.select_one('span.price-commission__current-price')
        if not (not price):
            price = str(price)
            price = price.replace('<', '>')
            # price = price.replace('₽','>')
            price = price.split('>')
            price2 = price[2]
            logger.info('%s', price2)
        else:
            price2 = 'nan'

        self.result.append(ParseResult(
            url='https://www.wildberries.ru' + url,
            brand_name=brand_name,
            goods_name=goods_name,
            search_num=i,
            Vcode=urlcode[2],
            Price=price2
        ))

    def save_result(self):
        path = 'C:/Users/Asus/Desktop/tz/parc.csv'
        with open(path, 'w', encoding='utf-8') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(HEADERS)
            for item in self.result:
                writer.writerow(item)

    def run(self, url):
        text = self.load_page(url)
        self.pars_page(text=text)
        logger.info(f'Получено {len(self.result)} элементов')
        print()
        self.save_result()


class Find:

    def read_data(self, start_data):
        data = start_data
        data2 = data[['ИП', 'Бренд', 'Название ', 'Поисковый Запрос', 'Артикул ВБ', 'Сегодняшняя цена', 'Рейтинг карточки ★★★★★', 'Количество отзывов',
                    'Место по поисковому запросу']]
        data2 = data2.drop(data2.index[[0]])
        return (data, data2)

    def rewiev_price_count(self, url):
        req = requests.get(url)
        tree = html.fromstring(req.content)
        if not tree.xpath('.//meta[@itemprop="ratingValue"]/@content'):
            rvalue = 0
        else:
            rvalue = int(tree.xpath('.//meta[@itemprop="ratingValue"]/@content')[0])
        if not tree.xpath('.//meta[@itemprop="reviewCount"]/@content'):
            rcount = 0
        else:
            rcount = int(tree.xpath('.//meta[@itemprop="reviewCount"]/@content')[0])
        pcount = float(tree.xpath('.//meta[@itemprop="price"]/@content')[0])
        return (rvalue, rcount, pcount)

    def find_chars(self, data2):
        rewiev = ['Дата']
        price = ['Дата']
        value = ['Дата']
        for i in data2['Артикул ВБ']:
            rrvalue, crewiev, pprice = self.rewiev_price_count('https://www.wildberries.ru/catalog/' + str(i) + '/detail.aspx')
            rewiev.append(crewiev)
            price.append(pprice)
            value.append(rrvalue)
            time.sleep(5)
        return (rewiev, price, value)

    def fill(self, data, rewiev, price, value, itemlist, sheet):
        data['Сегодняшняя цена'] = pd.Series(price)
        data['Количество отзывов'] = pd.Series(rewiev)
        data['Рейтинг карточки ★★★★★'] = pd.Series(value)
        data['Место по поисковому запросу'] = pd.Series(itemlist)
        # print(rewiev)
        # print(price)
        # print(value)
        # print(itemlist)
        data.to_excel('complete.xlsx')
        sheet.insert_rows(data.values.tolist())
        return (data)

    def run(self, sheet_data, sheetnumber, itemlist):
        data_tofill, data_toread = self.read_data(sheet_data)
        rewiev, price, value = self.find_chars(data_toread)
        self.fill(data_toread, rewiev, price, value, itemlist, sheetnumber)


class Google():

    def run(self):
        # define the scope
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        # add credentials to the account
        creds = ServiceAccountCredentials.from_json_keyfile_name('wildberries-331111-3ac7f58fb965.json', scope)

        # authorize the clientsheet
        client = gspread.authorize(creds)
        sheet = client.open('Вадим_ Ноябрь ежедневные продажи_2021')
        sheet_instance = sheet.get_worksheet(0)
        sheet.add_worksheet(rows=100, cols=100, title=f'{date.today()}')
        sheet_towrite = sheet.worksheet(f'{date.today()}')
        pd.set_option('display.max_columns', None)
        records_data = sheet_instance.get_all_records()
        records_df = pd.DataFrame.from_dict(records_data)
        return(records_df, sheet_towrite)

class Count():

    def get_req_list(self, arts, req, id):
        quest = '+'.join(req[id].split())
        first_req = requests.get(f'https://wbxsearch.wildberries.ru/exactmatch/v2/common?query={quest}')
        first_req_data = json.loads(first_req.text)
        query = first_req_data.get('query')
        presets = first_req_data.get('shardKey')
        final_req_url =f'https://wbxcatalog-ru.wildberries.ru/{presets}/catalog?spp=0&regions=64,75,4,38,30,33,70,68,22,31,66,40,71,82,1,80,69,48&stores=119261,122252,122256,117673,122258,122259,121631,122466,122467,122495,122496,122498,122590,122591,122592,123816,123817,123818,123820,123821,123822,124093,124094,124095,124096,124097,124098,124099,124100,124101,124583,124584,125238,125239,125240,132318,132320,143772,132871,132870,132869,126679,126680,126667,125186,507,3158,117501,120602,120762,6158,121709,124731,1699,130744,2737,117986,1733,686,132043&pricemarginCoeff=1.0&reg=0&appType=1&offlineBonus=0&onlineBonus=0&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1257786,-2162196,-102269,-1029256&{query}&sort=popular'
        final_req = requests.get(final_req_url)
        final_req_data = json.loads(final_req.text)
        list = final_req_data.get('data')
        final_list = list.get('products')
        for i in range(2, 22):
            add_req_url = f'https://wbxcatalog-ru.wildberries.ru/{presets}/catalog?spp=0&regions=64,75,4,38,30,33,70,68,22,31,66,40,71,82,1,80,69,48&stores=119261,122252,122256,117673,122258,122259,121631,122466,122467,122495,122496,122498,122590,122591,122592,123816,123817,123818,123820,123821,123822,124093,124094,124095,124096,124097,124098,124099,124100,124101,124583,124584,125238,125239,125240,132318,132320,143772,132871,132870,132869,126679,126680,126667,125186,507,3158,117501,120602,120762,6158,121709,124731,1699,130744,2737,117986,1733,686,132043&pricemarginCoeff=1.0&reg=0&appType=1&offlineBonus=0&onlineBonus=0&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21&dest=-1257786,-2162196,-102269,-1029256&{query}&sort=popular&page={str(i)}'
            add_req = requests.get(add_req_url)
            add_req_data = json.loads(add_req.text)
            add_list = add_req_data.get('data')
            final_add_list = add_list.get('products')
            final_list.extend(final_add_list)
            if not final_add_list:
                break
            print(final_add_list)

        # print(query)
        # print(presets)
        # print(final_list)

        for i in range(len(final_list)):
            if arts[id] in final_list[i].values():
                return(i+1)

    def run(self, df):
        arts = df['Артикул ВБ'].tolist()
        req = df['Поисковый Запрос'].tolist()
        arts.pop(0)
        req.pop(0)
        itemlist = ['Дата']
        for i in range(len(arts)):
            itemlist.append(self.get_req_list(arts, req, i))
        return(itemlist)


if __name__ == '__main__':
    start_time = time.time()
    get_data = Google()
    sheet, sheet_towrite = get_data.run()

    # parcer = CLient()
    # for i in [1]:
    #     url = 'https://www.wildberries.ru/catalog/aksessuary/perchatki-i-varezhki?sort=popular&page=' + str(i)
    #     parcer.run(url=url)

    count = Count()
    itemlist = count.run(sheet)
    for i in range(len(itemlist)):
        if itemlist[i] is None:
            itemlist[i] = 0
    # print(itemlist)

    find = Find()
    find.run(sheet_data=sheet, sheetnumber=sheet_towrite, itemlist=itemlist)
    print("--- %s seconds ---" % (time.time() - start_time))
