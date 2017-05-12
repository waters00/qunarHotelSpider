#!/usr/bin/env python
# -*- coding: utf-8 -*

import codecs
import json
import pprint
import re
import collections
from operator import itemgetter
import argparse

from bs4 import BeautifulSoup
import requests
import pandas

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pymongo

from config import inter_city_list, main_land_city_list, test_city_list, MONGO_URL, MONGO_DB


class qunarSpider():
    def __init__(self, city_name):
        self.city_name = city_name

        self.chromeOptions = webdriver.ChromeOptions()
        self.prefs = {"profile.managed_default_content_settings.images": 2}
        self.chromeOptions.add_experimental_option("prefs", self.prefs)
        self.browser = webdriver.Chrome(chrome_options=self.chromeOptions)
        self.browser.set_window_size(1400, 900)
        self.wait = WebDriverWait(self.browser, 10)

        self.client = pymongo.MongoClient(MONGO_URL)
        self.db = self.client[MONGO_DB]

        self.parse_basic_info()
        self.hotels = self.get_hotel_ids()

    def save_to_mongo(self, collection, data):
        if not self.db[collection].find_one({'hotel-id': data['hotel-id']}):
            self.db[collection].insert_one(data)
            print(data)

    def parse_basic_info(self):
        try:
            url = 'http://hotel.qunar.com/city/{}/'.format(self.city_name)
            self.browser.get(url)
            soup = BeautifulSoup(self.browser.page_source, 'lxml')
            _hotels = soup.find_all('div', attrs={'class': 'b_result_bd'})

            for hotel in _hotels:
                _hotel = {}

                if hotel.find_all('a', attrs={'class': 'comment-none'}):
                    continue

                if hotel.find_all('span', attrs={'class': 'no-comment'}):
                    continue

                item = hotel.find_all('span', attrs={'class': 'hotel_item'})[0]

                _hotel['url'] = item.a.get('href').split('?')[0]
                _hotel['name'] = item.a.get('title')
                _hotel['hotel-id'] = self.city_name + '/' + _hotel['url'].split('/')[-2]

                try:
                    _hotel['sleeper_cnt'] = int(hotel.find_all('span', attrs={"class": "num", })[0].get_text())
                except IndexError:
                    _hotel['sleeper_cnt'] = 0

                item_price = hotel.find_all('p', attrs={'class': 'item_price'})
                ref_price = hotel.find_all('p', attrs={'class': 'ref_price'})

                if item_price:
                    _hotel['lowest_price'] = item_price[0].b.get_text()
                elif ref_price:
                    _hotel['lowest_price'] = ref_price[0].a.get('title').split('：')[-1].split('元')[0]
                else:
                    _hotel['lowest_price'] = 0

                self.save_to_mongo('hotels', _hotel)

        except TimeoutException as e:
            return self.parse_basic_info()

    def get_hotel_ids(self):
        # <city_name>/dt-<id> => dt-<id>
        return self.db.hotels.distinct('hotel-id')

    def get_dangci(self, hotel_id):
        url = r'http://hotel.qunar.com/city/{city_name}/{hotel_id}'.format(
            city_name=self.city_name,
            hotel_id=hotel_id.split('/')[-1],
        )
        r = requests.get(url).text
        dangci = re.compile(r'var dangci="\d+"').findall(r)[0].split('\"')[1]
        return dangci

    def get_hotel_quotes(self, hotel_id):
        url = r'http://travel.qunar.com/travelbook/api/getQuoteByHotelSeq?hotelSeq={city_name}_{hotel_id}'.format(
            city_name=self.city_name,
            hotel_id=hotel_id.strip('-')[-1],
        )
        r = requests.get(url)
        quotes = json.loads(r.text)
        return quotes

    def get_hotel_fqas(self, hotel_id):
        url = r'http://review.qunar.com/api/h/faq/{city_name}_{hotel_id}/list?start=0&step=15'.format(
            city_name=self.city_name,
            hotel_id=hotel_id.strip('-')[-1],
        )
        r = requests.get(url)
        fqas = json.loads(r.text)
        return fqas

    def get_hotel_scores(self, hotel_id):
        url = r'http://review.qunar.com/api/h/{city_name}_{hotel_id}/v2/detail'.format(
            city_name=self.city_name,
            hotel_id=hotel_id.strip('-')[-1],
        )
        r = requests.get(url)
        try:
            scores = json.loads(r.text)['data']
        except KeyError:
            scores = {}
            scores['hotelScore'] = 0
            scores['countStat'] = {}
            scores['countStat']['guruCnt'] = 0

        return scores

    def get_comments(self, hotel_id):
        try:
            url = r'http://hotel.qunar.com/city/{city_name}/{hotel_id}/?tag=chengdu#fromDate={from_date}&toDate={to_date}'.format(
                city_name=self.city_name,
                hotel_id=hotel_id.split('/')[-1],
                from_date='2017-05-06',
                to_date='2017-05-07',
            )
            self.browser.get(url)
            commentCnts = self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 '#jd_comments > div > div.b_ugcheader > div.b_ugcfilter > div:nth-child(2) > form > dl.rank')
            ))

            positive_cnt = int(commentCnts.text.split()[2].strip('(').strip(')'))
            neutral_cnt = int(commentCnts.text.split()[4].strip('(').strip(')'))
            negative_cnt = int(commentCnts.text.split()[6].strip('(').strip(')'))
            cmmcnt = positive_cnt + neutral_cnt + negative_cnt

        except TimeoutException:
            return self.get_comments(hotel_id)
        # finally:
        #     self.browser.quit()

        return cmmcnt, positive_cnt, neutral_cnt, negative_cnt

    def parse_dangci(self):
        for hotel_id in self.hotels:
            if hotel_id not in self.db.dangci.distinct('hotel-id'):
                _dangci = {}
                _dangci['hotel-id'] = self.city_name + '/' + hotel_id
                _dangci['dangci'] = self.get_dangci(hotel_id)

                self.save_to_mongo('dangci', _dangci)

    def parse_quotes(self):
        for hotel_id in self.hotels:
            if hotel_id not in self.db.quotes.distinct('hotel-id'):
                _quote = {}
                _quote['hotel-id'] = self.city_name + '/' + hotel_id
                try:
                    _quote['多少家旅行攻略提到'] = self.get_hotel_quotes(hotel_id)['data']['quoteCount']
                except TypeError:
                    _quote['多少家旅行攻略提到'] = 0

                self.save_to_mongo('quotes', _quote)

    def parse_fqas(self):
        for hotel_id in self.hotels:
            if hotel_id not in self.db.fqas.distinct('hotel-id'):
                _fqa = {}
                _fqa['hotel-id'] = self.city_name + '/' + hotel_id
                _fqa['问答数目'] = self.get_hotel_fqas(hotel_id)['count']
                self.save_to_mongo('fqas', _fqa)

    def parse_scores(self):

        for hotel_id in self.hotels:
            if hotel_id not in self.db.scores.distinct('hotel-id'):
                _scores = {}
                _scores['hotel-id'] = self.city_name + '/' + hotel_id
                qunar_scores = self.get_hotel_scores(hotel_id)

                try:
                    _scores['整体评分'] = float(qunar_scores['hotelScore'])
                    _scores['专家点评数目'] = int(qunar_scores['countStat']['guruCnt'])

                except KeyError:
                    _scores['整体评分'] = 0
                    _scores['专家点评数目'] = 0

                for score in qunar_scores['itemList']:
                    _scores[score['name']] = float(score['score'])

                print('scores:', _scores)
                if not self.db.scores.find_one({'hotel-id': _scores['hotel-id']}):
                    self.db.scores.insert_one(_scores)

                self.save_to_mongo('scores', _scores)

    def parse_comments(self):
        for hotel_id in self.hotels:
            if hotel_id not in self.db.comment_cnts.distinct('hotel-id'):
                _cmmCnt = {}
                _cmmCnt['hotel-id'] = hotel_id

                _comments = self.get_comments(hotel_id)
                if not self.get_comments(hotel_id):
                    # continue
                    pass
                else:
                    _cmmCnt['评价总数'] = _comments[0]
                    _cmmCnt['好评数目'] = _comments[1]
                    _cmmCnt['中评数目'] = _comments[2]
                    _cmmCnt['差评数目'] = _comments[3]

                self.save_to_mongo('comment_cnts', _cmmCnt)

    def parse_to_xls(self, ):

        writer = pandas.ExcelWriter('hotels/' + self.city_name + '_hotels.xls')

        uL = self.reduce_collections()
        oL = []

        for item in uL:
            oD = collections.OrderedDict()

            attr_list = [
                ('序号', '序号',),
                ('酒店名', '酒店名'),
                ('酒店链接', '酒店链接'),
                ('星级', '星级'),
                ('最低房价', '最低房价'),
                ('问答数目', '问答数目'),
                ('多少家旅行攻略提到', '多少家旅行攻略提到'),
                ('评价总数', '评价总数'),
                ('好评数目', '好评数目'),
                ('中评数目', '中评数目'),
                ('差评数目', '差评数目'),
                ('设备设施', '设备设施'),
                ('环境卫生', '环境卫生'),
                ('服务质量', '服务质量'),
                ('地理位置', '地理位置'),
                ('餐饮服务', '餐饮服务'),
                ('性价比评分', '性价比'),
                ('整体评分', '整体评分'),
                ('专家点评数目', '专家点评数目'),
                ('多少位试睡员推荐', '多少位试睡员推荐'),
            ]

            print(item)

            for attr in attr_list:
                if attr[0] in item:
                    oD[attr[1]] = item[attr[0]]
                else:
                    oD[attr] = ''

            oL.append(oD)

        df = pandas.DataFrame(oL, index=range(1, len(oL) + 1))
        df.to_excel(writer)

        writer.save()

    def reduce_collections(self):
        COLLECTIONS = ['comment_cnts', 'dangci', 'fqas', 'hotels', 'quotes', 'scores']

        def extract_collection_data(collection):
            cursor = self.db[collection].find({})
            result_list = []
            for data in cursor:
                result_list.append(data)
            return result_list

        data_list = []
        for c in COLLECTIONS:
            data_list.append(extract_collection_data(c))

        merged_list = []

        for i in range(len(data_list[0])):
            K = {**data_list[0][i], **data_list[1][i], **data_list[2][i], **data_list[3][i], **data_list[4][i],
                 **data_list[5][i]}
            merged_list.append(K)

        # pprint.pprint(merged_list)
        print(len(merged_list))

        result_list = []

        for info in merged_list:
            hotel_info = collections.OrderedDict()
            hotel_info['序号'] = info['hotel-id']
            hotel_info['酒店名'] = info['name']
            hotel_info['酒店链接'] = info['url']
            hotel_info['星级'] = info['dangci']
            hotel_info['最低房价'] = info['lowest_price']

            hotel_info['问答数目'] = info['问答数目']
            hotel_info['多少家旅行攻略提到'] = info['多少家旅行攻略提到']
            hotel_info['多少位试睡员推荐'] = info['sleeper_cnt']

            hotel_info['评价总数'] = info['评价总数']
            hotel_info['好评数目'] = info['好评数目']
            hotel_info['中评数目'] = info['中评数目']
            hotel_info['差评数目'] = info['差评数目']

            key_tuple_list = [
                ('设备设施', '设备设施'),
                ('环境卫生', '环境卫生'),
                ('服务质量', '服务质量'),
                ('地理位置', '地理位置'),
                ('餐饮服务', '餐饮服务'),
                ('性价比评分', '性价比'),
                ('整体评分', '整体评分'),
                ('专家点评数目', '专家点评数目'),

            ]

            for t in key_tuple_list:
                if t[1] in info:
                    hotel_info[t[0]] = info[t[1]]
                else:
                    hotel_info[t[0]] = ''

            result_list.append(hotel_info)

        return result_list


if __name__ == '__main__':
    def crawl(city_name):
        s = qunarSpider(city_name)
        s.parse_dangci()
        s.parse_quotes()
        s.parse_fqas()
        s.parse_scores()
        s.parse_comments()
        s.reduce_collections()
        s.parse_to_xls()
        s.browser.quit()


    parser = argparse.ArgumentParser()
    parser.add_argument("city_name", help=u"你要爬的酒店名称")

    args = parser.parse_args()
    crawl(args.city_name)
