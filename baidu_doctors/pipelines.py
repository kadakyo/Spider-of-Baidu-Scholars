# -*- coding: utf-8 -*-
import re
import sqlite3
import datetime
from baidu_doctors.items import BaiduDoctorsItem, BaiduEssaysItem

# As is arrayed in settings.py:
# 'baidu_doctors.pipelines.DoctorPipeline': 300;
# 'baidu_doctors.pipelines.EssayPipeline': 301.
# Actually the 300 and 301 sequence doesn't matter,
# Because both pipelines will judge the Item first.
# DuplicatesPipeline is powered off due to its immaturity.


class DuplicatesPipeline:
    def __init__(self):
        self.seen = set()

    def process_item(self, item, spider):
        if item.get('id') in self.seen:
            raise DropItem('Duplicated item found: %s' % item.get('id'))
        else:
            self.seen.add(item.get('id'))
            return item


class DoctorPipeline:
    def __init__(self, job):
        self.connect = sqlite3.connect('baiduxueshu.db')
        self.cursor = self.connect.cursor()
        self.job = job

    @classmethod
    def from_crawler(cls, crawler):
        job = getattr(crawler.spider, 'job')
        return cls(job)

    def process_item(self, item, spider):
        if not isinstance(item, BaiduDoctorsItem):
            return item
        today = datetime.datetime.today().date().strftime('%Y%m%d')
        journal = str(item.get('journal')).replace('\'', '"')
        (cited_num, ach_num, H_index, G_index,) = item.get('stats')
        cited_trend = dict(map(lambda x: x.values(), item.get('cited_trend')))
        ach_trend = dict(map(lambda x: x.values(), item.get('ach_trend')))
        data = (
            self.job, today,
            item.get('scholar_id'), item.get('baidu_id'), item.get('scholar_name'),
            item.get('institution'), item.get('discipline'),
            cited_num, ach_num, H_index, G_index, journal,
            cited_trend, ach_trend
        )
        self.cursor.execute('''
            insert into %s_scholars_%s
            (scholar_id, baidu_id, scholar_name,
            institution, discipline, cited_num, ach_num,
            H_index, G_index, journal, cited_trend, ach_trend)
            values('%s', '%s', '%s', '%s', '%s', '%s',
            '%s', '%s', '%s', '%s', '%s', '%s');
            ''' % data
        )
        self.connect.commit()

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()


class EssayPipeline:
    def __init__(self, job):
        self.connect = sqlite3.connect('baiduxueshu.db')
        self.cursor = self.connect.cursor()
        self.job = job

    @classmethod
    def from_crawler(cls, crawler):
        job = getattr(crawler.spider, 'job')
        return cls(job)

    def process_item(self, item, spider):
        if not isinstance(item, BaiduEssaysItem):
            return item

        today = datetime.datetime.today().date().strftime('%Y%m%d')

        # Single quotation mark replacement: Down's Syndrome.
        title = item.get('title', '').strip().replace('\'', '')

        # Some author lists have blank value.
        authors = '; '.join(filter(lambda x: len(x) >= 2, item.get('authors', '')))
        
        # Some institution lists have blank value.
        # Institution data from cqvip needs further washing:
        # Get rid of the item that is too short or full of digits.
        institutions = item.get('institutions', '')
        if isinstance(institutions, list):
            institutions = filter(lambda x: len(x) >= 5 and not re.match(r'.*\d+$', x), institutions)
            institutions = set(map(lambda x: x.strip().replace('\r\n', '').replace('\t', '')\
                    .replace(' ','').replace("'", ""), institutions))
            institutions = '; '.join(institutions)
        elif isinstance(institutions, str):
            if '!' in institutions:
                institutions = institutions.split('!')[0]
            institutions = institutions.replace(';', ' ').strip()
            institutions = '; '.join(institutions.split(' '))

        # There are two types of journals: list and string.
        # Every item in the journal list should be washed.
        journal = item.get('journal', '')
        if isinstance(journal, list):
            journal = list(map(lambda x: x.strip().replace('\r\n', '').replace('\t', '')\
                .replace(' ','').replace("'", ""), journal))
            journal = ' '.join(journal)

        # Pure string.
        abstract = item.get('abstract', '').strip().replace('\r\n', '').replace('\t', '')\
                .replace('\n', '').replace(' ','').replace("'", "")
        DOI = item.get('DOI', '').strip().replace('\r\n', '').replace('\t', '')\
                .replace(' ','').replace("'", "")
        publish_time = item.get('publish_time', '').strip().replace('\r\n', '').replace('\t', '')\
                .replace(' ','').replace("'", "")

        # Keywords are all list.
        keywords = item.get('keywords', '')
        keywords = list(map(lambda x: x.strip().replace('\r\n', '').replace('\t', '')\
                .replace(' ','').replace("'", ""), keywords))
        keywords = '; '.join(keywords)

        data = (
            self.job, today,
            item.get('scholar_id', ''), item.get('baidu_cited_num', ''),
            item.get('source', ''), title, item.get('url', ''),
            authors, institutions, journal, abstract, keywords, DOI, publish_time,
        )
        self.cursor.execute('''
            insert into %s_essays_%s
            (scholar_id, baidu_cited_num, source, title, url, authors, institutions, journal,
            abstract, keywords, DOI, publish_time)
            values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');
            '''
            % data
        )
        self.connect.commit()

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()
