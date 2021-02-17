# -*- coding: UTF-8 -*-
import re
import time
import json
import scrapy
import urllib
import numpy as np
import pandas as pd
from baidu_doctors.items import BaiduDoctorsItem, BaiduEssaysItem


class MainSpider(scrapy.Spider):
    name = 'baiduxueshu'

    def start_requests(self):
        # ts, author and affiliate should be critical information to find our doctors.
        # id is a customed field for meta function.
        url = (
            'http://xueshu.baidu.com/usercenter/data/authorchannel'
            '?cmd=search_author&_ts=%d'
            '&&author=%s&affiliate=%s&curPageNum=1'
            '&id=%s'
        )  

        # Read, fill and request.
        # Core function that replaces "start_urls".
        try:
            frame = pd.read_excel('input_excel_files/%s' % self.file, sheet_name=0)
            columns = ['hcp_id', 'hcp_name', 'hco_name']
            for doctor in frame.values:
                strings = (int(time.time()), doctor[1], doctor[2], doctor[0])
                yield scrapy.Request(url % strings, self.parse)
        except:
            print('Problems are encountered while loading the file.')
            print('Please check your excel file "%s".' % self.file)
    
    def parse(self, response):
        '''
        Another layer of certification of searched doctors (March, 2020).
        When a doctor is seached, several results might be returned.
        Among which we need to figure out the "real" doctor that is needed.
        A comparison of insitutions is applied and the most possible one is obtained.
        '''
        url_params = urllib.parse.parse_qs(urllib.parse.urlparse(response.url).query)
        scholar = url_params.get('author')[0]
        scholar_id = url_params.get('id')[0]
        institution = url_params.get('affiliate')[0]  # Three pieces of information from url.
        meta = {'scholar_id': scholar_id}
        response_names = response.xpath('//a[contains(@class, "personName")]/text()').getall()
        if len(response_names) == 0:
            print('The doctor named "%s" could not be found (Rule 1).' % scholar)
            return
        response_institutions = response.xpath('//p[contains(@class, "personInstitution")]/text()').getall()
        doctor_pages = response.xpath('//a[contains(@class,"searchResult_pic")]/@href').getall()
        response_names = map(lambda x: json.loads('"%s"' % x), response_names)
        response_institutions = map(lambda x: json.loads('"%s"' % x), response_institutions)
        
        # Wash, filter and calculate.
        # The use of generator could save memory.
        results = filter(lambda x: x[0] == scholar, zip(response_names, response_institutions))
        results = map(lambda x: x[1], results)
        results = list(map(lambda x: len(set(x) & set(institution)), results))
        if max(map(lambda x: x / len(institution), results)) < 0.6:
            print('The doctor named "%s" could not be found (Rule 2).' % scholar)
            return  # 60% similarity threshold is based on former experience.
        doctor_page = doctor_pages[results.index(max(results))]
        doctor_page = doctor_page.replace('\\', '').replace('"', '')

        yield response.follow(
            url=doctor_page,
            meta=meta,
            callback=self.parse_scholar
        )
        
        ##################################################
        # Since author id could be encrypted,
        # A new way of obtaining all the essays is discovered.
        # Three parameters will be input to search the essays that related to the doctor:
        # doctor name, institution name and page number.
        # For the detailed information, check the function "parse_scholar".
        essay_search_page_url = 'http://xueshu.baidu.com/s?wd=author:(%s)&tag_filter=%s&tn=SE_baiduxueshu_c1gjeupa&pn=0'
        essay_search_page_url = essay_search_page_url % (scholar, institution)
        yield scrapy.Request(
            url=essay_search_page_url,
            meta=meta,
            callback=self.parse_essay_list
        )
        ##################################################

    def parse_scholar(self, response):
        # With a slight possibility, webpage is redirected to login page.
        # Since the lost data doesn't influence much, it is tolerated.
        if 'passport' in response.url:
            return
        item = BaiduDoctorsItem()
        item['scholar_id'] = response.meta['scholar_id']
        item['baidu_id'] = response.css('span.p_scholarID_id::text').get('')
        item['scholar_name'] = response.css('div.p_name::text').get('')
        item['institution'] = response.css('div.p_affiliate::text').get('')
        item['discipline'] = response.css('span.person_domain a::text').get('')
        item['stats'] = response.css('p.p_ach_num::text').getall()
        journal_key = response.css('div.pieBox p::text').getall()
        journal_value = response.css('div.pieBox p span.boxnum::text').getall()
        item['journal'] = dict(zip(journal_key, journal_value))
        script_chart = response.css('div#main_content_left script::text').get('')

        # Parse the string inside the <script> tag.
        for line in script_chart.splitlines():
            try:
                exec(line.strip())
            except (SyntaxError, NameError):
                continue
        item['cited_trend'] = locals().get('lineMapCitedData', '')
        item['ach_trend'] = locals().get('lineMapAchData', '')
        yield item  # Go to DoctorPipeline.

        ##################################################
        # # Parse the essays that belong to this scholar.
        # essay_pages = response.css('h3.res_t a::attr(href)').getall()
        # for essay_page in essay_pages:
        #     essay_page = 'http:' + essay_page
        #     yield scrapy.Request(essay_page, self.parse_baidu_essay)
        #
        # # Look for the "next page" button to grab other essays.
        # # Unknown fields: _token, _sign, entity_id, bsToken.
        # # They might lead to troubles in the future.
        # # Other fields like cookies are not included in this scenario.
        # # The entity_id indicated an encrypted name, which cannot be cracked now.
        # page_number = response.css('span.res-page-number::text').getall()[-1]
        # post_url = 'http://xueshu.baidu.com/usercenter/data/author'
        # post_data = {
        #     '_token': 'b8451e479c89d8fac50a9594387ac9086e7746b73619013b08a21e9913518bbc',
        #     '_ts': str(int(time.time())),
        #     '_sign': '780cf9fde7c413a1af06f2cb171d5832',
        #     'cmd': 'academic_paper',
        #     'entity_id': 'c7bf2452ea155f75d924e31bd5062501',
        #     'bsToken': 'the fisrt two args should be string type:0,1!',
        #     'sc_sort': 'sc_time',
        #     'curPageNum': '1'
        # }
        # for curPageNum in range(int(page_number)):
        #     post_data['curPageNum'] = str(curPageNum + 1)
        #     yield scrapy.FormRequest(
        #         url=post_url,
        #         formdata=post_data,
        #         callback=self.parse_essay_list
        #     )
        ##################################################

    def parse_essay_list(self, response):
        essay_url_list = response.css('h3.c_font a::attr(href)').getall()
        for essay_url in essay_url_list:
            if not essay_url.startswith('http:'):
                essay_url = 'http:' + essay_url
            yield scrapy.Request(
                url=essay_url,
                meta=response.meta,
                callback=self.parse_baidu_essay
            )

        # Judge if the next page arrow exists.
        # Since the url is splitted and joint manually,
        # Parameters are stabled, thus using a less Pythonic way.
        if response.css('i.c-icon-pager-next').get():
            temp_list = response.url.split('=')
            temp_list[-1] = str(int(response.url.split('=')[-1]) + len(essay_url_list))
            next_page_url = '='.join(temp_list)
            yield scrapy.Request(url=next_page_url,
                meta=response.meta,
                callback=self.parse_essay_list)

    def parse_baidu_essay(self, response):
        scholar_id = response.meta['scholar_id']
        essay_url = response.css('div.main-info h3 a::attr(href)').get()
        baidu_cited_num = response.css('div.ref_wr p.ref-wr-num a::text').get('0').strip()
        meta = {
            'scholar_id': scholar_id,
            'baidu_cited_num': baidu_cited_num,
        } 

        # Parse the different webpages according to its domain.
        # From experiments I found that the essay_url could be changed
        # due to different requests.
        try:
            if 'sciencedirect' in essay_url:
                yield scrapy.Request(essay_url, self.parse_sciencedirect, meta=meta)
            elif 'ncbi.nlm.nih.gov/pmc/articles/PMC' in essay_url:
                yield scrapy.Request(essay_url, self.parse_ncbi_full, meta=meta)
            elif 'ncbi.nlm.nih.gov/pubmed' in essay_url:
                yield scrapy.Request(essay_url, self.parse_ncbi_abstract, meta=meta)
            elif 'springer' in essay_url:
                essay_url = essay_url.replace('http://', 'https://')
                yield scrapy.Request(essay_url, self.parse_springer, meta=meta)
            elif 'en.cnki' in essay_url:
                yield scrapy.Request(essay_url, self.parse_cnki_en, meta=meta)
            # ##################################################
            # English webistes
            # Chinese webistes
            # ##################################################
            elif essay_url.startswith('http://kns.cnki.net/'):
                yield scrapy.Request(essay_url, self.parse_cnki_kns, meta=meta)
            elif essay_url.startswith('http://www.cnki.com.cn/')\
                or essay_url.startswith('http://cpfd.cnki.com.cn/'):
                yield scrapy.Request(essay_url, self.parse_cnki_ch, meta=meta)
            elif essay_url.startswith('http://d.old.wanfangdata.com.cn/'):
                yield scrapy.Request(essay_url, self.parse_wanfang_old, meta=meta)
            elif essay_url.startswith('http://med.wanfangdata.com.cn/'):
                yield scrapy.Request(essay_url, self.parse_wanfang_med, meta=meta)
            elif essay_url.startswith('http://www.wanfangdata.com.cn/'):
                yield scrapy.Request(essay_url, self.parse_wanfang, meta=meta)
            elif essay_url.startswith('http://d.wanfangdata.com.cn/'):
                temp_url = 'http://d.wanfangdata.com.cn/Detail/Periodical/'
                formdata = {'Id': essay_url.split('/')[-1]}
                yield scrapy.FormRequest(temp_url, self.parse_wanfang_d, formdata=formdata, meta=meta)
            elif essay_url.startswith('http://www.cqvip.com/'):
                yield scrapy.Request(essay_url, self.parse_cqvip, meta=meta)
        except TypeError:
            print('This url could not be recognized:')
            print('%s' % essay_url)
            return

    # def parse_sciencedirect(self, response):
    #     item = BaiduEssaysItem()
    #     item['source'] = 'sciencedirect'
    #     item['title'] = ''.join(response.css('span.title-text *::text').getall())
    #     given_name = response.css('div.author-group a.author span.given-name::text').getall()
    #     surname = response.css('div.author-group a.author span.surname::text').getall()
    #     authors = list(map(lambda x: ' '.join(x), zip(given_name, surname)))
    #     item['authors'] = ', '.join(authors)
    #     item['abstract'] = ''.join(response.css('div.author h2.section-title ~div *::text').getall())
    #     item['keywords'] = '; '.join(response.css('div.keywords-section div *::text').getall())
    #     item['DOI'] = ''
    #     yield item

    # def parse_ncbi_full(self, response):
    #     item = BaiduEssaysItem()
    #     item['source'] = 'ncbi'
    #     item['title'] = response.css('h1.content-title::text').get()
    #     # if not title:
    #     #     full_text_url = response.xpath('//div[@class="icons portlet"]//img[@alt="Icon for PubMed Central"]/parent::*/@href').get()
    #     #     yield scrapy.Request(full_text_url, self.parse_ncbi)
    #     authors = response.css('h1.content-title ~div.half_rhythm a.affpopup::text').getall()
    #     item['authors'] = ', '.join(authors)
    #     item['abstract'] = response.xpath('//h2[contains(text(), "Abstract")]/following-sibling::*/p/text()').get()
    #     item['keywords'] = ''
    #     item['DOI'] = response.css('span.doi a::text').get()
    #     yield item

    # def parse_ncbi_abstract(self, response):
    #     item = BaiduEssaysItem()
    #     item['source'] = 'ncbi'
    #     item['title'] = response.css('div.rprt h1::text').get()
    #     authors = response.css('div.auths a::text').getall()
    #     item['authors'] = ', '.join(authors)
    #     abstract = response.css('div.abstr p::text').get()
    #     if not abstract:
    #         abstract = ''
    #     item['abstract'] = abstract
    #     item['keywords'] = ''
    #     item['DOI'] = response.xpath('//dl[@class="rprtid"]/dt[contains(text(), "DOI:")]/following-sibling::dd/a/text()').get()
    #     yield item

    # def parse_springer(self, response):
    #     item = BaiduEssaysItem()
    #     item['source'] = 'springer'
    #     item['title'] = response.css('h1.c-article-title::text').get()
    #     authors = response.css('li.c-author-list__item span a[data-test="author-name"]::text').getall()
    #     item['authors'] = ', '.join(authors)
    #     item['abstract'] = response.xpath('//h2[contains(text(), "Abstract")]/following-sibling::div/p/text()').get()
    #     keywords = response.xpath('//h3[contains(text(), "Keywords")]/following-sibling::*//span[@itemprop="about"]/text()').getall()
    #     keywords = list(map(lambda x: x.strip(), keywords))
    #     while True:
    #         try:
    #             keywords.remove('')
    #         except ValueError:
    #             break
    #     item['keywords'] = '; '.join(keywords)
    #     item['DOI'] = response.xpath('//abbr[contains(text(), "DOI")]/following-sibling::*/a/text()').get()
    #     yield item

    # def parse_cnki_en(self, response):
    #     item = BaiduEssaysItem()
    #     item['source'] = 'cnki'
    #     item['title'] = response.css('div#content div h2::text').get()
    #     authors = response.css('div#content strong::text').get().split(';')
    #     institutions = ['Laboratory', 'Science', 'University', 'Education', 'College']
    #     for author in authors:
    #         if len(np.intersect1d(author.split(' '), institutions)) >= 1:
    #             index = authors.index(author)
    #             break
    #     try: 
    #         authors = authors[:index]
    #     except UnboundLocalError:
    #         authors
    #     item['authors'] = ', '.join(authors)
    #     item['abstract'] = response.xpath('//div[@id="content"]//strong/parent::div/following-sibling::div/text()').get()
    #     item['keywords'] = ''
    #     item['DOI'] = ''
    #     yield item

    # ##################################################
    # English webistes
    # Chinese webistes
    # ##################################################

    def parse_cnki_kns(self, response):
        item = BaiduEssaysItem()
        item['scholar_id'] = response.meta['scholar_id']
        item['baidu_cited_num'] = response.meta['baidu_cited_num']      
        item['source'] = 'cnki'
        item['url'] = response.url
        item['title'] = response.css('h2.title::text').get('')
        item['authors'] = response.css('div.author span a::text').getall()
        item['institutions'] = response.css('div.orgn span a::text').getall()
        item['abstract'] = response.css('span#ChDivSummary::text').get('')
        item['keywords'] = response.xpath(('//label[@id="catalog_KEYWORD"]'
            '/following-sibling::a/text()')).getall()
        item['journal'] = response.css('div.sourinfo p a::text').getall()
        yield item

    def parse_cnki_ch(self, response):
        item = BaiduEssaysItem()
        item['scholar_id'] = response.meta['scholar_id']
        item['baidu_cited_num'] = response.meta['baidu_cited_num']
        item['source'] = 'cnki'
        item['url'] = response.url
        item['title'] = response.css('h1.xx_title::text').get('')
        item['authors'] = response.xpath(('//h1[@class="xx_title"]/parent::div'
            '/following-sibling::div[@style="text-align:center; width:740px; height:30px;"]'
            '/a/text()')).getall()
        item['abstract'] = response.xpath(('//strong[contains(text(), "摘要")]'
            '/parent::font/following-sibling::text()')).get('')
        item['institutions'] = response.xpath(('//strong[contains(text(), "作者单位")]'
            '/parent::font/following-sibling::a[1]/text()')).get('')
        item['journal'] = response.xpath('//div/div/div[@style="float:left;"]/a//text()').getall()
        yield item

    def parse_wanfang_old(self, response):
        item = BaiduEssaysItem()
        item['scholar_id'] = response.meta['scholar_id']
        item['baidu_cited_num'] = response.meta['baidu_cited_num']    
        item['source'] = 'wanfang'
        item['url'] = response.url
        item['title'] = response.css('div.section-baseinfo h1::text').get('')
        item['authors'] = response.xpath(('//span[contains(text(), "作者：")]'
            '/following-sibling::span/a/text()')).getall()
        item['institutions'] = response.xpath(('//span[contains(text(), "作者单位：")]'
            '/following-sibling::span/span/text()')).getall()
        item['journal'] = response.xpath('//span[contains(text(), "母体文献：")]/following-sibling::span/text()').getall()
        if not item['journal']:
            keys = ['刊  名', 'Journal', '年，卷(期)']
            xpath_str = '//span[contains(text(), "%s")]/following-sibling::span/a/text()'
            item['journal'] = list(map(lambda x: response.xpath(xpath_str % x).get(''), keys))
        item['abstract'] = response.css('div.zh div.text::text').get('')
        item['keywords'] = response.xpath('//span[contains(text(), "关键词")]/following-sibling::span/a/text()').getall()
        item['publish_time'] = response.xpath('//span[contains(text(), "出版日期")]/following-sibling::span/text()').get('')
        yield item

    def parse_wanfang_med(self, response):
        item = BaiduEssaysItem()
        blocked_url = [
            'http://med.wanfangdata.com.cn/',
            'http://med.wanfangdata.com.cn/Error?aspxerrorpath=/searchcenter/ALLsearchInfomation.aspx',
        ]
        if response.url not in blocked_url:
            item['scholar_id'] = response.meta['scholar_id']
            item['baidu_cited_num'] = response.meta['baidu_cited_num']          
            item['source'] = 'wanfang'
            item['url'] = response.url
            item['title'] = response.css('div.headline h2::text').get('')
            item['authors'] = response.xpath(('//span[contains(text(), "作者：")]'
                '/following-sibling::span/span/a/text()')).getall()
            item['institutions'] = response.xpath(('//span[contains(text(), "作者单位：")]'
                '/following-sibling::span/span/a/text()')).getall()
            item['journal'] = response.xpath(('//span[contains(text(), "期刊：")]'
                '/following-sibling::span/a/text()')).getall()
            item['abstract'] = response.css('div.abstracts p::text').get('')
            item['keywords'] = response.xpath(('//span[contains(text(), "关键词：")]'
                '/following-sibling::span/a/text()')).getall()
            item['DOI'] = response.xpath(('//span[contains(text(), "DOI")]'
                '/following-sibling::span/em/text()')).get('')
            item['publish_time'] = response.xpath(('//span[contains(text(), "发布时间：")]'
                '/following-sibling::span/em/text()')).get('')
            yield item

    def parse_wanfang(self, response):
        # Name card logic in html code:
        # 为保障数据安全，避免我们已处理好的学者与机构的对应关系被别人抓取，
        # 故，在文献详情页的题录信息处及相关作者处，鼠标滑过作者姓名，不再显示学者名片。
        item = BaiduEssaysItem()
        if not response.xpath('//img[@src="/page/images/error/404.png"]').get():  # Essay not found.
            item['scholar_id'] = response.meta['scholar_id']
            item['baidu_cited_num'] = response.meta['baidu_cited_num']  
            item['source'] = 'wanfang'
            item['url'] = response.url
            item['title'] = response.css('div.left_con_top div.title::text').get('')
            item['authors'] = response.xpath(('//div[contains(text(), "作者：")]'
                '/following-sibling::div/a/text()')).getall()
            item['abstract'] = response.css('div.abstract div::text').get('')
            item['keywords'] = response.xpath(('//div[contains(text(), "关键词：")]'
                '/following-sibling::div/a/text()')).getall()
            item['institutions'] = response.xpath(('//div[contains(text(), "作者单位：")]'
                '/following-sibling::div/a/text()')).getall()
            keys = ['刊名', 'Journal', '年，卷(期)']
            xpath_str = '//div[contains(text(), "%s")]/following-sibling::div/a/text()'
            item['journal'] = list(map(lambda x: response.xpath(xpath_str % x).get(''), keys))
            item['DOI'] = response.xpath(('//div[contains(text(), "doi")]'
                '/following-sibling::div/a/text()')).get('')
            item['publish_time'] = response.xpath(('//div[contains(text(), "在线出版日期")]'
                '/following-sibling::div/text()')).get('')
            yield item

    def parse_wanfang_d(self, response):
        item = BaiduEssaysItem()
        item['scholar_id'] = response.meta['scholar_id']
        item['baidu_cited_num'] = response.meta['baidu_cited_num']  
        item['source'] = 'wanfang'
        item['url'] = response.url
        item['title'] = response.text
        yield item

    def parse_cqvip(self, response):
        item = BaiduEssaysItem()
        if not ((response.text.startswith('<script>alert(')) or ('/none.aspx?' in response.url)):  # If blocked.
            item['scholar_id'] = response.meta['scholar_id']
            item['baidu_cited_num'] = response.meta['baidu_cited_num']  
            item['source'] = 'cqvip'
            item['url'] = response.url
            item['title'] = response.css('span.detailtitle h1::text').get('')

            ##################################################
            # Since the journal, authors and institutions are all texts,
            # A less Pythonic way is used to recognize the fields.
            subtitles = response.xpath('//span/strong/i//text()').getall()
            item['journal'] = subtitles[subtitles.index(' | ') - 1]
            item['authors'] = subtitles[subtitles.index(' | ') + 1: subtitles.index(' \xa0\xa0')]
            item['institutions'] = subtitles[subtitles.index(' \xa0\xa0') + 1:]
            ##################################################

            item['abstract'] = response.xpath('//b[contains(text(), "要：")]/following-sibling::text()').get('')
            item['keywords'] = response.xpath('//b[contains(text(), "关键词")]/parent::td/following-sibling::td/a/text()').getall()
            yield item
