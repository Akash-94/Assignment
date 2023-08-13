# -*- coding: utf-8 -*-
import scrapy
from pathlib import Path
from scrapy import FormRequest
import csv


class BudgetSpider(scrapy.Spider):
    name = 'budget'
    start_urls = [
        'http://https://himkosh.nic.in/eHPOLTIS/PublicReports/wfrmBudgetAllocationbyFD.aspx/']

    def start_requests(self):
        urls = "https://himkosh.nic.in/eHPOLTIS/PublicReports/wfrmBudgetAllocationbyFD.aspx"
        for url in urls:
            yield scrapy.Request(url=urls, callback=self.parse)

    def parse(self, response):
        post_data = {
            'ctl00$MainContent$txtFromDate':  '01/04/2018',
            'ctl00$MainContent$txtQueryDate': '31/03/2022',
            'ctl00$MainContent$hdnMinDate':  '01/04/2016',
            'ctl00$MainContent$hdnMaxDate':  '07/08/2023',
            'ctl00$MainContent$ddlQuery':  'DmdCd,HOA',
            'ctl00$MainContent$txtDemand':  '',
            'ctl00$MainContent$txtHOD':  '',
            'ctl00$MainContent$txtMajor':  '',
            'ctl00$MainContent$txtSubmajor':  '',
            'ctl00$MainContent$txtMinor':  '',
            'ctl00$MainContent$rbtUnit':  '0',
            'ctl00$MainContent$btnGetdata':  'View Data'
        }
        yield FormRequest.from_response(response, url=response.url, formdata=post_data, callback=self.parse_results)

    def parse_results(self, response):
        rows = response.xpath('//*[@id="hodAllocation"]/tr')
        for row in rows[1:]:  # Skip the header row
            data = row.xpath('.//text()').getall()
            yield {
                'result_data': data
            }

        with open('himkosh.csv', 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([data])
