import scrapy
import csv
from scrapy import FormRequest


class BudgetSpider(scrapy.Spider):
    name = 'budget'
    allowed_domains = ['himkosh.nic.in']
    start_urls = [
        "https://himkosh.nic.in/eHPOLTIS/PublicReports/wfrmBudgetAllocationbyFD.aspx"]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

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
        for row in response.xpath('//*[@id="hodAllocation"]/table/tbody/tr'):
            data = row.xpath('td/text()').getall()
            yield {'result': data}

            with open('budget_data.csv', 'a', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(data)
        # for row in response.css("div#hodAllocation tbody tr"):
        #     data = row .css("td.success").getall()
        # yield {
        # 'result_data': data
        # 'DmdCd': data[0],
        # 'HOA': data[1],
        # 'Sanction Budget (April)': data[2],
        # 'Addition': data[3],
        # 'Saving': data[4],
        # 'Revised Budget (A)': data[5],
        # 'Expenditure (within selected period) (B)': data[6],
        # 'Balance (A-B)': data[7]
        # }

        # page = response.url.split("/")[-1]
        # filename = f"budget-{page}.html"
        # Path(filename).write_bytes(response.body)
        # self.log(f"Saved file {filename}")
