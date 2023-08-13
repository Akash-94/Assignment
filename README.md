
# To build a data pipeline to extract, transform and load financials of the state of Himachal Pradesh 

A brief description of what this project does and who it's for


## Objective
The objective is to build a data pipeline to monitor a few reports on the online platform (https://himkosh.nic.in/eHPOLTIS/PublicReports/wfrmBudgetAllocationbyFD.aspx) on a regular basis. The data from one of the reports must be mined, cleaned, and processed. The processed data collection must subsequently be imported into a database and connected into a data pipeline. Details of the objectives are as follows:


## Steps
Step 1. 
Mine data from Himachal Pradesh Treasury portal for a period of 5 years.
##    
    1. From Date - 01/04/2018
    2. To Date - 31/03/2022
    3. Select Report Data as per - Demand and Wise Summary
    4. Unit - Rupees

Step 2.
Data pre-processing to get clean dataset.
##  
    a) Step 1: Drop all the rows where the columns HOA and DmdCd
    contains Total
    b) Step 2: Fill all the blank cells in column DmdCd with relevant
    values
    c) Step 3: Split the column DmdCd at “-” and create two additional
    columns DemandCode and Demand from the splitted values.
    d) Step 4: Split column HOA at “-” and expand the result to
    additional columns. These should be named as:
        i) MajorHead
        ii) SubMajorHead
        iii) MinorHead
        iv) SubMinorHead
        v) DetailHead
        vi) SubDetailHead
        vii) BudgetHead
        viii) PlanNonPlan
        ix) VotedCharged
        x) StatementofExpenditure 
Step 3.
Create a python script to store the processed data in a SQLite database.

Step 4.
Integrate above steps using a data pipeline. Use either Airflow or
Prefect to automate the data flow.





## Installations

Scrapy

To install Scrapy and its dependendencis on Ubuntu (or Ubuntu-based) systems run: 
```bash
  sudo apt-get install python3 python3-dev python3-pip libxml2-dev libxslt1-dev zlib1g-dev libffi-dev libssl-dev

  pip install scrapy
```

Pandas

pandas can be installed via pip from PyPi, from your terminal run:
```bash
   pip install pandas
```

Sqlite3

To install the SQLite command-line interface on Ubuntu, first update your package list:
```bash
   sudo apt update && sudo apt upgrade
   sudo apt install sqlite3
```

Airflow

Please follow the link provided for detailed installation process.
```bash
Airflow - (https://drive.google.com/file/d/1Uk-CVpzdN721OEKX4zWnbWpch74g7It6/view?usp=sharing)
```
## Documentations

[Scrapy](https://docs.scrapy.org/en/latest/)

[pandas](https://pandas.pydata.org/docs/)

[sqlite](https://www.sqlite.org/docs.html)

[Airflow](https://airflow.apache.org/docs/)






