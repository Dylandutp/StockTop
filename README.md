# StockTop

## Keywords
**Category:** Web Crawler, Financial Data, Stock Analysis  
**Core Tech:** Python, Web Scraping, Database  
**Functions:** Top Shareholders Tracking, Auto Resume Crawling

## Overview
**StockTop** is a simple Python project designed to **crawl the top 10 shareholders of companies** from  
[IQValue](https://www.iqvalue.com/Frontend/stock/) and **store the data in a database**.  

The program includes a **resume feature**:  
- If the crawling process does not finish in one run, the remaining unprocessed companies are saved to `unfinished_list.txt`.  
- On the next run, the program will automatically continue from where it left off.

## How to Use

### **Run the Program**
- Run Python Script:
  ```bash
  python app.py

### **Resume Crawling**
- To continue crawling the unfinished companies, simply rerun the script:
  ```bash
  python app.py
- The program will:
  -  Detect the existing `unfinished_list.txt`.
  -  Automatically continue from the last incomplete position until all data has been successfully processed.
