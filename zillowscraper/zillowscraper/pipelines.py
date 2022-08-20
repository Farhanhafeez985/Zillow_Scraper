# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymssql
import mysql.connector
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ZillowscraperPipeline:
    # def __init__(self):
    #     config = {
    #         'user': 'robpolaris',
    #         'password': 'Rob!home3030',
    #         'host': '51.79.83.66',
    #         'port': '3306',
    #         'database': 'LeadFuzionDatabase',
    #         'raise_on_warnings': True, }
    #     connection = mysql.connector.connect(**config)
    #     cursor = connection.cursor()
    #     cursor.execute("SELECT * FROM ScraperTasks")
    #     db_filters = cursor.fetchall()
    #     cursor.close()

    # return item
    def process_item(self, item, spider):
        return item
