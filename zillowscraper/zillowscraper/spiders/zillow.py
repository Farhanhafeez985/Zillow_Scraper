import json
import re
from copy import deepcopy
from urllib.parse import unquote
import scrapy
import mysql.connector
from scrapy import Request
import math


class ZillowSpider(scrapy.Spider):
    selected_proxy = 'scraper_api'
    data = []
    name = 'zillow'
    allowed_domains = ['api.scraperapi.com']
    start_urls = ['https://zillow.com/']
    headers = {
        'authority': 'www.zillow.com',
        'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp'
                  ',image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }
    custom_settings = {'ROBOTSTXT_OBEY': False, 'LOG_LEVEL': 'INFO',
                       'CONCURRENT_REQUESTS_PER_DOMAIN': 5,
                       'RETRY_TIMES': 5,
                       'FEED_URI': 'Zillow.json',
                       'FEED_FORMAT': 'json',
                       }

    def start_requests(self):
        zipcodes_and_filters = self.read_zipcode_from_db()
        for zipcode_and_filter in zipcodes_and_filters:
            if zipcode_and_filter['property_filter'] == 'sold':
                zipcode_and_filter['property_filter'] = 'rs'
            elif zipcode_and_filter['property_filter'] == 'frbo':
                zipcode_and_filter['property_filter'] = 'fr'

            filters = {"fore": {"value": False}, "sort": {"value": "days"},
                       "auc": {"value": False}, "nc": {"value": False}, "fr": {"value": False}, "rs": {"value": False},
                       "fsbo": {"value": False},
                       "cmsn": {"value": False}, "fsba": {"value": False}}

            if filters.get(zipcode_and_filter['property_filter']):
                filters[zipcode_and_filter['property_filter']] = {'value': True}
            relative_url = 'https://www.zillow.com/homes/' + zipcode_and_filter[
                "zip_code"] + '/?searchQueryState={"pagination":{},"usersSearchTerm":"' + zipcode_and_filter[
                               "zip_code"] + '","filterState":' + json.dumps(
                filters) + ',"isMapVisible":true,"isListVisible":true,"mapZoom":14}'

            if not self.selected_proxy == 'scraper_api':
                url = f'https://app.scrapingbee.com/api/v1/?api_key=8OJVAT5XAAK9YDP85E9P8K6D7YFUAIB9IR7Y9KUCUCHQEU67ZUOIMZYWCK459HD5W8W3L7SXX1T70QRM&url={relative_url}&country_code=us'
            else:
                url = f'http://api.scraperapi.com/?api_key=e4c15df2593bb5a61585bcfdfcd74ff7&url={relative_url}&keep_headers=true&country_code=us'

            yield Request(url, callback=self.parse_pagination,
                          meta={'original_request': relative_url, 'selected_proxy': self.selected_proxy})

    def parse_pagination(self, response):
        for req in self.parse_listings(response):
            yield req
        total_records = int(response.text.split('"totalResultCount":')[1].split(',')[0])
        if total_records % 40 == 0:
            total_pages = total_records / 40
        else:
            total_pages = math.ceil(total_records / 40)
        for page in range(2, int(total_pages)):
            start = 'http://' + unquote(response.url).split('http://')[1].split('searchQueryState=')[
                0] + 'searchQueryState='
            end = '&keep' + unquote(response.url).split('&keep')[1].split('=us')[0] + '=us'
            payload = json.loads(unquote(response.url).split(start)[1].split(end)[0])
            # payload = json.loads(unquote(response.url).split('=')[-1])
            url_payload = deepcopy(payload)
            url_payload['pagination'] = {"currentPage": page}
            url = start + json.dumps(url_payload) + end
            # url = response.url.split('=')[0] + "=" + json.dumps(url_payload)
            yield Request(url, self.parse_listings, meta=response.meta)

    def parse_listings(self, response):
        listing_json_str = response.xpath("//script[@data-zrr-shared-data-key='mobileSearchPageStore']/text()").get()
        # listing_json = json.loads(listing_json_str.lstrip('<!--').rstrip('-->'))\
        json_str = re.findall(r'<!--(.*?)-->', listing_json_str)
        listing_json = json.loads(json_str[0])
        property_json = listing_json['cat1']['searchResults']['listResults']
        for property_obj in property_json:
            url = f'http://api.scraperapi.com/?api_key=e4c15df2593bb5a61585bcfdfcd74ff7&url={property_obj["detailUrl"]}&keep_headers=true&country_code=us'
            yield Request(url, self.parse_detail)

    # def get_url(self, zpid):
    #     api_url = 'https://www.zillow.com/graphql/'
    #
    #     query = {"operationName": "ForSaleShopperPlatformFullRenderQuery", "variables": {"zpid": str(zpid['zpid']),
    #                                                                                      "contactFormRenderParameter": {
    #                                                                                          "zpid": str(zpid['zpid']),
    #                                                                                          "platform": "desktop",
    #                                                                                          "isDoubleScroll": True}},
    #              "clientVersion": "home-details/6.1.615.master.7846e24", "queryId": "0b8c20a96bba4ffb2f548dc008a5e0f2"}
    #
    #     # query = '{"operationName":"ForSaleShopperPlatformFullRenderQuery","variables":{"zpid":' + str(
    #     #     zpid['zpid']) + ',"contactFormRenderParameter":{"zpid":' + str(
    #     #     zpid['zpid']) + ',"platform":"desktop","isDoubleScroll":true}},' \
    #     #                     '"clientVersion":"home-details/6.1.615.master.7846e24",' \
    #     #                     '"queryId":"0b8c20a96bba4ffb2f548dc008a5e0f2"}'
    #     return api_url, query

    def parse_detail(self, response):
        property_api_json_str = response.xpath("//script[@id='hdpApolloPreloadedData']/text()").get()
        property_api_json = json.loads(property_api_json_str)
        property_json_str = property_api_json['apiCache']
        property_json = json.loads(property_json_str)

        for key, val in property_json.items():
            if not 'ShopperPlatformFullRenderQuery' in key:
                continue
            else:
                property_json = property_json[key]
                property_detail_json = json.loads(json.dumps(property_json))
                detail_json = property_detail_json['property']
                equity = detail_json['resoFacts']
                situs_unit = detail_json['adTargets']
                detail = {
                    "tiger_line_id": "",
                    "county_parcel_id": detail_json['parcelId'],
                    "county_parcel_no_char": '',
                    "beds": detail_json['bedrooms'],
                    "baths": detail_json['bathrooms'],
                    "year_built": detail_json['yearBuilt'],
                    "land_area": detail_json['livingArea'],
                    "building_area": detail_json['resoFacts']['buildingArea'],
                    "equity": equity.get('equity', None),
                    "lead_type": detail_json['NFSHDPBottomSlot']['messages'][0]['decisionContext']['leadType'],
                    "owner_occupied": detail_json['isNonOwnerOccupied'],
                    "property_status": detail_json['homeStatus'],
                    "property_type": detail_json['homeType'],
                    "rental_estimate": "",
                    "total_value": detail_json['price'],
                    "floors": "",
                    "likely_to_sell": "",
                    "owner_county_name": "",
                    "owner_prefix": '',
                    "owner_first_name": "",
                    "owner_middle_name": "",
                    "owner_last_name": "",
                    "owner_suffix": "",
                    "last_sold_price": detail_json['lastSoldPrice'],
                    "last_sold_date": "",
                    "address_info": "",
                    "people_info": "",
                    "oa_info": "",
                    "lead_info": "",
                    'property_info': self.get_property_info(detail_json),
                    "situs_number": '',
                    "situs_street": detail_json['address']['streetAddress'],
                    "situs_unit": situs_unit.get('aamgnrc2', None),
                    "situs_city": detail_json['address']['city'],
                    "situs_state": detail_json['address']['state'],
                    "situs_country": detail_json['country'],
                    "situs_zip": detail_json['address']['zipcode'],
                    "situs_lat": detail_json['latitude'],
                    "situs_lon": detail_json['longitude'],
                    "situs_hash": str(detail_json['address']['streetAddress']).replace(' ', '-') + '-' + str(
                        detail_json['address']['state']).replace(' ', '-') + '-' + str(detail_json['country']).replace(
                        ' ', '-'),
                    # "35758-n-persimmon-trl-san-tan-valley-az",
                    "situs_official_street_address": detail_json['address']['city'] + " " + detail_json['address'][
                        'state'] + " " + detail_json['country'] + " " + detail_json['address']['zipcode'],
                    # 35758 N Persimmon Trl, San Tan Valley, AZ 85140",
                    "vacant": detail_json['resoFacts']['numberOfUnitsVacant'],
                    "preforeclosure": detail_json['listing_sub_type']['is_foreclosure'],
                    "oa_history": '',
                    "tags": ''

                }
                yield detail

    def get_property_info(self, detail_json):
        property_info = {
            "loans": [],
            "schools": self.get_schools(detail_json),
            "price_history": self.get_price_history(detail_json),
            "tax_history": self.get_tax_history(detail_json),
            "url": detail_json['hdpUrl'],
            "appliances": detail_json['resoFacts']['appliances'],
            "flooring": detail_json['resoFacts']['flooring'],
            "roof": detail_json['resoFacts']['roofType'],
            "water_source": detail_json['resoFacts']['waterSource'],
            "pets": detail_json['resoFacts']['hasPetsAllowed'],
            "zpid": detail_json['zpid'],
            "high_school": detail_json['resoFacts']['highSchool'],
            "school_district": detail_json['resoFacts']['highSchoolDistrict'],
            "laundry": "",
            "laundry_features": detail_json['resoFacts']['laundryFeatures'],
            "electric": detail_json['resoFacts']['electric'],
            "interior_features": detail_json['resoFacts']['interiorFeatures'],
            "utilities": detail_json['resoFacts']['utilities'],
            "exterior_material": detail_json['resoFacts']['constructionMaterials'],
            "exterior_features": detail_json['resoFacts']['exteriorFeatures'],
            "pool": detail_json['resoFacts']['hasPrivatePool'],
            "unit_count": "",
            "heating": detail_json['resoFacts']['heating'],
            "cooling": detail_json['resoFacts']['cooling'],
            "parking": detail_json['resoFacts']['parkingCapacity'],
            "parking_features": detail_json['resoFacts']['parkingFeatures'],
            "rooms": detail_json['resoFacts']['bedrooms'],
            "website_id": "",
            "days_on_zillow": detail_json['daysOnZillow'],
            "zestimate": detail_json['zestimate'],
            "structure_type": detail_json['resoFacts']['structureType'],
            "garage": "",
            "garage_spaces": detail_json['resoFacts']['garageParkingCapacity'],
            "garage_type": "",
            "fireplaces": detail_json['resoFacts']['fireplaces'],
            "sewer_water_system": detail_json['resoFacts']['sewer'],
            "kitchen_features": "",
            "additional_room": "",
            "construction_status": detail_json['resoFacts']['isNewConstruction'],
            "addition_equipment": detail_json['resoFacts']['hasAdditionalParcels'],
            "yard_description": "",
            "property_style": detail_json['resoFacts']['architecturalStyle'],
            "property_class": "",
            "images": self.get_images(detail_json),
            "status": "",
            "last_sold_price": detail_json['lastSoldPrice'],
            "last_sold_date": "",
            "lSP_lSD_date": "",
            "rental_estimate": "",
            "price_estimate": "",
            "area_2nd_floor": "",
            "sewer": detail_json['resoFacts']['sewer'],
            "year_renovated": "",
            "construction_quality": detail_json['resoFacts']['isNewConstruction'],
            "construction_materials": detail_json['resoFacts']['constructionMaterials'],
            "home_type": detail_json['homeType'],
            "results": "",
            "mak": "",
            "hoa": "",
            "address_type": '',
            "global_property_id": "",
            "property_id": detail_json['zpid'],
            "state_full_name": "",
            "country": "",
            "occupancy_status": "",
            "hide_state_license": "",
            "interior_access_available": "",
            "is_cash_only": "",
            "is_financible": ""
        }
        return property_info

    def get_schools(self, detail_json):
        school_list = []
        for school_info in detail_json['schools']:
            school = {
                "name": school_info['name'],
                "link": school_info['link'],
                "grades": school_info['grades'],
                "ratings": school_info['rating'],
                "distance_in_miles": school_info['distance'],
                "student_teacher_ratio": school_info['studentsPerTeacher'],
                "education_levels": school_info['level'],
                "size": school_info['size'],
                "school_type": school_info['type']
            }
            school_list.append(school)
        return school_list

    def get_price_history(self, detail_json):
        price_history_list = []
        for price_history in detail_json['priceHistory']:
            data = {
                "sold_date": price_history['date'],
                "event": price_history['event'],
                "price": price_history['price']
            }
            price_history_list.append(data)
        return price_history_list

    def get_tax_history(self, detail_json):
        tax_history_list = []
        for tax_history in detail_json['taxHistory']:
            data = {
                "time": tax_history['time'],
                "tax": tax_history['taxPaid'],
                "assessment": tax_history['value']
            }
            tax_history_list.append(data)
        return tax_history_list

    def get_images(self, detail_json):
        image_list = []
        for image_token in detail_json['responsivePhotos']:
            for image_Jpeg_token in image_token['mixedSources']['jpeg']:
                if image_Jpeg_token['width'] != 384:
                    continue
                else:
                    image_list.append(image_Jpeg_token['url'])
        return image_list

    def read_zipcode(self):
        data = []
        with open('../ZillowScraper/ZillowScraper/zipcode.json') as f:
            zip_code_json = json.load(f)
            for zip_code_token in zip_code_json:
                # data.append((zip_code_token['zip_code'], zip_code_token['property_filter']))
                data.append(zip_code_token)
            return data

    def read_zipcode_from_db(self):
        data = []
        config = {
            'user': 'robpolaris',
            'password': 'Rob!home3030',
            'host': '51.79.83.66',
            'port': '3306',
            'database': 'LeadFuzionDatabase',
            'raise_on_warnings': True, }
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        cursor.execute("SELECT LeadType,Zipcode FROM ScraperTasks")
        db_filters = cursor.fetchall()
        for row in db_filters:
            data_dict = {
                "zip_code": row[1],
                "property_filter": row[0]
            }
            data.append(data_dict)
        cursor.close()
        return data[:1]
