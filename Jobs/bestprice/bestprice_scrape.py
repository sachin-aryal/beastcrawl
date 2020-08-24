import logging
import os

from util.helper import build_url, request_data, bs_parse, get_date_object, random_sleep, get_safely


BEST_PRICE_BASE_URL = "https://www.bestpriceskipbins.com.au/bin-hire/"
logger = logging.getLogger("BestPriceScrape_{}".format(os.getpid()))


class BestPriceScrape:
    def __init__(self, row, result_handler, *args, **kwargs):
        self.job_id = row["job_id"]
        self.job_params_id = row["job_params_id"]
        self.post_code = row["post_code"]
        self.delivery_date = get_date_object(row["delivery_date"])
        self.collection_date = get_date_object(row["collection_date"])
        self.result_handler = result_handler

    def start_scrape(self):
        logger.info("BestPrice scrape started.......")
        row = {}
        final_result = []
        all_keys = set()
        post_code_url = build_url(BEST_PRICE_BASE_URL, 'zip_code', self.post_code, first=True)
        for waste_type_url in self.get_waste_type_url(post_code_url):
            if "Error occurred" in waste_type_url:
                row["job_id"] = self.job_id
                row["data"] = {"error": waste_type_url}
                self.result_handler.add_event(row)
                return
            random_sleep()
            for bin_url in self.get_bin_url(waste_type_url):
                random_sleep()
                result_row = self.get_final_result(bin_url)
                if result_row:
                    final_result.append(result_row)
                    all_keys.update(list(result_row.keys()))
                logger.info(len(final_result))

        row["job_id"] = self.job_id
        row["data"] = {"all_keys": list(all_keys), "rows": final_result}
        self.result_handler.add_event(row)

    def get_waste_type_url(self, post_code_url):
        response = request_data(url=post_code_url)
        error_message = "Error occurred, could not get the waste types. Please verify "\
                        "if following url content have waste types {}".format(post_code_url)
        if response:
            try:
                soup = bs_parse(response.text)
                waste_types = soup.find("div", {"class": "wasteTypes"})
                for a in waste_types.find_all('a', href=True):
                    logger.info(f"BestPriceScrape;waste_type_url={a['href']}")
                    yield a['href']
            except AttributeError as ae:
                logger.error(f"BestPriceScrape;get_waste_type_url;error={str(ae)}")
                yield error_message
            except Exception as ex:
                logger.error(f"BestPriceScrape;get_waste_type_url;error={str(ex)}")
                yield error_message

    def get_bin_url(self, waste_type_url):
        response = request_data(url=waste_type_url)
        if response:
            try:
                soup = bs_parse(response.content)
                cubic_meters = soup.find("div", {"id": "select-cubic-meter"})
                for a in cubic_meters.find_all('a', href=True):
                    logger.info("BestPriceScrape;bin_url={}".format(a['href']))
                    yield a['href']
            except AttributeError as ae:
                logger.error(f"BestPriceScrape;get_bin_url;error={str(ae)}")
            except Exception as ex:
                logger.error(f"BestPriceScrape;get_bin_url;error={str(ex)}")

    def get_final_result(self, bin_url):
        result_row = {}
        delivery_date_url = build_url(bin_url, 'delivery_date', self.delivery_date)
        collection_date_url = build_url(delivery_date_url, 'collection_date', self.collection_date)
        final_url = "{}&submit".format(collection_date_url)
        response = request_data(url=final_url)
        if response:
            try:
                soup = bs_parse(response.content)
                definitions = soup.find("div", {"class": "definitions"})
                rows = definitions.find_all("div", {"class": "row"})
                result_row["delivery_date"] = get_safely(rows, 0, 'rows[index].find_all("div", {"class": "cell"})[-1]'
                                                                  '.text.strip()')
                result_row["collection_date"] = get_safely(rows, 1, 'rows[index].find_all("div", {"class": "cell"})[-1]'
                                                                    '.text.strip()')
                result_row["bin_size"] = get_safely(rows, 2, 'rows[index].find_all("div", {"class": "cell"})[-1]'
                                                             '.text.strip()')
                waste_type = get_safely(rows, 3, 'rows[index].find("h3", {"class": "box-title"}).text.strip()')
                result_row["waste_type"] = waste_type
                items_not_permitted = []
                for p in definitions.find("div", {"class": "waste-type-row"}).find_all("p", {"class": "p-no"}):
                    items_not_permitted.append(p.text.strip())
                result_row["items_not_permitted"] = items_not_permitted
                result_row["bin_total"] = get_safely(rows, 4, 'rows[index].find_all("div", {"class": "cell"})[-1]'
                                                              '.text.strip()')
                row = 5
                if waste_type in ["General Waste", "Green Waste"]:
                    result_row["weight_limit"] = get_safely(rows, 5, 'rows[index].find_all("div", {"class": "cell"})'
                                                                     '[-1].text.strip()')
                    result_row["weight_limit_charge"] = get_safely(rows, 6, 'rows[index].find_all("div", {"class": '
                                                                            '"cell"})[-1].text.strip()')
                    row = 7
                result_row["delivery_zone"] = get_safely(rows, row, 'rows[index].find_all("div", {"class": "cell"})'
                                                                    '[-1].text.strip()')
            except AttributeError as ae:
                logger.error(f"BestPriceScrape;get_final_result;error={str(ae)}")
            except Exception as ex:
                logger.error(f"BestPriceScrape;get_final_result;error={str(ex)}")
        return result_row


