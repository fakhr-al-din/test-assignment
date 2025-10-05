import requests
import json
import re
import logging
from pythonjsonlogger.json import JsonFormatter

offer_limit = 20
user_agent = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0"
log_level = logging.DEBUG

logger = logging.getLogger("kaspi_parser")
logger.setLevel(log_level)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = JsonFormatter(
    "{asctime}{msg}",
    style="{",
    rename_fields={"asctime": "time", "msg": "status"},
    datefmt="%Y-%m-%dT%H:%M:%S"
)
ch.setFormatter(formatter)
logger.addHandler(ch)

product_item_pattern = re.compile(r"(?<=BACKEND\.components\.item = ){.+}(?=\s)")
product_category_pattern = re.compile(r'(?<="category": ").+(?=")')

def parse_main_page(product_url):
    headers = {"User-Agent": user_agent}
    main_page = requests.get(product_url, headers=headers)
    if main_page.status_code == 200:
        logger.debug("success", extra={"action": "fetch_main_page", "url": main_page.url})
    else:
        logger.error("failed", extra={"action": "fetch_main_page", "url": main_page.url})
        return None
    try:    
        product_item = json.loads(product_item_pattern.search(main_page.text).group(0))
        product_id = product_item["card"]["id"]
        product_name = product_item["card"]["title"]
        product_category = product_category_pattern.search(main_page.text).group(0)

        product_specifications = product_item["specifications"]
        new_product_specifications = {}
        for sub_specification in product_specifications:
            sub_specification_name = sub_specification["name"]
            new_product_specifications[sub_specification_name] = {}
            for feature in sub_specification["features"]:
                feature_name = feature["name"]
                feature_values = [x["value"] for x in feature["featureValues"]]
                feature_str_values = ", ".join(feature_values)
                new_product_specifications[sub_specification_name][feature_name] = feature_str_values

        del product_specifications

        product_images = product_item["galleryImages"]
        logger.debug("success", extra={"action": "parse_main_page", "url": main_page.url})
        return {
            "id": product_id,
            "name": product_name,
            "category": product_category,
            "specifications": new_product_specifications,
            "images": product_images

        } 
    except AttributeError and KeyError:
        logger.error("failed", extra={"action": "parse_main_page", "url": main_page.url})
        return None
    except Exception as e:
        raise e


def parse_reviews_page(product_id, product_url):
    headers = {
        "User-Agent": user_agent,
        "Referer": product_url
    }

    reviews_page = requests.get(f"https://kaspi.kz/yml/review-view/api/v1/reviews/product/{product_id}?withAgg=true", headers=headers)
    if reviews_page.status_code == 200:
        logger.debug("success", extra={"action": "fetch_reviews_page", "url": reviews_page.url})
    else:
        logger.error("failed", extra={"action": "fetch_reviews_page", "url": reviews_page.url})
        return None

    reviews = json.loads(reviews_page.text)

    try:
        product_rating = reviews["summary"]["global"]
        product_reviews_count = None
        for group_count in reviews["groupSummary"]:
            if group_count["id"] == "COMMENT":
                product_reviews_count = group_count["total"]
        logger.debug("success", extra={"action": "parse_reviews_page", "url": reviews_page.url})
        return {
            "rating": product_rating,
            "reviews_count": product_reviews_count
        }
    except KeyError:
        logger.error("failed", extra={"action": "parse_reviews_page", "url": reviews_page.url})
        return None
    except Exception as e:
        raise e


def parse_offers_page(product_id, product_url):
    headers = {
        "User-Agent": user_agent,
        "Referer": product_url
    }
    post_data = {
        "page": 0,
        "limit": offer_limit,
        "cityId": "710000000"
    }

    offers_page = requests.post(f"https://kaspi.kz/yml/offer-view/offers/{product_id}", headers=headers, json=post_data)
    if offers_page.status_code == 200:
        logger.debug("success", extra={"action": "fetch_offers_page", "url": offers_page.url})
    else:
        logger.error("failed", extra={"action": "fetch_offers_page", "url": offers_page.url})

    # парсим первую страницу, берем данные об количество офферов,
    # считаем сколько страниц
    offers = json.loads(offers_page.text)
    try:
        product_offers_number = offers["offersCount"]
        # делит кол-во офферов на кол-во офферов на странице, округляет в большую сторону и выдает кол-во страниц
        offers_pages_number = (product_offers_number // offer_limit) + (product_offers_number % offer_limit != 0)
        product_offers = offers["offers"]
        logger.debug("success", extra={"action": "parse_offers_page", "url": offers_page.url})
    except KeyError:
        logger.error("failed", extra={"action": "parse_offers_page", "url": offers_page.url})
        return None
    except Exception as e:
        raise e
    
    # парсим остальные страницы
    for i in range(1, offers_pages_number):
        post_data["page"] = i
        offers_page = requests.post(f"https://kaspi.kz/yml/offer-view/offers/{product_id}", headers=headers, json=post_data)
        if offers_page.status_code == 200:
            logger.debug("success", extra={"action": "fetch_offers_page", "url": offers_page.url})
        else:
            logger.error("failed", extra={"action": "fetch_offers_page", "url": offers_page.url})
            return None
        offers = json.loads(offers_page.text)
        try:
            product_offers.extend(offers["offers"])
            logger.debug("success", extra={"action": "parse_offers_page", "url": offers_page.url})
        except KeyError:
            logger.error("failed", extra={"action": "parse_offers_page", "url": offers_page.url})
            continue
        except Exception as e:
            raise e
        

    # выделяем только нужные данные
    new_product_offers = []
    for offer in product_offers:
        new_offer = {}
        new_offer["merchant_id"] = offer["merchantId"]
        new_offer["merchant_name"] = offer["merchantName"]
        new_offer["price"] = offer["price"]
        new_product_offers.append(new_offer)

    # список цен
    prices = [x["price"] for x in new_product_offers]

    product_min_price = min(prices)
    product_max_price = max(prices)

    return {
        "max_price": product_max_price,
        "min_price": product_min_price,
        "offers": new_product_offers
    }
    

def kaspi_parser(product_url):
    main_data = parse_main_page(product_url)
    if main_data is None:
        logger.error("failed", extra={"action": "fetch_product", "url":product_url})
        return None
    
    reviews_data = parse_reviews_page(main_data["id"], product_url)
    if reviews_data is not None:
        main_data.update(reviews_data)

    offers_data = parse_offers_page(main_data["id"], product_url)

    logger.info("success", extra={"action": "fetch_product", "url":product_url})

    if offers_data is not None:
        main_data.update(offers_data)
        offers = main_data.pop("offers")
        with open("export/offers.json", "w") as out:
            json.dump(offers, out, indent=4, ensure_ascii=False)

    with open("export/product.json", "w") as out:
        json.dump(main_data, out, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    with open("seed.json", "r") as f:
        product = json.load(f)
    product_url = product["product_url"]
    kaspi_parser(product_url)