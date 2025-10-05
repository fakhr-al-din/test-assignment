import requests
import json
import re
import logging
from pythonjsonlogger.json import JsonFormatter
from app.db.config import engine
from app.db.models import Product, Offer
from sqlalchemy.orm import Session
from sqlalchemy import select
from celery import Celery
from celery.signals import worker_ready

app = Celery('tasks', broker='pyamqp://guest@kaspi-parser-broker//')

offer_limit = 20
user_agent = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0"

logger = logging.getLogger("kaspi_parser")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = logging.FileHandler("app/logs/log.json")
fh.setLevel(logging.DEBUG)

formatter = JsonFormatter(
    "{asctime}{msg}",
    style="{",
    rename_fields={"asctime": "time", "msg": "status"},
    datefmt="%Y-%m-%dT%H:%M:%S"
)
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

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
        with open("app/export/offers.json", "w") as out:
            json.dump(offers, out, indent=4, ensure_ascii=False)
    with open("app/export/product.json", "w") as out:
        json.dump(main_data, out, indent=4, ensure_ascii=False)

    with Session(engine) as session:
        select(Product).where(Product.id == main_data["id"])
        existing_product = session.query(Product).where(Product.id == main_data["id"]).one_or_none()
        if existing_product is None:
            db_product = Product(
                id=main_data["id"],
                name=main_data["name"],
                category=main_data["category"],
                min_price=main_data["min_price"],
                max_price=main_data["max_price"],
                rating=main_data["rating"],
                reviews_count=main_data["reviews_count"]
            )
            session.add(db_product)
            session.commit()
        else:
            if existing_product.name != main_data["name"]:
                existing_product.name = main_data["name"]
            if existing_product.category != main_data["category"]:
                existing_product.category = main_data["category"]
            if existing_product.min_price != main_data["min_price"]:
                existing_product.min_price = main_data["min_price"]
            if existing_product.max_price != main_data["max_price"]:
                existing_product.max_price = main_data["max_price"]
            if existing_product.rating != main_data["rating"]:
                existing_product.rating = main_data["rating"]
            if existing_product.reviews_count != main_data["reviews_count"]:
                existing_product.reviews_count = main_data["reviews_count"]
            session.commit()

    with Session(engine) as session:
        offers_to_add = []
        for offer in offers:
            existing_offer = session.query(Offer).where((Offer.product_id == main_data["id"]) & (Offer.seller_id == offer["merchant_id"])).one_or_none()
            if existing_offer is None:
                offers_to_add.append(
                    Offer(
                        product_id=main_data["id"],
                        seller_name=offer["merchant_name"],
                        seller_id=offer["merchant_id"],
                        price=offer["price"]
                    )
                )
            else:
                if existing_offer.seller_name != offer["merchant_name"]:
                    existing_offer.seller_name = offer["merchant_name"]
                if existing_offer.price != offer["price"]:
                    existing_offer.price = offer["price"]
        session.add_all(offers_to_add)
        session.commit()

@app.task
def parse_task():
    with open("app/seed.json", "r") as f:
        product = json.load(f)
    product_url = product["product_url"]
    kaspi_parser(product_url)

@app.on_after_configure.connect
def setup_periodic_tasks(sender: Celery, **kwargs):
    sender.add_periodic_task(900.0, parse_task, name="product parser")

# запустить задание сразу после готовности, а не через 15 минут
@worker_ready.connect
def start_up(sender: Celery, **kwargs):
    with sender.app.connection() as conn:
        sender.app.send_task("app.main.parse_task", connection=conn)