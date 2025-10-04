import requests
import json
import re

product_item_pattern = re.compile(r"(?<=BACKEND\.components\.item = ){.+}(?=\s)")

with open("seed.json", "r") as f:
    product = json.load(f)

product_url = product["product_url"]

main_page_headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0"
}

main_page = requests.get(product_url, headers=main_page_headers)

with open("main_page.html", "w") as f:
    f.write(main_page.text)


result = {}

product_item = json.loads(product_item_pattern.search(main_page.text).group(0))
product_id = product_item["card"]["id"]
product_name = product_item["card"]["title"]
product_category_id = product_item["card"]["categoryId"]

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

product_images = product_item["galleryImages"]

reviews_page_headers = main_page_headers.copy()
reviews_page_headers.update({"Referer": product_url})

reviews_page = requests.get(f"https://kaspi.kz/yml/review-view/api/v1/reviews/product/{product_id}?withAgg=true", headers=reviews_page_headers)
reviews = json.loads(reviews_page.text)

product_rating = reviews["summary"]["global"]
product_reviews_number = None
for group_count in reviews["groupSummary"]:
    if group_count["id"] == "COMMENT":
        product_reviews_number = group_count["total"]

offers_request_data = {
    "page": 0,
    "limit": 20,
    "cityId": "710000000"
}

offers_page = requests.post(f"https://kaspi.kz/yml/offer-view/offers/{product_id}", headers=reviews_page_headers, json=offers_request_data)
offers = json.loads(offers_page.text)

product_offers_number = offers["offersCount"]
offers_pages_number = (product_offers_number // 20) + (product_offers_number % 20 != 0)

product_offers = offers["offers"]
for i in range(1, offers_pages_number):
    offers_request_data["page"] = i
    offers_page = requests.post(f"https://kaspi.kz/yml/offer-view/offers/{product_id}", headers=reviews_page_headers, json=offers_request_data)
    offers = json.loads(offers_page.text)
    product_offers.extend(offers["offers"])


new_product_offers = []
for offer in product_offers:
    new_offer = {}
    new_offer["merchant_id"] = offer["merchantId"]
    new_offer["merchant_name"] = offer["merchantName"]
    new_offer["price"] = offer["price"]
    new_product_offers.append(new_offer)


prices = [x["price"] for x in new_product_offers]

product_min_price = min(prices)
product_max_price = max(prices)


result["id"] = product_id
result["name"] = product_name
result["category_id"] = product_category_id
result["specifications"] = new_product_specifications
result["images"] = product_images
result["rating"] = product_rating
result["reviews_number"] = product_reviews_number
result["offers_number"] = product_offers_number
result["max_price"] = product_max_price
result["min_price"] = product_min_price

print(json.dumps(result, indent=4, ensure_ascii=False))