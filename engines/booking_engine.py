import requests
import json
import re
import time
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# --- Professional Path Management ---
root_path = str(Path(__file__).parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

import scripts.utils as utils

logger = logging.getLogger(__name__)

BOOKING_GQL_URL = "https://www.booking.com/dml/graphql"
AUTH_PATH = os.path.join(root_path, "authentication", "auth_booking.json")

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept-Language": "en-us,en;q=0.9",
    "Origin": "https://www.booking.com"
}

OFFICIAL_HOTEL_QUERY = """
query FullSearch($input: SearchQueryInput!) {
  searchQueries {
    search(input: $input) {
      ... on SearchQueryOutput {
        results {
          displayName { text }
          location { displayLocation, mainDistance }
          basicPropertyData {
            id
            pageName
            reviewScore: reviews {
              score: totalScore
              reviewCount: reviewsCount
            }
          }
          priceDisplayInfoIrene {
            displayPrice {
              amountPerStay { amount, currency }
            }
          }
        }
      }
    }
  }
}
"""

def run_adaptive_search(city, checkin, checkout, pages_to_scan=None, use_login=False):
    """ Core logic for fetching Booking.com data with standardized column names. """
    try:
        d1 = datetime.strptime(checkin, "%Y-%m-%d")
        d2 = datetime.strptime(checkout, "%Y-%m-%d")
        num_nights = max((d2 - d1).days, 1)
    except Exception as e:
        logger.error(f"Date error: {e}")
        num_nights = 1

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    
    if use_login and os.path.exists(AUTH_PATH):
        try:
            with open(AUTH_PATH, 'r') as f:
                auth_data = json.load(f)
                for cookie in auth_data.get('cookies', []):
                    session.cookies.set(cookie['name'], cookie['value'], domain=cookie.get('domain', '.booking.com'))
            logger.info(f"Session authenticated via {AUTH_PATH}")
        except Exception as e:
            logger.error(f"Failed to load auth_booking.json: {e}")
    else:
        session.get("https://www.booking.com/", timeout=15)
        session.cookies.set("lang", "en-us", domain=".booking.com")

    seen_hotel_ids = set()
    data_for_export = []
    current_offset = 0
    page_number = 1
    consecutive_empty_pages = 0
    
    logger.info(f"Starting crawl for {city}")

    while True:
        if pages_to_scan and page_number > pages_to_scan:
            break

        payload = {
            "operationName": "FullSearch",
            "variables": {
                "input": {
                    "dates": {"checkin": checkin, "checkout": checkout},
                    "location": {"searchString": city, "destType": "CITY"},
                    "nbRooms": 1, "nbAdults": 2, "nbChildren": 0,
                    "pagination": {"rowsPerPage": 50, "offset": current_offset},
                    "webSearchContext": {"reason": "CLIENT_SIDE_UPDATE", "source": "SEARCH_RESULTS", "outcome": "SEARCH_RESULTS"}
                }
            },
            "query": OFFICIAL_HOTEL_QUERY
        }

        try:
            response = session.post(BOOKING_GQL_URL, json=payload, timeout=20)
            data = response.json()
            results = data.get("data", {}).get("searchQueries", {}).get("search", {}).get("results", [])
            
            if not results:
                break

            new_hotels_this_page = 0
            for hotel in results:
                basic = hotel.get("basicPropertyData") or {}
                h_id = basic.get("id")
                
                if not h_id or h_id in seen_hotel_ids:
                    continue
                
                seen_hotel_ids.add(h_id)
                new_hotels_this_page += 1

                name = hotel.get("displayName", {}).get("text", "N/A")
                p_name = basic.get("pageName")
                hotel_url = f"https://www.booking.com/hotel/pt/{p_name}.en-gb.html" if p_name else f"https://www.booking.com/hotel/id/{h_id}.html"

                price_per_night = 0.0
                try:
                    p_info = hotel.get("priceDisplayInfoIrene", {}).get("displayPrice", {}).get("amountPerStay", {})
                    raw_amount = p_info.get("amount")
                    if raw_amount:
                        clean_price = re.sub(r'[^\d.]', '', str(raw_amount))
                        price_per_night = round(float(clean_price) / num_nights, 2)
                except: pass

                rev_data = basic.get("reviewScore") or {}
                
                # Column names standardized to include '_BOOKING'
                data_for_export.append({
                    "HOTEL_NAME": name,
                    "PRICE": price_per_night,
                    "RATING": rev_data.get("score", "N/A"),
                    "REVIEW_AMOUNT": rev_data.get("reviewCount", 0),
                    "DISTANCE": utils.extract_distance_km(hotel.get("location", {}).get("mainDistance")),
                    "URL": hotel_url
                })

            if new_hotels_this_page == 0:
                consecutive_empty_pages += 1
            else:
                consecutive_empty_pages = 0 

            if consecutive_empty_pages >= 3:
                break

            current_offset += 50
            page_number += 1
            time.sleep(1.5) 
            
        except Exception as e:
            logger.error(f"Error on page {page_number}: {e}")
            break

    return data_for_export, filename_factory(city, checkin, checkout)

def filename_factory(city, checkin, checkout):
    """ 
    Generates a filename: final_data_booking_city_DD-DD-MM.xlsx
    """
    try:
        # Parse the strings into datetime objects
        d1 = datetime.strptime(checkin, "%Y-%m-%d")
        d2 = datetime.strptime(checkout, "%Y-%m-%d")
        
        # Extract day and month components
        d1_str = d1.strftime("%d")
        d2_str = d2.strftime("%d")
        month = d1.strftime("%m")
        year = d1.strftime("%Y")
        
        city_clean = city.lower().replace(' ', '_')
        
        return f"final_data_booking.xlsx"
    except Exception:
        # Fallback in case of unexpected date format
        return f"final_data_booking.xlsx"