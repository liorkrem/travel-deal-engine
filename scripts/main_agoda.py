import os
import sys
import logging
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging 
from pathlib import Path
from datetime import datetime

# --- Environment Setup ---
# Define the project root and ensure it is in the system path for module resolution
BASE_DIR = Path(__file__).parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from engines.agoda_engine import AgodaFinalSpider
import scripts.utils as utils

# --- Configuration & File Paths ---
# Define paths for authentication assets and temporary data storage
AUTH_FILE = os.path.join(BASE_DIR, 'authentication', 'auth_agoda.json')
RAW_DATA_FILE = os.path.join(BASE_DIR, 'outputs', 'raw_data_agoda.csv')

# Static mapping of city names to Agoda-specific destination IDs
CITY_MAP = {
    'lisbon': '16364',
    'london': '15891',
    'new york': '3186',
    'tel aviv': '7315',
    'rome': '12565',
    'bangkok': '9395'
}

def generate_agoda_urls(city_name, checkin, checkout):
    """ 
    Constructs localized Agoda search URLs by injecting parameters into the URL schema.
    Calculates Length of Stay (LOS) and iterates through star rating categories.
    """
    city_id = CITY_MAP.get(city_name.lower())
    if not city_id:
        # Fallback to default city ID if input is not found in the map
        city_id = '16364'

    # Date parsing to calculate the duration of the stay
    d1 = datetime.strptime(checkin, "%Y-%m-%d")
    d2 = datetime.strptime(checkout, "%Y-%m-%d")
    los = (d2 - d1).days

    # Grouping star ratings to distribute the scraping load across multiple requests
    star_groups = ["2,1,5,101", "3", "4"]
    urls = []
    for stars in star_groups:
        url = (f"https://www.agoda.com/he-il/search?city={city_id}&"
               f"checkIn={checkin}&los={los}&rooms=1&adults=2&hotelStarRating={stars}")
        urls.append(url)
    return urls

def run(city=None, checkin=None, checkout=None):
    """ 
    Primary execution entry point. 
    Handles logging initialization, input normalization, and the Scrapy crawler lifecycle.
    """
    # --- Logger Configuration ---
    # Reset existing handlers to ensure clean log output for the Agoda phase
    logging.getLogger().handlers = []
    log_path = os.path.join(BASE_DIR, 'logs', 'scraper_agoda.log')
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    internal_logger = logging.getLogger()
    internal_logger.setLevel(logging.INFO)
    internal_logger.addHandler(file_handler)
    configure_logging(install_root_handler=False)

    print("\n" + "="*50)
    print("   AGODA DYNAMIC ENGINE STARTING...")
    print("="*50)
    
    # Priority: Function arguments > Manual user input
    u_city = city or input("Enter City (e.g. London): ").strip()
    u_in = checkin or input("Check-in Date (YYYY-MM-DD): ").strip()
    u_out = checkout or input("Check-out Date (YYYY-MM-DD): ").strip()

    # Define final Excel report destination
    final_output_name = f"final_data_agoda.xlsx"
    final_output_path = os.path.join(BASE_DIR, 'outputs', final_output_name)

    # URL generation based on validated inputs
    urls = generate_agoda_urls(u_city, u_in, u_out)

    # --- Crawler Initialization ---
    # Initialize CrawlerProcess with dynamic settings and Playwright integration
    process = CrawlerProcess({
        **AgodaFinalSpider.custom_settings,
        'FEEDS': {RAW_DATA_FILE: {'format': 'csv', 'encoding': 'utf8', 'overwrite': True}},
        'LOG_ENABLED': False,
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': False}, 
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, Gecko) Chrome/122.0.0.0 Safari/537.36'
    })

    # Register the spider with target URLs and authentication details
    process.crawl(AgodaFinalSpider, urls=urls, auth_file=AUTH_FILE)
    process.start() # Blocking call - execution waits here until spider finishes
    
    # --- Post-Processing ---
    # Validate raw output, finalize the structured report, and perform cleanup
    if os.path.exists(RAW_DATA_FILE) and os.path.getsize(RAW_DATA_FILE) > 0:
        utils.finalize_report(RAW_DATA_FILE, final_output_path)
        df = pd.read_excel(final_output_path)
        
        # Cleanup temporary CSV to keep output directory organized
        if os.path.exists(RAW_DATA_FILE):
            os.remove(RAW_DATA_FILE)
            
        print(f"✅ Agoda phase complete. ({len(df)} hotels found)\n")
        return df
    
    return None

if __name__ == "__main__":
    run()