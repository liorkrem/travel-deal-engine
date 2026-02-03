import pandas as pd
import logging
import os
import sys
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from engines import booking_engine
import scripts.utils as utils

load_dotenv(dotenv_path=os.path.join(BASE_DIR, '.env'))
os.makedirs(os.path.join(BASE_DIR, 'outputs'), exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, 'logs'), exist_ok=True)

# --- Logging Setup ---
log_path = os.path.join(BASE_DIR, 'logs', 'scraper_booking.log')
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_path,
    filemode='w',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

def get_input_with_default(prompt, env_var):
    default = os.getenv(env_var)
    user_val = input(f"{prompt} [{default}]: ").strip()
    return user_val if user_val else default

def run(city=None, checkin=None, checkout=None):
    """ Main execution logic for Booking.com - Streamlined without manual login prompts. """
    print("\n" + "="*50)
    print("   BOOKING.COM ENGINE STARTING...")
    print("="*50)
    
    # Professional Standard: Use provided args or fallback to manual input
    u_city = city or get_input_with_default("Enter City Name", "DEFAULT_CITY")
    u_in = checkin or get_input_with_default("Check-in Date (YYYY-MM-DD)", "DEFAULT_CHECKIN")
    u_out = checkout or get_input_with_default("Check-out Date (YYYY-MM-DD)", "DEFAULT_CHECKOUT")
    
    # Pages input remains to allow control over scrape depth
    # u_pages = input("Booking: Pages to scan (number/all): ").strip().lower()
    # pages_limit = None if u_pages == 'all' else int(1u_pages)
    pages_limit = None

    
    # AUTOMATION FIX: Removed manual login question. 
    # The engine uses storage_state from auth_booking.json by default.
    do_login = True

    print(f"\n🚀 Crawling Booking.com for {u_city}... (Check logs/scraper_booking.log)")

    results, filename = booking_engine.run_adaptive_search(
        u_city, u_in, u_out, pages_to_scan=pages_limit, use_login=do_login
    )

    if results:
        final_path = os.path.join(BASE_DIR, 'outputs', filename)
        raw_csv_path = os.path.join(BASE_DIR, 'outputs', 'raw_booking_data.csv')
        
        # Save raw results and finalize via utility
        pd.DataFrame(results).to_csv(raw_csv_path, index=False)
        utils.finalize_report(raw_csv_path, final_path)
        
        # Cleanup temporary CSV
        if os.path.exists(raw_csv_path):
            os.remove(raw_csv_path)

        # Load cleaned data for the unified main script
        df = pd.read_excel(final_path)
        print(f"✅ Booking phase complete. ({len(df)} hotels found)")
        return df
    else:
        print("⚠️ No data collected from Booking.com.")
        return None

if __name__ == "__main__":
    run()