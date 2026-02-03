import asyncio
from playwright.async_api import async_playwright
import os

# Professional Standard: Centralized path management using absolute paths
# This ensures files are always saved in the script's directory (Project Root)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUTH_BOOKING = os.path.join(BASE_DIR, 'auth_booking.json')
AUTH_AGODA = os.path.join(BASE_DIR, 'auth_agoda.json')

async def run_auth_process(site_name, url, output_file):
    """
    Handles the authentication process: 
    Opens browser, waits for user login, and saves the state.
    """
    async with async_playwright() as p:
        # Launching Chromium in non-headless mode so the user can interact
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        print(f"\n--- {site_name} Authentication ---")
        print(f"🌐 Opening {url}...")
        await page.goto(url)

        print(f"👉 Please log into your {site_name} account.")
        print("💡 Tip: Wait until the page fully loads and you see your name/profile.")
        
        input("✅ After login is complete, press ENTER here to save session...")

        # Capture cookies and local storage to a small JSON file
        await context.storage_state(path=output_file)
        print(f"💾 Success! Session saved to {output_file}")
        
        await browser.close()

async def main():
    """ Main entry point for the authentication utility. """
    while True:
        print("\n================================")
        print("   SCRAPER AUTHENTICATOR TOOL   ")
        print("================================")
        print("1. Authenticate Booking.com")
        print("2. Authenticate Agoda")
        print("3. Authenticate Both (Sequential)")
        print("4. Exit")
        
        choice = input("\nSelect an option (1-4): ")

        if choice == '1':
            await run_auth_process("Booking.com", "https://www.booking.com/", AUTH_BOOKING)
        elif choice == '2':
            await run_auth_process("Agoda", "https://www.agoda.com/", AUTH_AGODA)
        elif choice == '3':
            await run_auth_process("Booking.com", "https://www.booking.com/", AUTH_BOOKING)
            await run_auth_process("Agoda", "https://www.agoda.com/", AUTH_AGODA)
        elif choice == '4':
            print("Exiting authenticator. Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")