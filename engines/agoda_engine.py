import scrapy
from scrapy_playwright.page import PageMethod
import asyncio
import sys
from pathlib import Path
from scrapy import signals

# Professional Standard: Path management from root
ROOT_DIR = Path(__file__).parent.parent
# Updated path to the new authentication folder
DEFAULT_AUTH_AGODA = str(ROOT_DIR / 'authentication' / 'auth_agoda.json')

class AgodaFinalSpider(scrapy.Spider):
    name = 'agoda_engine'
    
    custom_settings = {
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_TIMEOUT': 2400,
    }

    def __init__(self, *args, **kwargs):
        super(AgodaFinalSpider, self).__init__(*args, **kwargs)
        self.auth_file = kwargs.get('auth_file') or DEFAULT_AUTH_AGODA
        self.urls = kwargs.get('urls')

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AgodaFinalSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider, reason):
        items_count = self.crawler.stats.get_value('item_scraped_count', 0)
        sys.stdout.write(f"\n🏁 AGODA ENGINE FINISHED - Scraped {items_count} unique hotels.\n")
        sys.stdout.flush()

    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context_kwargs": {
                        "storage_state": self.auth_file,
                        "viewport": {"width": 1280, "height": 1000},
                        "ignore_https_errors": True,
                    },
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 60000,
                    },
                    "playwright_page_init_callback": self.init_page_and_monitor_resources,
                },
                callback=self.parse,
                dont_filter=True
            )

    async def init_page_and_monitor_resources(self, page, request):
        async def handle_route(route):
            url = route.request.url.lower()
            excluded_patterns = [".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".woff", ".woff2"]
            if any(pattern in url for pattern in excluded_patterns):
                await route.abort()
            else:
                await route.continue_()
        await page.route("**/*", handle_route)

    async def parse(self, response):
        page = response.meta["playwright_page"]
        current_page_num = 1
        
        try:
            while True:
                sys.stdout.write(f"\r🕵️ Agoda: Processing Page {current_page_num}... ")
                sys.stdout.flush()
                
                await page.wait_for_selector('li.PropertyCard', timeout=15000)
                
                page_hotels = await page.evaluate("""
                    async () => {
                        const results = [];
                        const seenNames = new Set();
                        let noNewCounter = 0;
                        window.scrollBy(0, 400);
                        await new Promise(r => setTimeout(r, 1000));
                        while (noNewCounter < 4) {
                            const cards = document.querySelectorAll('li.PropertyCard');
                            let foundNew = false;
                            for (const card of cards) {
                                const name = card.querySelector('[data-selenium="hotel-name"], .PropertyCard__HotelName')?.innerText?.trim();
                                if (name && !seenNames.has(name)) {
                                    seenNames.add(name);
                                    foundNew = true;
                                    const priceText = card.querySelector('.PropertyCardPrice__Value, [data-selenium="display-price"], .pd-price')?.innerText;
                                    const priceClean = priceText ? priceText.replace(/[^0-9]/g, '') : "N/A";
                                    const rating = card.querySelector('[data-testid="review-score-badge"], .eyidZH, .PropertyCardRating__Value')?.innerText || "N/A";
                                    const distanceSpan = Array.from(card.querySelectorAll('span, p')).find(el => /מרכז|center/i.test(el.innerText));
                                    let distanceNumeric = "0";
                                    if (distanceSpan) {
                                        const numMatch = distanceSpan.innerText.match(/([0-9.]+)/);
                                        if (numMatch) {
                                            let num = parseFloat(numMatch[1]);
                                            const isKm = /ק"מ|km/i.test(distanceSpan.innerText);
                                            distanceNumeric = isKm ? num.toString() : (num / 1000).toString();
                                        }
                                    }
                                    const reviewsMatch = card.innerText.match(/([0-9,]+)\\s*(?:חוות דעת|reviews)/i);
                                    const reviewsCount = reviewsMatch ? reviewsMatch[1].replace(/,/g, '') : "0";
                                    
                                    // Professional Standard: Standardized column names with _AGODA suffix
                                    results.push({
                                        'HOTEL_NAME': name,
                                        'PRICE': priceClean,
                                        'RATING': rating,
                                        'REVIEW_AMOUNT': reviewsCount,
                                        'DISTANCE': distanceNumeric,
                                        'URL': card.querySelector('a')?.href || "N/A"
                                    });
                                }
                            }
                            window.scrollBy(0, 1000);
                            await new Promise(r => setTimeout(r, 1200));
                            if (!foundNew) noNewCounter++; else noNewCounter = 0;
                        }
                        return results;
                    }
                """)

                sys.stdout.write(f"Done (Found {len(page_hotels)})\n")
                sys.stdout.flush()

                for hotel in page_hotels:
                    hotel['Page_Number'] = current_page_num
                    yield hotel

                next_button = page.locator('button#paginationNext').first
                if await next_button.is_visible():
                    is_disabled = await next_button.evaluate('(btn) => btn.disabled || btn.classList.contains("disabled")')
                    if is_disabled: break
                    await next_button.click()
                    current_page_num += 1
                    await asyncio.sleep(5) 
                else: break
        finally:
            await page.close()