import asyncio
import re
import os
import pandas as pd
import random
from datetime import datetime
from playwright.async_api import async_playwright

BASE_URL = "https://www.trademe.co.nz/a/property/residential/rent/search"
DATA = []
FAILED = []
MAX_CONCURRENT = 3
MAX_PAGES = 50
OUTPUT_DIR = "failures"
os.makedirs(OUTPUT_DIR, exist_ok=True)

semaphore = asyncio.Semaphore(MAX_CONCURRENT)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.49 Safari/537.36",
]

HEADERS_LIST = [
    {
        "referer": "https://www.trademe.co.nz/",
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
    },
    {
        "referer": "https://www.trademe.co.nz/",
        "accept-language": "en-GB,en;q=0.8",
        "sec-ch-ua": '"Google Chrome";v="112", "Chromium";v="112", "Not:A-Brand";v="99"',
    },
    # add more variants if you want
]


async def scrape_listing(browser, listing_url: str, retry: int = 2):
    async with semaphore:
        user_agent = random.choice(USER_AGENTS)
        extra_headers = random.choice(HEADERS_LIST).copy()
        extra_headers["user-agent"] = user_agent

        context = await browser.new_context(
            user_agent=user_agent,
            extra_http_headers=extra_headers,
            locale="en-US",
            timezone_id="Pacific/Auckland",
            viewport={"width": 1280, "height": 800},
        )
        await asyncio.sleep(random.uniform(1, 2))  # delay changed to 1-2 seconds
        page = await context.new_page()
        try:
            await page.goto(listing_url, timeout=20000)
            await page.wait_for_selector(
                "h1[class*='tm-property-listing-body__location']", timeout=7000
            )

            try:
                show_more = page.locator(
                    "span.tm-property-listing-description__show-more-button-content"
                )
                if await show_more.count() > 0:
                    await show_more.click()
                    await page.wait_for_timeout(500)
            except Exception as e:
                print(f"⚠️ Show More click failed: {e}")

            address = await page.locator(
                "h1[class*='tm-property-listing-body__location']"
            ).text_content()
            price_text = await page.locator(
                "h2[class*='tm-property-listing-body__price']"
            ).text_content()

            match = re.search(r"\$([\d,\.]+)", price_text)
            weekly_rent = match.group(1).replace(",", "") if match else None

            bedrooms = bathrooms = parking = 0
            try:
                features = await page.locator(
                    "ul.tm-property-listing-attributes__tag-list li"
                ).all_inner_texts()
                for item in features:
                    item = item.lower().strip()
                    num = (
                        int(re.search(r"\d+", item).group())
                        if re.search(r"\d+", item)
                        else 0
                    )
                    if "bed" in item:
                        bedrooms = num
                    elif "bath" in item:
                        bathrooms = num
                    elif "parking" in item:
                        parking += num
            except:
                pass

            property_type = "Other"
            try:
                desc = await page.locator("div.tm-markdown").text_content()
                lowered = desc.lower()
                for typ in [
                    "townhouse",
                    "apartment",
                    "house",
                    "unit",
                    "flat",
                    "studio",
                ]:
                    if typ in lowered:
                        property_type = typ.capitalize()
                        break
            except:
                pass

            try:
                agent_name = await page.locator(
                    "h3.pt-agent-summary__agent-name"
                ).text_content()
            except:
                agent_name = None

            try:
                agency_name = await page.locator(
                    "h3.pt-agency-summary__agency-name"
                ).text_content()
            except:
                agency_name = None

            DATA.append(
                {
                    "Address": address.strip() if address else None,
                    "Weekly Rent": weekly_rent,
                    "Bedrooms": bedrooms,
                    "Bathrooms": bathrooms,
                    "Parking": parking,
                    "Property Type": property_type,
                    "Listing Agent": agent_name.strip() if agent_name else None,
                    "Agency": agency_name.strip() if agency_name else None,
                    "URL": listing_url,
                    "Scraped At": datetime.utcnow().isoformat(),
                }
            )
            print(f"✅ Scraped: {address}")
        except Exception as e:
            if retry > 0:
                print(f"🔁 Retry {3 - retry} failed for {listing_url}: {e}")
                await page.close()
                await context.close()
                await scrape_listing(browser, listing_url, retry=retry - 1)
                return
            else:
                print(f"❌ Failed after retries: {listing_url}")
                FAILED.append(listing_url)
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                safe_id = re.sub(r"[^a-zA-Z0-9]", "_", listing_url.split("/")[-1])
                await page.screenshot(path=f"{OUTPUT_DIR}/{safe_id}_{timestamp}.png")
                html = await page.content()
                with open(
                    f"{OUTPUT_DIR}/{safe_id}_{timestamp}.html", "w", encoding="utf-8"
                ) as f:
                    f.write(html)
        finally:
            await page.close()
            await context.close()


async def collect_listing_urls(page):
    urls = set()
    page_num = 1

    while True:
        if MAX_PAGES and page_num > MAX_PAGES:
            print("📛 Reached MAX_PAGES limit.")
            break

        print(f"🌐 Fetching search page {page_num}")
        await asyncio.sleep(random.uniform(1, 2))  # delay changed to 1-2 seconds
        try:
            await page.goto(f"{BASE_URL}?page={page_num}", timeout=20000)
            await page.wait_for_timeout(500)

            premium = await page.locator(
                "a.tm-property-premium-listing-card__link"
            ).all()
            standard = await page.locator("a.tm-property-search-card__link").all()

            for el in premium + standard:
                href = await el.get_attribute("href")
                if href:
                    urls.add("https://www.trademe.co.nz" + href)

            next_btn = page.locator(
                "a[title='Next'], a.ng-star-inserted:has-text('Next')"
            )
            if await next_btn.count() == 0 or not await next_btn.is_enabled():
                break

            page_num += 1
        except Exception as e:
            print(f"⚠️ Pagination error on page {page_num}: {e}")
            break

    return list(urls)


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        listing_urls = await collect_listing_urls(page)
        print(f"🔗 Collected {len(listing_urls)} listings")

        tasks = []
        for url in listing_urls:
            tasks.append(asyncio.create_task(scrape_listing(browser, url)))
        await asyncio.gather(*tasks)

        await browser.close()

        df = pd.DataFrame(DATA)
        df.to_csv("trademe_rentals.csv", index=False)
        print(f"\n✅ Done. {len(DATA)} listings saved to trademe_rentals.csv")

        if FAILED:
            with open("failed_listings.txt", "w") as f:
                f.write("\n".join(FAILED))
            print(f"⚠️ {len(FAILED)} failed listings saved to failed_listings.txt")


if __name__ == "__main__":
    asyncio.run(main())
