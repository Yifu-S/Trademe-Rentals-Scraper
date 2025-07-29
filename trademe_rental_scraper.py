import asyncio
import re
import os
import pandas as pd
import random
from datetime import datetime
from playwright.async_api import async_playwright
from urllib.parse import urljoin  # Import for robust URL joining

BASE_URL = "https://www.trademe.co.nz/a/property/residential/rent/search"  # Fixed URL
DATA = []
FAILED = []
MAX_CONCURRENT = 3
MAX_PAGES = 5
OUTPUT_DIR = "scraping_output"  # Updated output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- New constants for features ---
PROGRESS_LOCK = asyncio.Lock()
PROCESSED_COUNT = 0
TOTAL_LISTINGS_TO_SCRAPE = 0
SAVE_INTERVAL = 100  # Save every 1000 listings
TEMP_SAVE_FILE = os.path.join(OUTPUT_DIR, "temp_scraped_data.csv")
RESUME_FILE = TEMP_SAVE_FILE  # Resume from the temp file if it exists
# --- End new constants ---

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
]

# --- New function for progress update ---
async def update_progress():
    global PROCESSED_COUNT
    async with PROGRESS_LOCK:
        PROCESSED_COUNT += 1
        print(f"\rProgress: {PROCESSED_COUNT}/{TOTAL_LISTINGS_TO_SCRAPE}", end="", flush=True)
# --- End new function ---


async def scrape_listing(browser, listing_url: str, retry: int = 2):
    global DATA  # Access the global DATA list
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
        # Consider adding a slightly longer initial delay if needed
        await asyncio.sleep(random.uniform(1, 2))
        page = await context.new_page()
        try:
            await page.goto(listing_url, timeout=20000)
            # Wait for a key element that signifies the listing content has loaded
            #await page.wait_for_selector("div.tm-property-listing-body", timeout=20000)

            try:
                show_more = page.locator("span.tm-property-listing-description__show-more-button-content")
                if await show_more.count() > 0:
                    await show_more.click()
                    await page.wait_for_timeout(500) # Wait a bit after click
            except Exception as e:
                print(f"\n⚠️ Show More click failed for {listing_url}: {e}")

            address = await page.locator("h1[class*='tm-property-listing-body__location']").text_content(timeout=10000)
            price_text = await page.locator("h2[class*='tm-property-listing-body__price']").text_content(timeout=10000)

            match = re.search(r"\$([\d,\.]+)", price_text)
            weekly_rent = match.group(1).replace(",", "") if match else None

            bedrooms = bathrooms = parking = 0
            try:
                features = await page.locator("ul.tm-property-listing-attributes__tag-list li").all_inner_texts()
                for item in features:
                    item = item.lower().strip()
                    num = int(re.search(r"\d+", item).group()) if re.search(r"\d+", item) else 0
                    if "bed" in item:
                        bedrooms = num
                    elif "bath" in item:
                        bathrooms = num
                    elif "parking" in item:
                        parking += num
            except Exception as e:
                # Optional: Log parsing errors if needed
                pass

            property_type = "Other"
            try:
                desc_locator = page.locator("div.tm-markdown")
                # Check if the description element exists before trying to get text
                if await desc_locator.count() > 0:
                    desc = await desc_locator.text_content(timeout=10000)
                    lowered = desc.lower()
                    for typ in ["townhouse", "apartment", "house", "unit", "flat", "studio"]:
                        if typ in lowered:
                            property_type = typ.capitalize()
                            break
            except Exception as e:
                # Optional: Log parsing errors if needed
                pass

            agent_name = agency_name = None
            try:
                agent_name = await page.locator("h3.pt-agent-summary__agent-name").text_content(timeout=10000)
            except:
                 pass # Ignore if not found
            try:
                 agency_name = await page.locator("h3.pt-agency-summary__agency-name").text_content(timeout=10000)
            except:
                 pass # Ignore if not found

            # Append data to the global list
            DATA.append({
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
            })
            print(f"\n✅ Scraped: {address[:50]}...") # Print first 50 chars of address

        except Exception as e:
            if retry > 0:
                print(f"\n🔁 Retry {3 - retry} failed for {listing_url}: {e}")
                await page.close()
                await context.close()
                await scrape_listing(browser, listing_url, retry=retry - 1)
                # Update progress even on retry attempts if you want to count attempts
                # await update_progress()
                return
            else:
                print(f"\n❌ Failed after retries: {listing_url} - Error: {e}")
                FAILED.append(listing_url)
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                safe_id = re.sub(r"[^a-zA-Z0-9]", "_", listing_url.split("/")[-1])
                try:
                    await page.screenshot(path=f"{OUTPUT_DIR}/{safe_id}_{timestamp}.png", full_page=True)
                    html = await page.content()
                    with open(f"{OUTPUT_DIR}/{safe_id}_{timestamp}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                except Exception as screenshot_error:
                     print(f"\n⚠️ Failed to save failure artifacts for {listing_url}: {screenshot_error}")

        finally:
            await page.close()
            await context.close()
            # Update progress counter here, after the task finishes (success or failure)
            await update_progress()


async def collect_listing_urls(page):
    urls = set()
    page_num = 1

    while True:
        if MAX_PAGES and page_num > MAX_PAGES:
            print(f"\n📛 Reached MAX_PAGES limit ({MAX_PAGES}).")
            break

        print(f"\n🌐 Fetching search page {page_num}")
        await asyncio.sleep(random.uniform(1, 2))
        try:
            search_url = f"{BASE_URL}?page={page_num}"
            await page.goto(search_url, timeout=20000)
            # Wait for listings to appear on the search page
            await page.wait_for_selector("a.tm-property-search-card__link, a.tm-property-premium-listing-card__link", timeout=20000)

            premium = await page.locator("a.tm-property-premium-listing-card__link").all()
            standard = await page.locator("a.tm-property-search-card__link").all()

            for el in premium + standard:
                href = await el.get_attribute("href")
                if href:
                    # Use urljoin for correct URL construction
                    full_url = urljoin("https://www.trademe.co.nz", href)
                    urls.add(full_url)

            next_btn = page.locator("a[title='Next'], a.ng-star-inserted:has-text('Next')")
            if await next_btn.count() == 0 or not await next_btn.is_enabled():
                print("\n🏁 No more pages found.")
                break

            page_num += 1
        except Exception as e:
            print(f"\n⚠️ Pagination error on page {page_num}: {e}")
            # Decide whether to break or continue based on error type if needed
            break # For now, break on any pagination error

    return list(urls)


# --- New function to save data periodically ---
async def save_chunk(data_chunk, chunk_number):
    if not data_chunk:
        print(f"\nℹ️ No data to save for chunk {chunk_number}.")
        return
    df_chunk = pd.DataFrame(data_chunk)
    chunk_file = os.path.join(OUTPUT_DIR, f"scraped_data_chunk_{chunk_number}.csv")
    df_chunk.to_csv(chunk_file, index=False)
    print(f"\n💾 Saved chunk {chunk_number} ({len(data_chunk)} listings) to {chunk_file}")

async def save_temp_data(data_list):
    if not data_list:
        print("\nℹ️ No temporary data to save.")
        return
    df_temp = pd.DataFrame(data_list)
    df_temp.to_csv(TEMP_SAVE_FILE, index=False)
    print(f"\n💾 Saved temporary data ({len(data_list)} listings) to {TEMP_SAVE_FILE}")

# --- End new function ---

# --- New function to load previously saved data ---
def load_resume_data():
    global DATA
    if os.path.exists(RESUME_FILE):
        try:
            df_resume = pd.read_csv(RESUME_FILE)
            DATA = df_resume.to_dict('records')
            print(f"\n🔄 Resumed from {len(DATA)} previously scraped listings in {RESUME_FILE}.")
            return set(item['URL'] for item in DATA) # Return URLs already scraped
        except Exception as e:
             print(f"\n⚠️ Could not load resume data from {RESUME_FILE}: {e}. Starting fresh.")
             return set()
    else:
        print("\n🆕 No previous data found. Starting fresh.")
        return set()
# --- End new function ---


async def main():
    global DATA, TOTAL_LISTINGS_TO_SCRAPE # Access global variables
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True) # Set to False for debugging
        page = await browser.new_page()

        # --- Load previously scraped data ---
        scraped_urls = load_resume_data()
        # --- End loading ---

        listing_urls = await collect_listing_urls(page)
        print(f"\n🔗 Collected {len(listing_urls)} unique listing URLs.")

        # --- Filter out already scraped URLs ---
        listing_urls_to_scrape = [url for url in listing_urls if url not in scraped_urls]
        TOTAL_LISTINGS_TO_SCRAPE = len(listing_urls_to_scrape)
        print(f"\n🎯 {TOTAL_LISTINGS_TO_SCRAPE} listings remaining to scrape (after resume).")
        # --- End filtering ---

        if not listing_urls_to_scrape:
            print("\n✅ No new listings to scrape based on resume data.")
            await browser.close()
            return

        # --- Process URLs in chunks for periodic saving ---
        chunk_number = 1
        for i in range(0, len(listing_urls_to_scrape), SAVE_INTERVAL):
            chunk_urls = listing_urls_to_scrape[i:i + SAVE_INTERVAL]
            print(f"\n🚀 Starting chunk {chunk_number} ({len(chunk_urls)} listings)...")

            tasks = []
            for url in chunk_urls:
                tasks.append(asyncio.create_task(scrape_listing(browser, url)))

            # Wait for all tasks in the current chunk to complete
            await asyncio.gather(*tasks)

            # Save the data accumulated so far (including resumed data)
            await save_temp_data(DATA)

            # Optional: Save chunk separately if needed for analysis
            # await save_chunk(DATA[-len(chunk_urls):], chunk_number) # Gets last N items added

            print(f"\n🏁 Completed chunk {chunk_number}.")
            chunk_number += 1

        # --- End chunked processing ---

        await browser.close()

        # Final save to the main output file
        final_df = pd.DataFrame(DATA)
        final_output_file = os.path.join(OUTPUT_DIR, "trademe_rentals_final.csv")
        final_df.to_csv(final_output_file, index=False)
        print(f"\n✅ Done. {len(DATA)} total listings saved to {final_output_file}")

        # Cleanup temp file if desired after successful run
        # if os.path.exists(TEMP_SAVE_FILE):
        #     os.remove(TEMP_SAVE_FILE)
        #     print(f"\n🗑️ Deleted temporary file {TEMP_SAVE_FILE}")

        if FAILED:
            failed_file = os.path.join(OUTPUT_DIR, "failed_listings.txt")
            with open(failed_file, "w") as f:
                f.write("\n".join(FAILED))
            print(f"\n⚠️ {len(FAILED)} failed listings saved to {failed_file}")
        else:
             print("\n🎉 No failed listings!")


if __name__ == "__main__":
    asyncio.run(main())