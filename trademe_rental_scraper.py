import asyncio
import re
import os
import pandas as pd
import random
from datetime import datetime
from playwright.async_api import async_playwright
from urllib.parse import urljoin  # Import for robust URL joining
import argparse # Import for command-line arguments
import urllib.parse

BASE_URL = "https://www.trademe.co.nz/a/property/residential/rent/search"  # Fixed URL
DATA = []
FAILED = []
MAX_CONCURRENT = 3
MAX_PAGES = 1000
OUTPUT_DIR = "scraping_output"  # Updated output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- New constants for features ---
PROGRESS_LOCK = asyncio.Lock()
PROCESSED_COUNT = 0
TOTAL_LISTINGS_TO_SCRAPE = 0
SAVE_INTERVAL = 110  # Save every 110 listings
TEMP_SAVE_FILE = os.path.join(OUTPUT_DIR, "temp_scraped_data.csv")
RESUME_FILE = TEMP_SAVE_FILE  # Resume from the temp file if it exists
# --- End new constants ---
# Add new constants for URL saving/loading
COLLECTED_URLS_FILE = os.path.join(OUTPUT_DIR, "collected_listing_urls.txt")

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

def normalize_trademe_url(url: str) -> str:
    """
    Converts older Trade Me URLs to the new canonical format.

    Args:
        url (str): The URL to normalize.

    Returns:
        str: The normalized URL.
    """
    try:
        parsed_url = urllib.parse.urlparse(url)
        # Check if the path starts with /property/ and replace it with /a/property/
        if parsed_url.path.startswith("/property/"):
            # Ensure path starts with '/' and build the new path
            new_path = "/a" + parsed_url.path
            # Reconstruct the URL with the new path, keeping other components
            normalized_url = urllib.parse.urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                new_path,
                parsed_url.params,
                parsed_url.query, # Keep query params like rsqid if they work with new path
                parsed_url.fragment
            ))
            # print(f"Normalized URL: {url} -> {normalized_url}") # Optional debug print
            return normalized_url
        else:
            # If it doesn't start with /property/, assume it's already in the correct format or different
            # Optionally, you could add more checks here if needed (e.g., ensure it starts with /a/property/)
            return url
    except Exception as e:
        print(f"\n⚠️ Error normalizing URL {url}: {e}")
        # Return the original URL if normalization fails
        return url

# --- New function for progress update ---
async def update_progress():
    global PROCESSED_COUNT
    async with PROGRESS_LOCK:
        PROCESSED_COUNT += 1
        print(f"\rProgress: {PROCESSED_COUNT}/{TOTAL_LISTINGS_TO_SCRAPE}", end="", flush=True)
# --- End new function ---

# --- New function to save collected URLs ---
def save_collected_urls(urls_list):
    """Saves the list of collected URLs to a file."""
    try:
        with open(COLLECTED_URLS_FILE, "w") as f:
            for url in urls_list:
                f.write(url + "\n")
        print(f"\n🔗 Saved {len(urls_list)} collected URLs to {COLLECTED_URLS_FILE}")
    except Exception as e:
        print(f"\n⚠️ Failed to save collected URLs to {COLLECTED_URLS_FILE}: {e}")

# --- New function to load collected URLs ---
def load_collected_urls():
    """Loads the list of collected URLs from a file."""
    if os.path.exists(COLLECTED_URLS_FILE):
        try:
            with open(COLLECTED_URLS_FILE, "r") as f:
                urls = [line.strip() for line in f if line.strip()]
            print(f"\n🔗 Loaded {len(urls)} URLs from {COLLECTED_URLS_FILE}")
            return urls
        except Exception as e:
            print(f"\n⚠️ Failed to load collected URLs from {COLLECTED_URLS_FILE}: {e}")
            return None # Indicate failure to load
    else:
        print(f"\nℹ️ Collected URLs file {COLLECTED_URLS_FILE} not found.")
        return None # Indicate file not found


async def scrape_listing(browser, listing_url: str, retry: int = 2):
    global DATA  # Access the global DATA list
    # Normalize the incoming URL
    listing_url = normalize_trademe_url(listing_url)
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


async def collect_listing_urls(page, start_page: int = 1):
    urls = set()
    
    # --- Use the start_page argument ---
    page_num = start_page  # Initialize with the provided start page, or 1 if not provided
    # --- End modification ---

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
                    full_url = urljoin("https://www.trademe.co.nz", href) # Use urljoin for robustness
                    normalized_url = normalize_trademe_url(full_url) # Apply normalization
                    urls.add(normalized_url) # Add the normalized URL to the set

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


# --- Modified main function ---
async def main():
    global DATA, TOTAL_LISTINGS_TO_SCRAPE

    # --- Setup argument parser ---
    parser = argparse.ArgumentParser(description="Scrape Trade Me property listings.")
    parser.add_argument(
        "--skip-url-collection",
        action="store_true",
        help="Skip collecting URLs and load them from the saved file instead (scrapes all loaded URLs).",
    )
    # --- Add the new argument ---
    parser.add_argument(
        "--start-page",
        type=int,
        default=1, # Default to page 1 if not specified
        help="The search results page number to start collecting URLs from (default: 1).",
    )
    # --- End new argument ---
    args = parser.parse_args()
    # --- End argument parser ---

    # --- Constants for streaming ---
    PAGES_PER_BATCH = 5 # Collect and scrape in batches of 5 pages
    # --- End streaming constants ---

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True) # Set to False for debugging if needed

        # --- Load previously scraped data (Resume) ---
        scraped_urls_set = load_resume_data() # This function remains the same
        # --- End loading scraped data ---

        if args.skip_url_collection:
            # --- Existing logic for --skip-url-collection ---
            print("\n⏭️ Skipping URL collection, attempting to load from file...")
            loaded_urls = load_collected_urls()
            if loaded_urls is not None:
                listing_urls_to_scrape = [url for url in loaded_urls if url not in scraped_urls_set]
                TOTAL_LISTINGS_TO_SCRAPE = len(listing_urls_to_scrape) # Update global count for progress
                print(f"\n✅ Loaded {len(loaded_urls)} URLs, {len(listing_urls_to_scrape)} new URLs to scrape.")

                if not listing_urls_to_scrape:
                    print("\n✅ No new listings to scrape based on loaded URLs and resume data.")
                    await browser.close()
                    return

                # --- Process ALL loaded URLs in chunks ---
                print("\n🚀 Starting scraping of loaded URLs...")
                chunk_number = 1
                for i in range(0, len(listing_urls_to_scrape), SAVE_INTERVAL):
                    chunk_urls = listing_urls_to_scrape[i:i + SAVE_INTERVAL]
                    print(f"\n🚀 Starting scraping chunk {chunk_number} ({len(chunk_urls)} listings)...")

                    tasks = [asyncio.create_task(scrape_listing(browser, url)) for url in chunk_urls]
                    await asyncio.gather(*tasks)
                    await save_temp_data(DATA) # Save periodically

                    print(f"\n🏁 Completed scraping chunk {chunk_number}.")
                    chunk_number += 1
                # --- End processing loaded URLs ---
            else:
                print("\n❌ Failed to load URLs from file. Cannot proceed with --skip-url-collection.")
                await browser.close()
                return
            # --- End existing logic for --skip-url-collection ---

        else:
            # --- New streaming logic ---
            print("\n🌐 Starting streaming collection and scraping...")
            page = await browser.new_page()

            all_collected_urls = [] # Keep track of all URLs collected so far for final save
            page_num = args.start_page # Use the argument for the initial page number            
            batch_number = 1

            while True:
                # Determine the range of pages for this batch
                start_page = page_num
                # Collect up to PAGES_PER_BATCH pages, or until MAX_PAGES is reached
                end_page = min(page_num + PAGES_PER_BATCH - 1, MAX_PAGES if MAX_PAGES else float('inf'))
                if MAX_PAGES and start_page > MAX_PAGES:
                     print(f"\n📛 Reached MAX_PAGES limit ({MAX_PAGES}).")
                     break

                print(f"\n🌐 Collecting URL batch {batch_number} (pages {start_page} to {end_page})...")

                # --- Collect URLs for the current batch ---
                batch_urls_set = set() # Use a set for this batch to avoid internal duplicates
                current_page_to_fetch = start_page
                pages_fetched_in_batch = 0

                while pages_fetched_in_batch < PAGES_PER_BATCH and (MAX_PAGES is None or current_page_to_fetch <= MAX_PAGES):
                     print(f"\n🌐 Fetching search page {current_page_to_fetch} for batch {batch_number}")
                     await asyncio.sleep(random.uniform(1, 2))
                     try:
                         search_url = f"{BASE_URL}?page={current_page_to_fetch}"
                         await page.goto(search_url, timeout=20000)
                         await page.wait_for_selector("a.tm-property-search-card__link, a.tm-property-premium-listing-card__link", timeout=20000)

                         premium = await page.locator("a.tm-property-premium-listing-card__link").all()
                         standard = await page.locator("a.tm-property-search-card__link").all()

                         for el in premium + standard:
                             href = await el.get_attribute("href")
                             if href:
                                 full_url = urljoin("https://www.trademe.co.nz", href)
                                 normalized_url = normalize_trademe_url(full_url)
                                 batch_urls_set.add(normalized_url) # Add to current batch set
                                 all_collected_urls.append(normalized_url) # Add to overall list

                         next_btn = page.locator("a[title='Next'], a.ng-star-inserted:has-text('Next')")
                         if await next_btn.count() == 0 or not await next_btn.is_enabled():
                             print("\n🏁 No more pages found.")
                             current_page_to_fetch = MAX_PAGES + 1 if MAX_PAGES else float('inf') # Signal to break outer loop
                             break

                         current_page_to_fetch += 1
                         pages_fetched_in_batch += 1

                     except Exception as e:
                         print(f"\n⚠️ Pagination error on page {current_page_to_fetch} in batch {batch_number}: {e}")
                         # Decide: break this batch or try next page? For now, break batch.
                         break # Break the inner loop, might end the batch early

                # Convert batch set to list for processing
                batch_urls_list = list(batch_urls_set)
                print(f"\n🔗 Collected {len(batch_urls_list)} unique URLs in batch {batch_number}.")

                if not batch_urls_list:
                    print(f"\nℹ️ No URLs collected in batch {batch_number}. Ending collection.")
                    break # No point continuing if no URLs

                # --- Filter batch URLs against already scraped URLs ---
                # Important: We filter against the global scraped_urls_set which might grow
                batch_urls_to_scrape = [url for url in batch_urls_list if url not in scraped_urls_set]
                print(f"\n🎯 {len(batch_urls_to_scrape)} new URLs in batch {batch_number} to scrape.")

                if batch_urls_to_scrape:
                    # Update TOTAL_LISTINGS_TO_SCRAPE for progress display (approximation, gets better each batch)
                    # A more accurate way would be to keep a running total, but this is simpler for display
                    # TOTAL_LISTINGS_TO_SCRAPE is now less critical for the overall count as progress is per batch item
                    # Let's keep it as the size of the current batch to scrape for the progress bar relevance
                    current_total_for_progress = len(batch_urls_to_scrape)
                    original_total_to_scrape = TOTAL_LISTINGS_TO_SCRAPE
                    TOTAL_LISTINGS_TO_SCRAPE = current_total_for_progress
                    print(f"\n📈 Progress target for this batch: {current_total_for_progress}")

                    # --- Scrape the URLs collected in this batch ---
                    print(f"\n🚀 Starting scraping batch {batch_number} ({len(batch_urls_to_scrape)} new listings)...")
                    chunk_number = 1
                    # Process the current batch's URLs in smaller SAVE_INTERVAL chunks
                    for i in range(0, len(batch_urls_to_scrape), SAVE_INTERVAL):
                        chunk_urls = batch_urls_to_scrape[i:i + SAVE_INTERVAL]
                        print(f"\n🚀 Starting scraping sub-chunk {chunk_number} of batch {batch_number} ({len(chunk_urls)} listings)...")

                        tasks = [asyncio.create_task(scrape_listing(browser, url)) for url in chunk_urls]
                        await asyncio.gather(*tasks)
                        await save_temp_data(DATA) # Save periodically

                        print(f"\n🏁 Completed scraping sub-chunk {chunk_number} of batch {batch_number}.")
                        chunk_number += 1

                    # After scraping this batch, update scraped_urls_set with the URLs we just processed
                    # This ensures they won't be scraped again if they appear in a later batch (unlikely but safe)
                    for url in batch_urls_to_scrape:
                         scraped_urls_set.add(url)

                    # Reset TOTAL_LISTINGS_TO_SCRAPE if needed for global context, though progress is now batch-centric
                    # For a global progress, you'd need a different mechanism, perhaps counting scraped items
                    # Let's leave it as is for now, focusing on batch progress.
                    # TOTAL_LISTINGS_TO_SCRAPE = original_total_to_scrape # Restore if needed globally later

                else:
                    print(f"\nℹ️ No new listings to scrape in batch {batch_number}.")

                batch_number += 1
                page_num = current_page_to_fetch # Move to the next page after the current batch

                # Check if we've reached the end condition
                if MAX_PAGES and page_num > MAX_PAGES:
                    print(f"\n📛 Reached MAX_PAGES limit ({MAX_PAGES}) during batch collection.")
                    break
                if not batch_urls_list: # If the last batch was empty
                     break

            # --- End new streaming logic ---
            await page.close()

            # --- Save all collected URLs at the end ---
            if all_collected_urls:
                save_collected_urls(list(dict.fromkeys(all_collected_urls))) # Remove potential duplicates before saving
            # --- End saving collected URLs ---

        # --- Final steps (common to both paths) ---
        await browser.close()

        # Final save to the main output file
        final_df = pd.DataFrame(DATA)
        final_output_file = os.path.join(OUTPUT_DIR, "trademe_rentals_final.csv")
        final_df.to_csv(final_output_file, index=False)
        print(f"\n✅ Done. {len(DATA)} total listings saved to {final_output_file}")

        if FAILED:
            failed_file = os.path.join(OUTPUT_DIR, "failed_listings.txt")
            with open(failed_file, "w") as f:
                f.write("\n".join(FAILED))
            print(f"\n⚠️ {len(FAILED)} failed listings saved to {failed_file}")
        else:
             print("\n🎉 No failed listings!")
        # --- End final steps ---

# ... (Include your existing helper functions like update_progress, scrape_listing, collect_listing_urls,
# save_chunk, save_temp_data, load_resume_data, save_collected_urls, load_collected_urls, normalize_trademe_url) ...

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user.")
    except Exception as e:
        print(f"\n💥 An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()