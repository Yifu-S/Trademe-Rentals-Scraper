import asyncio
import re
import os
import pandas as pd
import random
from datetime import datetime, timedelta, timezone
from playwright.async_api import async_playwright
from urllib.parse import urljoin  # Import for robust URL joining
import argparse # Import for command-line arguments
import urllib.parse

# --- Define Base URLs for different listing types ---
BASE_URL_RENTAL = "https://www.trademe.co.nz/a/property/residential/rent/search"
BASE_URL_SALE = "https://www.trademe.co.nz/a/property/residential/sale/search"
# --- End Base URLs ---

# --- Global variable to hold the current BASE_URL based on listing type ---
CURRENT_BASE_URL = BASE_URL_RENTAL # Default, will be changed based on argument
# --- End Global variable ---

DATA = []
FAILED = []
MAX_CONCURRENT = 10
MAX_PAGES = 5 # Consider if this should also be configurable per type if needed
OUTPUT_DIR = "scraping_output"  # Updated output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)
TASK_START_DELAY = 0.5  # Delay in seconds between starting tasks (e.g., 0.2 = 200ms)

# --- New constants for features ---
PROGRESS_LOCK = asyncio.Lock()
PROCESSED_COUNT = 0
TOTAL_LISTINGS_TO_SCRAPE = 0
SAVE_INTERVAL = 110  # Save every 110 listings
TEMP_SAVE_FILE = os.path.join(OUTPUT_DIR, "temp_scraped_data.csv") # Will be updated based on listing type
RESUME_FILE = TEMP_SAVE_FILE  # Resume from the temp file if it exists
# --- End new constants ---

# Add new constants for URL saving/loading
COLLECTED_URLS_FILE = os.path.join(OUTPUT_DIR, "collected_listing_urls.txt") # Will be updated based on listing type

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

# --- Update normalize_trademe_url to remove query parameters ---
def normalize_trademe_url(url: str) -> str:
    """
    Converts older Trade Me URLs to the new canonical format and removes query parameters.
    Args:
        url (str): The URL to normalize.
    Returns:
        str: The normalized URL without query parameters.
    """
    try:
        parsed_url = urllib.parse.urlparse(url)
        # Check if the path starts with /property/ and replace it with /a/property/
        original_path = parsed_url.path
        if original_path.startswith("/property/"):
            # Ensure path starts with '/' and build the new path
            new_path = "/a" + original_path
            # Reconstruct the URL with the new path, removing query and fragment
            normalized_url = urllib.parse.urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                new_path,
                parsed_url.params,
                '', # Remove query params
                ''  # Remove fragment
            ))
            # print(f"Normalized URL: {url} -> {normalized_url}") # Optional debug print
            return normalized_url
        else:
            # If it doesn't start with /property/, assume it's already mostly correct format
            # Still remove query and fragment for consistency
            cleaned_url = urllib.parse.urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                original_path,
                parsed_url.params,
                '', # Remove query params
                ''  # Remove fragment
            ))
            return cleaned_url
    except Exception as e:
        print(f"\n⚠️ Error normalizing URL {url}: {e}")
        # Return the original URL if normalization fails
        return url
# --- End normalize_trademe_url update ---

# --- Helper function for date parsing ---
def parse_list_date(date_text: str) -> str:
    """Parses 'Listed: Mon, 4 Aug' or 'Listed: Today' into 'dd/mm/yyyy'."""
    if not date_text:
        return None
    date_text = date_text.strip()
    try:
        if "today" in date_text.lower():
            # Return today's date formatted as dd/mm/yyyy
            return datetime.now(timezone(timedelta(hours=12))).astimezone(timezone.utc).strftime("%d/%m/%Y")
        elif "yesterday" in date_text.lower():
             # Return yesterday's date formatted as dd/mm/yyyy
             return (datetime.now(timezone(timedelta(hours=12))).astimezone(timezone.utc) - pd.Timedelta(days=1)).strftime("%d/%m/%Y")
        else:
            # Assume format like "Listed: Mon, 4 Aug"
            # Remove "Listed:" prefix
            date_part = date_text.split(":", 1)[1].strip() # Split on first ':'
            # Parse the date string like "Mon, 4 Aug"
            # We need to add the year. Assuming current year for simplicity.
            # More robust handling might involve checking if the date is in the future and adjusting year.
            current_year = datetime.now(timezone(timedelta(hours=12))).astimezone(timezone.utc).year
            parsed_date = datetime.strptime(f"{date_part} {current_year}", "%a, %d %b %Y")
            return parsed_date.strftime("%d/%m/%Y")
    except Exception as e:
        print(f"\n⚠️ Error parsing list date '{date_text}': {e}")
        return None
# --- End helper function ---

# --- Update scrape_listing function ---
async def scrape_listing(browser, listing_url: str, retry: int = 2):
    global DATA, BASE_URL # Access the global DATA list and BASE_URL to determine type
    # Normalize the incoming URL (now also removes query params)
    listing_url = normalize_trademe_url(listing_url)

    # --- Determine listing type based on URL or global BASE_URL ---
    # This is a basic heuristic, might need refinement later
    is_rental = "/rent/" in listing_url or CURRENT_BASE_URL == BASE_URL_RENTAL
    is_sale = "/sale/" in listing_url or CURRENT_BASE_URL == BASE_URL_SALE
    listing_type = "rental" if is_rental else "sale" if is_sale else "unknown"
    # --- End determination ---

    async with semaphore:
        user_agent = random.choice(USER_AGENTS)
        extra_headers = random.choice(HEADERS_LIST).copy()
        extra_headers["user-agent"] = user_agent

        context = await browser.new_context(
            user_agent=user_agent,
            extra_http_headers=extra_headers,
            locale="en-US", # Consider if en-NZ is better?
            timezone_id="Pacific/Auckland",
            viewport={"width": 1280, "height": 800},
        )
        # Consider adding a slightly longer initial delay if needed
        await asyncio.sleep(random.uniform(2, 3))
        page = await context.new_page()
        try:
            await page.goto(listing_url, timeout=20000)
            # Wait for a key element that signifies the listing content has loaded
            # Using a more general selector that should exist on both types
            await page.wait_for_selector("h1[class*='tm-property-listing-body__location']", timeout=20000)

            try:
                show_more = page.locator("span.tm-property-listing-description__show-more-button-content")
                if await show_more.count() > 0:
                    await show_more.click()
                    await page.wait_for_timeout(500) # Wait a bit after click
            except Exception as e:
                print(f"\n⚠️ Show More click failed for {listing_url}: {e}")

            # --- Extract common fields ---
            address = await page.locator("h1[class*='tm-property-listing-body__location']").text_content(timeout=10000)
            
            # --- Extract price based on listing type ---
            price_text = ""
            price_locator = page.locator("h2[class*='tm-property-listing-body__price']")
            if await price_locator.count() > 0:
                 price_text = await price_locator.text_content(timeout=10000)
            # print(f"Debug Price Text ({listing_type}): {repr(price_text)}") # Debug print

            weekly_rent = None
            ask_price_nzd = None
            sale_type = None # For-sale specific
            
            if listing_type == "rental":
                 # --- Rental Price Parsing ---
                 # Basic extraction of numeric part
                 rent_match = re.search(r"\$([0-9,]+)", price_text)
                 if rent_match:
                     weekly_rent = rent_match.group(1).replace(",", "")
                 # TODO: Determine rent period if needed (weekly assumed for now based on field name)
                 # --- End Rental Price Parsing ---
            elif listing_type == "sale":
                 # --- Sales Price and Sale Type Parsing ---
                 price_text_lower = price_text.lower()
                 
                 # Determine Sale Type
                 if "auction" in price_text_lower:
                     sale_type = "Auction"
                 elif "tender" in price_text_lower:
                     sale_type = "Tender"
                 elif "deadline sale" in price_text_lower:
                     sale_type = "Deadline Sale"
                 elif "price by negotiation" in price_text_lower or "negotiation" in price_text_lower:
                     sale_type = "Price by Negotiation"
                 else:
                     # If none of the above keywords are found, assume Fixed Price if there's a price number
                     price_match = re.search(r"\$([0-9,]+)", price_text)
                     if price_match:
                         sale_type = "Fixed Price"
                         ask_price_nzd = price_match.group(1).replace(",", "")
                     # If no price number and no keywords, sale_type remains None or could be set to 'Unknown'
                 # If sale type was determined by keyword, also try to extract the price number
                 if sale_type and sale_type != "Fixed Price":
                     price_match = re.search(r"\$([0-9,]+)", price_text)
                     if price_match:
                         ask_price_nzd = price_match.group(1).replace(",", "")
                 # --- End Sales Price and Sale Type Parsing ---
            
            bedrooms = bathrooms = parking_spaces = 0 # Using parking_spaces as per schema
            try:
                features = await page.locator("ul.tm-property-listing-attributes__tag-list li").all_inner_texts()
                for item in features:
                    item = item.lower().strip()
                    num_match = re.search(r"\d+", item)
                    num = int(num_match.group()) if num_match else 0
                    if "bed" in item:
                        bedrooms = num
                    elif "bath" in item:
                        bathrooms = num
                    elif "parking" in item or "car" in item: # Sometimes it's "car space"
                        parking_spaces += num
            except Exception as e:
                # Optional: Log parsing errors if needed
                print(f"\n⚠️ Error parsing features for {listing_url}: {e}")
                pass

            property_type = "Other"
            try:
                desc_locator = page.locator("div.tm-markdown")
                # Check if the description element exists before trying to get text
                if await desc_locator.count() > 0:
                    desc = await desc_locator.text_content(timeout=10000)
                    lowered = desc.lower()
                    for typ in ["Apartment", "Condo", "Co-op", "home", "townhouse", "Cape Cod", "Colonial", 
                                "Contemporary", "Federal", "Craftsman", "Greek Revival", "Farmhouse", 
                                "French country", "Mediterranean", "Midcentury modern", "Ranch", 
                                "Split-level", "Tudor", "Victorian"]:
                        if typ in lowered:
                            property_type = typ.capitalize()
                            break
            except Exception as e:
                desc = None # Ensure desc is defined even if extraction fails
                # Optional: Log parsing errors if needed
                print(f"\n⚠️ Error parsing description for {listing_url}: {e}")
                pass

            # --- Extract NEW specific fields for Project Brief ---
            
            # --- listing_id ---
            listing_id_match = re.search(r"/listing/(\d+)", listing_url)
            listing_id = listing_id_match.group(1) if listing_id_match else None

            # --- list_date ---
            list_date_raw = None
            list_date = None
            try:
                list_date_raw = await page.locator("div[class*='tm-property-listing-body__date']").text_content(timeout=10000)
                if list_date_raw:
                    list_date = parse_list_date(list_date_raw)
            except Exception as e:
                 print(f"\n⚠️ Error extracting or parsing list_date for {listing_url}: {e}")
                 pass # list_date remains None

            # --- page_views ---
            page_views = None
            try:
                page_views_text = await page.locator("div.tm-property-listing__listing-metadata-page-views").text_content(timeout=10000)
                if page_views_text:
                    # Extract the number using regex
                    views_match = re.search(r"(\d+)", page_views_text)
                    page_views = int(views_match.group(1)) if views_match else None
            except Exception as e:
                 print(f"\n⚠️ Error extracting page_views for {listing_url}: {e}")
                 pass # page_views remains None

            # --- status ---
            # As requested, hardcode to "active" for now.
            # Future implementation would need to detect "sold", "withdrawn", "inactive"
            status = "active" # Default status

            # --- suburb, city, region from URL ---
            suburb = city = region = None
            try:
                # Parse the path: /a/property/residential/rent|sale/region/city/suburb/listing/...
                parsed_listing_url = urllib.parse.urlparse(listing_url)
                path_parts = [p for p in parsed_listing_url.path.split('/') if p] # Remove empty strings
                # print(f"Debug Path Parts: {path_parts}") # Debug print
                if len(path_parts) >= 7 and path_parts[3] in ['rent', 'sale']: # Check structure
                    region = path_parts[4].replace('-', ' ').title() if len(path_parts) > 4 else None
                    city = path_parts[5].replace('-', ' ').title() if len(path_parts) > 5 else None
                    suburb = path_parts[6].replace('-', ' ').title() if len(path_parts) > 6 else None
            except Exception as e:
                 print(f"\n⚠️ Error parsing location from URL {listing_url}: {e}")
                 pass # locations remain None

            # --- source_site ---
            source_site = 'trademe' # Hardcoded as per brief

                        # Initialize sale-specific fields
            cv_nzd = estimate_low_nzd = estimate_high_nzd = None

            if listing_type == "sale":
                print(f"  -> Attempting to extract data for sale listing ID {listing_id}...")

                # --- Ensure Main Page Load is Complete ---
                try:
                    await page.wait_for_selector("h1.tm-property-listing-body__location", state='visible', timeout=10000)
                    print(f"  -> Main listing content loaded for {listing_id}.")
                except asyncio.TimeoutError:
                    print(f"  -> Timeout waiting for main listing content for {listing_id}. Proceeding...")
                # --- End Ensure Load ---

                # --- Scroll to Bottom Gradually to Trigger Dynamic Loading ---
                try:
                    print(f"  -> Gradually scrolling to bottom of page for {listing_id}...")
                    page_height = await page.evaluate("document.body.scrollHeight")
                    viewport_height = await page.evaluate("window.innerHeight")
                    scroll_increment = int(viewport_height / 3) # Scroll 1/3 of viewport height each time
                    scroll_delay_ms = 800 # Wait 0.8 seconds between scrolls

                    current_position = 0
                    while current_position < page_height:
                        next_position = min(current_position + scroll_increment, page_height)
                        await page.evaluate(f"window.scrollTo(0, {next_position});")
                        current_position = next_position
                        await page.wait_for_timeout(scroll_delay_ms)

                    print(f"  -> Finished gradual scrolling for {listing_id}.")
                    # Add a final wait after reaching the bottom to ensure last bits load
                    await page.wait_for_timeout(2000)
                except Exception as e:
                    print(f"  -> Error during gradual scrolling for {listing_id}: {e}. Continuing...")
                # --- End Gradual Scroll ---

                # --- Extract Homes Estimate (After Scrolling) ---
                try:
                    print(f"  -> Trying to extract Homes Estimate for listing ID {listing_id} (after scroll)...")
                    
                    # --- CORRECTED SELECTOR ---
                    # Based on user feedback: div.tm-property-homes-pi-banner-homes-estimate__container
                    # The value is inside a <p class="p-h1"> within this container.
                    estimate_container_locator = page.locator("div.tm-property-homes-pi-banner-homes-estimate__container-left")

                    if await estimate_container_locator.count() > 0:
                        print(f"  -> Found Homes Estimate container, waiting for data to populate for {listing_id}...")
                        # Wait for the container to have text indicating the estimate is loaded (e.g., contains '$')
                        # Correct way to pass arguments to page.wait_for_function
                        await page.wait_for_function(
                            """
                            (selector) => {
                                const el = document.querySelector(selector);
                                return el && el.textContent && (el.textContent.includes('$') || el.textContent.includes('Estimate') || el.textContent.includes('K') || el.textContent.includes('M'));
                            }
                            """,
                            arg="div.tm-property-homes-pi-banner-homes-estimate__container-left", # Pass selector as 'arg'
                            timeout=15000
                        )
                        print(f"  -> Estimate data seems populated for {listing_id}.")

                        # Extract text from the specific <p class="p-h1"> inside the container
                        # The structure is: div.container > div.left > div.title-updated-group > p.p-h1
                        # Or simpler: div.container > ... > p.p-h1
                        # Let's target the P tag directly within the container if possible, or fallback.
                        estimate_value_locator = page.locator("div.tm-property-homes-pi-banner-homes-estimate__container-left p.p-h1")
                        estimate_text = ""
                        if await estimate_value_locator.count() > 0:
                            estimate_text = await estimate_value_locator.text_content(timeout=5000)
                        else:
                            # Fallback to container text if P tag selector fails
                            print(f"  -> P tag for estimate not found directly, trying container text for {listing_id}...")
                            estimate_text = await estimate_container_locator.text_content(timeout=5000)

                        estimate_text = estimate_text.strip() if estimate_text else ""

                        if estimate_text:
                            print(f"  -> Estimate text found: '{estimate_text}' for listing ID {listing_id}")
                            # Pattern like "$1,425,000 - $1,575,000" or "$325K - $365K" or "$1.03M - $1.16M"
                            range_match = re.search(r"\$([0-9,.KkMm]+)\s*[-–—]\s*\$([0-9,.KkMm]+)", estimate_text)
                            if range_match:
                                low_str, high_str = range_match.groups()
                                def parse_estimate_value(val_str):
                                    val_str = val_str.upper().replace(',', '')
                                    if val_str.endswith('K'):
                                        return str(int(float(val_str[:-1]) * 1000))
                                    elif val_str.endswith('M'):
                                        return str(int(float(val_str[:-1]) * 1000000))
                                    else:
                                        return val_str
                                estimate_low_nzd = parse_estimate_value(low_str)
                                estimate_high_nzd = parse_estimate_value(high_str)
                                print(f"  -> Successfully extracted Estimates: Low={estimate_low_nzd}, High={estimate_high_nzd} for listing ID {listing_id}")
                            else:
                                print(f"  -> Estimate text found but couldn't parse range for listing ID {listing_id}")
                        else:
                            print(f"  -> Estimate container found and waited, but text content is still empty for listing ID {listing_id}")
                    else:
                        print(f"  -> Homes Estimate container (div.tm-property-homes-pi-banner-homes-estimate__container) NOT found for listing ID {listing_id}")
                        # --- Debug: Save HTML if estimate container is not found ---
                        # try:
                        #     debug_filename_no_est = f"{OUTPUT_DIR}/debug_sale_no_estimate_corrected_{listing_id}.html"
                        #     with open(debug_filename_no_est, 'w', encoding='utf-8') as f:
                        #         f.write(await page.content())
                        #     print(f"  -> Debug HTML (no estimate corrected) saved to {debug_filename_no_est}")
                        # except Exception as e:
                        #     print(f"  -> Failed to save debug HTML (no estimate corrected): {e}")
                        # --- End Debug ---
                except asyncio.TimeoutError:
                    print(f"  -> Timeout (15s) waiting for estimate data to populate for listing ID {listing_id} (after scroll).")
                    # --- Debug: Save HTML on timeout ---
                    # try:
                    #     debug_filename_est_timeout = f"{OUTPUT_DIR}/debug_sale_estimate_timeout_corrected_{listing_id}.html"
                    #     with open(debug_filename_est_timeout, 'w', encoding='utf-8') as f:
                    #         f.write(await page.content())
                    #     print(f"  -> Debug HTML (estimate timeout corrected) saved to {debug_filename_est_timeout}")
                    # except Exception as e:
                    #     print(f"  -> Failed to save debug HTML (estimate timeout corrected): {e}")
                    # --- End Debug ---
                except Exception as e:
                    print(f"\n⚠️ Error during Homes Estimate extraction for {listing_url} (after scroll): {e}")
                    # --- Debug: Save HTML on general error ---
                    # try:
                    #     debug_filename_est_error = f"{OUTPUT_DIR}/debug_sale_estimate_error_general_corrected_{listing_id}.html"
                    #     with open(debug_filename_est_error, 'w', encoding='utf-8') as f:
                    #         f.write(await page.content())
                    #     print(f"  -> Debug HTML (estimate general error corrected) saved to {debug_filename_est_error}")
                    # except Exception as e:
                    #     print(f"  -> Failed to save debug HTML (estimate general error corrected): {e}")
                    # --- End Debug ---
                # --- End Homes Estimate ---

                # --- Extract Capital Value (After Scrolling) ---
                try:
                    print(f"  -> Trying to extract Capital Value for listing ID {listing_id} (after scroll)...")
                    
                    # 1. Find and click the 'Capital value' tab link
                    # The tab link text is likely 'Capital value'
                    cv_tab = page.locator("a.o-tabs__tab-link:has-text('Capital value')")

                    if await cv_tab.count() > 0 and await cv_tab.is_visible():
                        print(f"  -> Found 'Capital value' tab, clicking for listing ID {listing_id}...")
                        await cv_tab.click()

                        # 2. Wait a moment for the tab switch animation/content start
                        await page.wait_for_timeout(1500) # Increased wait slightly

                        # 3. Locate the container for the CV data using the CORRECTED SELECTOR
                        # Based on user feedback: div.tm-property-homes-pi-banner-capital-value__content
                        # The value is inside a <p class="p-h1"> within a <div class="title-updated-group"> inside this content div.
                        cv_content_locator = page.locator("div.tm-property-homes-pi-banner-capital-value__content")

                        if await cv_content_locator.count() > 0:
                            print(f"  -> Found CV content container, waiting for data to populate for {listing_id}...")
                            # Wait for the content container to have text indicating the CV is loaded (e.g., contains '$')
                            # Correct way to pass arguments to page.wait_for_function
                            await page.wait_for_function(
                                """
                                (selector) => {
                                    const el = document.querySelector(selector);
                                    return el && el.textContent && (el.textContent.includes('$') || el.textContent.includes('Capital Value'));
                                }
                                """,
                                arg="div.tm-property-homes-pi-banner-capital-value__content", # Pass selector as 'arg'
                                timeout=15000
                            )
                            print(f"  -> CV data seems populated for {listing_id}.")

                            # 4. Extract the text content (from the specific P tag inside the title-updated-group)
                            # Target: div.content > div.title-updated-group > p.p-h1
                            cv_value_locator = page.locator("div.tm-property-homes-pi-banner-capital-value__content div.tm-property-homes-pi-banner-capital-value__title-updated-group p.p-h1")
                            cv_text = ""
                            if await cv_value_locator.count() > 0:
                                cv_text = await cv_value_locator.text_content(timeout=5000)
                            else:
                                # Fallback to the content div text if specific P tag not found
                                print(f"  -> Specific P tag for CV not found, trying content div text for {listing_id}...")
                                cv_text = await cv_content_locator.text_content(timeout=5000)

                            cv_text = cv_text.strip() if cv_text else ""

                            if cv_text:
                                print(f"  -> CV text found: '{cv_text}' for listing ID {listing_id}")
                                # Extract numeric part, handling '$' and commas
                                cv_match = re.search(r"\$([0-9,]+)", cv_text)
                                if cv_match:
                                    cv_nzd = cv_match.group(1).replace(",", "")
                                    print(f"  -> Successfully extracted CV: {cv_nzd} for listing ID {listing_id}")
                                else:
                                    print(f"  -> CV text found but couldn't extract numeric value for listing ID {listing_id}")
                            else:
                                print(f"  -> CV content container found, clicked, waited, but text is empty for listing ID {listing_id}")
                        else:
                            print(f"  -> CV content container (div.tm-property-homes-pi-banner-capital-value__content) NOT found after clicking tab for listing ID {listing_id}")
                            # --- Debug: Save HTML if CV container is not found after click ---
                            # try:
                            #     debug_filename_no_cv_cont = f"{OUTPUT_DIR}/debug_sale_no_cv_container_corrected_{listing_id}.html"
                            #     with open(debug_filename_no_cv_cont, 'w', encoding='utf-8') as f:
                            #         f.write(await page.content())
                            #     print(f"  -> Debug HTML (no CV container corrected) saved to {debug_filename_no_cv_cont}")
                            # except Exception as e:
                            #     print(f"  -> Failed to save debug HTML (no CV container corrected): {e}")
                            # --- End Debug ---
                    else:
                        print(f"  -> 'Capital value' tab link NOT found or not visible for listing ID {listing_id}")
                        # --- Debug: Save HTML if CV tab is not found ---
                        # try:
                        #     debug_filename_no_cv_tab = f"{OUTPUT_DIR}/debug_sale_no_cv_tab_corrected_{listing_id}.html"
                        #     with open(debug_filename_no_cv_tab, 'w', encoding='utf-8') as f:
                        #         f.write(await page.content())
                        #     print(f"  -> Debug HTML (no CV tab corrected) saved to {debug_filename_no_cv_tab}")
                        # except Exception as e:
                        #     print(f"  -> Failed to save debug HTML (no CV tab corrected): {e}")
                        # --- End Debug ---
                except asyncio.TimeoutError:
                   print(f"  -> Timeout (15s) waiting for CV data to populate for listing ID {listing_id} (after scroll).")
                   # --- Debug: Save HTML on timeout ---
                   # try:
                   #     debug_filename_cv_timeout = f"{OUTPUT_DIR}/debug_sale_cv_timeout_corrected_{listing_id}.html"
                   #     with open(debug_filename_cv_timeout, 'w', encoding='utf-8') as f:
                   #         f.write(await page.content())
                   #     print(f"  -> Debug HTML (CV timeout corrected) saved to {debug_filename_cv_timeout}")
                   # except Exception as e:
                   #     print(f"  -> Failed to save debug HTML (CV timeout corrected): {e}")
                   # --- End Debug ---
                except Exception as e:
                    print(f"\n⚠️ Error during Capital Value extraction for {listing_url} (after scroll): {e}")
                    # --- Debug: Save HTML on general error ---
                    # try:
                    #     debug_filename_cv_error = f"{OUTPUT_DIR}/debug_sale_cv_error_general_corrected_{listing_id}.html"
                    #     with open(debug_filename_cv_error, 'w', encoding='utf-8') as f:
                    #         f.write(await page.content())
                    #     print(f"  -> Debug HTML (CV general error corrected) saved to {debug_filename_cv_error}")
                    # except Exception as e:
                    #     print(f"  -> Failed to save debug HTML (CV general error corrected): {e}")
                    # --- End Debug ---
                # --- End Capital Value ---

                print(f"  -> Finished data extraction attempts for sale listing ID {listing_id}. CV: {cv_nzd}, Estimates: Low={estimate_low_nzd}, High={estimate_high_nzd}")

           # --- Specific Adjustments ---

            # --- Agent Name(s) ---
            # For rentals: Keep as single string (agent_name)
            # For sales: Attempt to find multiple agents if possible
            agent_name = agency_name = None
            # --- Extract Agent and Agency Names ---
            # For rentals: Typically one agent.
            # For sales: Potentially multiple agents using the same locator.
            agent_names_list = [] # List to hold agent names, especially for sales
            try:
                # Use .all() to get a list of locators for all matching elements
                agent_name_locators = await page.locator("h3.pt-agent-summary__agent-name").all()
                # print(f"Debug: Found {len(agent_name_locators)} agent name locators for {listing_url}") # Debug print
                for locator in agent_name_locators:
                    name_text = await locator.text_content(timeout=5000)
                    cleaned_name = name_text.strip() if name_text else None
                    if cleaned_name:
                        agent_names_list.append(cleaned_name)
            except Exception as e:
                print(f"\n⚠️ Error extracting agent names for {listing_url}: {e}")
                pass # Ignore if agent names cannot be extracted

            # Set agent_name (singular) and agent_names_final based on listing type
            if agent_names_list:
                if listing_type == "rental":
                    # For rentals, use the first name found as the single agent_name
                    agent_name = agent_names_list[0]
                    agent_names_final = agent_name # String for rentals
                else: # listing_type == "sale"
                    # For sales, use the list of names
                    agent_names_final = agent_names_list # List for sales
            else:
                # No agents found
                agent_name = None
                agent_names_final = None # None for both types if no agents

            # Extract Agency Name (typically singular)
            try:
                agency_name_text = await page.locator("h3.pt-agency-summary__agency-name").text_content(timeout=10000)
                agency_name = agency_name_text.strip() if agency_name_text else None
            except Exception as e:
                 print(f"\n⚠️ Error extracting agency name for {listing_url}: {e}")
                 pass # Ignore if agency name not found
            # --- End Agent/Agency Extraction ---

            # --- Extract NEW specific fields for Project Brief ---
            # ... (rest of the field extractions like listing_id, list_date, etc.) ...

            # --- Specific Adjustments ---
            # (The agent_names_final is now correctly set above, so the conditional logic
            # for adding fields to data_entry can remain as previously discussed)
            # --- End Specific Adjustments ---

            # --- Rental Specific Fields ---
            # Define defaults, will be overridden/used only if listing_type is 'rental'
            rent_nzd = weekly_rent # Use the value extracted for rentals
            rent_period = "weekly" if rent_nzd else None # Assuming weekly based on field name and typical NZ rental ads
            # furnished, pets_allowed, available_date, property_id are not scraped, keep as None

            # --- Sales Specific Fields ---
            # Define defaults, values already extracted above for sales
            # sale_type, ask_price_nzd, cv_nzd, estimate_low_nzd, estimate_high_nzd are already set

            # --- End Specific Adjustments ---

            # --- Append data to the global list with NEW structure ---
            # Aligning closely with the Project Brief schema, conditionally including fields
            data_entry = {
                # --- Core Fields (Common) ---
                "listing_id": listing_id,
                "list_date": list_date, # Formatted dd/mm/yyyy
                "status": status, # Currently hardcoded 'active'
                "address": address.strip() if address else None, # Full address/location string
                "suburb": suburb, # Parsed from URL
                "city": city, # Parsed from URL
                "region": region, # Parsed from URL (e.g., Bay Of Plenty)
                "agency_name": agency_name.strip() if agency_name else None,
                # "agency_ref": None, # Explicitly omitted as requested
                # Use the potentially type-specific agent name field
                # "agent_name": agent_name.strip() if agent_name else None, # Old single agent field
                # Use the new potentially multi-agent field, name based on schema (agent_names vs agent_name)
                # The schema suggests 'agent_names' for sales and 'agent_name' for rentals.
                # We can use a conditional key or a single key that handles both.
                # Option 1: Conditional key name (more explicit for schema)
                # **("agent_names" if listing_type == "sale" else "agent_name"): agent_names_final,**
                # Option 2: Single key name (simpler data handling, relies on value type/list content)
                "agent_name": agent_names_final, # This will be a string for rental, list or None for sale
                # --- Property Details (Common) ---
                "property_type": property_type,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "parking_spaces": parking_spaces, # Mapped from parking
                # --- Source (Common) ---
                "source_site": source_site, # Hardcoded 'trademe'
                # --- Metadata (Common) ---
                "URL": listing_url, # Normalized URL
                "Scraped At": datetime.now(timezone(timedelta(hours=12))).astimezone(timezone.utc).isoformat(), # Updated from utcnow
                "Full Description": desc, # Full description text
                "Page Views": page_views, # Extracted number
            }

            # --- Conditionally Add Type-Specific Fields ---
            if listing_type == "rental":
                # Add Rental-Specific fields (exclude sales features)
                data_entry.update({
                    "rent_nzd": rent_nzd, # Assuming weekly rent for rentals
                    "rent_period": rent_period, # Inferred or assumed
                    # Note: As per point 3, features like furnished, pets_allowed, available_date, property_id,
                    # sale_type, ask_price_nzd, cv_nzd, estimate_low_nzd, estimate_high_nzd are omitted for rentals.
                    # They are not added to the data_entry dictionary for rentals.
                })
            elif listing_type == "sale":
                 # Add Sales-Specific fields (exclude rental features)
                 data_entry.update({
                     # Note: As per point 2, features like rent_nzd, rent_period, furnished, pets_allowed,
                     # available_date, property_id are omitted for sales.
                     "sale_type": sale_type, # Parsed (Auction/Tender/Deadline Sale/Price by Negotiation/Fixed Price)
                     "ask_price_nzd": ask_price_nzd, # Parsed for sales
                     "cv_nzd": cv_nzd, # Capital Value, requires tab click
                     "estimate_low_nzd": estimate_low_nzd, # Parsed range low
                     "estimate_high_nzd": estimate_high_nzd, # Parsed range high
                 })
            # --- End Conditionally Adding Fields ---

            DATA.append(data_entry)
            # --- End appending data ---

            print(f"\n✅ Scraped ({listing_type}): {address[:50]}... (ID: {listing_id})") # Print first 50 chars of address and ID

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
                # --- Enhanced Failure Logging ---
                try:
                    # Check if the page content indicates a blocking issue
                    content = await page.content()
                    if "requires javascript" in content.lower() or "upgrade your browser" in content.lower():
                        print(f"   -> Reason: Likely blocked by anti-bot measures (JS required/upgrade browser page).")
                    else:
                        print(f"   -> Reason: Other error during scraping.")
                except:
                    print(f"   -> Reason: Unknown (could not inspect page content).")
                # --- End Enhanced Failure Logging ---
                FAILED.append(listing_url)
                timestamp = datetime.now(timezone(timedelta(hours=12))).astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
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
# --- End updated scrape_listing function ---

# --- New function for progress update ---
async def update_progress():
    global PROCESSED_COUNT
    async with PROGRESS_LOCK:
        PROCESSED_COUNT += 1
        print(f"\rProgress: {PROCESSED_COUNT}/{TOTAL_LISTINGS_TO_SCRAPE}", end="", flush=True)
# --- End new function ---

# --- New function to save collected URLs ---
def save_collected_urls(urls_list, listing_type):
    """Saves the list of collected URLs to a file, differentiated by listing type."""
    specific_collected_urls_file = os.path.join(OUTPUT_DIR, f"collected_{listing_type}_listing_urls.txt")
    try:
        with open(specific_collected_urls_file, "w") as f:
            for url in urls_list:
                f.write(url + "\n")
        print(f"\n🔗 Saved {len(urls_list)} collected {listing_type} URLs to {specific_collected_urls_file}")
    except Exception as e:
        print(f"\n⚠️ Failed to save collected {listing_type} URLs to {specific_collected_urls_file}: {e}")
# --- New function to load collected URLs ---
def load_collected_urls(listing_type):
    """Loads the list of collected URLs from a file, differentiated by listing type."""
    specific_collected_urls_file = os.path.join(OUTPUT_DIR, f"collected_{listing_type}_listing_urls.txt")
    if os.path.exists(specific_collected_urls_file):
        try:
            with open(specific_collected_urls_file, "r") as f:
                urls = [line.strip() for line in f if line.strip()]
            print(f"\n🔗 Loaded {len(urls)} {listing_type} URLs from {specific_collected_urls_file}")
            return urls
        except Exception as e:
            print(f"\n⚠️ Failed to load collected {listing_type} URLs from {specific_collected_urls_file}: {e}")
            return None # Indicate failure to load
    else:
        print(f"\nℹ️ Collected {listing_type} URLs file {specific_collected_urls_file} not found.")
        return None # Indicate file not found


async def collect_listing_urls(page, start_page: int = 1, max_pages: int = 1000): # Default to 1000 or args.max_pages if preferred    urls = set()
    urls = set()
    # --- Use the start_page argument and CURRENT_BASE_URL ---
    page_num = start_page  # Initialize with the provided start page, or 1 if not provided
    local_base_url = CURRENT_BASE_URL # Use the global current base URL
    # --- End modification ---
    while True:
        if max_pages and page_num > max_pages:
            print(f"\n📛 Reached max-pages limit ({max_pages}).")
            break
        print(f"\n🌐 Fetching search page {page_num} for {local_base_url}")
        await asyncio.sleep(random.uniform(1, 2))
        try:
            search_url = f"{local_base_url}?page={page_num}"
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
async def save_chunk(data_chunk, chunk_number, listing_type):
    if not data_chunk:
        print(f"\nℹ️ No data to save for chunk {chunk_number} ({listing_type}).")
        return
    df_chunk = pd.DataFrame(data_chunk)
    chunk_file = os.path.join(OUTPUT_DIR, f"scraped_{listing_type}_data_chunk_{chunk_number}.csv")
    df_chunk.to_csv(chunk_file, index=False)
    print(f"\n💾 Saved chunk {chunk_number} ({listing_type}, {len(data_chunk)} listings) to {chunk_file}")

async def save_temp_data(data_list, listing_type):
    if not data_list:
        print(f"\nℹ️ No temporary {listing_type} data to save.")
        return
    df_temp = pd.DataFrame(data_list)
    temp_file_for_type = os.path.join(OUTPUT_DIR, f"temp_scraped_{listing_type}_data.csv")
    df_temp.to_csv(temp_file_for_type, index=False)
    print(f"\n💾 Saved temporary {listing_type} data ({len(data_list)} listings) to {temp_file_for_type}")
# --- End new function ---

# --- New function to load previously saved data ---
def load_resume_data(listing_type):
    global DATA
    resume_file_for_type = os.path.join(OUTPUT_DIR, f"temp_scraped_{listing_type}_data.csv")
    if os.path.exists(resume_file_for_type):
        try:
            df_resume = pd.read_csv(resume_file_for_type)
            # Filter data for the current listing type if resuming from a mixed file isn't intended
            # For now, assuming separate resume files per type
            DATA = df_resume.to_dict('records')
            print(f"\n🔄 Resumed from {len(DATA)} previously scraped {listing_type} listings in {resume_file_for_type}.")
            return set(item['URL'] for item in DATA if item.get('source_site') == 'trademe') # Return URLs already scraped, filter by source if mixed
            # Or simpler, if files are type-specific: return set(item['URL'] for item in DATA)
        except Exception as e:
             print(f"\n⚠️ Could not load resume data from {resume_file_for_type}: {e}. Starting fresh.")
             return set()
    else:
        print(f"\n🆕 No previous {listing_type} data found. Starting fresh.")
        return set()
# --- End new function ---

# --- Modified main function ---
async def main():
    global DATA, TOTAL_LISTINGS_TO_SCRAPE, CURRENT_BASE_URL, TEMP_SAVE_FILE, COLLECTED_URLS_FILE, RESUME_FILE
    # --- Setup argument parser ---
    parser = argparse.ArgumentParser(description="Scrape Trade Me property listings.")
    parser.add_argument(
        "--skip-url-collection",
        action="store_true",
        help="Skip collecting URLs and load them from the saved file instead (scrapes all loaded URLs).",
    )
    # --- Add the new argument for listing type ---
    parser.add_argument(
        "--listing-type",
        type=str,
        choices=['rental', 'sale', 'all'], # Allow scraping rentals, sales, or both
        default='rental', # Default to rental if not specified
        help="The type of listings to scrape: 'rental', 'sale', or 'all'.",
    )
    # --- Add the start-page argument ---
    parser.add_argument(
        "--start-page",
        type=int,
        default=1, # Default to page 1 if not specified
        help="The search results page number to start collecting URLs from (default: 1).",
    )
    # --- End new arguments ---
    # --- Add the max-pages argument ---
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1000, # Default to 1000 if not specified, or None for no limit
        help="Maximum number of search result pages to scrape per listing type (default: 1000). Set to 0 for no limit.",
    )
    # --- End max-pages argument ---
    args = parser.parse_args()
    # --- End argument parser ---

    # --- Constants for streaming ---
    PAGES_PER_BATCH = 5 # Collect and scrape in batches of 5 pages
    # --- End streaming constants ---

    # --- Determine listing types to scrape ---
    listing_types_to_scrape = []
    if args.listing_type == 'all':
        listing_types_to_scrape = ['rental', 'sale']
    else:
        listing_types_to_scrape = [args.listing_type]
    # --- End determination ---

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True) # Set to False for debugging if needed

        # --- Loop through each listing type ---
        for listing_type in listing_types_to_scrape:
            print(f"\n{'='*20} Starting scrape for {listing_type.upper()} listings {'='*20}")
            
            # --- Update global variables based on listing type ---
            if listing_type == 'rental':
                CURRENT_BASE_URL = BASE_URL_RENTAL
            elif listing_type == 'sale':
                CURRENT_BASE_URL = BASE_URL_SALE
            
            # Update file paths for this listing type
            TEMP_SAVE_FILE = os.path.join(OUTPUT_DIR, f"temp_scraped_{listing_type}_data.csv")
            RESUME_FILE = TEMP_SAVE_FILE
            COLLECTED_URLS_FILE = os.path.join(OUTPUT_DIR, f"collected_{listing_type}_listing_urls.txt")
            # --- End updating globals/files ---
            
            # --- Reset global data structures for this type ---
            # Note: If scraping 'all', you might want to accumulate data or handle separately.
            # For now, we'll process one type at a time, resetting DATA/FAILED for clarity per type.
            # If you want to accumulate, move DATA and FAILED outside this loop.
            global DATA, FAILED, TOTAL_LISTINGS_TO_SCRAPE # Declare globals to reset inside loop
            DATA = [] # Reset DATA for this listing type
            FAILED = [] # Reset FAILED for this listing type
            TOTAL_LISTINGS_TO_SCRAPE = 0 # Reset counter
            
             # --- Load previously scraped data (Resume) for this type ---
            scraped_urls_set = load_resume_data(listing_type) # Load resume data specific to this type
            # --- End loading scraped data ---

            if args.skip_url_collection:
                # --- Existing logic for --skip-url-collection ---
                print(f"\n⏭️ Skipping {listing_type} URL collection, attempting to load from file...")
                loaded_urls = load_collected_urls(listing_type) # Load URLs specific to this type
                if loaded_urls is not None:
                    listing_urls_to_scrape = [url for url in loaded_urls if url not in scraped_urls_set]
                    TOTAL_LISTINGS_TO_SCRAPE = len(listing_urls_to_scrape) # Update global count for progress
                    print(f"\n✅ Loaded {len(loaded_urls)} {listing_type} URLs, {len(listing_urls_to_scrape)} new URLs to scrape.")

                    if not listing_urls_to_scrape:
                        print(f"\n✅ No new {listing_type} listings to scrape based on loaded URLs and resume data.")
                        # Continue to next listing type if any
                        continue 

                    # --- Process ALL loaded URLs in chunks ---
                    print(f"\n🚀 Starting scraping of loaded {listing_type} URLs...")
                    chunk_number = 1
                    for i in range(0, len(listing_urls_to_scrape), SAVE_INTERVAL):
                        chunk_urls = listing_urls_to_scrape[i:i + SAVE_INTERVAL]
                        print(f"\n🚀 Starting scraping chunk {chunk_number} ({len(chunk_urls)} {listing_type} listings)...")
                        #tasks = [asyncio.create_task(scrape_listing(browser, url)) for url in chunk_urls]
                        tasks = []
                        for i, url in enumerate(chunk_urls):
                            # Add a small delay before starting each task (except the first one)
                            # This staggers the initial requests to be more human-like
                            if i > 0: # Don't delay before the very first task in the chunk
                                await asyncio.sleep(TASK_START_DELAY)
                            task = asyncio.create_task(scrape_listing(browser, url))
                            tasks.append(task)
                        await asyncio.gather(*tasks)
                        await save_temp_data(DATA, listing_type) # Save periodically, specific to type
                        print(f"\n🏁 Completed scraping chunk {chunk_number} ({listing_type}).")
                        chunk_number += 1
                    # --- End processing loaded URLs ---
                else:
                    print(f"\n❌ Failed to load {listing_type} URLs from file. Cannot proceed with --skip-url-collection for this type.")
                    # Continue to next listing type if any
                    continue
                # --- End existing logic for --skip-url-collection ---
            else:
                # --- New streaming logic ---
                print(f"\n🌐 Starting streaming collection and scraping for {listing_type}...")
                page = await browser.new_page()

                all_collected_urls = [] # Keep track of all URLs collected so far for final save
                # Use the start-page argument for the initial page number
                page_num = args.start_page # This correctly uses the --start-page argument now
                batch_number = 1

                while True:
                    # Determine the range of pages for this batch
                    start_page = page_num
                    # Collect up to PAGES_PER_BATCH pages, or until MAX_PAGES is reached
                    effective_max_pages = args.max_pages if args.max_pages > 0 else float('inf')
                    end_page = min(page_num + PAGES_PER_BATCH - 1, effective_max_pages)
                    if args.max_pages > 0 and start_page > args.max_pages:
                         print(f"\n📛 Reached --max-pages limit ({args.max_pages}) for {listing_type}.")
                         break

                    print(f"\n🌐 Collecting {listing_type} URL batch {batch_number} (pages {start_page} to {end_page})...")

                    # --- Collect URLs for the current batch ---
                    batch_urls_set = set() # Use a set for this batch to avoid internal duplicates
                    current_page_to_fetch = start_page
                    pages_fetched_in_batch = 0

                    while pages_fetched_in_batch < PAGES_PER_BATCH and (args.max_pages == 0 or current_page_to_fetch <= args.max_pages):
                        print(f"\n🌐 Fetching {listing_type} search page {current_page_to_fetch} for batch {batch_number}")
                        await asyncio.sleep(random.uniform(1, 2))
                        try:
                            search_url = f"{CURRENT_BASE_URL}?page={current_page_to_fetch}" # Use CURRENT_BASE_URL
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
                            print(f"\n⚠️ Pagination error on page {current_page_to_fetch} in batch {batch_number} for {listing_type}: {e}")
                            # Decide: break this batch or try next page? For now, break batch.
                            break # Break the inner loop, might end the batch early

                    # Convert batch set to list for processing
                    batch_urls_list = list(batch_urls_set)
                    print(f"\n🔗 Collected {len(batch_urls_list)} unique {listing_type} URLs in batch {batch_number}.")

                    if not batch_urls_list:
                        print(f"\nℹ️ No {listing_type} URLs collected in batch {batch_number}. Ending collection.")
                        break # No point continuing if no URLs

                    # --- Filter batch URLs against already scraped URLs ---
                    # Important: We filter against the global scraped_urls_set which might grow
                    batch_urls_to_scrape = [url for url in batch_urls_list if url not in scraped_urls_set]
                    print(f"\n🎯 {len(batch_urls_to_scrape)} new {listing_type} URLs in batch {batch_number} to scrape.")

                    if batch_urls_to_scrape:
                        # Update TOTAL_LISTINGS_TO_SCRAPE for progress display (approximation, gets better each batch)
                        # A more accurate way would be to keep a running total, but this is simpler for display
                        # TOTAL_LISTINGS_TO_SCRAPE is now less critical for the overall count as progress is per batch item
                        # Let's keep it as the size of the current batch to scrape for the progress bar relevance
                        current_total_for_progress = len(batch_urls_to_scrape)
                        original_total_to_scrape = TOTAL_LISTINGS_TO_SCRAPE
                        TOTAL_LISTINGS_TO_SCRAPE = current_total_for_progress
                        print(f"\n📈 Progress target for this {listing_type} batch: {current_total_for_progress}")

                        # --- Scrape the URLs collected in this batch ---
                        print(f"\n🚀 Starting scraping batch {batch_number} ({len(batch_urls_to_scrape)} new {listing_type} listings)...")
                        chunk_number = 1
                        # Process the current batch's URLs in smaller SAVE_INTERVAL chunks
                        for i in range(0, len(batch_urls_to_scrape), SAVE_INTERVAL):
                            chunk_urls = batch_urls_to_scrape[i:i + SAVE_INTERVAL]
                            print(f"\n🚀 Starting scraping sub-chunk {chunk_number} of batch {batch_number} ({len(chunk_urls)} {listing_type} listings)...")

                            tasks = []
                            for i, url in enumerate(chunk_urls):
                                if i > 0:
                                    await asyncio.sleep(TASK_START_DELAY)
                                task = asyncio.create_task(scrape_listing(browser, url))
                                tasks.append(task)
                            await asyncio.gather(*tasks)
                            await save_temp_data(DATA, listing_type) # Save periodically, specific to type

                            print(f"\n🏁 Completed scraping sub-chunk {chunk_number} of batch {batch_number} ({listing_type}).")
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
                        print(f"\nℹ️ No new {listing_type} listings to scrape in batch {batch_number}.")

                    batch_number += 1
                    page_num = current_page_to_fetch # Move to the next page after the current batch

                    # Check if we've reached the end condition
                    if args.max_pages and page_num > args.max_pages: # Assuming 0 means no limit
                        print(f"\n📛 Reached --max-pages limit ({args.max_pages}) during {listing_type} batch collection.")
                        break

                # --- End new streaming logic ---
                await page.close()

                # --- Save all collected URLs at the end for this type ---
                if all_collected_urls:
                    save_collected_urls(list(dict.fromkeys(all_collected_urls)), listing_type) # Remove potential duplicates before saving
                # --- End saving collected URLs ---
            
            # --- Final steps for this listing type ---
            # Final save to the main output file for this type
            final_df = pd.DataFrame(DATA)
            final_output_file = os.path.join(OUTPUT_DIR, f"trademe_{listing_type}_listings_final.csv")
            final_df.to_csv(final_output_file, index=False)
            print(f"\n✅ Done with {listing_type}. {len(DATA)} total {listing_type} listings saved to {final_output_file}")

            if FAILED:
                failed_file = os.path.join(OUTPUT_DIR, f"failed_{listing_type}_listings.txt")
                with open(failed_file, "w") as f:
                    f.write("\n".join(FAILED))
                print(f"\n⚠️ {len(FAILED)} failed {listing_type} listings saved to {failed_file}")
                FAILED = [] # Reset FAILED for next type
            else:
                 print(f"\n🎉 No failed {listing_type} listings!")
            # --- End final steps for this type ---
            
        # --- End loop through listing types ---
        await browser.close()

# ... (Include your existing helper functions like update_progress, scrape_listing, collect_listing_urls,
# save_chunk, save_temp_data, load_resume_data, save_collected_urls, load_collected_urls, normalize_trademe_url) ...
# Note: The functions save_chunk and save_temp_data/load_resume_data/save_collected_urls/load_collected_urls have been updated above.

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user.")
    except Exception as e:
        print(f"\n💥 An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
