# TradeMe Rental Listings Scraper

This is an asynchronous web scraper built with [Playwright](https://playwright.dev/python/) to collect rental property listings from TradeMe New Zealand. It fetches detailed information from individual property pages with concurrency control, randomized user agents, and polite delays to reduce the chance of being rate-limited or blocked.

---

## Features

- Scrapes rental listings from TradeMe's residential rent search pages.
- Supports scraping up to 50 search pages (configurable).
- Extracts property details including address, price, bedrooms, bathrooms, parking, property type, agent, and agency.
- Implements concurrency control with a semaphore (default 3 concurrent pages).
- Randomizes User-Agent headers and other HTTP headers per browser context.
- Adds randomized delays between requests to mimic human browsing behavior.
- Automatically retries failed pages up to 3 times.
- Saves results to a CSV file (`trademe_rentals.csv`).
- Saves failed URLs and HTML snapshots for troubleshooting.

---

## Requirements

- Python 3.8+
- [Playwright](https://playwright.dev/python/docs/intro) (with browsers installed)
- pandas

---

## Installation

1. Clone this repository or copy the script file.

2. Install dependencies:

```bash
pip install playwright pandas
playwright install
