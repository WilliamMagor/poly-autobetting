"""Browser helpers — Selenium-based portfolio scanning.

Used for checking portfolio positions via the Polymarket web UI when
the CLOB API balance endpoints are unreliable. This is a fallback
mechanism; the bot primarily uses the API for everything.
"""

import logging
import os
import time

log = logging.getLogger("selbot.browser")


def get_driver(headless: bool = True):
    """Create a Selenium Chrome WebDriver.

    Requires chromedriver in PATH or installed via webdriver-manager.
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    try:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
    except ImportError:
        service = Service()

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    return driver


def save_debug_snapshot(driver, label: str, screenshots_dir: str = "selbot/screenshots"):
    """Save a screenshot + page source for debugging failed interactions."""
    os.makedirs(screenshots_dir, exist_ok=True)
    ts = int(time.time())
    try:
        driver.save_screenshot(os.path.join(screenshots_dir, f"{label}_{ts}.png"))
    except Exception as e:
        log.debug("Screenshot failed: %s", e)
    try:
        with open(os.path.join(screenshots_dir, f"debug_{label}_{ts}.html"), "w",
                  encoding="utf-8") as f:
            f.write(driver.page_source)
    except Exception as e:
        log.debug("Page source save failed: %s", e)


def check_portfolio_balance(driver, wallet_address: str) -> dict[str, float]:
    """Check portfolio positions via Polymarket web UI.

    Returns a dict of {token_id: shares} for all active positions.
    This is a fallback — prefer the API-based check_token_balance() in prices.py.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    url = f"https://polymarket.com/portfolio?address={wallet_address}"
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='position-card']"))
        )
    except Exception:
        log.debug("No positions found or page load timeout")
        return {}

    positions = {}
    cards = driver.find_elements(By.CSS_SELECTOR, "[data-testid='position-card']")
    for card in cards:
        try:
            title = card.find_element(By.CSS_SELECTOR, "[data-testid='position-title']").text
            shares_text = card.find_element(By.CSS_SELECTOR, "[data-testid='position-shares']").text
            shares = float(shares_text.replace(",", "").split()[0])
            positions[title] = shares
        except Exception:
            continue

    return positions
