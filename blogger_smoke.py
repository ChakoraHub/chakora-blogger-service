
import os

import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = os.getenv(
    "BLOGGER_BASE_URL",
    "https://www.chakorahub.com"
).rstrip("/")

BLOG_URL = f"{BASE_URL}/blogger"
WAIT = 25


@pytest.fixture
def driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,1000")

    browser = webdriver.Chrome(options=options)
    browser.set_page_load_timeout(45)

    try:
        yield browser
    finally:
        browser.quit()


def wait_loaded(browser):
    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script(
            "return document.readyState"
        ) == "complete"
    )


def test_public_blog_opens_without_login(driver):
    """Verify the public Blogger page loads without signing in."""

    driver.get(BLOG_URL)
    wait_loaded(driver)

    # Verify the Blogger heading is visible.
    heading = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, ".hero-title")
        )
    )

    assert "ChakoraHub Blog" in heading.text

    # Verify the main blog content container exists and is visible.
    blog_container = driver.find_element(
        By.CSS_SELECTOR,
        ".blog-container"
    )
    assert blog_container.is_displayed()

    # The login modal may exist in the HTML, but should not be open
    # automatically when an unauthenticated visitor opens the page.
    open_login_modals = driver.find_elements(
        By.CSS_SELECTOR,
        ".modal-overlay.show"
    )
    assert len(open_login_modals) == 0


def test_maintenance_notice_visible_when_expected(driver):
    """
    Check the maintenance banner when the workflow expects it
    to be enabled. Skip this check during normal operation.
    """

    expected = os.getenv(
        "EXPECT_MAINTENANCE_NOTICE",
        "false"
    ).lower() == "true"

    if not expected:
        pytest.skip(
            "Maintenance notice is not expected for this run."
        )

    driver.get(BLOG_URL)
    wait_loaded(driver)

    notice = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.ID, "blogger-maintenance-notice")
        )
    )

    assert "Scheduled maintenance notice" in notice.text