from playwright.sync_api import sync_playwright, expect

def main():
    with sync_playwright() as p:
        browser = p.firefox.launch(
            headless=False
        )
        page = browser.new_page()
        page.goto("https://x.com")
        expect(page.locator('a[data-testid="loginButto"]')).to_be_visible()
        pass

if __name__ == "__main__":
    main()