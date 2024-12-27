import time
import random
import re
import logging
from urllib.parse import urlparse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging with both file and console output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ecommerce_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class EcommerceScraper:
    def __init__(self, headless=False):
        """Initialize the scraper with optional headless mode"""
        self.setup_driver(headless)
        self.seen_domains = set()
        self.blacklist_domains = {
            'facebook.com', 'twitter.com', 'instagram.com',
            'youtube.com', 'pinterest.com', 'linkedin.com'
        }
        self.excel_file = 'australian_ecommerce_businesses.xlsx'
        self.initialize_excel()
        logger.info("Scraper initialized with headless mode: %s", headless)

    def initialize_excel(self):
        """Initialize the Excel file with required columns"""
        df = pd.DataFrame(columns=[
            'website_name', 'domain', 'platform', 'email', 'phone', 'has_contact_info'
        ])
        df.to_excel(self.excel_file, index=False)
        logger.info("Excel file initialized: %s", self.excel_file)

    def append_to_excel(self, data):
        """Append new data to Excel file with error handling"""
        try:
            df = pd.DataFrame([data])
            with pd.ExcelWriter(self.excel_file, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                existing_df = pd.read_excel(self.excel_file)
                updated_df = pd.concat([existing_df, df], ignore_index=True)
                updated_df.to_excel(writer, index=False, sheet_name='Sheet1')
            logger.info("Successfully appended data for domain: %s", data['domain'])
        except Exception as e:
            logger.error("Failed to append data to Excel: %s", str(e))

    def setup_driver(self, headless):
        """Configure and initialize the Chrome WebDriver"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless=new')

        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        logger.info("Chrome WebDriver setup completed")

    def extract_and_validate_phone(self, text):
        """Extract and validate the most likely valid Australian phone number
        Prioritizes mobile numbers starting with +61 or 04
        Returns the single most likely valid number"""
        logger.debug("Starting phone number extraction from text")

        # Define Australian mobile number patterns in order of priority
        mobile_patterns = [
            (r'\+61\s*4\d{2}\s*\d{3}\s*\d{3}', 3),  # +61 format (highest priority)
            (r'04\d{2}\s*\d{3}\s*\d{3}', 2),  # 04 format (second priority)
        ]

        valid_numbers = []
        for pattern, base_score in mobile_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Clean the number and calculate score
                cleaned = ''.join(filter(str.isdigit, match))
                if len(cleaned) == 10 or (len(cleaned) == 11 and cleaned.startswith('61')):
                    score = base_score
                    # Additional scoring based on formatting
                    if ' ' in match: score += 0.5  # Properly spaced numbers
                    valid_numbers.append((match, score))

        if valid_numbers:
            # Sort by score and return the highest scoring number
            best_number = sorted(valid_numbers, key=lambda x: x[1], reverse=True)[0][0]
            # Format the number consistently
            cleaned = ''.join(filter(str.isdigit, best_number))
            if cleaned.startswith('61'):
                formatted = '+' + cleaned[:2] + ' ' + cleaned[2:5] + ' ' + cleaned[5:8] + ' ' + cleaned[8:]
            else:
                formatted = cleaned[:4] + ' ' + cleaned[4:7] + ' ' + cleaned[7:]
            logger.info("Found valid phone number: %s", formatted)
            return formatted

        logger.debug("No valid phone numbers found")
        return None

    def extract_and_validate_email(self, text):
        """Extract and validate the most likely valid business email
        Returns the single most likely valid email"""
        logger.debug("Starting email extraction from text")

        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails = re.findall(email_pattern, text)
        valid_emails = []

        # Priority email prefixes with their scores
        priority_prefixes = {
            'info@': 5,
            'contact@': 5,
            'sales@': 4,
            'hello@': 4,
            'support@': 3,
            'enquiries@': 3,
            'admin@': 2,
            'office@': 2
        }

        for email in set(emails):
            if 5 <= len(email) <= 50:  # Reasonable length check
                email_lower = email.lower()

                # Skip common invalid patterns
                if any(x in email_lower for x in ['example', 'test', 'noreply', 'no-reply', 'donotreply']):
                    continue

                # Calculate score based on prefix
                score = 0
                for prefix, prefix_score in priority_prefixes.items():
                    if email_lower.startswith(prefix):
                        score += prefix_score
                        break

                # Additional scoring factors
                if '@' in email_lower:
                    domain = email_lower.split('@')[1]
                    if '.com.au' in domain: score += 2  # Prefer Australian domains
                    if not any(char.isdigit() for char in email_lower): score += 1  # Prefer no numbers

                valid_emails.append((email, score))

        if valid_emails:
            best_email = sorted(valid_emails, key=lambda x: x[1], reverse=True)[0][0]
            logger.info("Found valid email: %s", best_email)
            return best_email

        logger.debug("No valid emails found")
        return None

    def detect_platform(self, domain, page_source):
        """Detect if the site is powered by Square or Shopify"""
        page_lower = page_source.lower()

        # Check Square indicators
        square_indicators = [
            '.square.site', 'squareup.com', 'square.com',
            'powered by square', 'square online store'
        ]
        for indicator in square_indicators:
            if indicator in page_lower or indicator in domain:
                logger.info("Detected Square platform")
                return 'Square'

        # Check Shopify indicators
        shopify_indicators = [
            '.myshopify.com', 'powered by shopify',
            'cdn.shopify.com', 'shopify.com/checkout'
        ]
        for indicator in shopify_indicators:
            if indicator in page_lower or indicator in domain:
                logger.info("Detected Shopify platform")
                return 'Shopify'

        logger.debug("No platform detected")
        return None

    def is_australian_site(self, domain, page_source):
        """Check if the site is Australian based on various indicators"""
        page_lower = page_source.lower()
        au_indicators = [
            'australia', 'australian', 'sydney', 'melbourne',
            'brisbane', 'perth', 'adelaide', 'gold coast',
            'aud', 'au$', 'australian dollar', '.com.au', '+61'
        ]

        if domain.endswith('.au'):
            logger.info("Australian domain detected: %s", domain)
            return True

        for indicator in au_indicators:
            if indicator in page_lower:
                logger.info("Australian indicator found: %s", indicator)
                return True

        logger.debug("No Australian indicators found")
        return False

    def get_base_domain(self, url):
        """Extract and normalize the base domain from a URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            return domain[4:] if domain.startswith('www.') else domain
        except Exception as e:
            logger.error("Error parsing domain from URL %s: %s", url, str(e))
            return None

    def find_contact_links(self):
        """Find and return relevant contact page links"""
        contact_keywords = ['contact', 'about', 'location', 'support']
        contact_links = []

        for keyword in contact_keywords:
            try:
                links = self.driver.find_elements(
                    By.XPATH,
                    f"//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{keyword}')]"
                )
                contact_links.extend(links)

                href_links = self.driver.find_elements(
                    By.XPATH,
                    f"//a[contains(@href, '{keyword}')]"
                )
                contact_links.extend(href_links)

            except Exception as e:
                logger.error("Error finding contact links with keyword %s: %s", keyword, str(e))
                continue

        # Remove duplicates while preserving order
        seen_urls = set()
        unique_links = []
        for link in contact_links:
            try:
                url = link.get_attribute('href')
                if url and url not in seen_urls and 'mailto:' not in url:
                    seen_urls.add(url)
                    unique_links.append(link)
            except Exception as e:
                logger.error("Error processing contact link: %s", str(e))
                continue

        logger.info("Found %d unique contact links", len(unique_links))
        return unique_links[:4]  # Return up to 4 unique links

    def scrape_website(self, url):
        """Scrape a single website for contact information"""
        main_window = None
        try:
            domain = self.get_base_domain(url)
            if not domain or domain in self.seen_domains:
                logger.debug("Skipping already seen domain: %s", domain)
                return

            self.seen_domains.add(domain)
            logger.info("Starting to scrape domain: %s", domain)

            self.driver.get(url)
            main_window = self.driver.current_window_handle
            time.sleep(random.uniform(1, 2))

            page_source = self.driver.page_source

            # Platform detection
            platform = self.detect_platform(domain, page_source)
            if not platform:
                logger.info("Not a Square/Shopify site: %s", domain)
                return

            # Australian site check
            if not self.is_australian_site(domain, page_source):
                logger.info("Not an Australian site: %s", domain)
                return

            logger.info("Found Australian %s site: %s", platform, domain)

            # Get website title
            title = self.driver.title or domain
            logger.info("Website title: %s", title)

            # Initial contact info collection
            email = self.extract_and_validate_email(page_source)
            phone = self.extract_and_validate_phone(page_source)

            # Check contact pages if needed
            if not email or not phone:
                logger.info("Searching contact pages for additional information")
                contact_links = self.find_contact_links()

                for link in contact_links:
                    try:
                        self.driver.execute_script("window.open(arguments[0]);", link.get_attribute('href'))
                        self.driver.switch_to.window(self.driver.window_handles[-1])
                        time.sleep(random.uniform(1, 2))

                        contact_page_source = self.driver.page_source
                        if not email:
                            email = self.extract_and_validate_email(contact_page_source)
                        if not phone:
                            phone = self.extract_and_validate_phone(contact_page_source)

                        self.driver.close()
                        self.driver.switch_to.window(main_window)

                        if email and phone:
                            break

                    except Exception as e:
                        logger.error("Error processing contact page: %s", str(e))
                        self.cleanup_windows(main_window)
                        continue

            # Prepare and save results
            result = {
                'website_name': title.strip() if title else domain,
                'domain': domain,
                'platform': platform,
                'email': email if email else 'N/A',
                'phone': phone if phone else 'N/A',
                'has_contact_info': 'Yes' if (email or phone) else 'No'
            }

            self.append_to_excel(result)

            # Log success details
            logger.info("Successfully scraped website:")
            logger.info("Domain: %s", domain)
            logger.info("Platform: %s", platform)
            logger.info("Email: %s", email if email else 'N/A')
            logger.info("Phone: %s", phone if phone else 'N/A')

        except Exception as e:
            logger.error("Error scraping website %s: %s", url, str(e))
        finally:
            self.cleanup_windows(main_window)

    def cleanup_windows(self, main_window):
        """Clean up browser windows, ensuring main window remains active"""
        try:
            while len(self.driver.window_handles) > 1:
                self.driver.switch_to.window(self.driver.window_handles[-1])
                self.driver.close()
            if main_window and main_window in self.driver.window_handles:
                self.driver.switch_to.window(main_window)
        except Exception as e:
            logger.error("Error cleaning up windows: %s", str(e))

    def search_websites(self, page_num):
        """Search Google for Square and Shopify websites"""
        search_queries = [
            '"Powered by Square" australia',
            '"square.site" australia',
            '"Powered by Shopify" australia',
            'site:myshopify.com australia',
            '"Powered by Square" australia contact',
            '"Powered by Shopify" australia contact'
        ]

        urls = set()
        for query in search_queries:
            try:
                search_url = f"https://www.google.com/search?q={query}&start={page_num * 10}"
                self.driver.get(search_url)
                time.sleep(random.uniform(1, 2))

                search_results = self.wait.until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.g'))
                )

                for result in search_results:
                    try:
                        link = result.find_element(By.CSS_SELECTOR, 'a')
                        url = link.get_attribute('href')
                        if url and url.startswith('http'):
                            domain = self.get_base_domain(url)
                            if domain and domain not in self.blacklist_domains:
                                urls.add(url)
                                logger.debug("Found potential website: %s", url)
                    except NoSuchElementException:
                        continue

                logger.info("Completed search for query: %s", query)
                time.sleep(random.uniform(1, 2))

            except Exception as e:
                logger.error("Search error for query '%s': %s", query, str(e))
                continue

        logger.info("Found %d unique URLs from search", len(urls))
        return list(urls)

    def run(self, num_pages=2):
        """Main execution method to run the scraper"""
        logger.info("Starting e-commerce website scraping process")
        logger.info("Target: Australian Square and Shopify websites")
        logger.info("Number of search pages to process: %d", num_pages)

        try:
            total_urls = set()

            # Collect URLs from search results
            for page in range(num_pages):
                logger.info("Processing search page %d of %d", page + 1, num_pages)
                urls = self.search_websites(page)
                total_urls.update(urls)
                time.sleep(random.uniform(1, 2))

            logger.info("Total unique websites found: %d", len(total_urls))

            # Process each URL
            for i, url in enumerate(total_urls, 1):
                logger.info("Processing website %d of %d: %s", i, len(total_urls), url)
                self.scrape_website(url)
                time.sleep(random.uniform(1, 2))

            logger.info("Scraping process completed successfully")

        except Exception as e:
            logger.error("Fatal error in scraping process: %s", str(e))
        finally:
            logger.info("Closing WebDriver")
            self.driver.quit()


if __name__ == "__main__":
    try:
        logger.info("Starting e-commerce scraper application")
        scraper = EcommerceScraper(headless=False)
        scraper.run(num_pages=20)
    except Exception as e:
        logger.error("Application failed: %s", str(e))
    finally:
        logger.info("Application terminated")