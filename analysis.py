import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import socket
import ssl
import requests
from urllib.parse import urlparse
import logging
from datetime import datetime


class WebsiteQualityAnalyzer:
    """
    A class to analyze website quality based on various technical metrics
    including response time, mobile compatibility, and security features.
    """

    def __init__(self, headless=True):
        """
        Initialize the website quality analyzer

        Args:
            headless (bool): Whether to run Chrome in headless mode
        """
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--no-sandbox')

        # Mobile emulation settings
        self.mobile_emulation = {
            "deviceMetrics": {
                "width": 360,
                "height": 640,
                "pixelRatio": 3.0
            },
            "userAgent": ("Mozilla/5.0 (Linux; Android 10; SM-G960U) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/91.0.4472.114 Mobile Safari/537.36")
        }
        self.chrome_options.add_experimental_option("mobileEmulation",
                                                    self.mobile_emulation)

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def analyze_website(self, domain):
        """
        Analyze a single website's quality based on various metrics

        Args:
            domain (str): The domain to analyze

        Returns:
            tuple: (is_quality_site, analysis_results)
                - is_quality_site (bool): Whether the site meets quality standards
                - analysis_results (dict): Detailed analysis metrics
        """
        url = (f"https://{domain}" if not
        domain.startswith(('http://', 'https://')) else domain)
        self.driver = webdriver.Chrome(options=self.chrome_options)
        score = 0
        analysis_results = {
            'domain': domain,
            'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        try:
            # 1. Measure Response Time
            start_time = time.time()
            self.driver.get(url)
            navigation_start = self.driver.execute_script(
                "return window.performance.timing.navigationStart"
            )
            response_end = self.driver.execute_script(
                "return window.performance.timing.responseEnd"
            )
            response_time = response_end - navigation_start
            analysis_results['response_time_ms'] = response_time

            # Score based on response time
            if response_time < 1000:  # Less than 1 second
                score += 15
            elif response_time < 2000:  # Less than 2 seconds
                score += 10
            elif response_time < 3000:  # Less than 3 seconds
                score += 5

            # 2. Analyze JavaScript Files
            scripts = self.driver.find_elements(By.TAG_NAME, 'script')
            js_count = len(scripts)
            analysis_results['javascript_count'] = js_count
            if js_count > 10:  # Complex web applications typically have many JS files
                score += 20

            # 3. Check Third-Party Services Integration
            page_source = self.driver.page_source.lower()
            third_party_services = {
                'google-analytics': 'Google Analytics',
                'googletagmanager': 'Google Tag Manager',
                'facebook': 'Facebook Integration',
                'cloudflare': 'Cloudflare CDN',
                'amazonaws': 'AWS Services',
                'stripe': 'Stripe Payment',
                'paypal': 'PayPal Integration'
            }

            detected_services = []
            for service_key, service_name in third_party_services.items():
                if service_key in page_source:
                    detected_services.append(service_name)
                    score += 5  # 5 points for each third-party service

            analysis_results['detected_services'] = detected_services

            # 4. Check Mobile Responsiveness
            viewport_meta = self.driver.find_elements(
                By.CSS_SELECTOR, 'meta[name="viewport"]'
            )
            responsive_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                'meta[name="viewport"][content*="width=device-width"]'
            )

            if viewport_meta and responsive_elements:
                score += 15
                analysis_results['mobile_friendly'] = True
            else:
                analysis_results['mobile_friendly'] = False

            # 5. Verify SSL Certificate
            try:
                ssl_context = ssl.create_default_context()
                with ssl_context.wrap_socket(
                        socket.socket(),
                        server_hostname=urlparse(url).netloc
                ) as s:
                    s.connect((urlparse(url).netloc, 443))
                    cert = s.getpeercert()
                    if cert:
                        score += 10
                        analysis_results['ssl_secured'] = True
            except Exception as e:
                analysis_results['ssl_secured'] = False
                self.logger.warning(f"SSL verification failed for {domain}: {str(e)}")

            # 6. Check Security Headers
            try:
                response = requests.get(url)
                security_headers = {
                    'Strict-Transport-Security': 'HSTS',
                    'Content-Security-Policy': 'CSP',
                    'X-Frame-Options': 'X-Frame',
                    'X-Content-Type-Options': 'X-Content-Type',
                    'X-XSS-Protection': 'XSS Protection'
                }

                detected_headers = []
                for header, header_name in security_headers.items():
                    if header in response.headers:
                        detected_headers.append(header_name)
                        score += 5  # 5 points for each security header

                analysis_results['security_headers'] = detected_headers
            except Exception as e:
                self.logger.warning(
                    f"Security headers check failed for {domain}: {str(e)}"
                )

            # Calculate final results
            analysis_results['total_score'] = score
            is_quality_site = score > 50  # Threshold can be adjusted

            return is_quality_site, analysis_results

        except Exception as e:
            self.logger.error(f"Error analyzing {domain}: {str(e)}")
            return False, {"error": str(e)}

        finally:
            self.driver.quit()


def analyze_domains_from_excel(input_file, output_file, domain_column='domain'):
    """
    Analyze domains from an Excel file and save results

    Args:
        input_file (str): Path to input Excel file
        output_file (str): Path to output Excel file
        domain_column (str): Name of the column containing domains
    """
    try:
        # Read Excel file
        df = pd.read_excel(input_file)
        analyzer = WebsiteQualityAnalyzer()

        low_quality_sites = []
        analysis_results = []

        # Process each domain
        for index, row in df.iterrows():
            domain = row[domain_column]
            print(f"Analyzing domain: {domain}")

            is_quality, results = analyzer.analyze_website(domain)

            # Store low quality websites
            if not is_quality:
                row_dict = row.to_dict()
                row_dict.update(results)
                low_quality_sites.append(row_dict)

            analysis_results.append(results)

        # Save results
        if low_quality_sites:
            output_df = pd.DataFrame(low_quality_sites)
            output_df.to_excel(output_file, index=False)
            print(f"Analysis complete. Results saved to {output_file}")
        else:
            print("No low quality websites found.")

        # Save complete analysis report
        report_file = output_file.replace('.xlsx', '_detailed_report.xlsx')
        pd.DataFrame(analysis_results).to_excel(report_file, index=False)
        print(f"Detailed analysis report saved to {report_file}")

    except Exception as e:
        print(f"Error during analysis: {str(e)}")


# Usage example
if __name__ == "__main__":
    input_file = "australian_ecommerce_businesses.xlsx"
    output_file = "low_quality_domains.xlsx"
    analyze_domains_from_excel(input_file, output_file)