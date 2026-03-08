import logging as log
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC

class AiopsUI:
    def __init__(self, base_url, headless=False):
        options = Options()
        if headless:
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
        self.driver = webdriver.Chrome(options=options)
        self.base_url = base_url
    def goto(self, route=None):
        if route:
            self.driver.get(f"{self.base_url}{route}")
        else:
            self.driver.get(self.base_url)

    def wait_for_element(self, locator, timeout=20):
        return WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located(locator))

    def wait_for_element_invisibility(self, locator, timeout=20, state='open'):
        overlay_xpath = f"//div[@data-state='{state}']"
        WebDriverWait(self.driver, timeout).until(EC.invisibility_of_element((By.XPATH, overlay_xpath)))
        return self.wait_for_element(locator)

    def login(self, username, password):
        oidc_element = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Sign in with IBM OIDC')]")
        if oidc_element:
            oidc_element.click()
            log.info(f'Required OIDC login with credentials: '
                     f'User: {username} & Pass: {"*" * len(password)}')
            self.wait_for_element((By.CSS_SELECTOR, "input[id='username']")).send_keys(username)
            self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
            self.wait_for_element((By.XPATH, "//label[contains(text(), 'Use your w3id and password.')]")).click()
            self.wait_for_element((By.CSS_SELECTOR, "input[name='username']")).send_keys(username)
            self.wait_for_element((By.CSS_SELECTOR, "input[name='password']")).send_keys(password)
            self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
            self.wait_for_element((By.XPATH, "//*[contains(text(), 'Resolution Assistant')]"))

        else:
            log.info("Already logged into IRA tool page successful")
    def close(self):
        self.driver.close()
    def click_button(self, route):
        locator = self.wait_for_element((By.XPATH, f"//button[contains(text(), '{route}')]"))
        locator.click()
    def click_dropdown(self):
        locator = self.wait_for_element((By.XPATH, "//button[@role='combobox']"))
        #locator = self.wait_for_element((By.XPATH, f"//button[.//span[text()='{route}']]"))
        self.driver.execute_script("arguments[0].click();", locator)
    """
    def click_dropdown(self):
        locator = self.wait_for_element((By.XPATH, "//svg[contains(@class, 'lucide-chevron-down')]"))
        locator.click()
    """
    def close_window(self):
        self.wait_for_element((By.XPATH, "//button[./span[text()='Close']]")).click()
