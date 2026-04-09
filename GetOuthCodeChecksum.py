import os
import pyotp
import time
import json
from urllib.parse import urlparse, parse_qs

# Replace with your details as per the PDF instructions
from dotenv import load_dotenv
load_dotenv()

CLIENT_ID    = os.getenv('SHOONYA_VC', "FN176274_U")
USER_ID      = os.getenv('SHOONYA_USER_ID', "FN176274")
PASSWORD     = os.getenv('SHOONYA_PASSWORD', "Hariomom@#1S")
TOTP_SECRET  = os.getenv('SHOONYA_TOTP_KEY', "76F5344DZS3ZBDTVP45UV5327ST4T6X4")
API_SECRET   = os.getenv('SHOONYA_API_KEY', "qLiXlxMB8BNXYJ0bLe2iME8jJkHHjAhmh0RaKgioZ3efrIZAhKh7YrEQLXcnNOmx")
REDIRECT_URL = os.getenv('SHOONYA_REDIRECT_URL', "https://trade.shoonya.com/OAuthlogin")

LOGIN_URL = f"https://trade.shoonya.com/OAuthlogin/authorize/oauth?api_key={CLIENT_ID}&route_to={REDIRECT_URL}"

def _get_auth_code_via_selenium():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import InvalidSessionIdException, WebDriverException

    def scan_network_for_code(driver):
        try:
            logs = driver.get_log("performance")
            for entry in logs:
                try:
                    message = json.loads(entry["message"])["message"]
                    if message.get("method") == "Network.requestWillBeSent":
                        url = message.get("params", {}).get("request", {}).get("url", "")
                        if "code=" in url and "shoonya" in url.lower():
                            parsed = urlparse(url)
                            code   = parse_qs(parsed.query).get("code", [None])[0]
                            if code:
                                return code
                except Exception:
                    continue
        except Exception:
            pass
        return None

    def fast_fill(driver, element, value):
        element.click()
        time.sleep(0.1)
        element.clear()
        element.send_keys(value)
        time.sleep(0.1)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    print("Initializing Headless Chrome for OAuth Login...")
    driver = webdriver.Chrome(options=options)
    wait   = WebDriverWait(driver, 30)

    auth_code = None
    try:
        print("Accessing Shoonya Login Portal...")
        driver.get(LOGIN_URL)

        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='password']")))
        time.sleep(1)

        all_inputs     = driver.find_elements(By.CSS_SELECTOR, "input:not([type='hidden']):not([type='checkbox']):not([type='radio'])")
        visible_inputs = [inp for inp in all_inputs if inp.is_displayed()]

        fast_fill(driver, visible_inputs[0], USER_ID)
        fast_fill(driver, visible_inputs[1], PASSWORD)

        otp_value = pyotp.TOTP(TOTP_SECRET).now()
        fast_fill(driver, visible_inputs[2], otp_value)

        wait.until(EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='LOGIN']"))).click()
        print("Credentials submitted. Waiting for OAuth code...")

        start = time.time()
        while True:
            auth_code = scan_network_for_code(driver)
            if auth_code:
                print(f"Successfully intercepted OAuth Code: {auth_code}")
                break

            if time.time() - start > 60:
                print("[TIMEOUT] Could not capture auth code. Ensure IP is whitelisted.")
                driver.save_screenshot('error_screen.png')
                break
            time.sleep(0.5)

    except Exception as e:
        print(f"Exception during headless login. Saving screenshot... {e}")
        try:
            print("=== PAGE SOURCE ===")
            print(driver.page_source)
            print("=== END PAGE SOURCE ===")
            driver.save_screenshot('error_screen.png')
        except:
            pass
    finally:
        try:
            driver.quit()
        except:
            pass

    return auth_code

if __name__ == "__main__":
    from NorenRestApiPy.NorenApi import NorenApi

    print("--- Starting OAuth Checksum Generation ---")
    auth_code = _get_auth_code_via_selenium()
    
    if auth_code:
        api = NorenApi(host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWClientTP/')
        res = api.getAccessToken(auth_code, API_SECRET, CLIENT_ID, USER_ID)
        if res and len(res) >= 4:
            acc_tok, usrid, ref_tok, actid = res
            print("Access Token Generated successfully!")
            print("Token:", acc_tok)
        else:
            print("Failed to turn Auth Code into Access Token:", res)
