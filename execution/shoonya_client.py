"""
Shoonya client — OAuth token-first login strategy.

Priority:
  1. Use cached access token from cred.yml (fast, no Selenium)
  2. If token expired → Selenium OAuth flow to get a fresh auth code
  3. Exchange auth code for new token, save to cred.yml

Usage:
    client = ShoonyaClient()
    api    = client.login()   # returns NorenApi or None
"""
import os
import logging
import pyotp
import time
import json
import yaml
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from NorenRestApiPy.NorenApi import NorenApi

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CRED_FILE = Path(__file__).resolve().parent.parent / 'Shoonya_oAuth_API.py-main' / 'cred.yml'

# NorenWClientAPI supports both OAuth token exchange AND market-data calls.
# NorenWClientTP is the old non-OAuth endpoint — getAccessToken fails there.
API_HOST = 'https://api.shoonya.com/NorenWClientAPI/'
API_WS   = 'wss://api.shoonya.com/NorenWSAPI/'


def _load_cred():
    if CRED_FILE.exists():
        with open(CRED_FILE) as f:
            return yaml.safe_load(f) or {}
    return {}


def _save_cred(cred: dict):
    with open(CRED_FILE, 'w') as f:
        yaml.dump(cred, f)


def _test_token(api, token, uid, actid) -> bool:
    """Inject token and make a lightweight API call. Returns True if valid."""
    try:
        api.injectOAuthHeader(token, uid, actid)
        ret = api.searchscrip(exchange='NFO', searchtext='SBIN')
        return ret is not None and ret.get('stat') == 'Ok'
    except Exception:
        return False


def _selenium_get_auth_code(client_id, user_id, password, totp_key):
    """Headless Chrome OAuth flow. Returns auth code or None."""
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.error("selenium not installed — cannot do OAuth refresh")
        return None

    login_url = (f"https://trade.shoonya.com/OAuthlogin/investor-entry-level/login"
                 f"?api_key={client_id}&route_to={user_id}")

    def scan_network(driver):
        try:
            for entry in driver.get_log("performance"):
                try:
                    msg = json.loads(entry["message"])["message"]
                    if msg.get("method") == "Network.requestWillBeSent":
                        url = msg.get("params", {}).get("request", {}).get("url", "")
                        if "code=" in url and "shoonya" in url.lower():
                            code = parse_qs(urlparse(url).query).get("code", [None])[0]
                            if code:
                                return code
                except Exception:
                    continue
        except Exception:
            pass
        return None

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    logger.info("Starting headless Chrome for OAuth token refresh...")
    driver = webdriver.Chrome(options=options)
    wait   = WebDriverWait(driver, 30)
    auth_code = None

    try:
        driver.get(login_url)
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='password']")))
        time.sleep(1)

        inputs = [i for i in driver.find_elements(By.CSS_SELECTOR,
                  "input:not([type='hidden']):not([type='checkbox']):not([type='radio'])")
                  if i.is_displayed()]

        for el, val in zip(inputs[:2], [user_id, password]):
            el.click(); time.sleep(0.1); el.clear(); el.send_keys(val); time.sleep(0.1)

        otp = pyotp.TOTP(totp_key).now()
        inputs[2].click(); time.sleep(0.1); inputs[2].clear(); inputs[2].send_keys(otp)

        wait.until(EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='LOGIN']"))).click()
        logger.info("Credentials submitted — waiting for OAuth code...")

        start = time.time()
        while True:
            auth_code = scan_network(driver)
            if auth_code:
                logger.info("OAuth code captured.")
                break
            if time.time() - start > 90:
                logger.error("Timeout waiting for OAuth code.")
                break
            time.sleep(0.5)

    except Exception as e:
        logger.exception(f"Selenium login error: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return auth_code


class ShoonyaClient:
    def __init__(self):
        from dotenv import load_dotenv
        load_dotenv()
        self.user_id    = os.getenv('SHOONYA_USER_ID')
        self.password   = os.getenv('SHOONYA_PASSWORD')
        self.totp_key   = os.getenv('SHOONYA_TOTP_KEY')
        self.client_id  = os.getenv('SHOONYA_CLIENT_ID', self.user_id)
        self.api_secret = os.getenv('SHOONYA_API_SECRET') or os.getenv('SHOONYA_API_KEY')
        self.api        = NorenApi(host=API_HOST, websocket=API_WS)

    def login(self):
        """Return authenticated NorenApi, or None on failure."""
        cred = _load_cred()

        # ── Step 1: Try cached token ──────────────────────────────────────────
        token  = cred.get('Access_token')
        uid    = cred.get('UID',        self.user_id)
        actid  = cred.get('Account_ID', self.user_id)

        if token:
            logger.info("Testing cached OAuth token from cred.yml...")
            if _test_token(self.api, token, uid, actid):
                logger.info(f"Token valid — logged in as {uid}")
                return self.api
            else:
                logger.warning("Cached token expired or invalid — refreshing via Selenium...")

        # ── Step 2: Selenium OAuth refresh ───────────────────────────────────
        if not all([self.client_id, self.api_secret, self.user_id, self.totp_key]):
            logger.error("Missing credentials in .env — cannot do OAuth refresh")
            return None

        auth_code = _selenium_get_auth_code(
            self.client_id, self.user_id, self.password, self.totp_key
        )

        if not auth_code:
            logger.error("Could not obtain OAuth auth code")
            return None

        # ── Step 3: Exchange auth code for access token ───────────────────────
        try:
            res = self.api.getAccessToken(auth_code, self.api_secret, self.client_id, self.user_id)
            if not res or len(res) < 4:
                logger.error(f"getAccessToken failed: {res}")
                return None

            acc_tok, usrid, ref_tok, actid_new = res
            self.api.injectOAuthHeader(acc_tok, usrid, actid_new)

            # Save fresh token to cred.yml
            cred.update({'Access_token': acc_tok, 'UID': usrid, 'Account_ID': actid_new})
            _save_cred(cred)

            logger.info(f"New token saved. Logged in as {usrid}")
            return self.api

        except Exception as e:
            logger.exception(f"Token exchange failed: {e}")
            return None


if __name__ == "__main__":
    client = ShoonyaClient()
    api = client.login()
    if api:
        print("Login OK")
        ret = api.searchscrip(exchange='NFO', searchtext='SBIN')
        print("Test call:", str(ret)[:200])
    else:
        print("Login FAILED")
