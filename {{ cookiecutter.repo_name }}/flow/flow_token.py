import os
import time
import jwt
from dotenv import set_key, load_dotenv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()

chrome_options = Options()
chrome_options.add_argument("--start-maximized")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.get(os.environ.get("FLOW_LOGIN_URL", "https://.com/account/sign-in"))

print("➡️ Faça login na janela aberta...")

flow_token = None
flow_tenant = None
max_wait = 120
start = time.time()

while time.time() - start < max_wait:
    try:
        cookies = driver.get_cookies() or [] 
        for cookie in cookies:
            name = cookie.get("name")
            if name == "FlowToken":
                flow_token = cookie.get("value")
            elif name == "FlowTenant":
                flow_tenant = cookie.get("value")
        if flow_token and flow_tenant:
            break
    except Exception as e:
        print("Erro ao tentar ler cookies:", e)
    time.sleep(2)

driver.quit()

print("\nCookies capturados:")
print("FlowToken:", flow_token[:40] + "..." if flow_token else "NÃO encontrado")
print("FlowTenant:", flow_tenant or "NÃO encontrado")

if flow_token:
    try:
        decoded = jwt.decode(flow_token, options={"verify_signature": False})
        if datetime.fromtimestamp(decoded["exp"]) > datetime.now():
            set_key(".env", "FLOW_TOKEN", flow_token)
            print("FlowToken salvo no .env")
        else:
            print("FlowToken expirado")
    except Exception as e:
        print("Erro ao decodificar token:", e)

if flow_tenant:
    set_key(".env", "FLOW_TENANT", flow_tenant)
    print("FlowTenant salvo no .env")

if not flow_token or not flow_tenant:
    print("Não foi possível capturar os cookies necessários.")
