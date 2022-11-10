from selenium import webdriver
from kindle_clippings_credentials import clippings_password, clippings_username
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

filepath = "D:\documents\My Clippings.txt"

browser = webdriver.Firefox(executable_path='geckodriver.exe')
browser.get('https://my.clippings.io/#/login')

username_input = browser.find_element(by=By.XPATH, value="//input[@placeholder='Adres e-mail lub nazwa użytkownika']")
password_input = browser.find_element(by=By.XPATH, value="//input[@placeholder='Hasło']")

username_input.send_keys(clippings_username)
password_input.send_keys(clippings_password)

login_button = browser.find_element(by=By.XPATH, value="//button[@data-cy='loginbutton']")
login_button.click()

# import_button = browser.find_element_by_id('importbutton')
# import_button.click()

browser.get('https://my.clippings.io/import/file')

upload_input = browser.find_element(by=By.XPATH, value="//input[@type='file']")
upload_input.send_keys(filepath)

WebDriverWait(browser, 30).until(
    EC.visibility_of_element_located((By.XPATH, "//button[text()='Zamknij']"))
)

browser.close()
