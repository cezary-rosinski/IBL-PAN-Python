from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
from selenium.common.exceptions import NoSuchElementException

browser = webdriver.Chrome()
browser.get("http://fp.amu.edu.pl/wp-admin/edit-comments.php")

while len(browser.find_elements_by_css_selector('strong')) > 0:
    box = browser.find_element_by_css_selector("#bulk-action-selector-top")
    box.send_keys(Keys.ENTER, Keys.DOWN, Keys.DOWN, Keys.DOWN, Keys.DOWN, Keys.ENTER)
    all_box = browser.find_element_by_css_selector('#cb-select-all-1')
    all_box.click()
    apply_box = browser.find_element_by_css_selector('#doaction')
    apply_box.click()

