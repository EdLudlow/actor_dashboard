from bs4 import BeautifulSoup as bs
import requests
import os.path

#Global variables
URL_STEM = 'https://www.bfi.org.uk/'
URL = 'https://www.bfi.org.uk/education-research/film-industry-statistics-research/weekend-box-office-figures'
FILETYPE = '.xls'
OUTPUT_DIRECTORY = '/choose_file_directory/spreadsheet_directory'

#soupify webpage
def get_soup(url):
    return bs(requests.get(url).text, 'html.parser')

#find list of webpages to scrape
def find_web_page_suffixes(soup):
    webpage_suffix_list= []
    container = soup.find('div', attrs = {'class', 'primary box'})
    for webpages in container.find_all("a", text=lambda text: text and "box office reports" in text):
        webpage_suffix_list.append(webpages.get('href'))
    return webpage_suffix_list

#scrape pages for .xls files
def xls_scraper(url):
    for link in get_soup(url).find_all('a'):
        file_name_complete_path = os.path.join(OUTPUT_DIRECTORY, link.text.replace(" ", "_"))
        file_link = link.get('href')
        if FILETYPE in file_link:
            with open (file_name_complete_path, 'wb') as file:
                response = requests.get(file_link)
                file.write(response.content)

if __name__ == '__main__':

    #scrape index page for .xls files
    xls_scraper(URL)

    #find URLs for previous years' box office data
    soup = get_soup(URL)
    webpage_suffixes = find_web_page_suffixes(soup)

    #collect .xls files from those URLs
    for webpage_suffix in webpage_suffixes:
        complete_url = URL_STEM + webpage_suffix
        xls_scraper(complete_url)