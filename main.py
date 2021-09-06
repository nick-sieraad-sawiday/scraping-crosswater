import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from requests_html import HTMLSession
import warnings

warnings.filterwarnings("ignore")

crosswater_urls = ["https://www.crosswater.co.uk/furniture",
                   "https://www.crosswater.co.uk/basins",
                   "https://www.crosswater.co.uk/toilets",
                   "https://www.crosswater.co.uk/enclosures",
                   "https://www.crosswater.co.uk/showers",
                   "https://www.crosswater.co.uk/taps",
                   "https://www.crosswater.co.uk/baths",
                   "https://www.crosswater.co.uk/bathroom-accessories"]


def start_session(url: str):
    """ Starts a session with the website of the competitor

    :param url: The url of the website of the competitor
    :return: The connection with the website of the competitor
    """
    session = HTMLSession()
    response = session.get(url)

    return response


def visit_product_page(max_threads: int, product_urls_list: list, function):
    """ Runs function simultaneously

    :param max_threads: The maximum amount of threads
    :param product_urls_list: List with the url's of the products of the competitor
    :param function: The function that scrapes the competitor
    """
    threads = min(max_threads, len(product_urls_list))

    with ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(function, product_urls_list)


def get_products(url):
    try:
        # starts session and 'visits' product page
        response = start_session(url)
        print(response.status_code, response.url)

        try:
            nr_pages = int(response.html.find(".page-item")[-2].text)
        except:
            nr_pages = 1

        print('Nr. of pages:', nr_pages)

        product_urls_elem = list(response.html.find("#products-wrapper")[0].absolute_links)
        product_url = [url for url in product_urls_elem if "product" in url]
        if nr_pages == 1:
            all_product_url.extend(product_url)
            print('Products:', len(product_url))
        else:
            temp_all_product_url = []
            temp_all_product_url.extend(product_url)
            print('Products:', len(temp_all_product_url))

            for page in range(nr_pages):
                print('Page:', page + 1, '/', nr_pages)
                if page != nr_pages - 1:
                    page_ = page + 2

                    # goes to the next page
                    next_page = url + '?SortBy=1&Page=%s' % page_
                    print(next_page)

                    session = HTMLSession()
                    response = session.get(next_page)

                    product_urls_elem = list(response.html.find("#products-wrapper")[0].absolute_links)
                    product_url = [url for url in product_urls_elem if "product" in url]
                    temp_all_product_url.extend(product_url)
                    print('Products:', len(temp_all_product_url))

            all_product_url.extend(temp_all_product_url)

    except Exception as e:
        print(e)


def run_get_products(main_urls: list):

    all_all_product_url = []
    url_count = 0
    for sub in main_urls:
        url_count += 1
        print('url_count:', url_count)
        global all_product_url
        all_product_url = []
        print(sub)
        visit_product_page(30, [sub], get_products)
        all_all_product_url.append(all_product_url)

    return all_all_product_url


def create_dataframe(main_urls: list, product_url_list: list):

    dataframe = pd.DataFrame()
    dataframe['main_category_url'] = main_urls
    dataframe['product_url'] = product_url_list
    dataframe["main_category"] = [main_cat.split('/')[-1] for main_cat in dataframe["main_category_url"]]
    dataframe = dataframe[["main_category", "product_url"]]

    return dataframe


def stack_urls(dataframe: pd.DataFrame):
    s = dataframe.apply(lambda x: pd.Series(x['product_url']), axis=1).stack().reset_index(level=1, drop=True)
    s.name = 'product_url'
    dataframe["product_url"] = pd.Series(dataframe["product_url"], dtype=object)
    dataframe = dataframe.drop('product_url', axis=1).join(s)
    dataframe = dataframe.reset_index(drop=True)

    return dataframe


all_all_product_url = run_get_products(crosswater_urls)
product_urls = create_dataframe(crosswater_urls, all_all_product_url)
product_urls = stack_urls(product_urls)


def visit_product_page_specs(max_threads: int, product_urls_list: list, main_cats: list, function):
    """ Runs function simultaneously

    :param max_threads: The maximum amount of threads
    :param product_urls_list: List with the urls of the products of the competitor
    :param main_cats: List with the main category urls
    :param function: The function that scrapes the competitor
    """
    threads = min(max_threads, len(product_urls))

    with ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(function, product_urls_list, main_cats)


def get_product_specs(url: str, main_cat: str):
    """Extracts the specifications of the products of the competitor

    :param url: The url of the alternative product of the competitor
    :param main_cat: The main category
    """
    try:
        print(url)
        # starts session and 'visits' product page
        response = start_session(url)

        crosswater_dict[url] = {}
        crosswater_dict[url]["main_category"] = main_cat

        art_nrs = response.html.find(".dashed-area")
        if len(art_nrs) > 2:
            crosswater_dict[url]["art_nrs"] = {}
            for art in range(len(art_nrs) - 2):
                name = art_nrs[art].text.split("\n")[0]
                if name != "Finish":
                    number = art_nrs[art].text.split("\n")[-2]
                    crosswater_dict[url]["art_nrs"].update({name: number})
        else:
            try:
                number = response.html.xpath("//*[@itemprop='identifier']/@content")[0].split("sku:")[-1]
            except:
                number = art_nrs[0].text.split("\n")[0]
            crosswater_dict[url] = {}
            crosswater_dict[url]["art_nrs"] = number

        headers = response.html.xpath("//h4")
        headers = [header.text.split(" ")[-1] for header in headers]

        tables = response.html.find(".table")
        specs = [table.text.split("\n") for table in tables]
        specs = specs[:-1]

        if len(headers) != 0:
            for header, spec in zip(headers, specs):
                for value in range(0, len(spec) - 1, 2):
                    crosswater_dict[url][spec[value] + "_" + header] = spec[value + 1]
        else:
            for spec in specs:
                for value in range(0, len(spec) - 1, 2):
                    crosswater_dict[url][spec[value]] = spec[value + 1]

    except:
        print("error", url)


def run_get_product_specs(dataframe: pd.DataFrame, crosswater_dict: dict):
    visit_product_page_specs(5, list(dataframe["product_url"]), list(dataframe["main_category"]),
                             get_product_specs)

    return crosswater_dict


def create_df_from_dict(dictionary: dict, index: str):
    df = pd.DataFrame.from_dict(dictionary)
    crosswater = df.T.reset_index(0).rename(columns={"index": index})

    return crosswater


crosswater_dict = {}
crosswater_dict = run_get_product_specs(product_urls[:3], crosswater_dict)
crosswater = create_df_from_dict(crosswater_dict, "crosswater_url")
crosswater.to_excel("crosswater_technical_specifications.xlsx", index=False)
