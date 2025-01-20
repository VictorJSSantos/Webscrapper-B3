import pandas as pd
import logging
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementClickInterceptedException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


# Setting basic configs for Logging purposes
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Setting driver configs and starting it
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--incognito")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)


# Implementing this function because it maintains the right parsing for every table row independent of the table contest size (20, 50, 120, etc)
def divide_list(list, rows):
    """
    Divide the list by the number of rows and generate a list of lists
    representing the the sequence of data by row for all the columns

    :param list: List of the data tha will be divided
    :param rows: Quantity of rows the list would have if in a Dataframe

    """

    init = 0
    for i in range(rows):
        stop = init + len(list[i::rows])
        yield list[init:stop]
        init = stop

    logger.info("Lista dividida com sucesso.")


def create_content_dataframe(dataframe: str):
    """
    Perform web scraping on the referred website by locating data elements and organizing the data into a DataFrame.

    :param dataframe (str): The name of the variable that will store the DataFrame. It's only for logging purposes.

    """
    try:
        logger.info(
            f"Iniciando a coleta de informações para montar o DataFrame {dataframe}"
        )
        # Accessing all the table headrs
        headers = driver.find_elements(
            By.XPATH,
            '//*[@id="divContainerIframeB3"]/div/div[1]/form/div[2]/div/table/thead/tr/th',
        )
        # Making a list transforming the elements into strings
        headers_list = [item.text for item in headers]

        # Acessig table content
        table = driver.find_elements(
            By.XPATH,
            '//*[@id="divContainerIframeB3"]/div/div[1]/form/div[2]/div/table/tbody/tr/td',
        )
        # Transform table content in text
        table_data_list = [item.text for item in table]

        data_points = int(len(table_data_list))
        columns = int(len(headers_list))
        rows = int(data_points / columns)

        table_data_structured = divide_list(table_data_list, rows)

        table_data_structured = list(table_data_structured)

        content_dataframe = pd.DataFrame(
            table_data_structured, columns=headers_list, index=None
        )
        logger.info("DataFrame criado com sucesso.")
        return content_dataframe
    except Exception as e:
        logger.error(f"A criação do DataFrame apresentou um erro: {e}", exc_info=True)
        return None


def get_pages():
    """
    Perform web scraping on the referred website by locating elements related to pagination information.

    """
    try:
        # Finding the other pages
        pagination = driver.find_elements(
            By.XPATH,
            '//*[@id="listing_pagination"]/pagination-template/ul/li/a/span[2]',
        )
        # Making a list transforming the elements into strings
        pagination_list = [int(item.text) for item in pagination]

        # Finding the current page
        current_page = driver.find_element(
            By.CSS_SELECTOR, "li.current > span:nth-child(2)"
        )
        logger.info("Páginas coletadas com sucesso.")
        return int(current_page.text), pagination_list
    except Exception as e:
        logger.error(
            f"Não foi possível coletar os headers da tabela: {e}", exc_info=True
        )
        return None


def wait_time_to_render(driver):
    """
    Sets a wait time for the driver to render all elements

    :param driver: Selected driver to perform the web scrapping

    """

    WAIT_TIME = 10
    WebDriverWait(driver, WAIT_TIME).until(
        EC.presence_of_all_elements_located(
            (
                By.XPATH,
                '//*[@id="divContainerIframeB3"]/div/div[1]/form/div[2]/div/table/tbody/tr/td',
            )
        )
    )


# Ir para a próxima página
def go_to_next_page(driver):
    """
    Click on the next page button

    :param driver: Selected driver to perform the web scrapping
    """

    try:
        # Localizar o botão da próxima página
        next_button = driver.find_element(By.CSS_SELECTOR, "li.pagination-next > a")
        # Verificar se o botão está habilitado
        if next_button.is_enabled():
            next_button.click()
            wait_time_to_render(driver)
        else:
            logger.info("Botão de próxima página desativado.")
    except NoSuchElementException as e:
        logger.error(f"Botão de próxima página não encontrado.: {e}", exc_info=True)
    except ElementClickInterceptedException as e:
        logger.error(
            f"Erro ao tentar clicar no botão de próxima página.: {e}", exc_info=True
        )


def go_to_previous_page(driver):
    """
    Click on the previous page button

    :param driver: Selected driver to perform the web scrapping
    """

    try:
        # Localizar o botão da página anterior
        previous_button = driver.find_element(By.CSS_SELECTOR, "li.pagination-prev > a")
        # Verificar se o botão está habilitado
        if previous_button.is_enabled():
            previous_button.click()
            wait_time_to_render(driver)
        else:
            logger.info("Botão de página anterior desativado.")
    except NoSuchElementException as e:
        logger.error(f"Botão de página anterior não encontrado.: {e}", exc_info=True)
    except ElementClickInterceptedException as e:
        logger.error(
            f"Erro ao tentar clicar no botão de página anterior.: {e}", exc_info=True
        )


def get_date():
    """
    Perform web scraping on the referred website by locating a element related to date information.

    """

    try:
        date = driver.find_element(
            By.XPATH, '//*[@id="divContainerIframeB3"]/div/div[1]/form/h2'
        )
        date = date.text
        date = date.split(" - ")[1]
        date = date.replace("/", "-")
        logger.info("Data coletada com sucesso.")
        return date
    except Exception as e:
        logger.error(f"Tivemos um erro ao processar a data.: {e}", exc_info=True)
        return None
