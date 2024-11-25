import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementClickInterceptedException,
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)


# Implementing this function because it maintains the right parsing for every table row independent of the table contest size (20, 50, 120, etc)
def divide_list(list, rows):
    init = 0
    for i in range(rows):
        stop = init + len(list[i::rows])
        yield list[init:stop]
        init = stop


def create_content_dataframe():
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

    table_data_structured = divide_list(
        table_data_list, rows
    )  # 20 == len(table_data_list)/len(headers_list) == 100/

    table_data_structured = list(table_data_structured)

    content_dataframe = pd.DataFrame(
        table_data_structured, columns=headers_list, index=None
    )
    return content_dataframe


def get_pages():
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
    return int(current_page.text), pagination_list


def wait_time_to_render(driver):
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
    try:
        # Localizar o botão da próxima página
        next_button = driver.find_element(By.CSS_SELECTOR, "li.pagination-next > a")
        # Verificar se o botão está habilitado
        if next_button.is_enabled():
            next_button.click()
            wait_time_to_render(driver)
        else:
            print("Botão de próxima página desativado.")
    except NoSuchElementException:
        print("Botão de próxima página não encontrado.")
    except ElementClickInterceptedException:
        print("Erro ao tentar clicar no botão de próxima página.")


def go_to_previous_page(driver):
    try:
        # Localizar o botão da página anterior
        previous_button = driver.find_element(By.CSS_SELECTOR, "li.pagination-prev > a")
        # Verificar se o botão está habilitado
        if previous_button.is_enabled():
            previous_button.click()
            wait_time_to_render(driver)
        else:
            print("Botão de página anterior desativado.")
    except NoSuchElementException:
        print("Botão de página anterior não encontrado.")
    except ElementClickInterceptedException:
        print("Erro ao tentar clicar no botão de página anterior.")


url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
url_test = "https://web.archive.org/web/20241110001337/https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
driver.get(url_test)
html = driver.page_source

wallet_date = driver.find_element(
    By.XPATH, '//*[@id="divContainerIframeB3"]/div/div[1]/form/h2'
)
wallet_date = wallet_date.text
wallet_date = wallet_date.split(" - ")[1]
wallet_date = pd.to_datetime(wallet_date)
print(wallet_date)

content_dataframe_page_1 = create_content_dataframe()
current_page, page_list = get_pages()
print(
    f"""A extração da página {current_page} deu certo e contém o total de {len(content_dataframe_page_1)}: \n {content_dataframe_page_1[::5]}"""
)

go_to_next_page(driver)
content_dataframe_page_2 = create_content_dataframe()
current_page, page_list = get_pages()
print(f"Indo para a próxima página {current_page}")
print(
    f"""Deu certo e contém o total de {len(content_dataframe_page_2)}: \n {content_dataframe_page_2[:3]}\n"""
)

go_to_next_page(driver)
content_dataframe_page_3 = create_content_dataframe()
current_page, page_list = get_pages()
print(f"Indo para a próxima página {current_page}")
print(
    f"""Deu certo e contém o total de {len(content_dataframe_page_3)}: \n {content_dataframe_page_3[:3]}\n"""
)

go_to_next_page(driver)
content_dataframe_page_4 = create_content_dataframe()
current_page, page_list = get_pages()
print(f"Indo para a próxima página {current_page}")
print(
    f"""Deu certo e contém o total de {len(content_dataframe_page_4)}: \n {content_dataframe_page_4[:3]}\n"""
)

go_to_next_page(driver)
content_dataframe_page_5 = create_content_dataframe()
current_page, page_list = get_pages()
print(f"Indo para a próxima página {current_page}")
print(
    f"""Deu certo e contém o total de {len(content_dataframe_page_5)}: \n {content_dataframe_page_5[:3]}\n"""
)

content_dataframe = pd.concat(
    [
        content_dataframe_page_1,
        content_dataframe_page_2,
        content_dataframe_page_3,
        content_dataframe_page_4,
        content_dataframe_page_5,
    ],
    ignore_index=True,
)

content_dataframe["Data de Extração"] = wallet_date
print(f"\n Agora finalmente o df está da seguinte forma: \n{content_dataframe[:3]}")

content_dataframe.to_parquet("app/data/teste.parquet")
