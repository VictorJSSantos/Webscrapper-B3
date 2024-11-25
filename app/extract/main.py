from utils.requisition import *


url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
url_test = "https://web.archive.org/web/20241110001337/https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
driver.get(url_test)
html = driver.page_source

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

wallet_date = driver.find_element(
    By.XPATH, '//*[@id="divContainerIframeB3"]/div/div[1]/form/h2'
)
wallet_date = wallet_date.text
wallet_date = wallet_date.split(" - ")[1]
content_dataframe["Data de Extração"] = wallet_date

print(f"\nAgora finalmente o df está da seguinte forma: \n{content_dataframe[:3]}")
print(50 * ("-\n"))

analytics = pd.DataFrame(content_dataframe.info())
print(analytics)

content_dataframe.to_csv("app/data/teste.csv")
