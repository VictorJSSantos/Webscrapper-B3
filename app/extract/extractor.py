from extract.utils.requisition import *


def extract_and_save() -> None:

    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    driver.get(url)
    wait_time_to_render(driver)
    html = driver.page_source

    # Get info about pagination
    current_page, page_list = get_pages()
    page_list.insert(0, current_page)
    max_page = max(page_list)
    logger.info(f"O numero de páginas é {max_page}. A lista é {page_list}")

    # Webscrapping data from the website and setting a DataFrame
    dataframes_lists = []
    for i in range(1, max_page + 1):
        content_dataframe = create_content_dataframe(f"content_dataframe_page_{i}")
        dataframes_lists.append(content_dataframe)
        if i < max_page:
            go_to_next_page(driver)

    content_dataframe = pd.concat(dataframes_lists, ignore_index=True)

    # Adding date info and renaming columns
    wallet_date = get_date()
    content_dataframe["info_extraction_date"] = wallet_date
    content_dataframe = content_dataframe.rename(
        {
            "Código": "codigo",
            "Ação": "acao",
            "Tipo": "tipo",
            "Qtde. Teórica": "qtde_teorica",
            "Part. (%)": "participacao_percentual",
        },
        axis=1,
    )

    # Formatting columns
    content_dataframe["qtde_teorica"] = (
        content_dataframe["qtde_teorica"].str.replace(".", "", regex=False).astype(int)
    )
    content_dataframe["participacao_percentual"] = (
        content_dataframe["participacao_percentual"]
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

    content_dataframe.to_parquet(f"app/data/{wallet_date}.parquet.gzip")


if __name__ == "__main__":
    extract_and_save()
