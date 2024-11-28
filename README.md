<div align="justify">

## Descrição 

O objetivo do presente trabalho é aplicar o conhecimento em Computação em Nuvem que aprendemos ao longo deste módulo para que façamos um pipeline de dados utilizando a AWS, mais precisamente os serviços Glue, Lambda, Catalog e Athena. Faremos um pipeline de dados batch dos dados da Bovespa (B3), onde iremos fazer um ciclo completo de análise: extrair, transformar e carregar.
O primeiro passo será realizar o web scrapping, onde utilizaremos a biblioteca 'Selenium', após isso, iremos fazer o upload do arquivo via biblioteca 'boto3' para o serviço S3 da AWS. Deveremos também configurar a AWS, de forma que quando houver um arquivo sendo adicionado ao S3, ele ative uma 'Lambda Function' que irá ativar um job do AWS Glue para fazer o tratamento dos dados e, por fim, fazer a disponibilização destes dados tratados em outro bucket S3.

## Objetivos

Em linhas gerais, o que devemos garantir neste projeto é o descrito abaixo:
  - O primeiro passo é a realização do webscrapping da página da b3, este [link](https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br). Realizando o webscrapping, devemos salvar os dados em um arquivo parquet; 
  - O segundo passo é a realização da ingestão do arquivo parquet num bucket s3 com partição diária;
  - O terceiro passo é relacionado à ativação de um gatilho no Lambda quando ocorrer a ingestão de arquivos no bucket s3 para que este chame o job de ETL no Glue. A lambda pode ser escrita em qualquer linguagem, ela deve iniciar o job no Glue;
  - O quarto passo é relacionado às especificações do ETL no Glue, onde devemos garantir aconteçam as operações de refinamento solicitadas:

    - A: agrupamento numérico, sumarização, contagem ou soma.

    - B: renomear duas colunas existentes além das de agrupamento.

    - C: realizar um cálculo com campos de data, exemplo, poder ser duração, comparação, diferença entre datas.;
    
  - O quinto passo é que após refinar os dados deveremos colocar como *ponto do job no Glue* a geração de um novo arquivo parquet no bucket 'refined', este arquivo deverá ser particionado por data e pelo nome/abreviação da ação no pregão;
  - O sexto passo é que deveremos garantir, também, que o job no Glue automaticamente deve catalogar os dados no Glue Catalog e criar uma tabela no banco de dados default do Glue Catalog;
  - O sétimo e último passo é relacionado ao acesso à estes dados, devemos garantir que os dados estejam disponíveis e legíveis no Athena. Como opcional podemos fazer um notebook com visualizações gráficas dos dados ingeridos.

Esse projeto está sendo desenvolvido para a realização do Tech Challenge do segundo módulo da Pós-graduação em Engenharia de Machine Learning da FIAP.
  
## Diretórios do projeto

Como o projeto é um exemplo de um ELT, separamos as pastas por contexto, então teremos as pastas conforme a parte do processo que aquele código é responsável por:
  - extract: Contém os arquivos relativos ao web scrapping da página da B3;
    - utils: Contém as funções utilizadas no arquivo main.py da pasta extract;
  - load: Contém os arquivos relativos ao load na AWS e à função Lambda que é acionada na AWS;
  - transform: Contém o arquivo do job Glue que é executado;

## Pré-requisitos

- Python version
> Python 3.11.9

## Setup de Ambiene

1. Realize o clone do repositório:
  > git clone https://github.com/VictorJSSantos/Modulo-1.git

2. Recomendado:: Crie o ambiente virtual: 
  > python -m venv venv

3. Ativando o ambiente virtual: 
No Windows:
  > venv\Scripts\activate
No Linux:
  > source venv/bin/activate

4. Configure o interpretador python no ambiente virtual:
Ctrl + Shift + P para abrir a paleta de comandos.
  > Digite Python: Select Interpreter e escolha o Python dentro da pasta venv.

5. Atualize o pip para garantir a instalação devida das dependências:
  > python -m pip install --upgrade pip

5. Instale as dependências:
  > pip install -r requirements.txt

</div>
