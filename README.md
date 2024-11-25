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

## Integrantes

Esse projeto está sendo desenvolvido com o objetivo de ser o entregável do segundo módulo da Pós-graduação em Engenharia de Machine Learning da FIAP. 
O grupo que está desenvolvendo este projeto é composto pelas pessoas listadas a seguir:
  - Alex Barros
  - Janis Silva
  - Tatiana Haddad
  - Victor Santos

</div>
