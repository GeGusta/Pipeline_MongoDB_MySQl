# Pipeline_MongoDB_MySQl
O projeto visa a construção de uma pipeline de dados implementada em python. As informações serão extraídas da API (https://labdados.com/produtos), salvas no MongoDB, tratadas e armazenadas em um banco MySQL.

## Objetivo
Nossa missão é construir uma pipeline de dados que garanta o fluxo contínuo e a disponibilização dos dados de produtos vendidos para as equipes internas. Essa pipeline visa atender a necessidades específicas:
- Para a equipe de Ciência de Dados: Fornecer acesso aos dados brutos extraídos de uma API, armazenados no MongoDB, permitindo análises exploratórias e o desenvolvimento de modelos.
- Para a equipe de Business Intelligence (BI): Disponibilizar os dados transformados e estruturados em tabelas no MySQL, possibilitando a criação de relatórios e dashboards para insights de negócios.

## Método
A construção da pipeline foi feita em python e a seguir tem-se o diagrama dela e tarefas feitas em sequência:
![Pipeline](/modelo_pipeline_mongodb_mysql.png)
- A extração das informações foi feita de uma API;
- O dados foram salvos no MongoDB, após extração. Utilizado MongoDB Atlas para fazer a tarefa. Criado banco "dbprodutos" e coleção "produtos" para armazenar as informações;
- Depois foi feito tratamento dessas informações e salvo em .csv. Foi separado a categoria "Livros" e as compras a partir de 2021, ajustado o formato de data e salvas em arquivos diferentes;
- A partir desses arquivos .csv as informações foram salvas no MySQL em duas tabelas diferentes "tb_livros" e "tb_compras_mais_2021" no banco "dbprodutos".
Para proteção das chave de acesso foi utilizado o método de variáveis de ambiente com arquivos .env e utilização da biblioteca dotenv.

## Resultado
Fizemos uma pipeline que faz a extração dos dados de uma API, salva no MongoDB os dados brutos para o time de Data Science trabalhar, trata essas informações e salva no MySQL para o time de BI fazer os relatórios.
Na pasta Notebooks estão as explorações dos dados e ideias de tratattivas. Na pasta Scripts temos os seguintes arquivos e suas funções:
- extract_and_save_data.py: Faz a extração dos dados da API, cria ou conecta o banco e coleção no Mongo DB e salva as informações nele;
- transform_data.py: separa as informações pertinentes para o time de BI - categoria livros e compras depois de 2021 - e salva em arquivos .csv, que estão na pasta Data;
- save_data_mysql.py: conecta ao MySQL, cria o banco de dados, as tabelas necessárias e salva as informações dos arquivos .csv nelas.
