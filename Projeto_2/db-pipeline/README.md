### PROJETO 02 
#### Análise de Dados de Voo

Este projeto tem como objetivo processar, limpar e realizar a engenharia de dados em uma base de dados de voos, utilizando Python. O pipeline desenvolvido realiza várias etapas, incluindo a padronização de dados, verificação de chaves, tratamento de nulos, e criação de novas colunas para análise. O resultado final é armazenado em um banco de dados SQLite.

#### Organização do Projeto

Descrição dos Arquivos:
- assets/utils.py: Contém funções auxiliares para o saneamento e validação dos dados, como exclusão de nulos, padronização de strings, e verificação de chaves;
- data/NyflightsDB.db: Arquivo do banco de dados SQLite onde os dados processados são armazenados;
- data/flights_pipe_log.log: Arquivo de log que registra as atividades realizadas durante o processo de saneamento e análise.
- data/metadados.xlsx: Arquivo Excel que contém os metadados necessários para guiar o tratamento dos dados, como nomes das colunas, tipos de dados, e regras de tolerância de nulos.
- app.py: Script principal que executa todo o pipeline de dados, desde o carregamento dos dados brutos até o armazenamento dos dados processados no banco de dados.
- .env: Arquivo de configuração que armazena variáveis de ambiente, como os caminhos para os arquivos de dados e metadados.
- requirements.txt: Lista de dependências necessárias para rodar o projeto. Deve ser instalado usando pip install -r requirements.txt.

#### Modo de execução:

1. Faça o clone do projeto no git pra sua máquina:

    `git clone https://github.com/camilo-8/Disciplina-Python-for-Data-Engineer.git`

2. Acesse o diretório onde esta seu projeto clonado:
    
    Ex.:

    `cd C:\Users\user\Documents\..\Disciplina-Python-for-Data-Engineer\Projeto2\db-pipeline>`

3. Crie um ambiente virtual:

    `python -m venv env`

4. Ative o ambiente virtual:

    `.\env\Scripts\activate`

5. Instale as biliotecas necessárias:

    `pip install -r requirements.txt`

6. Execute a aplicação:

    `python app.py`
