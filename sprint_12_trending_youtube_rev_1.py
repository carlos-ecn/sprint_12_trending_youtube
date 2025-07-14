import pandas as pd # Biblioteca essencial para manipulação e análise de dados (DataFrames)
import datetime as dt # Módulo para trabalhar com datas e horas (usado indiretamente via pandas)
import numpy as np # Biblioteca para operações numéricas de alto desempenho (não usada diretamente aqui, mas comum)
import matplotlib.pyplot as plt # Biblioteca para criação de gráficos (não usada diretamente aqui, mas comum)
from scipy import stats as st # Biblioteca para estatística (não usada diretamente aqui, mas comum)
import math as mth # Módulo para funções matemáticas (não usado diretamente aqui, mas comum)
import seaborn as sns # Biblioteca para visualização de dados baseada em matplotlib (não usada diretamente aqui, mas comum)
from plotly import graph_objects as go # Biblioteca para gráficos interativos (não usada diretamente aqui, mas comum)
import plotly.express as px # Biblioteca para gráficos interativos simplificados (não usada diretamente aqui, mas comum)
import sys # Módulo para interagir com o interpretador Python (usado para argumentos de linha de comando e sair do script)
import getopt # Módulo para analisar argumentos de linha de comando (para passar o caminho do arquivo)
import re # Módulo para expressões regulares (usado para extrair o ano do nome do arquivo)
import os # Módulo para interagir com o sistema operacional (usado para caminhos de arquivo/diretório)
import sqlalchemy # Biblioteca principal para interagir com bancos de dados relacionais
from sqlalchemy import create_engine, text # Funções específicas do SQLAlchemy para criar o "motor" do DB e executar texto SQL
from sqlalchemy.exc import OperationalError # Exceção específica do SQLAlchemy para erros de operação (ex: DB não encontrado)

# ==============================================================================
# 1. Funções Auxiliares: Análise de Argumentos da Linha de Comando
# ==============================================================================

def parse_arguments():
    """
    Analisa os argumentos passados pela linha de comando para obter o caminho de um arquivo.
    Isso permite que o script seja executado como: python seu_script.py -f caminho/do/arquivo.csv
    ou python seu_script.py --file=caminho/do/arquivo.csv

    Retorna:
        str: O caminho completo para o arquivo fornecido pelo usuário via linha de comando.
             Retorna uma string vazia se nenhum arquivo for especificado.
    """
    # Define as opções que o script aceitará na linha de comando
    # 'f:' indica que -f espera um valor (o caminho do arquivo)
    unixOptions = 'f:'
    # 'file=' indica que --file= espera um valor (o caminho do arquivo)
    gnuOptions = ['file=']

    # sys.argv contém todos os argumentos da linha de comando, onde o primeiro elemento é o nome do script
    fullCmdArguments = sys.argv
    # argumentList exclui o nome do script, pegando apenas os argumentos reais
    argumentList = fullCmdArguments[1:]

    file_path = '' # Inicializa o caminho do arquivo como vazio
    try:
        # getopt.getopt analisa a lista de argumentos com base nas opções definidas
        # Retorna:
        # - arguments: uma lista de tuplas (opção, valor) ex: [('-f', 'meu_arquivo.csv')]
        # - values: uma lista de argumentos restantes não processados (neste caso, esperamos que esteja vazia)
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
        
        # Itera sobre os argumentos encontrados
        for currentArgument, currentValue in arguments:
            # Se o argumento atual for -f ou --file, atribui seu valor ao file_path
            if currentArgument in ('-f', '--file'):
                file_path = currentValue
    except getopt.error as err:
        # Captura erros se houver problemas na análise dos argumentos (ex: opção inválida)
        print(f"Erro ao analisar argumentos: {err}")
        sys.exit(2) # Sai do script com código de erro 2

    return file_path

# ==============================================================================
# 2. Funções Auxiliares: Extração de Informações do Caminho do Arquivo
# ==============================================================================

def extract_year_from_path(file_path):
    """
    Extrai o ano do nome do arquivo.
    Assume que o nome do arquivo segue um padrão como 'trending_by_time_YYYY.csv'.

    Args:
        file_path (str): O caminho completo do arquivo.

    Retorna:
        int: O ano extraído como um número inteiro.
        None: Se o ano não puder ser extraído do caminho do arquivo.
    """
    # Usa uma expressão regular (regex) para encontrar um número de 4 dígitos
    # que está precedido por 'trending_by_time_' e seguido por '.csv' no final do nome.
    # O parênteses em '(\d{4})' captura os 4 dígitos.
    match = re.search(r'trending_by_time_(\d{4})\.csv$', file_path)
    if match:
        # Se um padrão for encontrado, retorna o grupo capturado (os 4 dígitos do ano) como int
        return int(match.group(1))

    # Se o padrão principal não for encontrado, tenta um método de fallback mais genérico
    try:
        # Pega a última parte do caminho (o nome do arquivo), divide pelo '.',
        # pega a primeira parte (o nome sem extensão) e os últimos 4 caracteres.
        # Ex: 'trending_by_time_2018.csv' -> 'trending_by_time_2018' -> '2018'
        year_str = file_path.split('/')[-1].split('.')[0][-4:]
        return int(year_str) # Converte para inteiro
    except (ValueError, IndexError):
        # Captura erros se a conversão para int falhar ou se o índice for inválido
        print(f"[ERRO]: Não foi possível extrair o ano do caminho do arquivo: {file_path}")
        return None # Retorna None em caso de falha

# ==============================================================================
# 3. Funções Auxiliares: Verificação de Existência de Dados no Banco de Dados
# ==============================================================================

def data_already_exist(engine, table_name, year):
    """
    Verifica se já existem dados para um determinado ano na tabela do banco de dados.
    Esta função simplifica a verificação de duplicatas: se qualquer dado para o
    'ano' já estiver no DB, assume-se que o arquivo correspondente já foi processado.

    Args:
        engine (sqlalchemy.engine.base.Engine): O motor de conexão do SQLAlchemy.
        table_name (str): O nome da tabela no banco de dados a ser verificada.
        year (int): O ano que se deseja verificar.

    Retorna:
        bool: True se dados para o ano já existirem na tabela; False caso contrário.
    """
    if year is None:
        print("[AVISO]: Ano não fornecido para verificação de existência de dados. Assumindo que os dados podem não existir.")
        return False
        
    try:
        # Abre uma conexão com o banco de dados usando o 'engine'
        with engine.connect() as connection:
            # Define a consulta SQL para verificar a existência de qualquer registro
            # cuja 'trending_date' comece com o 'ano' fornecido (ex: '2018-%').
            # LIMIT 1 otimiza a consulta, pois só precisamos saber se existe *pelo menos um* registro.
            query = text(f"SELECT 1 FROM {table_name} WHERE trending_date LIKE :year_pattern LIMIT 1")
            
            # Executa a consulta, passando o ano formatado para o parâmetro ':year_pattern'
            result = connection.execute(query, {'year_pattern': f"{year}-%"})
            
            # fetchone() retorna a primeira linha ou None se não houver linhas.
            # exists será True se uma linha for encontrada (ou seja, dados já existem), False caso contrário.
            exists = result.fetchone() is not None
            if exists:
                print(f"[INFO]: Dados para o ano {year} já parecem existir na tabela '{table_name}'.")
            return exists
    except OperationalError as e:
        # Captura erros operacionais (ex: a tabela ainda não existe)
        if "no such table" in str(e).lower():
            print(f"[INFO]: Tabela '{table_name}' não existe ainda. Dados não existem.")
        else:
            print(f"[ERRO]: Erro operacional ao verificar existência de dados: {e}")
        return False
    except Exception as e:
        # Captura qualquer outro erro inesperado
        print(f"[ERRO]: Ocorreu um erro inesperado ao verificar existência de dados: {e}")
        return False

# ==============================================================================
# 4. Funções Auxiliares: Carregamento e Pré-processamento de Dados
# ==============================================================================

def load_data(file_path):
    """
    Carrega dados de um arquivo CSV para um DataFrame do pandas.

    Args:
        file_path (str): O caminho completo do arquivo CSV a ser carregado.

    Retorna:
        pd.DataFrame: O DataFrame contendo os dados do CSV.
        pd.DataFrame vazio: Em caso de erro (ex: arquivo não encontrado).
    """
    try:
        # Tenta ler o arquivo CSV. 'encoding='latin1'' é comum para arquivos brasileiros.
        # 'sep=','' especifica que o separador de colunas é uma vírgula.
        df = pd.read_csv(file_path, encoding='latin1', sep=',')
        return df
    except FileNotFoundError:
        print(f"[ERROR]: Arquivo não encontrado em {file_path}")
        return pd.DataFrame() # Retorna um DataFrame vazio se o arquivo não for encontrado
    except Exception as e:
        print(f"[ERROR]: Erro ao carregar dados de {file_path}: {e}")
        return pd.DataFrame() # Retorna um DataFrame vazio para outros erros

def preprocess_data(df, threshold=0.5):
    """
    Pré-processa o DataFrame. As etapas incluem:
    1. Remover linhas onde a maioria das colunas (baseado no threshold) contêm o caractere '*'.
       (Comentado para simplicidade, mas a estrutura está lá para reativar se precisar)
    2. Converter a coluna 'trending_date' para o tipo datetime e formatar para 'YYYY-MM-DD'.
    3. Converter a coluna 'videos_count' para o tipo inteiro.

    Args:
        df (pd.DataFrame): O DataFrame de entrada a ser pré-processado.
        threshold (float): A proporção de colunas que podem conter '*' antes que a linha seja removida.
                           (Parâmetro mantido, mas lógica de remoção está comentada para simplicidade).

    Retorna:
        pd.DataFrame: O DataFrame limpo e formatado.
    """
    if df.empty:
        print("[AVISO]: DataFrame vazio passado para preprocess_data. Retornando DataFrame vazio.")
        return df

    # Cria uma cópia do DataFrame para evitar o SettingWithCopyWarning
    cleaned_df = df.copy()

    # --- Tratamento da coluna de data e hora ('trending_date') ---
    date_column = 'trending_date'
    if date_column in cleaned_df.columns:
        try:
            # 1. Converter para tipo datetime:
            # pd.to_datetime é robusto e tenta inferir o formato da data e hora.
            cleaned_df[date_column] = pd.to_datetime(cleaned_df[date_column])
            
            # 2. Formatar para 'YYYY-MM-DD':
            # .dt.strftime() permite formatar objetos datetime para strings.
            # '%Y': Ano com 4 dígitos (ex: 2023)
            # '%m': Mês com 2 dígitos (01-12)
            # '%d': Dia do mês com 2 dígitos (01-31)
            # Removemos %H, %M, %S para ter apenas a data.
            cleaned_df[date_column] = cleaned_df[date_column].dt.strftime('%Y-%m-%d')
            print(f"[INFO]: Coluna '{date_column}' convertida e formatada para 'YYYY-MM-DD'.")
        except Exception as e:
            # Se houver qualquer erro na conversão ou formatação, imprime um aviso.
            # A coluna permanecerá no seu tipo/formato original, o que pode causar problemas no DB.
            print(f"[ERRO]: Falha ao converter ou formatar '{date_column}': {e}. Mantendo formato original ou tipo de objeto.")
    else:
        print(f"[AVISO]: Coluna '{date_column}' não encontrada para processamento de data/hora.")

    # --- Conversão da coluna 'videos_count' para inteiro ---
    int_columns_to_convert = ['videos_count']
    for col in int_columns_to_convert:
        if col in cleaned_df.columns:
            # pd.to_numeric: Tenta converter a coluna para números.
            # errors='coerce': Se encontrar um valor não numérico, substitui por NaN (Not a Number).
            # .fillna(0): Preenche quaisquer NaN (criados por 'coerce') com 0.
            # .astype(int): Converte a coluna para o tipo inteiro.
            cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').fillna(0).astype(int)
        else:
            print(f"[AVISO]: Coluna '{col}' não encontrada para conversão para inteiro. Pulando.")
    
    return cleaned_df

# ==============================================================================
# 5. Funções Auxiliares: Gerenciamento e Salveamento no Banco de Dados
# ==============================================================================

def create_db_engine(db_path):
    """
    Cria um "motor" de conexão com o banco de dados SQLite usando SQLAlchemy.
    Garante que o diretório onde o arquivo do banco de dados será armazenado exista.

    Args:
        db_path (str): O caminho completo para o arquivo do banco de dados (ex: 'database/meu_banco.db').

    Retorna:
        sqlalchemy.engine.base.Engine: O objeto 'engine' do SQLAlchemy para interações com o banco de dados.
    """
    # Extrai o caminho do diretório do caminho completo do banco de dados
    db_dir = os.path.dirname(db_path)
    
    # Verifica se o diretório existe e se ele não está vazio (ou seja, se há um diretório a ser criado)
    if db_dir and not os.path.exists(db_dir):
        try:
            # Tenta criar o diretório. 'exist_ok=True' evita um erro se o diretório já existir (caso de corrida)
            os.makedirs(db_dir, exist_ok=True)
            print(f"[INFO]: Diretório do banco de dados criado: {db_dir}")
        except OSError as e:
            # Captura erros se o diretório não puder ser criado (ex: permissões)
            print(f"[ERRO]: Não foi possível criar o diretório {db_dir}: {e}")
            sys.exit(1) # Sai do script, pois o DB não pode ser criado

    # Cria a string de conexão para um banco de dados SQLite
    # 'sqlite:///' é o prefixo para bancos de dados SQLite locais
    connection_string = f'sqlite:///{db_path}'
    try:
        # Cria o 'engine' do SQLAlchemy. Este objeto gerencia a conexão com o DB.
        engine = create_engine(connection_string)
        # Testa a conexão executando uma consulta SQL simples (SELECT 1)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print(f'[INFO]: Conexão com o banco de dados bem-sucedida: {connection_string}')
        return engine
    except OperationalError as e:
        # Captura erros se a conexão com o banco de dados falhar (ex: caminho inválido)
        print(f'[ERRO CRÍTICO]: Falha ao conectar ao banco de dados em {db_path}. Detalhes: {e}')
        print("Por favor, verifique se o caminho está correto e se você tem permissões de escrita.")
        sys.exit(1) # Sai do script, pois o DB é essencial

def save_to_database(df, engine, table_name):
    """
    Salva o DataFrame limpo na tabela SQL especificada no banco de dados.

    Args:
        df (pd.DataFrame): O DataFrame a ser salvo.
        engine (sqlalchemy.engine.base.Engine): O motor de conexão do SQLAlchemy.
        table_name (str): O nome da tabela onde os dados serão salvos.
    """
    if df.empty:
        print("[AVISO]: DataFrame vazio fornecido para salvar no banco de dados. Operação de salvamento ignorada.")
        return

    try:
        # df.to_sql() é um método do pandas para gravar DataFrames em tabelas SQL.
        # name: o nome da tabela no banco de dados.
        # con: a conexão SQLAlchemy (o 'engine').
        # if_exists='append': se a tabela já existe, os novos dados são anexados.
        # index=False: não escreve o índice do DataFrame como uma coluna na tabela SQL.
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"[INFO]: Dados carregados com sucesso na tabela '{table_name}'.")
    except Exception as e:
        # Captura qualquer erro que ocorra durante o salvamento dos dados
        print(f"[ERRO]: Falha ao salvar dados na tabela do banco de dados '{table_name}': {e}")

# ==============================================================================
# 6. Funções Auxiliares: Validação e Exportação do Banco de Dados
# ==============================================================================

def validate_data(engine, table_name):
    """
    Valida os dados no banco de dados, imprimindo a contagem de registros por data.
    Isso ajuda a verificar se os dados foram inseridos corretamente.

    Args:
        engine (sqlalchemy.engine.base.Engine): O motor de conexão do SQLAlchemy.
        table_name (str): O nome da tabela a ser validada.
    """
    print(f"\n--- Validação do Banco de Dados: Registros por data em '{table_name}' ---")
    try:
        with engine.connect() as connection:
            # Consulta SQL para contar o número de registros para cada data
            # GROUP BY trending_date: agrupa os resultados pela coluna de data
            # ORDER BY trending_date: ordena os resultados por data para melhor visualização
            query = text(f'SELECT trending_date, count(*) FROM {table_name} GROUP BY trending_date ORDER BY trending_date')
            result = connection.execute(query) # Executa a consulta
            rows = result.fetchall() # Obtém todas as linhas do resultado

            if rows:
                # Imprime os primeiros 20 resultados para evitar uma saída muito longa
                for row in rows[:20]:
                    print(f"Data: {row[0]}, Registros: {row[1]}")
                if len(rows) > 20:
                    print(f"... e mais {len(rows) - 20} registros de datas.")
            else:
                print(f"Nenhum registro encontrado na tabela '{table_name}'.")
    except OperationalError as e:
        # Captura erro se a tabela não existir, informando que não há dados para validar
        if "no such table" in str(e).lower():
            print(f"[INFO]: A tabela '{table_name}' ainda não existe no banco de dados. Não há dados para validar.")
        else:
            print(f"[ERRO]: Erro operacional inesperado durante a validação do banco de dados: {e}")
    except Exception as e:
        # Captura qualquer outro erro inesperado durante a validação
        print(f"[ERRO]: Ocorreu um erro inesperado durante a validação do banco de dados: {e}")

def export_db_to_csv(engine, table_name, output_csv_path):
    """
    Exporta todo o conteúdo de uma tabela do banco de dados para um arquivo CSV.
    Esta função tenta ler a tabela e, se ela não existir, captura o erro
    e informa ao usuário, em vez de usar `sqlalchemy.inspect`.

    Args:
        engine (sqlalchemy.engine.base.Engine): O motor de conexão do SQLAlchemy.
        table_name (str): O nome da tabela a ser exportada.
        output_csv_path (str): O caminho completo para o arquivo CSV de saída.
    """
    print(f"\n--- Exportando dados da tabela '{table_name}' para CSV ---")
    try:
        # Abre uma conexão com o banco de dados
        with engine.connect() as connection:
            # pd.read_sql_table() lê uma tabela SQL diretamente para um DataFrame.
            # Esta função levantará um erro se a tabela não existir, que será capturado.
            df_export = pd.read_sql_table(table_name, connection)

        if not df_export.empty:
            # Extrai o diretório do caminho de saída CSV
            output_dir = os.path.dirname(output_csv_path)
            # Cria o diretório de saída se ele não existir
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                print(f"[INFO]: Diretório de saída CSV criado: {output_dir}")

            # Salva o DataFrame em um arquivo CSV.
            # index=False: não inclui o índice do DataFrame como uma coluna no CSV.
            # encoding='utf-8': usa codificação UTF-8, padrão para CSVs.
            df_export.to_csv(output_csv_path, index=False, encoding='utf-8')
            print(f"[INFO]: Dados da tabela '{table_name}' exportados com sucesso para '{output_csv_path}'.")
            print(f"[INFO]: {len(df_export)} linhas exportadas.")
        else:
            print(f"[AVISO]: Tabela '{table_name}' está vazia. Nenhum dado exportado para CSV.")
    except Exception as e:
        # Captura qualquer erro durante a exportação.
        # Verifica se o erro é devido à tabela não existir.
        if "no such table" in str(e).lower():
            print(f"[INFO]: Tabela '{table_name}' não existe no banco de dados. Nada para exportar.")
        else:
            print(f"[ERRO]: Falha ao exportar dados para CSV: {e}")

# ==============================================================================
# 7. Lógica Principal de Execução do Script
# ==============================================================================

if __name__ == "__main__":
    """
    Bloco principal de execução do script.
    Gerencia o fluxo de trabalho:
    1. Define caminhos de diretórios e nomes de tabelas.
    2. Cria/conecta ao banco de dados.
    3. Itera sobre os arquivos na pasta 'data/', processando e salvando-os.
    4. Realiza a validação final dos dados no DB.
    5. Exporta o conteúdo do DB para um arquivo CSV.
    """

    # Define o diretório onde os arquivos de dados brutos CSV estão localizados.
    # 'os.path.dirname(__file__)' obtém o diretório do script atual.
    # 'os.path.join()' é usado para construir caminhos de forma segura (compatível com diferentes SOs).
    data_directory = os.path.join(os.path.dirname(__file__), 'data')
    
    # Define o nome da tabela no banco de dados onde os dados serão armazenados.
    table_name = 'trending_by_time' 
    
    # Define o caminho completo para o arquivo do banco de dados SQLite.
    # Ele será criado dentro de uma pasta 'database' ao lado do script.
    db_path = os.path.join(os.path.dirname(__file__), 'database', 'trending_by_time.db')

    # Passo 1: Cria o motor do banco de dados e garante que o diretório 'database/' exista.
    engine = create_db_engine(db_path)

    # Flag para verificar se algum arquivo foi realmente processado e salvo no DB.
    processed_any_file = False
    
    # Passo 2: Verifica se o diretório 'data/' existe. Se não, o script é encerrado.
    if not os.path.exists(data_directory):
        print(f"[ERRO]: Diretório de dados '{data_directory}' não encontrado. Por favor, crie-o e coloque seus arquivos CSV dentro.")
        sys.exit(1) # Sai do script com erro

    print(f"\nIniciando verificação de arquivos em: {data_directory}")
    
    # Passo 3: Itera sobre todos os arquivos e pastas dentro do 'data_directory'.
    for filename in os.listdir(data_directory):
        # Usa regex para filtrar apenas os arquivos CSV que seguem o padrão esperado
        # (ex: 'trending_by_time_2018.csv').
        if re.match(r'trending_by_time_\d{4}\.csv$', filename):
            file_path = os.path.join(data_directory, filename) # Caminho completo para o arquivo atual
            
            print(f"\n--- Processando arquivo: {filename} ---")
            
            # Extrai o ano do nome do arquivo.
            year_from_file = extract_year_from_path(file_path)

            # Se o ano não puder ser extraído, pula para o próximo arquivo.
            if year_from_file is None:
                print(f"[PULAR]: Não foi possível extrair o ano de {filename}. Pulando este arquivo.")
                continue # Vai para a próxima iteração do loop

            # Passo 4: Verifica se dados para este ano já existem no banco de dados.
            # Se existirem, pula o processamento do arquivo para evitar duplicatas.
            if data_already_exist(engine, table_name, year_from_file):
                # A função 'data_already_exist' já imprime a mensagem de INFO.
                continue # Vai para a próxima iteração do loop

            # Se os dados para o ano não existirem (ou a tabela não existir), prossegue.
            print(f"[INFO]: Dados para o ano {year_from_file} de '{filename}' não encontrados no DB. Carregando e processando.")
            
            # Passo 5: Carrega os dados do CSV para um DataFrame.
            raw_data = load_data(file_path)
            
            # Verifica se o DataFrame carregado não está vazio.
            if not raw_data.empty:
                print(f'[INFO]: {len(raw_data)} linhas carregadas de "{filename}". Colunas originais: {raw_data.columns.tolist()}')
                
                # Passo 6: Pré-processa os dados (limpeza, formatação de data, etc.).
                processed_data = preprocess_data(raw_data)
                
                # Verifica se o DataFrame após o pré-processamento não está vazio.
                if not processed_data.empty:
                    # Passo 7: Salva o DataFrame processado no banco de dados.
                    save_to_database(processed_data, engine, table_name)
                    processed_any_file = True # Define a flag como True, pois um arquivo foi processado
                else:
                    print(f"[AVISO]: Dados processados para '{filename}' estão vazios. Não salvando no DB.")
            else:
                print(f"[AVISO]: Falha ao carregar dados de '{filename}'. Pulando pré-processamento e salvamento.")
        else:
            print(f"[INFO]: Pulando arquivo não correspondente ao padrão ('trending_by_time_YYYY.csv'): {filename}")

    # Mensagem final sobre o processamento dos arquivos.
    if not processed_any_file:
        print("\nNenhum arquivo novo foi processado ou salvo no banco de dados.")
    else:
        print("\nProcessamento de todos os arquivos novos/não processados concluído.")
    
    # Passo 8: Realiza uma validação final, imprimindo a contagem de registros por data no DB.
    validate_data(engine, table_name)

    # Passo 9: Exporta o conteúdo completo do banco de dados para um arquivo CSV.
    output_csv_filename = 'trending_by_time_full_export.csv'
    # O arquivo CSV de saída será salvo em uma pasta 'exports' ao lado do script.
    output_csv_path = os.path.join(os.path.dirname(__file__), 'exports', output_csv_filename)
    export_db_to_csv(engine, table_name, output_csv_path)

    print("\nProcessamento finalizado. Verifique a pasta 'exports' para o CSV gerado.")