# sprint_12_trending_youtube

Elaborador: Carlos Eduardo Cruz Nakandakare

# ==============================================================================
# 1. Visão Geral do Projeto
# ==============================================================================

Este projeto tem como objetivo analisar o histórico de vídeos de tendências do YouTube, fornecendo insights valiosos para gerentes de planejamento de anúncios em vídeo. O dashboard interativo permite visualizar tendências ao longo do tempo, a distribuição de vídeos por país e uma correspondência detalhada entre categorias de vídeo e países.

A solução é composta por uma pipeline de dados em Python que automatiza o carregamento, pré-processamento e armazenamento dos dados em um banco de dados SQLite, e um dashboard interativo desenvolvido no Tableau Public.

# ==============================================================================
# 2. Objetivo de Negócios
# ==============================================================================

Analisar o histórico de vídeos de tendências no YouTube para apoiar as decisões de gerentes de planejamento de anúncios em vídeo. O dashboard será utilizado pelo menos uma vez ao dia para monitorar e entender as dinâmicas dos vídeos em alta.

# ==============================================================================
# 3. Conteúdo de Dados do Dashboard
# ==============================================================================

O dashboard apresenta as seguintes informações:

Histórico de tendências: Valores absolutos e proporções percentuais de vídeos de tendências, divididos por dia e categoria.

Vídeos de tendências por país: Valores relativos (em %) de vídeos em alta, discriminados por países.

Correspondência entre categorias e países: Uma tabela detalhada exibindo os valores absolutos de vídeos em alta para cada categoria em cada país.

Parâmetros de Agrupamento de Dados
Os dados são agrupados e analisados com base nos seguintes parâmetros:

Data e hora da tendência (trending_date)

Categoria de vídeo (category_title)

País (region)

# ==============================================================================
# 4. Fontes de Dados
# ==============================================================================
Os dados são provenientes de uma tabela agregada chamada trending_by_time, armazenada em um banco de dados SQLite local (youtube.db). A estrutura da tabela é a seguinte:

record_id (chave primária)
region (país / região geográfica)
trending_date (data e hora da tendência)
category_title (a categoria de vídeo)
videos_count (o número de vídeos na seção de tendências)

Os engenheiros de dados prometem criar esta tabela agregada e fornecer os arquivos CSV para o carregamento inicial e subsequentes atualizações.

- Intervalo de Atualização de Dados
Os dados são atualizados uma vez a cada 24 horas, à meia-noite UTC, garantindo que o dashboard sempre exiba informações recentes.

# ==============================================================================
# 5. Aspectos Técnicos (Pipeline Python)
# ==============================================================================
O projeto inclui um script Python (main.py) responsável pela automatização da pipeline de dados. Este script realiza as seguintes etapas:

Análise de Argumentos de Linha de Comando: Permite que o script aceite o caminho de um arquivo CSV como argumento, embora a versão atual itere sobre um diretório de dados.

Extração de Informações: Extrai o ano do nome do arquivo CSV para fins de organização e verificação.

Verificação de Existência de Dados: Antes de carregar novos dados, o script verifica se já existem registros para o ano correspondente no banco de dados, evitando duplicatas.

Carregamento de Dados: Lê os dados de arquivos CSV (espera-se trending_by_time_YYYY.csv) para DataFrames do pandas.

Pré-processamento de Dados:
Converte a coluna trending_date para o formato YYYY-MM-DD.
Converte a coluna videos_count para o tipo inteiro.
(Comentado no código mas com estrutura para futura implementação) Lida com valores marcados com * em certas colunas.

Gerenciamento e Salvemento no Banco de Dados:
Cria ou conecta-se a um banco de dados SQLite (trending_by_time.db) usando SQLAlchemy. O banco de dados é armazenado em uma pasta database/.
Salva os DataFrames pré-processados na tabela trending_by_time do banco de dados, anexando novos dados se a tabela já existir.
Validação de Dados: Após o salvamento, realiza uma validação básica exibindo a contagem de registros por data no banco de dados.

Exportação do Banco de Dados: Exporta todo o conteúdo da tabela trending_by_time para um arquivo CSV (trending_by_time_full_export.csv) na pasta exports/, para facilitar o uso em outras ferramentas ou análises.


# ==============================================================================
# 6. Links Externos
# ==============================================================================
Dashboard no Tableau Public: Visualize o dashboard interativo com os dados analisados.
https://public.tableau.com/views/sprint_12_trending_youtube_rev_1/Dashboard?:language=pt-BR&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

Repositório GitHub: Acesse o código-fonte completo deste projeto.
https://github.com/carlos-ecn/sprint_12_trending_youtube.git
