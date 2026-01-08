#######################################################
#                   OBJETIVO DO ESTUDO
# Investigar quais ocupações financeiras e contábeis cresceram ou perderam participação
# nos estados brasileiros entre 2019 e 2023 e como essas mudanças se relacionam com a
# evolução dos salários médios no setor.
#
# Elisangela Gelatti — contato: contato@elisangelagelatti
#__________________________________________________________
#
# Este script contempla:
# - ETAPA III — Tratamento de dados (R)
#   (Para replicar: realizar a extração no BigQuery/RAIS — ETAPAS I e II (SQL) — 
#   ou utilizar os arquivos compartilhados no Google Drive indicados neste script.)
# - ETAPA IV — Análise e visualização dos dados
######################################################


#######################################################
#Pacotes  
library(googledrive)
library(data.table)
library(tidyverse)  # dplyr, tidyr, ggplot2...
library(tibble)
library(writexl)
library(gt)
library(sf)
library(ggpubr)
library(geobr)
library(scales)
library(showtext)
library(readxl)
library(glue)
library(rlang)
#######################################################

#######################################################
# ETAPA III — TRATAMENTO DE DADOS (R)
# (Após a extração no BigQuery/RAIS e exportação do arquivo)
# Objetivo: baixar os arquivos compartilhados no Google Drive e carregar no R para tratamento
#######################################################

#------------------------------------------------------
# 1) CARREGAMENTO DOS DADOS (BigQuery -> Google Drive → R)
# Observação: os arquivos utilizados nesta etapa foram disponibilizados no Google Drive
# e compartilhados para fins de replicação.
#------------------------------------------------------

## 1.1) Autenticação no Google Drive
# Ao executar, uma janela será aberta para login e autorização de acesso.
drive_auth()  # conta utilizada: trabalhor296@gmail.com -> 2

## 1.2) Dicionário oficial de variáveis da RAIS (Base dos Dados)
file_id_dicionario <- "1c24rMKJR5xw99XnAI-WdekQnV5spvA8MII0B42PsTco" # dicionário no Google Drive
drive_download(as_id(file_id_dicionario), path = "dicionario_rais.csv", overwrite = TRUE) # Baixar o dicionários
dicionario <- fread("dicionario_rais.csv") # Carregar no R
head(dicionario) #ver rapidamente as primeiras linhas

##1.3) Dicionário de ocupações (Financeiro-Contábil) — classificação do projeto (Base dos Dados)
file_id_dicionariofc <- "179QZ3NvYf5djefO_sFGxZw_fVwBy7RC1"
drive_download(as_id(file_id_dicionariofc), path = "dicionario_ocupacao_fc.xlsx", overwrite = TRUE)
dicionariofc <- read_excel("dicionario_ocupacao_fc.xlsx")
head(dicionariofc)

##1.4) Dados da Rais 
file_id_dados <- "1RrYOqR11ylmYcC7l1yQIXubG8SgQYrv4"
drive_download(as_id(file_id_dados), path = "dados_brasil.csv", overwrite = TRUE)
dados <- fread("dados_brasil.csv")
head(dados)

#------------------------------------------------------
# 2) TRATAMENTO DOS DADOS
#------------------------------------------------------

## 2.1) Estrutura dos dados: checagem inicial / explorar os dados
glimpse(dados)
summary(dados)  # Resumo estatístico
n_distinct(dados$cbo_2002)  # Quantas ocupações diferentes existem
n_distinct(dados$sigla_uf)  # Ver quantos estados têm na base
unique(dados$sigla_uf)      # Ver siglas de estados

## 2.2) Remover registros onde UF está como "IGNORADO"
dados <- dados[dados$sigla_uf != "IGNORADO", ]
n_distinct(dados$sigla_uf)
unique(dados$sigla_uf)

## 2.3) Confirmando CBO FC e o público-alvo (18 a 64 anos)
# (Mesmo que os dados tenham sido extraídos com esses filtros, esta etapa funciona como validação.)
cbos_fc <- c(
  252505, 411050, 252510, 252515, 252525, 252530, 252535, 252540, 252545, 413205, 252205, 413110,
  413115, 413210, 353235, 421305, 421310, 413215, 413220, 252210, 253305, 123105, 123110, 122705,
  122720, 122725, 122730, 122735, 122715, 122740, 122745, 122710, 122750, 122755, 123115, 251215,
  413225, 142105, 141710, 141715, 253205, 253210, 253215, 141720, 141725, 141730, 253220, 141705,
  141735, 142110, 142115, 413230, 253225, 252215, 410210, 420110, 410215, 410220, 410225, 410230,
  410235, 353205, 353210, 353215, 353220, 353225, 142120, 353230
)

# Filtrar base para manter apenas trabalhadores do setor financeiro-contábil e idade 18–64
dados_FC <- dados %>%
  filter(cbo_2002 %in% cbos_fc) %>%
  filter(idade >= 18 & idade <= 64)

head(dados_FC)

# 2.4) MERGE: fazer o merge com o dicionário de ocupações (classificação)

# Garantir que ambas as colunas estejam como texto
dados_FC$cbo_2002 <- as.character(dados_FC$cbo_2002)
dicionariofc$CBO <- as.character(dicionariofc$CBO)

# Realizar o merge (junção) com o dicionário de ocupações
dados_FC_classificados <- merge(dados_FC, dicionariofc, by.x = "cbo_2002", by.y = "CBO", all.x = TRUE)

# Conferir se todas as ocupações foram classificadas corretamente
table(dados_FC_classificados$CLASSIFICAÇÃO, useNA = "ifany")

## 2.5) Criar coluna de macrorregiões com base na sigla_uf
dados_FC_classificados <- dados_FC_classificados %>%
  mutate(macrorregiao = case_when(
    sigla_uf %in% c("AC", "AP", "AM", "PA", "RO", "RR", "TO") ~ "Norte",
    sigla_uf %in% c("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE") ~ "Nordeste",
    sigla_uf %in% c("DF", "GO", "MT", "MS") ~ "Centro-Oeste",
    sigla_uf %in% c("ES", "MG", "RJ", "SP") ~ "Sudeste",
    sigla_uf %in% c("PR", "RS", "SC") ~ "Sul",
    TRUE ~ "Não Informado"  # caso tenha alguma sigla não reconhecida
  ))

head(dados_FC_classificados %>% select(sigla_uf, macrorregiao))  #Ver os primeiros registros com a nova coluna
unique(dados_FC_classificados$macrorregiao)                      # Listar as macrorregiões únicas presentes
table(dados_FC_classificados$macrorregiao)                       # Ver a distribuição de registros por macrorregião

## 2.6) MAPAS: TRATAR VARIÁVEIS CATEGÓRICAS
### 2.6.1  Criar mapas de categorias a partir do dicionário
mapa_sexo <- dicionario %>%
  filter(nome_coluna == "sexo") %>%
  select(chave, valor) %>%
  deframe()

mapa_raca_cor <- dicionario %>%
  filter(nome_coluna == "raca_cor") %>%
  select(chave, valor) %>%
  deframe()

mapa_grau_instrucao <- dicionario %>%
  filter(nome_coluna == "grau_instrucao_apos_2005") %>%
  select(chave, valor) %>%
  deframe()

mapa_tamanho_estab <- dicionario %>%
  filter(nome_coluna == "tamanho_estabelecimento") %>%
  select(chave, valor) %>%
  deframe()

mapa_faixa_etaria <- dicionario %>%
  filter(nome_coluna == "faixa_etaria") %>%
  select(chave, valor) %>%
  deframe()

mapa_faixa_remuneracao <- dicionario %>%
  filter(nome_coluna == "faixa_remuneracao_media_sm") %>%
  select(chave, valor) %>%
  deframe()

mapa_tipo_estabelecimento <- dicionario %>%
  filter(nome_coluna == "tipo_estabelecimento") %>%
  select(chave, valor) %>%
  deframe()

### 2.6.2 Substituir os códigos pelos valores descritivos
dados_FC_classificados$sexo <- mapa_sexo[as.character(dados_FC_classificados$sexo)]
dados_FC_classificados$raca_cor <- mapa_raca_cor[as.character(dados_FC_classificados$raca_cor)]
dados_FC_classificados$grau_instrucao_apos_2005 <- mapa_grau_instrucao[as.character(dados_FC_classificados$grau_instrucao_apos_2005)]
dados_FC_classificados$tamanho_estabelecimento <- mapa_tamanho_estab[as.character(dados_FC_classificados$tamanho_estabelecimento)]
dados_FC_classificados$faixa_etaria <- mapa_faixa_etaria[as.character(dados_FC_classificados$faixa_etaria)]
dados_FC_classificados$faixa_remuneracao_media_sm <- mapa_faixa_remuneracao[as.character(dados_FC_classificados$faixa_remuneracao_media_sm)]
dados_FC_classificados$tipo_estabelecimento <- mapa_tipo_estabelecimento[ as.character(dados_FC_classificados$tipo_estabelecimento)]

### 2.6.3  Tabelas de frequência
table(dados_FC_classificados$sexo)
table(dados_FC_classificados$raca_cor)
table(dados_FC_classificados$grau_instrucao_apos_2005)
table(dados_FC_classificados$tamanho_estabelecimento)
table(dados_FC_classificados$faixa_etaria)
table(dados_FC_classificados$faixa_remuneracao_media_sm)
table(dados_FC_classificados$tipo_estabelecimento)

### 2.6.4  Verificar categorias únicas
unique(dados_FC_classificados$sexo)
unique(dados_FC_classificados$raca_cor)
unique(dados_FC_classificados$grau_instrucao_apos_2005)
unique(dados_FC_classificados$faixa_etaria)
unique(dados_FC_classificados$faixa_remuneracao_media_sm)
unique(dados_FC_classificados$tipo_estabelecimento)
unique(dados_FC_classificados$tamanho_estabelecimento)
unique(dicionario$nome_coluna)

## 2.7) VERIFICAR N/A OU "OUTROS PROBLEMAS" NOS DADOS          

### 2.7.1) NA (Usando 'useNA = "always"')
table(dados_FC_classificados$sexo, useNA = "always")
table(dados_FC_classificados$raca_cor, useNA = "always")
table(dados_FC_classificados$grau_instrucao_apos_2005, useNA = "always")
table(dados_FC_classificados$faixa_etaria, useNA = "always")
table(dados_FC_classificados$faixa_remuneracao_media_sm, useNA = "always")
table(dados_FC_classificados$tipo_estabelecimento, useNA = "always")
table(dados_FC_classificados$tamanho_estabelecimento, useNA = "always")

## 2.7.2) Substituir "Código não encontrado nos dicionários oficiais" por "Não Informado"
dados_FC_classificados$faixa_remuneracao_media_sm[dados_FC_classificados$faixa_remuneracao_media_sm == 
                                                    "Código não encontrado nos dicionários oficiais."] <- "Não informado"
dados_FC_classificados$tipo_estabelecimento[dados_FC_classificados$tipo_estabelecimento == 
                                              "Código não encontrado nos dicionários oficiais."] <- "Não informado"

table(dados_FC_classificados$tipo_estabelecimento)
table(dados_FC_classificados$faixa_remuneracao_media_sm)

#-------------------------------------------------------------------
# SALVAR BASE FINAL (OPCIONAL)
fwrite(dados_FC_classificados, "dados_FC_tratados_2019_2023.csv")

id_pasta_drive <- "1zPdCSWLuWb2YvxS8dwIslVThET2KdSDE" #"SUBSTITUIR_O_ID_DA_PASTA"
drive_upload(
  media = "dados_FC_tratados_2019_2023.csv",
  path  = as_id(id_pasta_drive),
  overwrite = TRUE
)
#___________________________________________________________________

#######################################################
# ETAPA IV — ANÁLISE DE DADOS E VISUALIZAÇÃO (R)
# *Objetivo: analisar a distribuição e a evolução das ocupações financeiras e contábeis
#            por unidades da federação e Brasil: 2019 a 2023.
#######################################################
colnames(dados_FC_classificados)  # Exibe os nomes das colunas do dataset

#************************************************************************
# I - Análise: Evolução dos vínculos formais FC

#***BRASIL***
# GRÁFICO 1 - Contagem de vínculos
vinculos_ano <- dados_FC_classificados %>%
  group_by(ano) %>%
  summarise(vinculos = n())

print(vinculos_ano)

## Plot 
fmt <- scales::label_number(big.mark = ".", decimal.mark = ",") #Formatação

ggplot(vinculos_ano, aes(x = ano, y = vinculos)) +
  geom_area(alpha = 0.15, fill = "grey60") +
  geom_line(linewidth = 1.2, color = "black") +
  geom_point(size = 3, color = "black") +
  geom_text(
    aes(label = fmt(vinculos)),
    vjust = -0.6,
    size = 3.6,
    family = "Arial",
    fontface = "bold"
  ) +
  scale_y_continuous(
    labels = fmt,
    expand = expansion(mult = c(0, 0.15))
  ) +
  scale_x_continuous(
    breaks = vinculos_ano$ano,
    expand = expansion(mult = c(0.02, 0.05))
  ) +
  labs(
    title = "Evolução do número de vínculos no setor financeiro-contábil no Brasil:",
    subtitle = "2019–2023, RAIS vínculos formais, população 18–64 anos",
    x = "Ano",
    y = "Número de vínculos",
    caption = "Fonte: RAIS (2025)"
  ) +
  theme_minimal(base_family = "Arial") +
  theme(
    plot.title = element_text(face = "bold", size = 16),
    plot.subtitle = element_text(size = 11),
    axis.title.y = element_text(size = 11),
    axis.text = element_text(size = 11),
    panel.grid.minor = element_blank(),
    plot.caption = element_text(size = 9, hjust = 0)
  ) +
  coord_cartesian(clip = "off")


ggsave("grafico_vinculos_fc_2019_2023.png", width = 10, height = 6, dpi = 300) #Salvar imagem


#*** Por UF´s***
#GRÁFICO 2 — MAPA: distribuição de vínculos (fotografia) por UF — 2019-2023

## Filtrar dados do setor FC para o ano de 2019
dados_fc_2019 <- dados_FC_classificados %>%
  filter(ano == 2019) %>%
  count(sigla_uf, name = "Total_Trabalhadores")

## Plot
### Baixar mapa dos estados do Brasil
mapa_estados <- read_state(year = 2020, showProgress = FALSE) %>%
  left_join(dados_fc_2019, by = c("abbrev_state" = "sigla_uf"))

#### Mapa de vínculos no setor FC por estado - 2019
ggplot() +
  geom_sf(data = mapa_estados, aes(fill = Total_Trabalhadores), color = "white", size = 0.3) +
  geom_sf_text(data = st_centroid(mapa_estados), aes(label = abbrev_state),
               size = 2.5, color = "black", family = "Arial") +
  scale_fill_gradient(
    low = "#9ecae1", high = "#08306b", na.value = "gray90",  
    labels = scales::number_format(big.mark = ".", decimal.mark = ",")
  ) +
  labs(
    title = "Distribuição de Vínculos no Setor FC por Estado - 2019",
    fill = "Trabalhadores"
  ) +
  theme_void() +
  theme(
    plot.title = element_text(size = 12, hjust = 0.3, family = "Arial", face = "bold"),
    legend.title = element_text(size = 11, family = "Arial"),
    legend.text = element_text(size = 10, family = "Arial")
  )

ggsave("Mapa_vinculos_fc_2019.png", width = 10, height = 6, dpi = 300) #Salvar imagem


#2020 
dados_fc_2020 <- dados_FC_classificados %>%
  filter(ano == 2020) %>%
  count(sigla_uf, name = "Total_Trabalhadores")

mapa_estados_2020 <- read_state(year = 2020, showProgress = FALSE) %>%
  left_join(dados_fc_2020, by = c("abbrev_state" = "sigla_uf"))

ggplot() +
  geom_sf(data = mapa_estados_2020, aes(fill = Total_Trabalhadores), color = "white", size = 0.3) +
  geom_sf_text(data = st_centroid(mapa_estados_2020), aes(label = abbrev_state),
               size = 2.5, color = "black", family = "Arial") +
  scale_fill_gradient(
    low = "#9ecae1", high = "#08306b", na.value = "gray90",
    labels = scales::number_format(big.mark = ".", decimal.mark = ",")
  ) +
  labs(
    title = "Distribuição de Vínculos no Setor FC por Estado - 2020",
    fill = "Trabalhadores"
  ) +
  theme_void() +
  theme(
    plot.title = element_text(size = 12, hjust = 0.3, family = "Arial", face = "bold"),
    legend.title = element_text(size = 11, family = "Arial"),
    legend.text = element_text(size = 10, family = "Arial")
  )

ggsave("Mapa_vinculos_fc_2020.png", width = 10, height = 6, dpi = 300)

#2021
dados_fc_2021 <- dados_FC_classificados %>%
  filter(ano == 2021) %>%
  count(sigla_uf, name = "Total_Trabalhadores")

mapa_estados_2021 <- read_state(year = 2020, showProgress = FALSE) %>%
  left_join(dados_fc_2021, by = c("abbrev_state" = "sigla_uf"))

ggplot() +
  geom_sf(data = mapa_estados_2021, aes(fill = Total_Trabalhadores), color = "white", size = 0.3) +
  geom_sf_text(data = st_centroid(mapa_estados_2021), aes(label = abbrev_state),
               size = 2.5, color = "black", family = "Arial") +
  scale_fill_gradient(
    low = "#9ecae1", high = "#08306b", na.value = "gray90",
    labels = scales::number_format(big.mark = ".", decimal.mark = ",")
  ) +
  labs(
    title = "Distribuição de Vínculos no Setor FC por Estado - 2021",
    fill = "Trabalhadores"
  ) +
  theme_void() +
  theme(
    plot.title = element_text(size = 12, hjust = 0.3, family = "Arial", face = "bold"),
    legend.title = element_text(size = 11, family = "Arial"),
    legend.text = element_text(size = 10, family = "Arial")
  )

ggsave("Mapa_vinculos_fc_2021.png", width = 10, height = 6, dpi = 300)

#2022
dados_fc_2022 <- dados_FC_classificados %>%
  filter(ano == 2022) %>%
  count(sigla_uf, name = "Total_Trabalhadores")

mapa_estados_2022 <- read_state(year = 2020, showProgress = FALSE) %>%
  left_join(dados_fc_2022, by = c("abbrev_state" = "sigla_uf"))

ggplot() +
  geom_sf(data = mapa_estados_2022, aes(fill = Total_Trabalhadores), color = "white", size = 0.3) +
  geom_sf_text(data = st_centroid(mapa_estados_2022), aes(label = abbrev_state),
               size = 2.5, color = "black", family = "Arial") +
  scale_fill_gradient(
    low = "#9ecae1", high = "#08306b", na.value = "gray90",
    labels = scales::number_format(big.mark = ".", decimal.mark = ",")
  ) +
  labs(
    title = "Distribuição de Vínculos no Setor FC por Estado - 2022",
    fill = "Trabalhadores"
  ) +
  theme_void() +
  theme(
    plot.title = element_text(size = 12, hjust = 0.3, family = "Arial", face = "bold"),
    legend.title = element_text(size = 11, family = "Arial"),
    legend.text = element_text(size = 10, family = "Arial")
  )

ggsave("Mapa_vinculos_fc_2022.png", width = 10, height = 6, dpi = 300)

#2023
dados_fc_2023 <- dados_FC_classificados %>%
  filter(ano == 2023) %>%
  count(sigla_uf, name = "Total_Trabalhadores")

mapa_estados_2023 <- read_state(year = 2020, showProgress = FALSE) %>%
  left_join(dados_fc_2023, by = c("abbrev_state" = "sigla_uf"))

ggplot() +
  geom_sf(data = mapa_estados_2023, aes(fill = Total_Trabalhadores), color = "white", size = 0.3) +
  geom_sf_text(data = st_centroid(mapa_estados_2023), aes(label = abbrev_state),
               size = 2.5, color = "black", family = "Arial") +
  scale_fill_gradient(
    low = "#9ecae1", high = "#08306b", na.value = "gray90",
    labels = scales::number_format(big.mark = ".", decimal.mark = ",")
  ) +
  labs(
    title = "Distribuição de Vínculos no Setor FC por Estado - 2023",
    fill = "Trabalhadores"
  ) +
  theme_void() +
  theme(
    plot.title = element_text(size = 12, hjust = 0.3, family = "Arial", face = "bold"),
    legend.title = element_text(size = 11, family = "Arial"),
    legend.text = element_text(size = 10, family = "Arial")
  )

ggsave("Mapa_vinculos_fc_2023.png", width = 10, height = 6, dpi = 300)

#___________________________________________________________________________________

#************************************************************************
#   II - Análise:  Tipos/Tamanho de Estabelicimentos FC BRASIL

#***BRASIL***
# GRÁFICO 3 - COMPOSIÇÃO DOS VÍNCULOS POR PORTE 
# Recodificar para os portes de empresa segundo a classificação do Sebrae
dados_FC_classificados <- dados_FC_classificados %>%
  mutate(porte_empresa = case_when(
    tamanho_estabelecimento %in% c("Até 4", "De 5 a 9", "De 10 a 19") ~ "Micro",
    tamanho_estabelecimento %in% c("De 20 a 49", "De 50 a 99") ~ "Pequena",
    tamanho_estabelecimento %in% c("De 100 a 249", "De 250 a 499") ~ "Média",
    tamanho_estabelecimento %in% c("De 500 a 999", "1000 ou mais") ~ "Grande",
    tamanho_estabelecimento == "Zero" ~ "Sem vínculo ativo em 31/12",
    TRUE ~ "Não informado"
  ))

# Agrupar por porte e ano e contar número de vínculos
vinculos_por_porte <- dados_FC_classificados %>%
  group_by(ano, porte_empresa) %>%
  summarise(total_vinculos = n(), .groups = "drop")

# Filtrar somente os portes válidos da classificação Sebrae
vinculos_por_porte <- dados_FC_classificados %>%
  filter(porte_empresa %in% c("Micro", "Pequena", "Média", "Grande")) %>%
  group_by(ano, porte_empresa) %>%
  summarise(total_vinculos = n(), .groups = "drop")

## Plot
ggplot(vinculos_por_porte, aes(x = ano, y = total_vinculos, fill = porte_empresa)) +
  geom_col(position = "stack") +
  geom_text(aes(label = format(total_vinculos, big.mark = ".", decimal.mark = ",")),
            position = position_stack(vjust = 0.5), size = 3, family = "Arial", color = "black") +
  scale_y_continuous(labels = scales::label_number(big.mark = ".", decimal.mark = ",")) +
  scale_fill_manual(
    values = c("Pequena" = "#084594", "Micro" = "#4292c6", "Média" = "#c6dbef", "Grande" = "#bdbdbd")
  ) +
  labs(
    title = "Evolução dos Vínculos por Porte de Empresa no Setor FC no Brasil:",
    subtitle = "        2019–2023, RAIS vínculos formais, população 18–64 anos",
    caption = "Fonte: RAIS (2025)",
    x = "Ano",
    y = "Número de Vínculos",
    fill = "Porte da Empresa"
  ) +
  theme_minimal(base_family = "Arial") +
  theme(
    panel.grid.major.x = element_blank(),   # Remove linhas verticais
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank(),
    panel.background = element_blank(),
    plot.background = element_blank(),
    plot.title = element_text(hjust = 0.5, face = "bold", size = 14, color = "black"),
    axis.title = element_text(size = 12, color = "black", face = "bold"),
    axis.text = element_text(size = 11, color = "black"),
    legend.title = element_text(size = 11, color = "black"),
    legend.text = element_text(size = 10, color = "black"),
    legend.position = "bottom"
  )

ggsave("Por_porte_Sebrae.png", width = 10, height = 6, dpi = 300)

#____________________________________________________________________________________

#************************************************************************
#   III - Análise:  Vínculos por Nível Hierárquico - CBO 2002 

#***BRASIL***
# GRÁFÍCO 4 - VÍNCULOS POR CLASSIFICAÇÃO (Nível Hierárquico)
# Agrupar por ano e classificação
ocupacoes_por_classe <- dados_FC_classificados %>%
  group_by(ano, CLASSIFICAÇÃO) %>%
  summarise(Total_Vinculos = n(), .groups = "drop") %>%
  arrange(ano, desc(Total_Vinculos))

## Plot
ggplot(ocupacoes_por_classe, aes(x = ano, y = Total_Vinculos, color = CLASSIFICAÇÃO, group = CLASSIFICAÇÃO)) +
  geom_line(size = 1.2) +
  geom_point(size = 3) +
  labs(
    title = "Evolução dos Vínculos por Nível Hierárquico em FC Brasil:",
    subtitle = "2019–2023, RAIS vínculos formais, população 18–64 anos",
    caption = "Fonte: RAIS (2025)",
    x = "Ano",
    y = "Total de Vínculos",
    color = "Classificação"
  ) +
  scale_y_continuous(labels = scales::comma_format(big.mark = ".")) +
  theme_classic(base_family = "Arial") +
  theme(
    legend.position = "right",
    legend.title = element_text(size = 12),
    legend.text = element_text(size = 11),
    axis.text = element_text(size = 11, color = "black"),
    axis.title = element_text(size = 12, face = "plain", color = "black"),
    plot.title = element_text(size = 13, face = "bold", hjust = 0.5, color = "black")
  )

ggsave("por_Nível_Hierárquico.png", width = 10, height = 6, dpi = 300)

#*** Por UF´s***
#TABELA 1 - Distribuição Percentual dos Cargos por Estado
#         Comparação entre 2019 e 2023, com total de vínculos

#Calcular percentuais por estado, ano e classificação
tabela_perc <- dados_FC_classificados %>%
  filter(ano %in% c(2019, 2023)) %>%
  group_by(sigla_uf, ano, CLASSIFICAÇÃO) %>%
  summarise(Total = n(), .groups = "drop") %>%
  group_by(sigla_uf, ano) %>%
  mutate(Percentual = Total / sum(Total) * 100) %>%
  ungroup()

#Pivotar com colunas tipo "Diretores_2019"
tabela_wide <- tabela_perc %>%
  mutate(nome_coluna = paste0(CLASSIFICAÇÃO, "_", ano)) %>%
  select(sigla_uf, nome_coluna, Percentual) %>%
  pivot_wider(names_from = nome_coluna, values_from = Percentual, values_fill = 0)

#Renomear coluna de estado
tabela_wide <- tabela_wide %>%
  rename(Estado = sigla_uf)

#Adicionar totais por estado (número de vínculos)
total_estado <- dados_FC_classificados %>%
  filter(ano %in% c(2019, 2023)) %>%
  group_by(sigla_uf, ano) %>%
  summarise(Total_Vinculos = n(), .groups = "drop") %>%
  pivot_wider(names_from = ano, values_from = Total_Vinculos) %>%
  rename(Estado = sigla_uf, Total_2019 = `2019`, Total_2023 = `2023`)

#Juntar os dados
tabela_final <- tabela_wide %>%
  left_join(total_estado, by = "Estado")

#Visualizar
tabela_final %>%
  gt() %>%
  fmt_number(decimals = 1, sep_mark = ".", dec_mark = ",") %>%
  tab_header(
    title = "Distribuição Percentual dos Cargos por Estado",
    subtitle = "Comparação entre 2019 e 2023, com total de vínculos"
  ) %>%
  tab_source_note("Fonte: RAIS (Elaboração própria)")

# Exportar para Excel
write_xlsx(tabela_final, "tabela_ocupacao_estado_horizontal.xlsx")
#______________________________________________________________________________

#************************************************************************
#   IV - Análise: Perfil dos Trabalhadores em FC no Brasil (2019-2024) 

#***BRASIL***
# TABELA 2 - PERFIL DOS TRABALHADORES FC (2019–2023)

anos_colunas <- 2019:2023

# Gênero
total_genero <- dados_FC_classificados %>%
  filter(!is.na(sexo)) %>%
  group_by(ano, sexo) %>%
  summarise(Total = n(), .groups = "drop") %>%
  group_by(ano) %>%
  mutate(Percentual = Total / sum(Total) * 100) %>%
  ungroup() %>%
  transmute(
    Grupo = "Sexo",
    ano,
    Categoria = sexo,
    Percentual
  ) %>%
  pivot_wider(
    names_from  = ano,
    values_from = Percentual,
    values_fill = 0
  ) %>%
  # ORDEM do maior pro menor
  arrange(desc(.data[["2023"]]))

# Faixa Etária
total_faixa_etaria <- dados_FC_classificados %>%
  filter(!is.na(faixa_etaria)) %>%
  group_by(ano, faixa_etaria) %>%
  summarise(Total = n(), .groups = "drop") %>%
  group_by(ano) %>%
  mutate(Percentual = Total / sum(Total) * 100) %>%
  ungroup() %>%
  transmute(
    Grupo = "Faixa Etária",
    ano,
    Categoria = faixa_etaria,
    Percentual
  ) %>%
  pivot_wider(
    names_from  = ano,
    values_from = Percentual,
    values_fill = 0
  ) %>%
  arrange(desc(.data[["2023"]]))

# Escolaridade 
total_escolaridade <- dados_FC_classificados %>%
  filter(!is.na(grau_instrucao_apos_2005)) %>%
  group_by(ano, grau_instrucao_apos_2005) %>%
  summarise(Total = n(), .groups = "drop") %>%
  group_by(ano) %>%
  mutate(Percentual = Total / sum(Total) * 100) %>%
  ungroup() %>%
  transmute(
    Grupo = "Escolaridade",
    ano,
    Categoria = grau_instrucao_apos_2005,
    Percentual
  ) %>%
  pivot_wider(
    names_from  = ano,
    values_from = Percentual,
    values_fill = 0
  ) %>%
  arrange(desc(.data[["2023"]]))

# Unir todas as tabelas
tabela_perfil_FC <- bind_rows(total_genero, total_faixa_etaria, total_escolaridade)

#Gerar a tabela completa
tabela_perfil_gt <- tabela_perfil_FC %>%
  gt(
    rowname_col   = "Categoria",
    groupname_col = "Grupo"
  ) %>%
  cols_label(.list = setNames(as.list(as.character(anos_colunas)), as.character(anos_colunas))) %>%
  fmt_number(
    columns = all_of(as.character(anos_colunas)),
    decimals = 2, sep_mark = ".", dec_mark = ","
  ) %>%
  tab_header(
    title = "Tabela 2. Perfil dos Trabalhadores em FC no Brasil (2019–2023)",
    subtitle = "Dados em ordem decrescente por categoria"
  ) %>%
  tab_source_note(source_note = "Fonte: RAIS (Elaboração própria)") %>%
  tab_options(
    row_group.font.weight = "bold",
    row_group.font.size = px(14)
  )

#exibir
tabela_perfil_gt

# Exportar para Excel - diretório R pessoal 
write_xlsx(tabela_perfil_FC, "perfil_trabalhadores_FC.xlsx")
#______________________________________________________________________________


#************************************************************************
#   IV - Análise: Salários
#   (com teto R$ 200 mil + mediana + deflacionado IPCA base 2023)
#************************************************************************

#0) Definir teto 
# salários com teto de R$ 200 mil para reduzir influência de valores extremos no setor FC.
TETO_SALARIO <- 200000

# 0.1) Garantir tipo numérico
dados_FC_classificados <- dados_FC_classificados %>%
  mutate(valor_remuneracao_media = as.numeric(valor_remuneracao_media))

# 1) Base válida de salários + criação do salário com teto
dados_fc_sal <- dados_FC_classificados %>%
  filter(!is.na(valor_remuneracao_media),
         valor_remuneracao_media > 0) %>%
  mutate(
    salario_bruto = valor_remuneracao_media,
    salario_teto  = pmin(valor_remuneracao_media, TETO_SALARIO),
    flag_teto     = valor_remuneracao_media > TETO_SALARIO
  )

# Checagem: quantos foram afetados por ano
impacto_teto_ano <- dados_fc_sal %>%
  group_by(ano) %>%
  summarise(
    n = n(),
    acima_teto = sum(flag_teto),
    perc_acima_teto = mean(flag_teto) * 100,
    max_bruto = max(salario_bruto),
    .groups = "drop"
  )

print(impacto_teto_ano)

# 2) Salário médio (bruto) x salário médio (com teto) + mediana — por ano
salario_ano_fc <- dados_fc_sal %>%
  group_by(ano) %>%
  summarise(
    n = n(),
    Media_Bruta   = mean(salario_bruto),
    Media_Teto    = mean(salario_teto),
    Mediana_Bruta = median(salario_bruto),
    .groups = "drop"
  ) %>%
  arrange(ano)

print(salario_ano_fc)

# 3) Deflacionar (IPCA base 2023)
fatores_inflacao <- data.frame(
  ano = c(2019, 2020, 2021, 2022, 2023),
  fator_ipca = c(1.265, 1.211, 1.099, 1.046, 1.000)
)

salario_ano_fc_defl <- salario_ano_fc %>%
  left_join(fatores_inflacao, by = "ano") %>%
  mutate(
    Media_Bruta_Real = Media_Bruta / fator_ipca,
    Media_Teto_Real  = Media_Teto  / fator_ipca,
    Mediana_Real     = Mediana_Bruta / fator_ipca
  ) %>%
  select(ano, n, Media_Bruta, Media_Teto, Mediana_Bruta,
         Media_Bruta_Real, Media_Teto_Real, Mediana_Real)

print(salario_ano_fc_defl)

# 4) Gráfico — evolução do salário real (base 2023): Média com teto + Mediana
sal_long_fc <- salario_ano_fc_defl %>%
  select(ano, Media_Teto_Real, Mediana_Real) %>%
  pivot_longer(
    cols = c(Media_Teto_Real, Mediana_Real),
    names_to = "medida",
    values_to = "valor"
  ) %>%
  mutate(
    medida = recode(medida,
                    Media_Teto_Real = "Média (teto R$ 200 mil) — real (base 2023)",
                    Mediana_Real    = "Mediana — real (base 2023)")
  )

ggplot(sal_long_fc, aes(x = ano, y = valor, color = medida)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  geom_text(aes(label = scales::comma(valor, big.mark = ".")),
            vjust = -0.5, size = 4, family = "Arial", color = "black") +
  scale_y_continuous(labels = scales::comma_format(big.mark = ".")) +
  labs(
    title = "Evolução do Salário Real no Setor Financeiro-Contábil (2019–2023)",
    subtitle = "Comparação entre média com teto (R$ 200 mil) e mediana — valores reais (base 2023)",
    x = "Ano",
    y = "Salário real (R$)",
    color = ""
  ) +
  theme_classic(base_family = "Arial", base_size = 14) +
  theme(
    plot.title = element_text(hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(hjust = 0.5),
    legend.position = "bottom"
  )

ggsave("salario_real_fc_media_teto_mediana_2019_2023.png", width = 10, height = 6, dpi = 300)

# 5) Salário real por sexo — média com teto + mediana
# Transformar para formato longo (Média e Mediana no mesmo gráfico)
salario_sexo_long <- salario_sexo_fc %>%
  select(ano, sexo, Media_Teto_Real, Mediana_Real) %>%
  pivot_longer(
    cols = c(Media_Teto_Real, Mediana_Real),
    names_to = "medida",
    values_to = "valor"
  ) %>%
  mutate(
    medida = recode(medida,
                    Media_Teto_Real = "Média (teto R$ 200 mil)",
                    Mediana_Real    = "Mediana")
  )

ggplot(salario_sexo_long,
       aes(x = ano, y = valor, color = sexo, linetype = medida,
           group = interaction(sexo, medida))) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  geom_text(aes(label = scales::comma(valor, big.mark = ".")),
            vjust = -0.5, size = 3.5, family = "Arial", color = "black") +
  scale_y_continuous(labels = scales::comma_format(big.mark = ".")) +
  labs(
    title = "Evolução do Salário Real por Sexo (2019–2023) — Setor FC",
    subtitle = "Média com teto de R$ 200 mil e mediana — valores reais (base 2023)",
    x = "Ano",
    y = "Salário real (R$)",
    color = "Sexo",
    linetype = "Medida"
  ) +
  theme_classic(base_family = "Arial", base_size = 14) +
  theme(
    plot.title = element_text(hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(hjust = 0.5),
    legend.position = "bottom",
    legend.box = "vertical"
  )

ggsave("salario_real_fc_media_teto_e_mediana_por_sexo_2019_2023.png",
       width = 10, height = 6, dpi = 300)


# 2) Gráfico: média com teto real por nível hierárquico (linhas)
ggplot(salario_classe_fc,
       aes(x = ano, y = Media_Teto_Real, color = CLASSIFICAÇÃO, group = CLASSIFICAÇÃO)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  geom_text(aes(label = scales::comma(Media_Teto_Real, big.mark = ".")),
            vjust = -0.5, size = 3.2, family = "Arial", color = "black") +
  scale_y_continuous(labels = scales::comma_format(big.mark = ".")) +
  labs(
    title = "Evolução do Salário Real por Nível Hierárquico — Setor FC (2019–2023)",
    subtitle = "Média com teto de R$ 200 mil — valores reais (base 2023)",
    x = "Ano",
    y = "Salário real (R$)",
    color = "Nível hierárquico"
  ) +
  theme_classic(base_family = "Arial", base_size = 14) +
  theme(
    plot.title = element_text(hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(hjust = 0.5),
    legend.position = "bottom"
  )

ggsave("salario_real_fc_media_teto_por_nivel_hierarquico_2019_2023.png",
       width = 10, height = 6, dpi = 300)


# 3) TABELA TOP 5
#************************************************************************
#   IV - Tabela: Top 5 Maiores e Menores Salários Médios por Ocupação (2023)
#   (média com teto R$ 200 mil)
#************************************************************************

ANO_ANALISE <- 2023
MIN_VINCULOS_OCUP <- 50   # ajustar para + ou -

# 1) Salário médio por ocupação em 2023 (com teto)
salario_ocupacao_2023 <- dados_fc_sal %>%
  filter(ano == ANO_ANALISE, !is.na(OCUPAÇÕES), !is.na(CLASSIFICAÇÃO)) %>%
  group_by(CLASSIFICAÇÃO, OCUPAÇÕES) %>%
  summarise(
    Total_Vinculos = n(),
    Salario_Medio_Teto = mean(salario_teto, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(Total_Vinculos >= MIN_VINCULOS_OCUP) %>%   # evita ruído de ocupação com poucos vínculos
  arrange(desc(Salario_Medio_Teto))

# 2) Top 5 maiores
top5_maiores_fc <- salario_ocupacao_2023 %>%
  slice_head(n = 5) %>%
  transmute(
    `CBO - Ocupações` = OCUPAÇÕES,
    Categoria = CLASSIFICAÇÃO,
    `Salário médio (R$)` = Salario_Medio_Teto
  )

# 3) Top 5 menores
top5_menores_fc <- salario_ocupacao_2023 %>%
  arrange(`Salário médio (R$)` = Salario_Medio_Teto) %>%
  slice_head(n = 5) %>%
  transmute(
    `CBO - Ocupações` = OCUPAÇÕES,
    Categoria = CLASSIFICAÇÃO,
    `Salário médio (R$)` = Salario_Medio_Teto
  )

# 4) Juntar lado a lado
tabela_top5_fc <- bind_cols(
  top5_maiores_fc %>% rename_with(~ paste0(.x, " (Top 5)")),
  top5_menores_fc %>% rename_with(~ paste0(.x, " (Bottom 5)"))
)

# 5) Exibir tabela formatada no GT
tabela_top5_fc_gt <- tabela_top5_fc %>%
  gt() %>%
  tab_header(
    title = glue("Tabela. Maiores e Menores Salários Médios por Ocupação — Setor FC ({ANO_ANALISE})"),
    subtitle = glue("Média com teto de R$ {format(TETO_SALARIO, big.mark = '.', decimal.mark = ',')} | Ocupações com ≥ {MIN_VINCULOS_OCUP} vínculos")
  ) %>%
  tab_spanner(
    label = glue("5 maiores salários médios {ANO_ANALISE}"),
    columns = ends_with("(Top 5)")
  ) %>%
  tab_spanner(
    label = glue("5 menores salários médios {ANO_ANALISE}"),
    columns = ends_with("(Bottom 5)")
  ) %>%
  fmt_currency(
    columns = matches("Salário médio"),
    currency = "BRL",
    decimals = 2
  ) %>%
  tab_source_note(source_note = "Fonte: RAIS. Elaboração própria.") %>%
  tab_source_note(source_note = "Nota: salários com teto de R$ 200 mil para reduzir influência de valores extremos.")

tabela_top5_fc_gt

# 6) Exportar para Excel
write_xlsx(tabela_top5_fc, glue("tabela_top5_maiores_menores_salarios_fc_{ANO_ANALISE}.xlsx"))

#TABELA FINAL  - Salário por Ano x UF x Macrorregião x Sexo

#Tabela final (Ano x UF x Macrorregião x Sexo) — com salário com teto (nominal e real)
tabela_trabalhadores_fc <- dados_fc_sal %>%
  group_by(ano, sigla_uf, macrorregiao, sexo) %>%
  summarise(
    Total_Trabalhadores = n(),
    Salario_Medio_Teto  = mean(salario_teto),
    Mediana_Salario     = median(salario_bruto),
    .groups = "drop"
  ) %>%
  left_join(fatores_inflacao, by = "ano") %>%
  mutate(
    Salario_Medio_Teto_Real = Salario_Medio_Teto / fator_ipca,
    Mediana_Salario_Real    = Mediana_Salario / fator_ipca
  ) %>%
  arrange(ano, sigla_uf, sexo)

print(tabela_trabalhadores_fc)
write_xlsx(tabela_trabalhadores_fc, "tabela_trabalhadores_fc_salarios.xlsx")

#______________________________________________________________________________

################
#FIM         ###    
################

#Elisangela Gelatti
