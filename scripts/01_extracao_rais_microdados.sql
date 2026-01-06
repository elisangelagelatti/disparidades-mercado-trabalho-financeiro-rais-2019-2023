CREATE TABLE "mytable" (
  "-- Projeto: Disparidades regionais e dinâmicas do mercado de trabalho financeiro-contábil (20192023)" text
);

INSERT INTO "mytable" ("-- Projeto: Disparidades regionais e dinâmicas do mercado de trabalho financeiro-contábil (20192023)")
VALUES
('-- ETAPA 1: Seleção dos dados da RAIS conforme variáveis de interesse (pré-definidas no projeto)'),
('-- Fonte: RAIS via Base dos Dados (BigQuery) | Dataset: basedosdados.br_me_rais.microdados_vinculos'),
('-- Recortes: 2019–2023 | Idade: 18–64 | Ocupações: lista de CBO 2002 (financeiro/contábil)'),
('-- Saída esperada: subconjunto de variáveis para análises descritivas e comparativas'),
('SELECT'),
('ano'),
('sigla_uf'),
('idade'),
('faixa_etaria'),
('sexo'),
('raca_cor'),
('grau_instrucao_apos_2005'),
('cbo_2002'),
('faixa_remuneracao_media_sm'),
('valor_remuneracao_media'),
('tamanho_estabelecimento'),
('tipo_estabelecimento'),
('FROM `basedosdados.br_me_rais.microdados_vinculos`'),
('WHERE ano BETWEEN 2019 AND 2023'),
('AND idade BETWEEN 18 AND 64'),
('AND cbo_2002 IN ('),
('''252505'''),
('''413205'''),
('''413220'''),
('''122725'''),
('''413225'''),
('''253205'''),
('''410220'''),
('''142110'''),
(');');
