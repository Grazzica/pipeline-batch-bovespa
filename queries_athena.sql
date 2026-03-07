-- Queries para demonstração - Tech Challenge 2
--=============================================================
-- 1. Validação Básica (Requisito 8)

SELECT * FROM refined LIMIT 10; -- Amostra de Dados refinados

SELECT * FROM sumarizado; -- Amostra de Dados sumarizados

--=============================================================
-- 2. Validação de Agrupamento Numérico (Requisito 5A)

-- Dados Sumarizados por Ticker: média, soma e contagem
SELECT 
    ticker,
    ROUND(avg_close, 2) AS media_fechamento,
    total_volume,
    dias_negociados
FROM sumarizado
ORDER BY media_fechamento DESC;

--=============================================================
-- 3. Validação de Estrutura: Colunas Renomeadas (Requisito 5B)

SELECT
    ticker,
    date,
    preco_fechamento, -- Anteriormente Close
    volume_negociado, -- Anteriormente Volume
    media_movel_7d
FROM refined
WHERE date = '2026-03-04'
ORDER BY ticker;

--=============================================================
-- 4. Validação de Cálculo Baseado em Data (Requisito 5C)

-- Média Movel de 7 dias vs preço de fechamento

SELECT
    ticker,
    date,
    ROUND(preco_fechamento, 2) AS preco,
    ROUND(media_movel_7d, 2) AS media_7d,
    ROUND(preco_fechamento - media_movel_7d, 2) AS desvio_da_media
FROM refined
WHERE ticker = 'PETR4.SA'
    AND date BETWEEN '2026-02-01' AND '2026-03-05'
ORDER BY date;

--=============================================================
-- 5. Validação de Particionamento (Requisito 6)

-- Consulta filtrando por partição de data
SELECT ticker, preco_fechamento, volume_negociado
FROM refined
WHERE date = '2025-06-11'
ORDER BY volume_negociado DESC;

-- Consulta filtrando por partição de ticker
SELECT date, preco_fechamento, media_movel_7d
FROM refined
WHERE ticker = 'VALE3.SA'
    AND date >= '2026-01-01'
ORDER BY date;

--=============================================================
-- 6 Queries Analíticas

-- 6a. Dia com maior volume de negociação por ação
SELECT ticker, date, volume_negociado
FROM refined
WHERE (ticker, volume_negociado) IN (
    SELECT ticker, MAX(volume_negociado)
    FROM refined
    GROUP BY ticker
)
ORDER BY volume_negociado DESC;

-- 6b. Variação entre primeiro e último dia do período por ticker

SELECT 
    primeiro.ticker,
    ROUND(primeiro.preco_fechamento, 2) AS preco_inicial,
    ROUND(ultimo.preco_fechamento, 2) AS preco_final,
    ROUND(((ultimo.preco_fechamento - primeiro.preco_fechamento) 
        / primeiro.preco_fechamento) * 100, 2) AS variacao_percentual
FROM (
    SELECT ticker, preco_fechamento, date,
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date ASC) AS rn
    FROM refined
) primeiro
JOIN (
    SELECT ticker, preco_fechamento, date,
           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
    FROM refined
) ultimo ON primeiro.ticker = ultimo.ticker
WHERE primeiro.rn = 1 AND ultimo.rn = 1
ORDER BY variacao_percentual DESC;

-- 6c. Dias em que o preço ficou acima da média móvel (tendência de alta)

SELECT
    ticker,
    COUNT(*) AS dias_acima_media,
    ROUND(COUNT(*)* 100.0 / (SELECT COUNT (*) FROM refined r2 WHERE r2.ticker = refined.ticker), 1) AS percentual
FROM refined
WHERE preco_fechamento > media_movel_7d
GROUP by ticker
ORDER BY percentual DESC; 