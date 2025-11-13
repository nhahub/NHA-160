IF DB_ID('movies_db') IS NULL
BEGIN
  CREATE DATABASE movies_db;
END
GO

USE movies_db;
GO
------------------------------------------
USE movies_db;
GO

-- staging for main movies
IF OBJECT_ID('staging_movies', 'U') IS NOT NULL DROP TABLE staging_movies;
CREATE TABLE staging_movies (
  id BIGINT,
  title NVARCHAR(500),
  original_title NVARCHAR(500),
  overview NVARCHAR(MAX),
  runtime INT,
  original_language NVARCHAR(50),
  release_date DATE,
  release_year INT,
  budget BIGINT,
  revenue BIGINT,
  vote_count INT,
  vote_average FLOAT,
  popularity FLOAT
);
GO

-- staging for genres (movie_id, genre_name)
IF OBJECT_ID('staging_genres', 'U') IS NOT NULL DROP TABLE staging_genres;
CREATE TABLE staging_genres (
  id BIGINT,
  genre_name NVARCHAR(200)
);
GO

-- staging for countries (movie_id, country_code_or_name)
IF OBJECT_ID('staging_countries', 'U') IS NOT NULL DROP TABLE staging_countries;
CREATE TABLE staging_countries (
  id BIGINT,
  country_code_or_name NVARCHAR(200)
);
GO

-- staging for companies (movie_id, company_name)
IF OBJECT_ID('staging_companies', 'U') IS NOT NULL DROP TABLE staging_companies;
CREATE TABLE staging_companies (
  id BIGINT,
  company_name NVARCHAR(300)
);
GO
---------------------------------------
USE movies_db;
GO

IF OBJECT_ID('staging_movies_raw','U') IS NOT NULL DROP TABLE staging_movies_raw;
CREATE TABLE staging_movies_raw (
  id NVARCHAR(100),
  title NVARCHAR(MAX),
  original_title NVARCHAR(MAX),
  overview NVARCHAR(MAX),
  runtime NVARCHAR(100),
  original_language NVARCHAR(50),
  release_date NVARCHAR(100),
  release_year NVARCHAR(50),
  budget NVARCHAR(200),      -- خليها NVARCHAR مؤقتاً
  revenue NVARCHAR(200),
  vote_count NVARCHAR(100),
  vote_average NVARCHAR(100),
  popularity NVARCHAR(100)
);
GO
-------------------------------
BULK INSERT staging_movies_raw
FROM 'D:\mnt\data\prepared_staging\movies_clean.csv'
WITH (
  FIRSTROW = 2,
  FORMAT = 'CSV',
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',
  CODEPAGE = '65001',   -- UTF-8
  TABLOCK,
  ERRORFILE = 'D:\mnt\data\prepared_staging\bulk_error_log'  -- يحفظ الصفوف التي فشلت في ملفات خطأ
);
GO
----------------------------------------------
SELECT TOP 200 id, budget
FROM staging_movies_raw
WHERE budget IS NOT NULL AND LTRIM(RTRIM(budget)) <> ''
  AND TRY_CAST(REPLACE(REPLACE(REPLACE(budget,'$',''),',',''),' ', '') AS BIGINT) IS NULL;

--------------------------------------------
-- اجعل بعض القيم الشائعة NULL
UPDATE staging_movies_raw
SET budget = NULL
WHERE budget IS NOT NULL AND LTRIM(RTRIM(budget)) IN ('', 'Unknown', 'unknown', 'N/A', 'na', '-', '--');

UPDATE staging_movies_raw
SET revenue = NULL
WHERE revenue IS NOT NULL AND LTRIM(RTRIM(revenue)) IN ('', 'Unknown', 'unknown', 'N/A', 'na', '-', '--');

-- إزالة علامات $ و commas و spaces من budget & revenue حيث يمكن
UPDATE staging_movies_raw
SET budget = REPLACE(REPLACE(REPLACE(budget, '$', ''), ',', ''), ' ', '')
WHERE budget IS NOT NULL;

UPDATE staging_movies_raw
SET revenue = REPLACE(REPLACE(REPLACE(revenue, '$', ''), ',', ''), ' ', '')
WHERE revenue IS NOT NULL;
Go

-------------------------------------------------------
SELECT COUNT(*) AS cnt_problem_budget
FROM staging_movies_raw
WHERE budget IS NOT NULL AND TRY_CAST(budget AS BIGINT) IS NULL;

SELECT TOP 50 id, budget
FROM staging_movies_raw
WHERE budget IS NOT NULL AND TRY_CAST(budget AS BIGINT) IS NULL;
GO

-----------------------------------------

-- 1. أمثلة من القيم المسببة للمشكلة (أول 200)
SELECT TOP 200 id, budget
FROM staging_movies_raw
WHERE budget IS NOT NULL AND LTRIM(RTRIM(budget)) <> ''
  AND TRY_CAST(REPLACE(REPLACE(REPLACE(budget,'$',''),',',''),' ', '') AS BIGINT) IS NULL;

-- 2. عدد القيم الفارغة/Unknown
SELECT
  SUM(CASE WHEN LTRIM(RTRIM(budget)) = '' THEN 1 ELSE 0 END) AS cnt_empty,
  SUM(CASE WHEN LOWER(LTRIM(RTRIM(budget))) IN ('unknown','n/a','na','--','-') THEN 1 ELSE 0 END) AS cnt_unknown
FROM staging_movies_raw;

-- 3. توزيع بسيط لأنماط الحروف/رموز في القيم (helpful)
SELECT
  SUM(CASE WHEN budget LIKE '%,%' THEN 1 ELSE 0 END) AS cnt_with_comma,
  SUM(CASE WHEN budget LIKE '%.%' THEN 1 ELSE 0 END) AS cnt_with_dot,
  SUM(CASE WHEN budget LIKE '%$%' THEN 1 ELSE 0 END) AS cnt_with_dollar,
  SUM(CASE WHEN budget LIKE '%-%' THEN 1 ELSE 0 END) AS cnt_with_dash,
  SUM(CASE WHEN budget LIKE '%[^0-9.,$ -]%' ESCAPE '^' THEN 1 ELSE 0 END) AS cnt_with_other_chars
FROM staging_movies_raw;

-- 4. اعرض أكثر 50 قيمة فريدة متكررة بين القيم المشكلة (helpful to spot common tokens)
WITH problem_vals AS (
  SELECT budget FROM staging_movies_raw
  WHERE budget IS NOT NULL AND LTRIM(RTRIM(budget)) <> ''
    AND TRY_CAST(REPLACE(REPLACE(REPLACE(budget,'$',''),',',''),' ', '') AS BIGINT) IS NULL
)
SELECT TOP 50 budget, COUNT(*) AS cnt
FROM problem_vals
GROUP BY budget
ORDER BY cnt DESC;
GO
------------------------------------------
UPDATE staging_movies_raw
SET budget = NULL
WHERE budget IS NOT NULL
  AND LOWER(LTRIM(RTRIM(budget))) IN ('', 'unknown', 'n/a', 'na', '-', '--', 'none', 'null');
GO

---------------------------------
UPDATE staging_movies_raw
SET budget = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(budget, '$', ''), '€', ''), '£', ''), '¥', ''), ',', ''), ' ', '')
WHERE budget IS NOT NULL;
GO
-----------------------------------
-- حذف النقاط إذا لا يوجد فاصلة عشرية واضحة (نفترض أن أرقام الميزانيات صحيحة بدون كسور)
UPDATE staging_movies_raw
SET budget = REPLACE(budget, '.', '')
WHERE budget LIKE '%.%' AND budget NOT LIKE '%,%';
GO

---------------------------------
-- سلسلة استبدالات لإزالة علامات محتملة؛ يمكن توسيعها حسب الحاجة
UPDATE staging_movies_raw
SET budget = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(budget,
          '(', ''), ')', ''), '+', ''), '/', ''), '\\', ''), '*', ''), '#', ''), '\"', ''), '''', '')
WHERE budget IS NOT NULL;
GO

--------------------------------- اعرض أمثلة ما زالت لا تتحول إلى BIGINT
SELECT TOP 200 id, budget
FROM staging_movies_raw
WHERE budget IS NOT NULL AND TRY_CAST(budget AS BIGINT) IS NULL;
GO

-- احسب المتبقين
SELECT COUNT(*) AS cnt_remaining_bad_budget
FROM staging_movies_raw
WHERE budget IS NOT NULL AND TRY_CAST(budget AS BIGINT) IS NULL;
GO
--------------------------------


-- هل موجود staging_movies_raw؟
SELECT OBJECT_ID('staging_movies_raw') AS obj_id, 
       OBJECT_ID('staging_movies_final') AS obj_final_id;

-- بديل: قائمة الجداول التي تبدأ ب'staging%'
SELECT name FROM sys.tables WHERE name LIKE 'staging%';
GO

----------------------

IF OBJECT_ID('staging_movies_final', 'U') IS NOT NULL
  PRINT 'staging_movies_final already exists.';
ELSE
BEGIN
  CREATE TABLE staging_movies_final (
    id BIGINT,
    title NVARCHAR(500),
    original_title NVARCHAR(500),
    overview NVARCHAR(MAX),
    runtime INT,
    original_language NVARCHAR(50),
    release_date DATE,
    release_year INT,
    budget BIGINT,
    revenue BIGINT,
    vote_count INT,
    vote_average FLOAT,
    popularity FLOAT
  );
  PRINT 'staging_movies_final created.';
END
GO
--------------------------


INSERT INTO staging_movies_final (id, title, original_title, overview, runtime, original_language, release_date, release_year, budget, revenue, vote_count, vote_average, popularity)
SELECT
  TRY_CAST(id AS BIGINT) AS id,
  title,
  original_title,
  overview,
  TRY_CAST(runtime AS INT),
  original_language,
  TRY_CAST(release_date AS DATE),
  TRY_CAST(release_year AS INT),
  TRY_CAST(budget AS BIGINT),
  TRY_CAST(revenue AS BIGINT),
  TRY_CAST(vote_count AS INT),
  TRY_CAST(vote_average AS FLOAT),
  TRY_CAST(popularity AS FLOAT)
FROM staging_movies_raw;
GO
------------------------------

SELECT COUNT(*) AS rows_raw FROM staging_movies_raw;
SELECT COUNT(*) AS rows_final FROM staging_movies_final;
SELECT COUNT(*) AS null_budgets_in_final FROM staging_movies_final WHERE budget IS NULL;

-- أمثلة من القيم التي لا تُحوّل بسهولة (عرض من الجدول الخام)
SELECT TOP 30 id, budget
FROM staging_movies_raw
WHERE budget IS NOT NULL AND TRY_CAST(REPLACE(REPLACE(REPLACE(budget,'$',''),',',''),' ', '') AS BIGINT) IS NULL;
GO
----------------------------
USE movies_db;
GO

-- اذا جداول staging غير موجودة يمكنك استيرادها الآن (عدل المسارات)
-- BULK INSERT staging_genres FROM 'C:\data\prepared_staging\movie_genres.csv' WITH (FIRSTROW=2, FORMAT='CSV', CODEPAGE='65001');
-- BULK INSERT staging_countries FROM 'C:\data\prepared_staging\movie_countries.csv' WITH (FIRSTROW=2, FORMAT='CSV', CODEPAGE='65001');
-- BULK INSERT staging_companies FROM 'C:\data\prepared_staging\movie_companies.csv' WITH (FIRSTROW=2, FORMAT='CSV', CODEPAGE='65001');
GO

-- تأكد من وجود الجداول وعدد الصفوف
SELECT 'staging_movies_final' AS tbl, COUNT(*) AS cnt FROM staging_movies_final
UNION ALL
SELECT 'staging_genres', COUNT(*) FROM sys.tables WHERE name='staging_genres' 
UNION ALL
SELECT 'staging_countries', COUNT(*) FROM sys.tables WHERE name='staging_countries'
UNION ALL
SELECT 'staging_companies', COUNT(*) FROM sys.tables WHERE name='staging_companies';
GO

-----------------------------



-- genres
TRUNCATE TABLE staging_genres;  -- افرغ الجدول قبل الاستيراد لإعادة المحاولة
BULK INSERT staging_genres
FROM 'D:\mnt\data\prepared_staging\movie_genres.csv'
WITH (
  FORMAT = 'CSV',
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',
  CODEPAGE = '65001',  -- UTF-8
  TABLOCK,
  KEEPNULLS
);
GO

-- countries
TRUNCATE TABLE staging_countries;
BULK INSERT staging_countries
FROM 'D:\mnt\data\prepared_staging\movie_countries.csv'
WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', CODEPAGE='65001', TABLOCK, KEEPNULLS);
GO

-- companies
TRUNCATE TABLE staging_companies;
BULK INSERT staging_companies
FROM 'D:\mnt\data\prepared_staging\movie_companies.csv'
WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', CODEPAGE='65001', TABLOCK, KEEPNULLS);
GO

-----------------------------------------
SELECT COUNT(*) FROM staging_genres;
SELECT TOP 10 * FROM staging_genres;

SELECT COUNT(*) FROM staging_countries;
SELECT TOP 10 * FROM staging_countries;

SELECT COUNT(*) FROM staging_companies;
SELECT TOP 10 * FROM staging_companies;

--------------------------

-- ============================
-- 1) أنشئ جداول الأبعاد/الفيلم/الفاكت/الbridges لو مش موجودة
-- ============================
IF OBJECT_ID('dbo.dim_language','U') IS NULL
BEGIN
  CREATE TABLE dbo.dim_language (
    language_key INT IDENTITY(1,1) PRIMARY KEY,
    language_code NVARCHAR(50) UNIQUE,
    language_name NVARCHAR(200) NULL
  );
  PRINT 'Created dim_language';
END

IF OBJECT_ID('dbo.dim_genre','U') IS NULL
BEGIN
  CREATE TABLE dbo.dim_genre (
    genre_key INT IDENTITY(1,1) PRIMARY KEY,
    genre_name NVARCHAR(200) UNIQUE
  );
  PRINT 'Created dim_genre';
END

IF OBJECT_ID('dbo.dim_country','U') IS NULL
BEGIN
  CREATE TABLE dbo.dim_country (
    country_key INT IDENTITY(1,1) PRIMARY KEY,
    country_code NVARCHAR(200) UNIQUE,
    country_name NVARCHAR(200) NULL
  );
  PRINT 'Created dim_country';
END

IF OBJECT_ID('dbo.dim_company','U') IS NULL
BEGIN
  CREATE TABLE dbo.dim_company (
    company_key INT IDENTITY(1,1) PRIMARY KEY,
    company_name_normalized NVARCHAR(300) UNIQUE,
    company_name_original NVARCHAR(300)
  );
  PRINT 'Created dim_company';
END

IF OBJECT_ID('dbo.dim_movie','U') IS NULL
BEGIN
  CREATE TABLE dbo.dim_movie (
    movie_key INT IDENTITY(1,1) PRIMARY KEY,
    tmdb_id BIGINT UNIQUE,
    title NVARCHAR(500),
    original_title NVARCHAR(500),
    overview NVARCHAR(MAX),
    runtime INT,
    original_language_key INT NULL,  -- added FK later if desired
    release_date DATE NULL
  );
  PRINT 'Created dim_movie';
END

IF OBJECT_ID('dbo.dim_date','U') IS NULL
BEGIN
  CREATE TABLE dbo.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT, quarter INT, month INT, day INT, day_of_week INT
  );
  PRINT 'Created dim_date';
END

IF OBJECT_ID('dbo.fact_movie_metrics','U') IS NULL
BEGIN
  CREATE TABLE dbo.fact_movie_metrics (
    fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    movie_key INT NOT NULL,
    release_date_key INT NULL,
    budget BIGINT NULL,
    revenue BIGINT NULL,
    vote_count INT NULL,
    vote_average FLOAT NULL,
    popularity FLOAT NULL,
    load_timestamp DATETIME2 DEFAULT SYSUTCDATETIME()
  );
  PRINT 'Created fact_movie_metrics';
END

IF OBJECT_ID('dbo.bridge_movie_genre','U') IS NULL
BEGIN
  CREATE TABLE dbo.bridge_movie_genre (
    movie_key INT NOT NULL,
    genre_key INT NOT NULL,
    PRIMARY KEY (movie_key, genre_key)
  );
  PRINT 'Created bridge_movie_genre';
END

IF OBJECT_ID('dbo.bridge_movie_country','U') IS NULL
BEGIN
  CREATE TABLE dbo.bridge_movie_country (
    movie_key INT NOT NULL,
    country_key INT NOT NULL,
    PRIMARY KEY (movie_key, country_key)
  );
  PRINT 'Created bridge_movie_country';
END

IF OBJECT_ID('dbo.bridge_movie_company','U') IS NULL
BEGIN
  CREATE TABLE dbo.bridge_movie_company (
    movie_key INT NOT NULL,
    company_key INT NOT NULL,
    PRIMARY KEY (movie_key, company_key)
  );
  PRINT 'Created bridge_movie_company';
END
GO

-- ============================
-- 2) الآن نفّذ MERGE لإدخال الأبعاد (languages, genres, countries, companies)
-- ============================
-- Languages
MERGE dbo.dim_language AS target
USING (
  SELECT DISTINCT LTRIM(RTRIM(original_language)) AS language_code
  FROM dbo.staging_movies_final
  WHERE original_language IS NOT NULL AND LTRIM(RTRIM(original_language)) <> ''
) AS src(language_code)
ON target.language_code = src.language_code
WHEN NOT MATCHED THEN
  INSERT (language_code) VALUES (src.language_code);
GO

-- Genres
MERGE dbo.dim_genre AS target
USING (
  SELECT DISTINCT genre_name FROM dbo.staging_genres WHERE genre_name IS NOT NULL AND LTRIM(RTRIM(genre_name)) <> ''
) AS src(genre_name)
ON target.genre_name = src.genre_name
WHEN NOT MATCHED THEN
  INSERT (genre_name) VALUES (src.genre_name);
GO

-- Countries
MERGE dbo.dim_country AS target
USING (
  SELECT DISTINCT LTRIM(RTRIM(country_code_or_name)) AS country_code
  FROM dbo.staging_countries
  WHERE country_code_or_name IS NOT NULL AND LTRIM(RTRIM(country_code_or_name)) <> ''
) AS src(country_code)
ON target.country_code = src.country_code
WHEN NOT MATCHED THEN
  INSERT (country_code) VALUES (src.country_code);
GO

-- Companies (basic normalization lower+trim)
MERGE dbo.dim_company AS target
USING (
  SELECT DISTINCT LOWER(LTRIM(RTRIM(company_name))) AS company_name_normalized,
         MIN(LTRIM(RTRIM(company_name))) AS company_name_original
  FROM dbo.staging_companies
  WHERE company_name IS NOT NULL AND LTRIM(RTRIM(company_name)) <> ''
  GROUP BY LOWER(LTRIM(RTRIM(company_name)))
) AS src (company_name_normalized, company_name_original)
ON target.company_name_normalized = src.company_name_normalized
WHEN NOT MATCHED THEN
  INSERT (company_name_normalized, company_name_original)
  VALUES (src.company_name_normalized, src.company_name_original);
GO

-- ============================
-- 3) dim_movie upsert + dim_date fill + fact insert
-- ============================
-- dim_movie (upsert)
MERGE dbo.dim_movie AS target
USING (
  SELECT id AS tmdb_id, title, original_title, overview, runtime, original_language, release_date
  FROM dbo.staging_movies_final
) AS src
ON target.tmdb_id = src.tmdb_id
WHEN MATCHED THEN
  UPDATE SET
    title = src.title,
    original_title = src.original_title,
    overview = src.overview,
    runtime = src.runtime,
    original_language_key = (SELECT language_key FROM dbo.dim_language WHERE language_code = src.original_language),
    release_date = src.release_date
WHEN NOT MATCHED THEN
  INSERT (tmdb_id, title, original_title, overview, runtime, original_language_key, release_date)
  VALUES (src.tmdb_id, src.title, src.original_title, src.overview, src.runtime,
          (SELECT language_key FROM dbo.dim_language WHERE language_code = src.original_language),
          src.release_date);
GO

-- dim_date
INSERT INTO dbo.dim_date(date_key, full_date, year, quarter, month, day, day_of_week)
SELECT DISTINCT
  CONVERT(INT, FORMAT(release_date,'yyyyMMdd')) AS date_key,
  release_date,
  DATEPART(YEAR, release_date) AS year,
  DATEPART(QUARTER, release_date) AS quarter,
  DATEPART(MONTH, release_date) AS month,
  DATEPART(DAY, release_date) AS day,
  DATEPART(WEEKDAY, release_date) AS day_of_week
FROM dbo.staging_movies_final
WHERE release_date IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dbo.dim_date d WHERE d.date_key = CONVERT(INT, FORMAT(dbo.staging_movies_final.release_date,'yyyyMMdd')));
GO

-- fact_movie_metrics
INSERT INTO dbo.fact_movie_metrics (movie_key, release_date_key, budget, revenue, vote_count, vote_average, popularity)
SELECT m.movie_key,
       CASE WHEN s.release_date IS NOT NULL THEN CONVERT(INT, FORMAT(s.release_date,'yyyyMMdd')) ELSE NULL END as date_key,
       s.budget, s.revenue, s.vote_count, s.vote_average, s.popularity
FROM dbo.staging_movies_final s
JOIN dbo.dim_movie m ON m.tmdb_id = s.id;
GO

-- ============================
-- 4) Bridges (genres, countries, companies)
-- ============================
-- bridge_movie_genre
INSERT INTO dbo.bridge_movie_genre(movie_key, genre_key)
SELECT DISTINCT m.movie_key, g.genre_key
FROM dbo.staging_genres sg
JOIN dbo.dim_movie m ON m.tmdb_id = sg.id
JOIN dbo.dim_genre g ON g.genre_name = sg.genre_name
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.bridge_movie_genre b WHERE b.movie_key = m.movie_key AND b.genre_key = g.genre_key
);
GO

-- bridge_movie_country
INSERT INTO dbo.bridge_movie_country(movie_key, country_key)
SELECT DISTINCT m.movie_key, c.country_key
FROM dbo.staging_countries sc
JOIN dbo.dim_movie m ON m.tmdb_id = sc.id
JOIN dbo.dim_country c ON c.country_code = sc.country_code_or_name
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.bridge_movie_country b WHERE b.movie_key = m.movie_key AND b.country_key = c.country_key
);
GO

-- bridge_movie_company
INSERT INTO dbo.bridge_movie_company(movie_key, company_key)
SELECT DISTINCT m.movie_key, c.company_key
FROM dbo.staging_companies sc
JOIN dbo.dim_movie m ON m.tmdb_id = sc.id
JOIN dbo.dim_company c ON c.company_name_normalized = LOWER(LTRIM(RTRIM(sc.company_name)))
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.bridge_movie_company b WHERE b.movie_key = m.movie_key AND b.company_key = c.company_key
);
GO

-- ============================
-- 5) Summary counts
-- ============================
SELECT 
  (SELECT COUNT(*) FROM dbo.dim_movie) AS dim_movie_count,
  (SELECT COUNT(*) FROM dbo.fact_movie_metrics) AS fact_count,
  (SELECT COUNT(*) FROM dbo.dim_genre) AS dim_genre_count,
  (SELECT COUNT(*) FROM dbo.dim_company) AS dim_company_count,
  (SELECT COUNT(*) FROM dbo.bridge_movie_company) AS bridge_company_count;
GO

---------------------------
-- dim_language
MERGE dim_language AS target
USING (
  SELECT DISTINCT LTRIM(RTRIM(original_language)) AS language_code
  FROM staging_movies_final
  WHERE original_language IS NOT NULL AND LTRIM(RTRIM(original_language)) <> ''
) AS src(language_code)
ON target.language_code = src.language_code
WHEN NOT MATCHED THEN
  INSERT (language_code) VALUES (src.language_code);
GO

-- dim_genre
MERGE dim_genre AS target
USING (
  SELECT DISTINCT genre_name FROM staging_genres WHERE genre_name IS NOT NULL AND LTRIM(RTRIM(genre_name)) <> ''
) AS src(genre_name)
ON target.genre_name = src.genre_name
WHEN NOT MATCHED THEN
  INSERT (genre_name) VALUES (src.genre_name);
GO

-- dim_country
MERGE dim_country AS target
USING (
  SELECT DISTINCT LTRIM(RTRIM(country_code_or_name)) AS country_code
  FROM staging_countries
  WHERE country_code_or_name IS NOT NULL AND LTRIM(RTRIM(country_code_or_name)) <> ''
) AS src(country_code)
ON target.country_code = src.country_code
WHEN NOT MATCHED THEN
  INSERT (country_code) VALUES (src.country_code);
GO

-- dim_company (basic normalization)
MERGE dim_company AS target
USING (
  SELECT DISTINCT LOWER(LTRIM(RTRIM(company_name))) AS company_name_normalized,
         MIN(LTRIM(RTRIM(company_name))) AS company_name_original
  FROM staging_companies
  WHERE company_name IS NOT NULL AND LTRIM(RTRIM(company_name)) <> ''
  GROUP BY LOWER(LTRIM(RTRIM(company_name)))
) AS src (company_name_normalized, company_name_original)
ON target.company_name_normalized = src.company_name_normalized
WHEN NOT MATCHED THEN
  INSERT (company_name_normalized, company_name_original)
  VALUES (src.company_name_normalized, src.company_name_original);
GO

--------------------------------


-- كم id مكرر
SELECT COUNT(*) AS duplicate_id_groups
FROM (
  SELECT id
  FROM dbo.staging_movies_final
  GROUP BY id
  HAVING COUNT(*) > 1
) t;
GO

-- أمثلة على بعض الـ ids المكررة مع عدد التكرارات (top 20)
SELECT TOP 20 id, COUNT(*) AS cnt
FROM dbo.staging_movies_final
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;
GO

-- عينات من أحد الـ ids المكررة (مثال id = 675053)
SELECT *
FROM dbo.staging_movies_final
WHERE id = 675053;
GO


-------------------------------


-- ملخّص للـ duplicates: عدد المجموعات و min/max/avg للـ budget لكل id مكرر
SELECT id,
       COUNT(*) AS cnt,
       MIN(budget) AS min_budget,
       MAX(budget) AS max_budget,
       AVG(CAST(budget AS FLOAT)) AS avg_budget
FROM dbo.staging_movies_final
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
GO
----------------------------------



WITH dedup AS (
  SELECT s.*,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY 
             COALESCE(release_date, '1900-01-01') DESC,   -- أحدث تاريخ أولاً
             CASE WHEN ABS(COALESCE(budget,0)) > 10000000000 THEN 1 ELSE 0 END ASC, -- ضع المريب في المؤخرة
             ABS(COALESCE(budget,0)) ASC,                 -- ميزانية أصغر مفضلة
             COALESCE(revenue,0) DESC                    -- كفاية tie-breaker: أكبر إيراد
         ) AS rn
  FROM dbo.staging_movies_final s
)
MERGE dbo.dim_movie AS target
USING (
  SELECT id AS tmdb_id, title, original_title, overview, runtime, original_language, release_date
  FROM dedup
  WHERE rn = 1
) AS src
ON target.tmdb_id = src.tmdb_id
WHEN MATCHED THEN
  UPDATE SET
    title = src.title,
    original_title = src.original_title,
    overview = src.overview,
    runtime = src.runtime,
    original_language_key = (SELECT language_key FROM dbo.dim_language WHERE language_code = src.original_language),
    release_date = src.release_date
WHEN NOT MATCHED THEN
  INSERT (tmdb_id, title, original_title, overview, runtime, original_language_key, release_date)
  VALUES (src.tmdb_id, src.title, src.original_title, src.overview, src.runtime,
          (SELECT language_key FROM dbo.dim_language WHERE language_code = src.original_language),
          src.release_date);
GO
---------------------------------


-- ---------- A: refill dim_date (يدرج تواريخ جديدة فقط) ----------
INSERT INTO dbo.dim_date(date_key, full_date, year, quarter, month, day, day_of_week)
SELECT DISTINCT
  CONVERT(INT, FORMAT(release_date,'yyyyMMdd')) AS date_key,
  release_date,
  DATEPART(YEAR, release_date) AS year,
  DATEPART(QUARTER, release_date) AS quarter,
  DATEPART(MONTH, release_date) AS month,
  DATEPART(DAY, release_date) AS day,
  DATEPART(WEEKDAY, release_date) AS day_of_week
FROM dbo.staging_movies_final s
WHERE s.release_date IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM dbo.dim_date d 
    WHERE d.date_key = CONVERT(INT, FORMAT(s.release_date,'yyyyMMdd'))
  );
GO

-- تحقق من عدد الصفوف في dim_date بعد الإدراج
SELECT COUNT(*) AS dim_date_count FROM dbo.dim_date;
GO


-- ---------- B: insert facts (use dedup to pick one row per id) ----------
WITH dedup AS (
  SELECT s.*,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY 
             COALESCE(release_date, '1900-01-01') DESC,
             CASE WHEN ABS(COALESCE(budget,0)) > 10000000000 THEN 1 ELSE 0 END ASC,
             ABS(COALESCE(budget,0)) ASC,
             COALESCE(revenue,0) DESC
         ) AS rn
  FROM dbo.staging_movies_final s
)
INSERT INTO dbo.fact_movie_metrics (movie_key, release_date_key, budget, revenue, vote_count, vote_average, popularity)
SELECT m.movie_key,
       CASE WHEN d.full_date IS NOT NULL THEN d.date_key ELSE NULL END as date_key,
       s.budget, s.revenue, s.vote_count, s.vote_average, s.popularity
FROM (
  SELECT * FROM dedup WHERE rn = 1
) s
JOIN dbo.dim_movie m ON m.tmdb_id = s.id
LEFT JOIN dbo.dim_date d ON d.full_date = s.release_date
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.fact_movie_metrics f WHERE f.movie_key = m.movie_key
);
GO

-- تحقق نتائج facts
SELECT COUNT(*) AS fact_count FROM dbo.fact_movie_metrics;
GO


-- ---------- C: bridge_movie_genre (dedup repeated) ----------
WITH dedup AS (
  SELECT s.*,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY 
             COALESCE(release_date, '1900-01-01') DESC,
             CASE WHEN ABS(COALESCE(budget,0)) > 10000000000 THEN 1 ELSE 0 END ASC,
             ABS(COALESCE(budget,0)) ASC,
             COALESCE(revenue,0) DESC
         ) AS rn
  FROM dbo.staging_movies_final s
)
INSERT INTO dbo.bridge_movie_genre(movie_key, genre_key)
SELECT DISTINCT m.movie_key, g.genre_key
FROM dbo.staging_genres sg
JOIN (SELECT id FROM dedup WHERE rn = 1) s ON s.id = sg.id
JOIN dbo.dim_movie m ON m.tmdb_id = s.id
JOIN dbo.dim_genre g ON g.genre_name = sg.genre_name
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.bridge_movie_genre b WHERE b.movie_key = m.movie_key AND b.genre_key = g.genre_key
);
GO

-- تحقق genres bridge
SELECT COUNT(*) AS bridge_genre_count FROM dbo.bridge_movie_genre;
GO


-- ---------- D: bridge_movie_country (dedup repeated) ----------
WITH dedup AS (
  SELECT s.*,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY 
             COALESCE(release_date, '1900-01-01') DESC,
             CASE WHEN ABS(COALESCE(budget,0)) > 10000000000 THEN 1 ELSE 0 END ASC,
             ABS(COALESCE(budget,0)) ASC,
             COALESCE(revenue,0) DESC
         ) AS rn
  FROM dbo.staging_movies_final s
)
INSERT INTO dbo.bridge_movie_country(movie_key, country_key)
SELECT DISTINCT m.movie_key, c.country_key
FROM dbo.staging_countries sc
JOIN (SELECT id FROM dedup WHERE rn = 1) s ON s.id = sc.id
JOIN dbo.dim_movie m ON m.tmdb_id = s.id
JOIN dbo.dim_country c ON c.country_code = sc.country_code_or_name
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.bridge_movie_country b WHERE b.movie_key = m.movie_key AND b.country_key = c.country_key
);
GO

-- تحقق countries bridge
SELECT COUNT(*) AS bridge_country_count FROM dbo.bridge_movie_country;
GO


-- ---------- E: bridge_movie_company (dedup repeated) ----------
WITH dedup AS (
  SELECT s.*,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY 
             COALESCE(release_date, '1900-01-01') DESC,
             CASE WHEN ABS(COALESCE(budget,0)) > 10000000000 THEN 1 ELSE 0 END ASC,
             ABS(COALESCE(budget,0)) ASC,
             COALESCE(revenue,0) DESC
         ) AS rn
  FROM dbo.staging_movies_final s
)
INSERT INTO dbo.bridge_movie_company(movie_key, company_key)
SELECT DISTINCT m.movie_key, c.company_key
FROM dbo.staging_companies sc
JOIN (SELECT id FROM dedup WHERE rn = 1) s ON s.id = sc.id
JOIN dbo.dim_movie m ON m.tmdb_id = s.id
JOIN dbo.dim_company c ON c.company_name_normalized = LOWER(LTRIM(RTRIM(sc.company_name)))
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.bridge_movie_company b WHERE b.movie_key = m.movie_key AND b.company_key = c.company_key
);
GO

-- تحقق companies bridge
SELECT COUNT(*) AS bridge_company_count FROM dbo.bridge_movie_company;
GO

--------------------
SELECT 
  (SELECT COUNT(*) FROM dbo.dim_movie) AS dim_movie_count,
  (SELECT COUNT(*) FROM dbo.fact_movie_metrics) AS fact_count,
  (SELECT COUNT(*) FROM dbo.bridge_movie_company) AS bridge_company_count;
GO
--------------------
SELECT 
  (SELECT COUNT(*) FROM dbo.dim_movie) AS dim_movie_count,
  (SELECT COUNT(*) FROM dbo.fact_movie_metrics) AS fact_count,
  (SELECT COUNT(*) FROM dbo.dim_movie WHERE movie_key NOT IN (SELECT movie_key FROM dbo.fact_movie_metrics)) AS movies_without_fact;
GO
------------------------
SELECT TOP 100 s.id
FROM dbo.staging_movies_final s
LEFT JOIN dbo.dim_movie d ON d.tmdb_id = s.id
WHERE d.tmdb_id IS NULL
GROUP BY s.id;
GO
--------------------------------
-- أفلام موجودة في dim_movie لكن بدون fact
SELECT TOP 50 d.tmdb_id, d.title
FROM dbo.dim_movie d
LEFT JOIN dbo.fact_movie_metrics f ON f.movie_key = d.movie_key
WHERE f.movie_key IS NULL;
GO

-------------------------------
-- متوسط عدد الأنواع لكل فيلم
SELECT AVG(cnt) AS avg_genres_per_movie
FROM (
  SELECT COUNT(*) AS cnt FROM dbo.bridge_movie_genre GROUP BY movie_key
) t;

-- متوسط عدد الشركات لكل فيلم
SELECT AVG(cnt) AS avg_companies_per_movie
FROM (
  SELECT COUNT(*) AS cnt FROM dbo.bridge_movie_company GROUP BY movie_key
) t;
GO
-------------------------------

-- index helper (create if not exists)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_dim_movie_tmdb')
  CREATE INDEX ix_dim_movie_tmdb ON dbo.dim_movie (tmdb_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_fact_movie_moviekey')
  CREATE INDEX ix_fact_movie_moviekey ON dbo.fact_movie_metrics (movie_key);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_fact_movie_datekey')
  CREATE INDEX ix_fact_movie_datekey ON dbo.fact_movie_metrics (release_date_key);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_bridge_genre_moviekey')
  CREATE INDEX ix_bridge_genre_moviekey ON dbo.bridge_movie_genre (movie_key);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_bridge_company_moviekey')
  CREATE INDEX ix_bridge_company_moviekey ON dbo.bridge_movie_company (movie_key);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_dim_company_norm')
  CREATE INDEX ix_dim_company_norm ON dbo.dim_company (company_name_normalized);
GO
------------------------
USE movies_db;
GO

-- 1. أنشئ جدول استقبال للـ CSV
IF OBJECT_ID('dbo.company_canonical_map','U') IS NOT NULL DROP TABLE dbo.company_canonical_map;
CREATE TABLE dbo.company_canonical_map (
  company_name_normalized NVARCHAR(300),
  canonical_name NVARCHAR(300),
  group_id INT
);
GO

-- 2. استورد الملف (عدّل المسار إن كان مختلفاً)
BULK INSERT dbo.company_canonical_map
FROM 'D:\mnt\data\company_canonical_map.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR = '\n',
  CODEPAGE = '65001',  -- UTF-8
  TABLOCK
);
GO

-- تأكد من الأعداد
SELECT COUNT(*) AS rows_in_map FROM dbo.company_canonical_map;
SELECT TOP 20 * FROM dbo.company_canonical_map;
GO

-------------------------
USE movies_db;
GO

-- 1. أنشئ dim_company_canonical
IF OBJECT_ID('dbo.dim_company_canonical','U') IS NOT NULL DROP TABLE dbo.dim_company_canonical;
CREATE TABLE dbo.dim_company_canonical (
  canonical_key INT IDENTITY(1,1) PRIMARY KEY,
  canonical_name NVARCHAR(300) UNIQUE
);

-- 2. أدخل canonical names (distinct)
INSERT INTO dbo.dim_company_canonical (canonical_name)
SELECT DISTINCT canonical_name
FROM dbo.company_canonical_map
WHERE canonical_name IS NOT NULL AND LTRIM(RTRIM(canonical_name)) <> '';
GO

SELECT COUNT(*) AS canonical_count FROM dbo.dim_company_canonical;
SELECT TOP 10 * FROM dbo.dim_company_canonical;
GO

------------------------------
USE movies_db;
GO

-- 1. أنشئ جدول ربط جديد
IF OBJECT_ID('dbo.bridge_movie_company_canonical','U') IS NOT NULL DROP TABLE dbo.bridge_movie_company_canonical;
CREATE TABLE dbo.bridge_movie_company_canonical (
  movie_key INT,
  canonical_key INT,
  PRIMARY KEY (movie_key, canonical_key)
);

-- 2. تأكد أن dim_company يحتوي company_name_normalized (الذي استُخدم في الـ mapping)
SELECT TOP 10 company_key, company_name_normalized FROM dbo.dim_company;

-- 3. املأ bridge_movie_company_canonical عبر الانضمام على mapping
INSERT INTO dbo.bridge_movie_company_canonical (movie_key, canonical_key)
SELECT DISTINCT m.movie_key, c.canonical_key
FROM dbo.bridge_movie_company b
JOIN dbo.dim_company mco ON mco.company_key = b.company_key
JOIN dbo.company_canonical_map cmap ON cmap.company_name_normalized = mco.company_name_normalized
JOIN dbo.dim_company_canonical c ON c.canonical_name = cmap.canonical_name
JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.bridge_movie_company_canonical x 
  WHERE x.movie_key = m.movie_key AND x.canonical_key = c.canonical_key
);
GO

-- تحقق من الأعداد
SELECT COUNT(*) AS new_bridge_count FROM dbo.bridge_movie_company_canonical;
SELECT TOP 20 * FROM dbo.bridge_movie_company_canonical;
GO

-------------------------------
-- (اختياري - نفّذ فقط بعد المراجعة)
BEGIN TRAN;
-- backup existing bridge
SELECT * INTO dbo.bridge_movie_company_backup FROM dbo.bridge_movie_company;

-- حذف القديم وإعادة إنشاء من canonical map
TRUNCATE TABLE dbo.bridge_movie_company;

INSERT INTO dbo.bridge_movie_company (movie_key, company_key)
SELECT bmc.movie_key, dc.company_key
FROM dbo.bridge_movie_company_canonical bmc
JOIN dbo.dim_company dc ON LOWER(dc.company_name_normalized) = LOWER(
    (SELECT TOP 1 company_name_normalized FROM dbo.company_canonical_map WHERE canonical_name = (SELECT canonical_name FROM dbo.dim_company_canonical WHERE canonical_key = bmc.canonical_key))
);
-- تحقق ثم COMMIT
COMMIT TRAN;
-------------------------------
USE movies_db;
GO
PRINT '=== STEP 1: Create or backup company_canonical_map ===';

-- Backup old version if exists
IF OBJECT_ID('dbo.company_canonical_map_bak','U') IS NOT NULL DROP TABLE dbo.company_canonical_map_bak;
IF OBJECT_ID('dbo.company_canonical_map','U') IS NOT NULL
BEGIN
    SELECT * INTO dbo.company_canonical_map_bak FROM dbo.company_canonical_map;
    DROP TABLE dbo.company_canonical_map;
    PRINT 'Old company_canonical_map backed up as company_canonical_map_bak.';
END
ELSE
    PRINT 'No previous company_canonical_map table found.';

-- Create the mapping table
CREATE TABLE dbo.company_canonical_map (
    company_name_normalized NVARCHAR(300),
    canonical_name NVARCHAR(300),
    group_id INT
);
GO

PRINT '=== STEP 2: Bulk insert canonical map from CSV ===';
BULK INSERT dbo.company_canonical_map
FROM 'D:\mnt\data\company_canonical_map.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001',
    TABLOCK
);
GO

PRINT 'Inserted rows:';
SELECT COUNT(*) AS total_rows FROM dbo.company_canonical_map;
GO

PRINT '=== STEP 3: Create dim_company_canonical ===';

-- Backup if exists
IF OBJECT_ID('dbo.dim_company_canonical_bak','U') IS NOT NULL DROP TABLE dbo.dim_company_canonical_bak;
IF OBJECT_ID('dbo.dim_company_canonical','U') IS NOT NULL
BEGIN
    SELECT * INTO dbo.dim_company_canonical_bak FROM dbo.dim_company_canonical;
    DROP TABLE dbo.dim_company_canonical;
    PRINT 'Old dim_company_canonical backed up as dim_company_canonical_bak.';
END
ELSE
    PRINT 'No previous dim_company_canonical table found.';

-- Create canonical dimension
CREATE TABLE dbo.dim_company_canonical (
    canonical_key INT IDENTITY(1,1) PRIMARY KEY,
    canonical_name NVARCHAR(300) UNIQUE
);

-- Insert distinct canonical names
INSERT INTO dbo.dim_company_canonical (canonical_name)
SELECT DISTINCT LTRIM(RTRIM(canonical_name))
FROM dbo.company_canonical_map
WHERE canonical_name IS NOT NULL AND LTRIM(RTRIM(canonical_name)) <> '';
GO

SELECT COUNT(*) AS canonical_count FROM dbo.dim_company_canonical;
GO

PRINT '=== STEP 4: Create bridge_movie_company_canonical ===';

-- Backup if exists
IF OBJECT_ID('dbo.bridge_movie_company_canonical_bak','U') IS NOT NULL DROP TABLE dbo.bridge_movie_company_canonical_bak;
IF OBJECT_ID('dbo.bridge_movie_company_canonical','U') IS NOT NULL
BEGIN
    SELECT * INTO dbo.bridge_movie_company_canonical_bak FROM dbo.bridge_movie_company_canonical;
    DROP TABLE dbo.bridge_movie_company_canonical;
    PRINT 'Old bridge_movie_company_canonical backed up as bridge_movie_company_canonical_bak.';
END
ELSE
    PRINT 'No previous bridge_movie_company_canonical found.';

-- Create new bridge
CREATE TABLE dbo.bridge_movie_company_canonical (
    movie_key INT NOT NULL,
    canonical_key INT NOT NULL,
    PRIMARY KEY (movie_key, canonical_key)
);
GO

PRINT '=== STEP 5: Populate bridge_movie_company_canonical ===';

INSERT INTO dbo.bridge_movie_company_canonical (movie_key, canonical_key)
SELECT DISTINCT b.movie_key, c2.canonical_key
FROM dbo.bridge_movie_company b
JOIN dbo.dim_company dc ON dc.company_key = b.company_key
JOIN dbo.company_canonical_map cmap ON cmap.company_name_normalized = dc.company_name_normalized
JOIN dbo.dim_company_canonical c2 ON c2.canonical_name = cmap.canonical_name
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.bridge_movie_company_canonical x 
    WHERE x.movie_key = b.movie_key AND x.canonical_key = c2.canonical_key
);
GO

SELECT COUNT(*) AS bridge_count FROM dbo.bridge_movie_company_canonical;
GO

PRINT '=== STEP 6: Create indexes for performance ===';
CREATE INDEX ix_dim_company_canonical_name ON dbo.dim_company_canonical(canonical_name);
CREATE INDEX ix_bridge_movie_company_canonical_movie ON dbo.bridge_movie_company_canonical(movie_key);
CREATE INDEX ix_bridge_movie_company_canonical_canonical ON dbo.bridge_movie_company_canonical(canonical_key);
GO

PRINT '=== ✅ Process complete successfully ===';

-----------------------
SELECT COUNT(*) FROM dbo.dim_company_canonical;             -- عدد الأسماء الموحّدة
SELECT COUNT(*) FROM dbo.bridge_movie_company_canonical;    -- عدد روابط الفيلم ↔ الشركة
---------------------------
SELECT TOP 20 c.canonical_name, COUNT(*) AS movie_count
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
GROUP BY c.canonical_name
ORDER BY movie_count DESC;

--------------------------------
SELECT TOP 20 c.canonical_name, 
       SUM(COALESCE(f.revenue,0)) AS total_revenue,
       COUNT(DISTINCT f.movie_key) AS movies_count
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
GROUP BY c.canonical_name
ORDER BY total_revenue DESC;

---------------------------------
SELECT TOP 20 c.canonical_name,
       AVG(COALESCE(f.revenue,0)) AS avg_revenue,
       COUNT(DISTINCT f.movie_key) AS movies_count
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
GROUP BY c.canonical_name
HAVING COUNT(DISTINCT f.movie_key) >= 5 -- فلترة: شركات على الاقل 5 افلام
ORDER BY avg_revenue DESC;
----------------------------------
SELECT TOP 20 
    c.canonical_name,
    SUM(CAST(COALESCE(f.budget, 0) AS DECIMAL(38, 0))) AS total_budget,
    COUNT(DISTINCT f.movie_key) AS movies_count
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
GROUP BY c.canonical_name
ORDER BY total_budget DESC;

--------------------------------
SELECT TOP 20 
    c.canonical_name,
    SUM(CAST(COALESCE(f.revenue, 0) AS DECIMAL(38, 2))) AS total_revenue,
    SUM(CAST(COALESCE(f.budget, 0) AS DECIMAL(38, 2))) AS total_budget,
    CASE 
        WHEN SUM(CAST(COALESCE(f.budget, 0) AS DECIMAL(38, 2))) = 0 THEN NULL
        ELSE 
            (SUM(CAST(COALESCE(f.revenue, 0) AS DECIMAL(38, 2))) 
             - SUM(CAST(COALESCE(f.budget, 0) AS DECIMAL(38, 2))))
            / NULLIF(SUM(CAST(COALESCE(f.budget, 0) AS DECIMAL(38, 2))), 0)
    END AS aggregate_roi,
    COUNT(DISTINCT f.movie_key) AS movies_count
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c 
    ON c.canonical_key = b.canonical_key
JOIN dbo.fact_movie_metrics f 
    ON f.movie_key = b.movie_key
GROUP BY c.canonical_name
HAVING COUNT(DISTINCT f.movie_key) >= 5
ORDER BY aggregate_roi DESC;

----------------------------------
SELECT TOP 50 c.canonical_name, COUNT(*) AS suspicious_count
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
WHERE ABS(COALESCE(f.budget,0)) > 10000000000
GROUP BY c.canonical_name
ORDER BY suspicious_count DESC;
------------------------------------
-- شركات في dim_company بدون mapping
SELECT TOP 100 dc.company_key, dc.company_name_normalized
FROM dbo.dim_company dc
LEFT JOIN dbo.company_canonical_map cmap ON cmap.company_name_normalized = dc.company_name_normalized
WHERE cmap.company_name_normalized IS NULL;
--------------------------------------------
SELECT c.canonical_name,
       COUNT(DISTINCT b.movie_key) AS movies_count,
       SUM(CASE WHEN COALESCE(f.revenue,0) > 0 THEN 1 ELSE 0 END) AS movies_with_revenue,
       CAST(100.0 * SUM(CASE WHEN COALESCE(f.revenue,0)>0 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT b.movie_key),0) AS DECIMAL(5,2)) AS pct_with_revenue
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
LEFT JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
GROUP BY c.canonical_name
ORDER BY movies_count DESC;
-------------------------------------------
USE movies_db;
GO

-- Top companies by movie count
IF OBJECT_ID('dbo.vw_company_movie_count','V') IS NOT NULL DROP VIEW dbo.vw_company_movie_count;
GO
CREATE VIEW dbo.vw_company_movie_count AS
SELECT c.canonical_key, c.canonical_name, COUNT(DISTINCT b.movie_key) AS movie_count
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
GROUP BY c.canonical_key, c.canonical_name;
GO

-- Company revenue summary (total revenue, movies_count)
IF OBJECT_ID('dbo.vw_company_revenue_summary','V') IS NOT NULL DROP VIEW dbo.vw_company_revenue_summary;
GO
CREATE VIEW dbo.vw_company_revenue_summary AS
SELECT c.canonical_key, c.canonical_name,
       COUNT(DISTINCT f.movie_key) AS movies_count,
       SUM(COALESCE(f.revenue,0)) AS total_revenue,
       SUM(COALESCE(f.budget,0)) AS total_budget,
       CASE WHEN SUM(COALESCE(f.budget,0)) = 0 THEN NULL
            ELSE CAST(SUM(COALESCE(f.revenue,0)) - SUM(COALESCE(f.budget,0)) AS FLOAT) / SUM(COALESCE(f.budget,0))
       END AS aggregate_roi,
       AVG(COALESCE(f.revenue,0)) AS avg_revenue
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
GROUP BY c.canonical_key, c.canonical_name;
GO

-- Suspicious budgets per company
IF OBJECT_ID('dbo.vw_company_suspicious_budgets','V') IS NOT NULL DROP VIEW dbo.vw_company_suspicious_budgets;
GO
CREATE VIEW dbo.vw_company_suspicious_budgets AS
SELECT c.canonical_key, c.canonical_name,
       SUM(CASE WHEN ABS(COALESCE(f.budget,0)) > 10000000000 THEN 1 ELSE 0 END) AS suspicious_count,
       SUM(CASE WHEN ABS(COALESCE(f.budget,0)) > 10000000000 THEN COALESCE(f.budget,0) ELSE 0 END) AS suspicious_budget_sum
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
GROUP BY c.canonical_key, c.canonical_name;
GO

-- Company coverage: movies with revenue > 0 percentage
IF OBJECT_ID('dbo.vw_company_revenue_coverage','V') IS NOT NULL DROP VIEW dbo.vw_company_revenue_coverage;
GO
CREATE VIEW dbo.vw_company_revenue_coverage AS
SELECT c.canonical_key, c.canonical_name,
       COUNT(DISTINCT b.movie_key) AS movies_count,
       SUM(CASE WHEN COALESCE(f.revenue,0) > 0 THEN 1 ELSE 0 END) AS movies_with_revenue,
       CAST(100.0 * SUM(CASE WHEN COALESCE(f.revenue,0) > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT b.movie_key),0) AS DECIMAL(6,2)) AS pct_with_revenue
FROM dbo.bridge_movie_company_canonical b
JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
LEFT JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
GROUP BY c.canonical_key, c.canonical_name;
GO




----------------------------------
USE movies_db;
GO

IF OBJECT_ID('dbo.sp_top_companies_stats','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_top_companies_stats;
GO

CREATE PROCEDURE dbo.sp_top_companies_stats
    @topN INT = 20,
    @minMovies INT = 5,
    @fromYear INT = NULL
AS
BEGIN
  SET NOCOUNT ON;

  -- temp table for filtered movies (apply year filter if requested)
  IF OBJECT_ID('tempdb..#filtered_movies') IS NOT NULL DROP TABLE #filtered_movies;

  SELECT f.movie_key, f.budget, f.revenue, f.vote_count, f.vote_average, f.popularity
  INTO #filtered_movies
  FROM dbo.fact_movie_metrics f
  JOIN dbo.dim_movie m ON m.movie_key = f.movie_key
  WHERE (@fromYear IS NULL OR YEAR(m.release_date) >= @fromYear);

  -- Top by movie count
  SELECT TOP (@topN) c.canonical_name, COUNT(DISTINCT b.movie_key) AS movie_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  ORDER BY movie_count DESC;

  -- Top by total revenue (with minMovies filter)
  SELECT TOP (@topN) c.canonical_name,
       SUM(COALESCE(fm.revenue,0)) AS total_revenue,
       SUM(COALESCE(fm.budget,0)) AS total_budget,
       COUNT(DISTINCT fm.movie_key) AS movies_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
  ORDER BY total_revenue DESC;

  -- Top by avg revenue per movie
  SELECT TOP (@topN) c.canonical_name,
       AVG(COALESCE(fm.revenue,0)) AS avg_revenue,
       COUNT(DISTINCT fm.movie_key) AS movies_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
  ORDER BY avg_revenue DESC;

  -- Top by aggregate ROI
  SELECT TOP (@topN) c.canonical_name,
       SUM(COALESCE(fm.revenue,0)) AS total_revenue,
       SUM(COALESCE(fm.budget,0)) AS total_budget,
       CASE WHEN SUM(COALESCE(fm.budget,0)) = 0 THEN NULL
            ELSE CAST(SUM(COALESCE(fm.revenue,0)) - SUM(COALESCE(fm.budget,0)) AS FLOAT) / SUM(COALESCE(fm.budget,0))
       END AS aggregate_roi,
       COUNT(DISTINCT fm.movie_key) AS movies_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
  ORDER BY aggregate_roi DESC;

  -- Suspicious budgets
  SELECT TOP (@topN) c.canonical_name,
       SUM(CASE WHEN ABS(COALESCE(fm.budget,0)) > 10000000000 THEN 1 ELSE 0 END) AS suspicious_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  ORDER BY suspicious_count DESC;

  -- coverage
  SELECT TOP (@topN) c.canonical_name,
       COUNT(DISTINCT b.movie_key) AS movies_count,
       SUM(CASE WHEN COALESCE(fm.revenue,0) > 0 THEN 1 ELSE 0 END) AS movies_with_revenue,
       CAST(100.0 * SUM(CASE WHEN COALESCE(fm.revenue,0) > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT b.movie_key),0) AS DECIMAL(6,2)) AS pct_with_revenue
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  ORDER BY movies_count DESC;

END
GO
-------------------------
USE movies_db;
GO

IF OBJECT_ID('dbo.sp_top_companies_stats','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_top_companies_stats;
GO

CREATE PROCEDURE dbo.sp_top_companies_stats
    @topN INT = 20,
    @minMovies INT = 5,
    @fromYear INT = NULL
AS
BEGIN
  SET NOCOUNT ON;

  IF OBJECT_ID('tempdb..#filtered_movies') IS NOT NULL DROP TABLE #filtered_movies;

  SELECT f.movie_key,
         CAST(f.budget AS DECIMAL(38,2)) AS budget,
         CAST(f.revenue AS DECIMAL(38,2)) AS revenue,
         f.vote_count, f.vote_average, f.popularity
  INTO #filtered_movies
  FROM dbo.fact_movie_metrics f
  JOIN dbo.dim_movie m ON m.movie_key = f.movie_key
  WHERE (@fromYear IS NULL OR YEAR(m.release_date) >= @fromYear);

  -----------------------------------------------------
  -- 1️⃣ Top by movie count
  -----------------------------------------------------
  SELECT TOP (@topN) c.canonical_name,
         COUNT(DISTINCT b.movie_key) AS movie_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  ORDER BY movie_count DESC;

  -----------------------------------------------------
  -- 2️⃣ Top by total revenue
  -----------------------------------------------------
  SELECT TOP (@topN) c.canonical_name,
         SUM(CAST(fm.revenue AS DECIMAL(38,2))) AS total_revenue,
         SUM(CAST(fm.budget AS DECIMAL(38,2))) AS total_budget,
         COUNT(DISTINCT fm.movie_key) AS movies_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
  ORDER BY total_revenue DESC;

  -----------------------------------------------------
  -- 3️⃣ Top by average revenue per movie
  -----------------------------------------------------
  SELECT TOP (@topN) c.canonical_name,
         AVG(CAST(fm.revenue AS DECIMAL(38,2))) AS avg_revenue,
         COUNT(DISTINCT fm.movie_key) AS movies_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
  ORDER BY avg_revenue DESC;

  -----------------------------------------------------
  -- 4️⃣ Top by aggregate ROI
  -----------------------------------------------------
  SELECT TOP (@topN) c.canonical_name,
         SUM(CAST(fm.revenue AS DECIMAL(38,2))) AS total_revenue,
         SUM(CAST(fm.budget AS DECIMAL(38,2))) AS total_budget,
         CASE WHEN SUM(CAST(fm.budget AS DECIMAL(38,2))) = 0 THEN NULL
              ELSE (SUM(CAST(fm.revenue AS DECIMAL(38,2))) - SUM(CAST(fm.budget AS DECIMAL(38,2))))
                   / NULLIF(SUM(CAST(fm.budget AS DECIMAL(38,2))),0)
         END AS aggregate_roi,
         COUNT(DISTINCT fm.movie_key) AS movies_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
  ORDER BY aggregate_roi DESC;

  -----------------------------------------------------
  -- 5️⃣ Suspicious budgets
  -----------------------------------------------------
  SELECT TOP (@topN) c.canonical_name,
         SUM(CASE WHEN ABS(CAST(fm.budget AS DECIMAL(38,2))) > 10000000000 THEN 1 ELSE 0 END) AS suspicious_count
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  ORDER BY suspicious_count DESC;

  -----------------------------------------------------
  -- 6️⃣ Coverage (movies with revenue)
  -----------------------------------------------------
  SELECT TOP (@topN) c.canonical_name,
         COUNT(DISTINCT b.movie_key) AS movies_count,
         SUM(CASE WHEN COALESCE(fm.revenue,0) > 0 THEN 1 ELSE 0 END) AS movies_with_revenue,
         CAST(100.0 * SUM(CASE WHEN COALESCE(fm.revenue,0) > 0 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(DISTINCT b.movie_key),0) AS DECIMAL(6,2)) AS pct_with_revenue
  FROM dbo.bridge_movie_company_canonical b
  JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
  JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
  GROUP BY c.canonical_name
  ORDER BY movies_count DESC;

END
GO


-----------------------------------
-- Example 1: top 20, min 5 movies, all years
EXEC dbo.sp_top_companies_stats @topN=20, @minMovies=5, @fromYear=NULL;
GO

-- Example 2: top 50 companies, only movies from 2010 onwards
EXEC dbo.sp_top_companies_stats @topN=50, @minMovies=3, @fromYear=2010;
GO


EXEC dbo.sp_top_companies_stats @topN = 20, @minMovies = 5, @fromYear = NULL;
GO

EXEC dbo.sp_top_companies_stats @topN = 50, @minMovies = 3, @fromYear = 2010;
GO
-------------------------------------------------
USE movies_db;
GO

IF OBJECT_ID('dbo.sp_top_companies_stats','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_top_companies_stats;
GO

CREATE PROCEDURE dbo.sp_top_companies_stats
    @topN INT = 20,
    @minMovies INT = 5,
    @fromYear INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------------------------
    -- إعداد مؤقت: تصفية الأفلام بالسنة إن لزم
    -------------------------------------------------
    IF OBJECT_ID('tempdb..#filtered_movies') IS NOT NULL DROP TABLE #filtered_movies;

    SELECT f.movie_key,
           CAST(f.budget AS DECIMAL(38,2)) AS budget,
           CAST(f.revenue AS DECIMAL(38,2)) AS revenue,
           f.vote_count, f.vote_average, f.popularity
    INTO #filtered_movies
    FROM dbo.fact_movie_metrics f
    JOIN dbo.dim_movie m ON m.movie_key = f.movie_key
    WHERE (@fromYear IS NULL OR YEAR(m.release_date) >= @fromYear);

    -------------------------------------------------
    -- 1️⃣ Top by Movie Count
    -------------------------------------------------
    PRINT '>>> Top companies by number of movies';
    SELECT TOP (@topN)
           c.canonical_name,
           COUNT(DISTINCT b.movie_key) AS movie_count,
           COUNT(DISTINCT gc.genre_key) AS genres_count,
           COUNT(DISTINCT cc.country_key) AS countries_count
    FROM dbo.bridge_movie_company_canonical b
    JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
    JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_genre gc ON gc.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_country cc ON cc.movie_key = b.movie_key
    GROUP BY c.canonical_name
    ORDER BY movie_count DESC;

    -------------------------------------------------
    -- 2️⃣ Top by Total Revenue
    -------------------------------------------------
    PRINT '>>> Top companies by total revenue';
    SELECT TOP (@topN)
           c.canonical_name,
           COUNT(DISTINCT fm.movie_key) AS movies_count,
           COUNT(DISTINCT gc.genre_key) AS genres_count,
           COUNT(DISTINCT cc.country_key) AS countries_count,
           SUM(CAST(fm.revenue AS DECIMAL(38,2))) AS total_revenue,
           SUM(CAST(fm.budget AS DECIMAL(38,2))) AS total_budget
    FROM dbo.bridge_movie_company_canonical b
    JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
    JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_genre gc ON gc.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_country cc ON cc.movie_key = b.movie_key
    GROUP BY c.canonical_name
    HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
    ORDER BY total_revenue DESC;

    -------------------------------------------------
    -- 3️⃣ Top by Average Revenue per Movie
    -------------------------------------------------
    PRINT '>>> Top companies by average revenue per movie';
    SELECT TOP (@topN)
           c.canonical_name,
           COUNT(DISTINCT fm.movie_key) AS movies_count,
           COUNT(DISTINCT gc.genre_key) AS genres_count,
           COUNT(DISTINCT cc.country_key) AS countries_count,
           AVG(CAST(fm.revenue AS DECIMAL(38,2))) AS avg_revenue
    FROM dbo.bridge_movie_company_canonical b
    JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
    JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_genre gc ON gc.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_country cc ON cc.movie_key = b.movie_key
    GROUP BY c.canonical_name
    HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
    ORDER BY avg_revenue DESC;

    -------------------------------------------------
    -- 4️⃣ Top by ROI
    -------------------------------------------------
    PRINT '>>> Top companies by ROI';
    SELECT TOP (@topN)
           c.canonical_name,
           COUNT(DISTINCT fm.movie_key) AS movies_count,
           COUNT(DISTINCT gc.genre_key) AS genres_count,
           COUNT(DISTINCT cc.country_key) AS countries_count,
           SUM(CAST(fm.revenue AS DECIMAL(38,2))) AS total_revenue,
           SUM(CAST(fm.budget AS DECIMAL(38,2))) AS total_budget,
           CASE WHEN SUM(CAST(fm.budget AS DECIMAL(38,2))) = 0 THEN NULL
                ELSE (SUM(CAST(fm.revenue AS DECIMAL(38,2))) - SUM(CAST(fm.budget AS DECIMAL(38,2))))
                     / NULLIF(SUM(CAST(fm.budget AS DECIMAL(38,2))),0)
           END AS aggregate_roi
    FROM dbo.bridge_movie_company_canonical b
    JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
    JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_genre gc ON gc.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_country cc ON cc.movie_key = b.movie_key
    GROUP BY c.canonical_name
    HAVING COUNT(DISTINCT fm.movie_key) >= @minMovies
    ORDER BY aggregate_roi DESC;

    -------------------------------------------------
    -- 5️⃣ Top by Genre & Country Diversity Index
    -------------------------------------------------
    PRINT '>>> Top diversified companies (genre + country variety)';
    SELECT TOP (@topN)
           c.canonical_name,
           COUNT(DISTINCT b.movie_key) AS movies_count,
           COUNT(DISTINCT gc.genre_key) AS genres_count,
           COUNT(DISTINCT cc.country_key) AS countries_count,
           (COUNT(DISTINCT gc.genre_key) + COUNT(DISTINCT cc.country_key)) AS diversity_index
    FROM dbo.bridge_movie_company_canonical b
    JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
    LEFT JOIN dbo.bridge_movie_genre gc ON gc.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_country cc ON cc.movie_key = b.movie_key
    JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
    GROUP BY c.canonical_name
    HAVING COUNT(DISTINCT b.movie_key) >= @minMovies
    ORDER BY diversity_index DESC;

END
GO

-- كل السنين، أفضل 20 شركة
EXEC dbo.sp_top_companies_stats @topN = 20, @minMovies = 5, @fromYear = NULL;
GO

-- من سنة 2010 فقط
EXEC dbo.sp_top_companies_stats @topN = 30, @minMovies = 3, @fromYear = 2010;
GO
--------------------------------------
USE movies_db;
GO

-- حذف الإجراء القديم لو موجود
IF OBJECT_ID('dbo.sp_top_companies_analysis_save','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_top_companies_analysis_save;
GO

CREATE PROCEDURE dbo.sp_top_companies_analysis_save
    @minMovies INT = 5,
    @fromYear INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @run_time DATETIME = GETDATE();

    PRINT '=== Running company analysis and saving results ===';
    PRINT CONCAT('Run ID: ', @run_id);

    ---------------------------------------------------
    -- إنشاء جدول النتائج لو مش موجود
    ---------------------------------------------------
    IF OBJECT_ID('dbo.company_analysis_results','U') IS NULL
    BEGIN
        CREATE TABLE dbo.company_analysis_results (
            run_id UNIQUEIDENTIFIER,
            run_time DATETIME,
            canonical_name NVARCHAR(300),
            movies_count INT,
            genres_count INT,
            countries_count INT,
            diversity_index INT,
            total_revenue DECIMAL(38,2),
            total_budget DECIMAL(38,2),
            avg_revenue DECIMAL(38,2),
            aggregate_roi FLOAT,
            from_year INT
        );
        PRINT 'Created table: company_analysis_results';
    END

    ---------------------------------------------------
    -- إعداد بيانات الأفلام (تصفية بالسنة إن لزم)
    ---------------------------------------------------
    IF OBJECT_ID('tempdb..#filtered_movies') IS NOT NULL DROP TABLE #filtered_movies;

    SELECT f.movie_key,
           CAST(f.budget AS DECIMAL(38,2)) AS budget,
           CAST(f.revenue AS DECIMAL(38,2)) AS revenue
    INTO #filtered_movies
    FROM dbo.fact_movie_metrics f
    JOIN dbo.dim_movie m ON m.movie_key = f.movie_key
    WHERE (@fromYear IS NULL OR YEAR(m.release_date) >= @fromYear);

    ---------------------------------------------------
    -- تحليل الشركات
    ---------------------------------------------------
    INSERT INTO dbo.company_analysis_results (
        run_id, run_time, canonical_name,
        movies_count, genres_count, countries_count, diversity_index,
        total_revenue, total_budget, avg_revenue, aggregate_roi, from_year
    )
    SELECT 
        @run_id AS run_id,
        @run_time AS run_time,
        c.canonical_name,
        COUNT(DISTINCT b.movie_key) AS movies_count,
        COUNT(DISTINCT g.genre_key) AS genres_count,
        COUNT(DISTINCT co.country_key) AS countries_count,
        (COUNT(DISTINCT g.genre_key) + COUNT(DISTINCT co.country_key)) AS diversity_index,
        SUM(CAST(fm.revenue AS DECIMAL(38,2))) AS total_revenue,
        SUM(CAST(fm.budget AS DECIMAL(38,2))) AS total_budget,
        AVG(CAST(fm.revenue AS DECIMAL(38,2))) AS avg_revenue,
        CASE WHEN SUM(CAST(fm.budget AS DECIMAL(38,2))) = 0 THEN NULL
             ELSE (SUM(CAST(fm.revenue AS DECIMAL(38,2))) - SUM(CAST(fm.budget AS DECIMAL(38,2))))
                  / NULLIF(SUM(CAST(fm.budget AS DECIMAL(38,2))),0)
        END AS aggregate_roi,
        @fromYear AS from_year
    FROM dbo.bridge_movie_company_canonical b
    JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
    JOIN #filtered_movies fm ON fm.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_genre g ON g.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_country co ON co.movie_key = b.movie_key
    GROUP BY c.canonical_name
    HAVING COUNT(DISTINCT b.movie_key) >= @minMovies;

    ---------------------------------------------------
    -- ملخص النتائج
    ---------------------------------------------------
    DECLARE @count INT;
    SELECT @count = COUNT(*) FROM dbo.company_analysis_results WHERE run_id = @run_id;

    PRINT CONCAT('Inserted rows: ', @count);
    PRINT CONCAT('Analysis complete at ', CONVERT(VARCHAR(19), @run_time, 120));

END
GO

---*-----------------------*-------
EXEC dbo.sp_top_companies_analysis_save;

---------*-----------------*
EXEC dbo.sp_top_companies_analysis_save @minMovies = 3, @fromYear = 2010;
---------*-------------*----------
-- أحدث تشغيل
SELECT TOP 50 *
FROM dbo.company_analysis_results
ORDER BY run_time DESC, total_revenue DESC;

-- مقارنة تشغيلين
SELECT run_id, run_time, COUNT(*) AS companies, AVG(aggregate_roi) AS avg_roi
FROM dbo.company_analysis_results
GROUP BY run_id, run_time
ORDER BY run_time DESC;

--*------------------*
SELECT name
FROM sys.procedures
WHERE name = 'sp_top_companies_analysis_save';
-------*------------
SELECT TOP 10 run_id, run_time
FROM dbo.company_analysis_results
ORDER BY run_time DESC;



----*----------------*-------
DECLARE @rid UNIQUEIDENTIFIER = (SELECT TOP 1 run_id FROM dbo.company_analysis_results ORDER BY run_time DESC);

SELECT TOP 20 canonical_name, total_revenue, total_budget, movies_count, aggregate_roi, diversity_index
FROM dbo.company_analysis_results
WHERE run_id = @rid
ORDER BY total_revenue DESC;

----------------------*----------------*--------
USE movies_db;
GO

IF OBJECT_ID('dbo.sp_top_companies_analysis_save','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_top_companies_analysis_save;
GO

CREATE PROCEDURE dbo.sp_top_companies_analysis_save
    @minMovies INT = 5,
    @fromYear INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @run_time DATETIME = GETDATE();

    IF OBJECT_ID('dbo.company_analysis_results','U') IS NULL
    BEGIN
        CREATE TABLE dbo.company_analysis_results (
            run_id UNIQUEIDENTIFIER,
            run_time DATETIME,
            canonical_name NVARCHAR(300),
            movies_count INT,
            genres_count INT,
            countries_count INT,
            diversity_index INT,
            total_revenue DECIMAL(38,2),
            total_budget DECIMAL(38,2),
            avg_revenue DECIMAL(38,2),
            aggregate_roi FLOAT,
            from_year INT
        );
    END

    INSERT INTO dbo.company_analysis_results (
        run_id, run_time, canonical_name,
        movies_count, genres_count, countries_count, diversity_index,
        total_revenue, total_budget, avg_revenue, aggregate_roi, from_year
    )
    SELECT 
        @run_id AS run_id,
        @run_time AS run_time,
        c.canonical_name,
        COUNT(DISTINCT b.movie_key) AS movies_count,
        COUNT(DISTINCT g.genre_key) AS genres_count,
        COUNT(DISTINCT co.country_key) AS countries_count,
        (COUNT(DISTINCT g.genre_key) + COUNT(DISTINCT co.country_key)) AS diversity_index,
        SUM(COALESCE(f.revenue, 0)) AS total_revenue,
        SUM(COALESCE(f.budget, 0)) AS total_budget,
        AVG(COALESCE(f.revenue, 0)) AS avg_revenue,
        CASE WHEN SUM(COALESCE(f.budget, 0)) = 0 THEN NULL
             ELSE (SUM(COALESCE(f.revenue, 0)) - SUM(COALESCE(f.budget, 0))) 
                  / NULLIF(SUM(COALESCE(f.budget, 0)),0)
        END AS aggregate_roi,
        @fromYear AS from_year
    FROM dbo.bridge_movie_company_canonical b
    JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
    JOIN dbo.fact_movie_metrics f ON f.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_genre g ON g.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_country co ON co.movie_key = b.movie_key
    GROUP BY c.canonical_name
    HAVING COUNT(DISTINCT b.movie_key) >= @minMovies;
END;
GO



------------*---------*---------
-- يطبع نص الإجراء الحالي
SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.sp_top_companies_analysis_save')) AS proc_text;
GO

---------*------------*----
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_movie_metrics'
  AND COLUMN_NAME IN ('budget','revenue');
---------*----------------*---------
SELECT TOP 20 *
FROM dbo.fact_movie_metrics f
WHERE f.revenue IS NOT NULL
  AND TRY_CAST(f.revenue AS DECIMAL(38,2)) IS NULL;
-- نفس الشيء للـ budget
SELECT TOP 20 *
FROM dbo.fact_movie_metrics f
WHERE f.budget IS NOT NULL
  AND TRY_CAST(f.budget AS DECIMAL(38,2)) IS NULL;
-----------*-------------*--------
SELECT MAX(TRY_CAST(revenue AS DECIMAL(38,2))) AS max_revenue,
       MIN(TRY_CAST(revenue AS DECIMAL(38,2))) AS min_revenue,
       MAX(TRY_CAST(budget AS DECIMAL(38,2))) AS max_budget
FROM dbo.fact_movie_metrics;


-----------*----------*-**------------
USE movies_db;
GO

IF OBJECT_ID('dbo.sp_top_companies_analysis_save','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_top_companies_analysis_save;
GO

CREATE PROCEDURE dbo.sp_top_companies_analysis_save
    @minMovies INT = 5,
    @fromYear INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @run_time DATETIME = GETDATE();

    IF OBJECT_ID('dbo.company_analysis_results','U') IS NULL
    BEGIN
        CREATE TABLE dbo.company_analysis_results (
            run_id UNIQUEIDENTIFIER,
            run_time DATETIME,
            canonical_name NVARCHAR(300),
            movies_count INT,
            genres_count INT,
            countries_count INT,
            diversity_index INT,
            total_revenue DECIMAL(38,2),
            total_budget DECIMAL(38,2),
            avg_revenue DECIMAL(38,2),
            aggregate_roi FLOAT,
            from_year INT
        );
    END

    -- Build a safe per-movie numeric projection (TRY_CAST), limit extreme values if needed
    ;WITH safe_metrics AS (
      SELECT
        f.movie_key,
        -- try convert, will be NULL if not numeric
        TRY_CAST(f.revenue AS DECIMAL(38,2)) AS revenue_dec,
        TRY_CAST(f.budget  AS DECIMAL(38,2)) AS budget_dec
      FROM dbo.fact_movie_metrics f
    ),
    -- optional: cap absurdly large numbers (example cap at 1e30)
    capped AS (
      SELECT
        movie_key,
        CASE WHEN ABS(revenue_dec) > 1E30 THEN NULL ELSE revenue_dec END AS revenue_dec,
        CASE WHEN ABS(budget_dec)  > 1E30 THEN NULL ELSE budget_dec  END AS budget_dec
      FROM safe_metrics
    )
    INSERT INTO dbo.company_analysis_results (
        run_id, run_time, canonical_name,
        movies_count, genres_count, countries_count, diversity_index,
        total_revenue, total_budget, avg_revenue, aggregate_roi, from_year
    )
    SELECT 
        @run_id,
        @run_time,
        c.canonical_name,
        COUNT(DISTINCT b.movie_key) AS movies_count,
        COUNT(DISTINCT g.genre_key) AS genres_count,
        COUNT(DISTINCT co.country_key) AS countries_count,
        (COUNT(DISTINCT g.genre_key) + COUNT(DISTINCT co.country_key)) AS diversity_index,
        -- sum over safe numeric column (NULLs treated as 0)
        SUM(COALESCE(cap.revenue_dec, 0))  AS total_revenue,
        SUM(COALESCE(cap.budget_dec,  0))  AS total_budget,
        AVG(COALESCE(cap.revenue_dec, 0))  AS avg_revenue,
        CASE WHEN SUM(COALESCE(cap.budget_dec,0)) = 0 THEN NULL
             ELSE (SUM(COALESCE(cap.revenue_dec,0)) - SUM(COALESCE(cap.budget_dec,0)))
                  / NULLIF(SUM(COALESCE(cap.budget_dec,0)),0)
        END AS aggregate_roi,
        @fromYear
    FROM dbo.bridge_movie_company_canonical b
    JOIN dbo.dim_company_canonical c ON c.canonical_key = b.canonical_key
    JOIN capped cap ON cap.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_genre g ON g.movie_key = b.movie_key
    LEFT JOIN dbo.bridge_movie_country co ON co.movie_key = b.movie_key
    GROUP BY c.canonical_name
    HAVING COUNT(DISTINCT b.movie_key) >= @minMovies;
END;
GO

-------------*--------------------
EXEC dbo.sp_top_companies_analysis_save @minMovies = 3, @fromYear = NULL;
--********************--

SELECT TOP 5 run_id, run_time FROM dbo.company_analysis_results ORDER BY run_time DESC;
--********************-
DECLARE @rid UNIQUEIDENTIFIER = (SELECT TOP 1 run_id FROM dbo.company_analysis_results ORDER BY run_time DESC);

SELECT canonical_name, movies_count, genres_count, countries_count, total_revenue, total_budget, avg_revenue, aggregate_roi
FROM dbo.company_analysis_results
WHERE run_id = @rid
ORDER BY total_revenue DESC;
--***********************--
-- أعلى 50 budget مفرد (بعد TRY_CAST)
SELECT TOP 50 f.movie_key, f.budget, TRY_CAST(f.budget AS DECIMAL(38,2)) AS budget_dec
FROM dbo.fact_movie_metrics f
ORDER BY TRY_CAST(f.budget AS DECIMAL(38,2)) DESC;

-- budgets سالبة أو صغيرة جدا (قد تكون أخطاء)
SELECT TOP 50 movie_key, budget FROM dbo.fact_movie_metrics WHERE budget < 0 ORDER BY budget ASC;

-- صفوف التي لا تُحوَّل لـ DECIMAL (نادرة هنا لأن نوع bigint، لكن في حالات نصية ستكون مهمة)
SELECT TOP 50 movie_key, revenue FROM dbo.fact_movie_metrics WHERE TRY_CAST(revenue AS DECIMAL(38,2)) IS NULL AND revenue IS NOT NULL;

--**************************-
USE movies_db;
GO

-- orphan in fact_movie_metrics.movie_key (child) vs dim_movie.movie_key (parent)
SELECT f.movie_key
FROM dbo.fact_movie_metrics f
LEFT JOIN dbo.dim_movie m ON m.movie_key = f.movie_key
WHERE m.movie_key IS NULL;

-- orphan in bridge_movie_company
SELECT b.*
FROM dbo.bridge_movie_company b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_company c ON c.company_key = b.company_key
WHERE m.movie_key IS NULL OR c.company_key IS NULL;

-- orphan in bridge_movie_genre
SELECT b.*
FROM dbo.bridge_movie_genre b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_genre g ON g.genre_key = b.genre_key
WHERE m.movie_key IS NULL OR g.genre_key IS NULL;

-- orphan in bridge_movie_country
SELECT b.*
FROM dbo.bridge_movie_country b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_country c ON c.country_key = b.country_key
WHERE m.movie_key IS NULL OR c.country_key IS NULL;

-- bridge canonical
SELECT b.*
FROM dbo.bridge_movie_company_canonical b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_company_canonical cc ON cc.canonical_key = b.canonical_key
WHERE m.movie_key IS NULL OR cc.canonical_key IS NULL;
---*****************
USE movies_db;
GO

-- 1) fact_movie_metrics.movie_key -> dim_movie.movie_key
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_movie_metrics_dim_movie')
BEGIN
  ALTER TABLE dbo.fact_movie_metrics
  ADD CONSTRAINT FK_fact_movie_metrics_dim_movie
    FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
END
GO

-- 2) fact_movie_metrics.release_date_key -> dim_date.date_key (if column exists)
IF COL_LENGTH('dbo.fact_movie_metrics','release_date_key') IS NOT NULL
AND OBJECT_ID('dbo.dim_date','U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_movie_metrics_dim_date')
BEGIN
  ALTER TABLE dbo.fact_movie_metrics
  ADD CONSTRAINT FK_fact_movie_metrics_dim_date
    FOREIGN KEY (release_date_key) REFERENCES dbo.dim_date(date_key);
END
GO

-- 3) bridge_movie_company -> dim_movie, dim_company
IF OBJECT_ID('dbo.bridge_movie_company','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company
    ADD CONSTRAINT FK_bridge_movie_company_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END

  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_dim_company')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company
    ADD CONSTRAINT FK_bridge_movie_company_dim_company
      FOREIGN KEY (company_key) REFERENCES dbo.dim_company(company_key);
  END
END
GO

-- 4) bridge_movie_genre -> dim_movie, dim_genre
IF OBJECT_ID('dbo.bridge_movie_genre','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_genre_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_genre
    ADD CONSTRAINT FK_bridge_movie_genre_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_genre_dim_genre')
  BEGIN
    ALTER TABLE dbo.bridge_movie_genre
    ADD CONSTRAINT FK_bridge_movie_genre_dim_genre
      FOREIGN KEY (genre_key) REFERENCES dbo.dim_genre(genre_key);
  END
END
GO

-- 5) bridge_movie_country -> dim_movie, dim_country
IF OBJECT_ID('dbo.bridge_movie_country','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_country_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_country
    ADD CONSTRAINT FK_bridge_movie_country_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_country_dim_country')
  BEGIN
    ALTER TABLE dbo.bridge_movie_country
    ADD CONSTRAINT FK_bridge_movie_country_dim_country
      FOREIGN KEY (country_key) REFERENCES dbo.dim_country(country_key);
  END
END
GO

-- 6) bridge_movie_company_canonical -> dim_movie, dim_company_canonical (if exists)
IF OBJECT_ID('dbo.bridge_movie_company_canonical','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_canonical_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company_canonical
    ADD CONSTRAINT FK_bridge_movie_company_canonical_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_canonical_dim_company_canonical')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company_canonical
    ADD CONSTRAINT FK_bridge_movie_company_canonical_dim_company_canonical
      FOREIGN KEY (canonical_key) REFERENCES dbo.dim_company_canonical(canonical_key);
  END
END
GO

--******************-
SELECT fk.name AS fk_name,
       OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
       OBJECT_NAME(fk.parent_object_id) AS child_table,
       c1.name AS child_column,
       OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS parent_schema,
       OBJECT_NAME(fk.referenced_object_id) AS parent_table,
       c2.name AS parent_column
FROM sys.foreign_key_columns fkc
JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id
JOIN sys.columns c1 ON fkc.parent_column_id = c1.column_id AND c1.object_id = fk.parent_object_id
JOIN sys.columns c2 ON fkc.referenced_column_id = c2.column_id AND c2.object_id = fk.referenced_object_id
ORDER BY fk.name;
------*********
-- count orphans (should be zero)
SELECT
  SUM(CASE WHEN m.movie_key IS NULL THEN 1 ELSE 0 END) AS orphans_in_fact_movie_metrics
FROM dbo.fact_movie_metrics f
LEFT JOIN dbo.dim_movie m ON m.movie_key = f.movie_key;

-- for bridge_movie_company
SELECT
  SUM(CASE WHEN m.movie_key IS NULL OR c.company_key IS NULL THEN 1 ELSE 0 END) AS orphans_bridge_movie_company
FROM dbo.bridge_movie_company b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_company c ON c.company_key = b.company_key;
------**************-
-- مثال: عدد شركات مرتبطة بكل فيلم
SELECT TOP 10 m.movie_key, COUNT(b.company_key) AS companies_count
FROM dbo.dim_movie m
LEFT JOIN dbo.bridge_movie_company b ON b.movie_key = m.movie_key
GROUP BY m.movie_key
ORDER BY companies_count DESC;

----------*************
USE movies_db;
GO

-- orphans في fact_movie_metrics.movie_key
SELECT COUNT(*) AS orphans_fact_movie_metrics
FROM dbo.fact_movie_metrics f
LEFT JOIN dbo.dim_movie m ON m.movie_key = f.movie_key
WHERE m.movie_key IS NULL;

-- orphans في bridge_movie_company
SELECT COUNT(*) AS orphans_bridge_movie_company
FROM dbo.bridge_movie_company b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_company c ON c.company_key = b.company_key
WHERE m.movie_key IS NULL OR c.company_key IS NULL;
--*****************
USE movies_db;
GO

-- fact_movie_metrics.movie_key -> dim_movie.movie_key
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_movie_metrics_dim_movie')
BEGIN
  ALTER TABLE dbo.fact_movie_metrics
  ADD CONSTRAINT FK_fact_movie_metrics_dim_movie
    FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
END
GO

-- fact_movie_metrics.release_date_key -> dim_date.date_key (if exists)
IF COL_LENGTH('dbo.fact_movie_metrics','release_date_key') IS NOT NULL
AND OBJECT_ID('dbo.dim_date','U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_movie_metrics_dim_date')
BEGIN
  ALTER TABLE dbo.fact_movie_metrics
  ADD CONSTRAINT FK_fact_movie_metrics_dim_date
    FOREIGN KEY (release_date_key) REFERENCES dbo.dim_date(date_key);
END
GO

-- bridge_movie_company -> dim_movie, dim_company
IF OBJECT_ID('dbo.bridge_movie_company','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company
    ADD CONSTRAINT FK_bridge_movie_company_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END

  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_dim_company')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company
    ADD CONSTRAINT FK_bridge_movie_company_dim_company
      FOREIGN KEY (company_key) REFERENCES dbo.dim_company(company_key);
  END
END
GO

-- bridge_movie_genre -> dim_movie, dim_genre
IF OBJECT_ID('dbo.bridge_movie_genre','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_genre_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_genre
    ADD CONSTRAINT FK_bridge_movie_genre_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_genre_dim_genre')
  BEGIN
    ALTER TABLE dbo.bridge_movie_genre
    ADD CONSTRAINT FK_bridge_movie_genre_dim_genre
      FOREIGN KEY (genre_key) REFERENCES dbo.dim_genre(genre_key);
  END
END
GO

-- bridge_movie_country -> dim_movie, dim_country
IF OBJECT_ID('dbo.bridge_movie_country','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_country_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_country
    ADD CONSTRAINT FK_bridge_movie_country_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_country_dim_country')
  BEGIN
    ALTER TABLE dbo.bridge_movie_country
    ADD CONSTRAINT FK_bridge_movie_country_dim_country
      FOREIGN KEY (country_key) REFERENCES dbo.dim_country(country_key);
  END
END
GO

-- bridge_movie_company_canonical -> dim_movie, dim_company_canonical (if exists)
IF OBJECT_ID('dbo.bridge_movie_company_canonical','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_canonical_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company_canonical
    ADD CONSTRAINT FK_bridge_movie_company_canonical_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_canonical_dim_company_canonical')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company_canonical
    ADD CONSTRAINT FK_bridge_movie_company_canonical_dim_company_canonical
      FOREIGN KEY (canonical_key) REFERENCES dbo.dim_company_canonical(canonical_key);
  END
END
GO

--*******
USE movies_db;
GO

-- عرض كل الـ foreign keys المتعلقة بالجداول اللي عايزينها
SELECT fk.name AS fk_name,
       OBJECT_SCHEMA_NAME(fk.parent_object_id) AS child_schema,
       OBJECT_NAME(fk.parent_object_id) AS child_table,
       OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS parent_schema,
       OBJECT_NAME(fk.referenced_object_id) AS parent_table,
       fk.is_disabled, fk.is_not_trusted
FROM sys.foreign_keys fk
WHERE OBJECT_NAME(fk.parent_object_id) IN (
  'fact_movie_metrics','bridge_movie_company','bridge_movie_genre',
  'bridge_movie_country','bridge_movie_company_canonical','dim_movie'
)
ORDER BY fk.name;
---********
-- عرض الأعمدة التي هي مفاتيح رئيسية لكل جدول مهم
SELECT t.name AS table_name, c.name AS pk_column
FROM sys.tables t
JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
WHERE t.name IN ('dim_movie','dim_company','dim_genre','dim_country','dim_date','dim_company_canonical')
ORDER BY t.name;
---*******
USE movies_db;
GO

-- 1) عرض أعمدة الجدول
SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_genre';

-- 2) عرض المفاتيح (PK) للجدول
EXEC sp_help 'dbo.dim_genre';

--******

SELECT TOP 20 * FROM dbo.dim_genre;

--**********
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='dim_genre';
EXEC sp_help 'dbo.dim_genre';
SELECT TOP 20 * FROM dbo.dim_genre;
-------**********
SELECT fk.name, OBJECT_NAME(fk.parent_object_id) child_table, OBJECT_NAME(fk.referenced_object_id) parent_table
FROM sys.foreign_keys fk
WHERE OBJECT_NAME(fk.parent_object_id) IN ('fact_movie_metrics','bridge_movie_genre','bridge_movie_company','bridge_movie_country');
---------**
USE movies_db;
GO

-- count orphans in fact_movie_metrics (movie_key)
SELECT COUNT(*) AS orphans_fact_movie_metrics
FROM dbo.fact_movie_metrics f
LEFT JOIN dbo.dim_movie m ON m.movie_key = f.movie_key
WHERE m.movie_key IS NULL;

-- count orphans in bridge_movie_company
SELECT COUNT(*) AS orphans_bridge_movie_company
FROM dbo.bridge_movie_company b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_company c ON c.company_key = b.company_key
WHERE m.movie_key IS NULL OR c.company_key IS NULL;

-- count orphans in bridge_movie_genre
SELECT COUNT(*) AS orphans_bridge_movie_genre
FROM dbo.bridge_movie_genre b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_genre g ON g.genre_key = b.genre_key
WHERE m.movie_key IS NULL OR g.genre_key IS NULL;

-- count orphans in bridge_movie_country
SELECT COUNT(*) AS orphans_bridge_movie_country
FROM dbo.bridge_movie_country b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_country c ON c.country_key = b.country_key
WHERE m.movie_key IS NULL OR c.country_key IS NULL;
-------------***
USE movies_db;
GO

-- bridge_movie_company orphans
SELECT b.* INTO dbo.orphan_bridge_movie_company
FROM dbo.bridge_movie_company b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_company c ON c.company_key = b.company_key
WHERE m.movie_key IS NULL OR c.company_key IS NULL;

-- bridge_movie_genre orphans
SELECT b.* INTO dbo.orphan_bridge_movie_genre
FROM dbo.bridge_movie_genre b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_genre g ON g.genre_key = b.genre_key
WHERE m.movie_key IS NULL OR g.genre_key IS NULL;

-- bridge_movie_country orphans
SELECT b.* INTO dbo.orphan_bridge_movie_country
FROM dbo.bridge_movie_country b
LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key
LEFT JOIN dbo.dim_country c ON c.country_key = b.country_key
WHERE m.movie_key IS NULL OR c.country_key IS NULL;

-- fact_movie_metrics orphans (should be rare)
SELECT f.* INTO dbo.orphan_fact_movie_metrics
FROM dbo.fact_movie_metrics f
LEFT JOIN dbo.dim_movie m ON m.movie_key = f.movie_key
WHERE m.movie_key IS NULL;
-------------***-
USE movies_db;
GO

-- fact_movie_metrics.movie_key -> dim_movie.movie_key
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_movie_metrics_dim_movie')
BEGIN
  ALTER TABLE dbo.fact_movie_metrics
  ADD CONSTRAINT FK_fact_movie_metrics_dim_movie
    FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
END
GO

-- fact_movie_metrics.release_date_key -> dim_date.date_key (if exists)
IF COL_LENGTH('dbo.fact_movie_metrics','release_date_key') IS NOT NULL
AND OBJECT_ID('dbo.dim_date','U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_fact_movie_metrics_dim_date')
BEGIN
  ALTER TABLE dbo.fact_movie_metrics
  ADD CONSTRAINT FK_fact_movie_metrics_dim_date
    FOREIGN KEY (release_date_key) REFERENCES dbo.dim_date(date_key);
END
GO

-- bridge_movie_company -> dim_movie, dim_company
IF OBJECT_ID('dbo.bridge_movie_company','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company
    ADD CONSTRAINT FK_bridge_movie_company_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END

  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_dim_company')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company
    ADD CONSTRAINT FK_bridge_movie_company_dim_company
      FOREIGN KEY (company_key) REFERENCES dbo.dim_company(company_key);
  END
END
GO

-- bridge_movie_genre -> dim_movie, dim_genre
IF OBJECT_ID('dbo.bridge_movie_genre','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_genre_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_genre
    ADD CONSTRAINT FK_bridge_movie_genre_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_genre_dim_genre')
  BEGIN
    ALTER TABLE dbo.bridge_movie_genre
    ADD CONSTRAINT FK_bridge_movie_genre_dim_genre
      FOREIGN KEY (genre_key) REFERENCES dbo.dim_genre(genre_key);
  END
END
GO

-- bridge_movie_country -> dim_movie, dim_country
IF OBJECT_ID('dbo.bridge_movie_country','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_country_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_country
    ADD CONSTRAINT FK_bridge_movie_country_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_country_dim_country')
  BEGIN
    ALTER TABLE dbo.bridge_movie_country
    ADD CONSTRAINT FK_bridge_movie_country_dim_country
      FOREIGN KEY (country_key) REFERENCES dbo.dim_country(country_key);
  END
END
GO

-- bridge_movie_company_canonical -> dim_movie, dim_company_canonical (if exists)
IF OBJECT_ID('dbo.bridge_movie_company_canonical','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_canonical_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company_canonical
    ADD CONSTRAINT FK_bridge_movie_company_canonical_dim_movie
      FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_canonical_dim_company_canonical')
  BEGIN
    ALTER TABLE dbo.bridge_movie_company_canonical
    ADD CONSTRAINT FK_bridge_movie_company_canonical_dim_company_canonical
      FOREIGN KEY (canonical_key) REFERENCES dbo.dim_company_canonical(canonical_key);
  END
END
GO
-----------------------************
USE movies_db;
GO

-- أعمدة bridge_movie_company
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'bridge_movie_company';

-- وجود عمود original_language_key في dim_movie؟
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_movie' AND COLUMN_NAME = 'original_language_key';

-- وجود عمود language_key في dim_language؟
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_language' AND COLUMN_NAME = 'language_key';

-- قائمة الـ foreign keys المهمة وحالتها
SELECT fk.name AS fk_name,
       OBJECT_NAME(fk.parent_object_id) AS child_table,
       OBJECT_NAME(fk.referenced_object_id) AS parent_table,
       fk.is_disabled, fk.is_not_trusted
FROM sys.foreign_keys fk
WHERE OBJECT_NAME(fk.parent_object_id) IN (
  'fact_movie_metrics','bridge_movie_company','bridge_movie_genre','bridge_movie_country','dim_movie'
)
ORDER BY fk.name;
-------------********
-- حساب orphans المحتملين قبل إضافة أي FK جديدة
SELECT
  (SELECT COUNT(*) FROM dbo.fact_movie_metrics f LEFT JOIN dbo.dim_movie m ON m.movie_key = f.movie_key WHERE m.movie_key IS NULL) AS orphans_fact_movie_metrics,
  (SELECT COUNT(*) FROM dbo.bridge_movie_company b LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key LEFT JOIN dbo.dim_company c ON c.company_key = b.company_key WHERE m.movie_key IS NULL OR c.company_key IS NULL) AS orphans_bridge_movie_company,
  (SELECT COUNT(*) FROM dbo.bridge_movie_genre b LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key LEFT JOIN dbo.dim_genre g ON g.genre_key = b.genre_key WHERE m.movie_key IS NULL OR g.genre_key IS NULL) AS orphans_bridge_movie_genre,
  (SELECT COUNT(*) FROM dbo.bridge_movie_country b LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key LEFT JOIN dbo.dim_country c ON c.country_key = b.country_key WHERE m.movie_key IS NULL OR c.country_key IS NULL) AS orphans_bridge_movie_country;

  ----------*****************
  -- ربط dim_movie.original_language_key -> dim_language.language_key
IF COL_LENGTH('dbo.dim_movie','original_language_key') IS NOT NULL
AND OBJECT_ID('dbo.dim_language','U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_movie_dim_language')
BEGIN
  -- تأكد من عدم وجود orphans
  IF EXISTS (
    SELECT 1 FROM dbo.dim_movie m
    LEFT JOIN dbo.dim_language l ON l.language_key = m.original_language_key
    WHERE m.original_language_key IS NOT NULL AND l.language_key IS NULL
  )
  BEGIN
    RAISERROR('There are orphan original_language_key values in dim_movie. Move or clean them before adding FK.', 16, 1);
  END
  ELSE
  BEGIN
    ALTER TABLE dbo.dim_movie
      ADD CONSTRAINT FK_dim_movie_dim_language
      FOREIGN KEY (original_language_key) REFERENCES dbo.dim_language(language_key);
  END
END
-------***********
-- إذا company_key موجودة كعمود، أضف FK للشركة
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='bridge_movie_company' AND COLUMN_NAME='company_key')
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_bridge_movie_company_dim_company')
  BEGIN
    -- تأكد من orphans
    IF EXISTS (
      SELECT 1 FROM dbo.bridge_movie_company b
      LEFT JOIN dbo.dim_company c ON c.company_key = b.company_key
      WHERE b.company_key IS NOT NULL AND c.company_key IS NULL
    )
    BEGIN
      RAISERROR('There are orphan company_key values in bridge_movie_company. Move or clean them before adding FK.',16,1);
    END
    ELSE
    BEGIN
      ALTER TABLE dbo.bridge_movie_company
      ADD CONSTRAINT FK_bridge_movie_company_dim_company
        FOREIGN KEY (company_key) REFERENCES dbo.dim_company(company_key);
    END
  END
END
ELSE
BEGIN
  RAISERROR('bridge_movie_company.company_key column not found. Cannot add FK until column exists.',16,1);
END

--------------*********
-- bridge_movie_genre (movie & genre) - عادة موجود لكن نتأكد
IF OBJECT_ID('dbo.bridge_movie_genre','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name='FK_bridge_movie_genre_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_genre
      ADD CONSTRAINT FK_bridge_movie_genre_dim_movie FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name='FK_bridge_movie_genre_dim_genre')
  BEGIN
    ALTER TABLE dbo.bridge_movie_genre
      ADD CONSTRAINT FK_bridge_movie_genre_dim_genre FOREIGN KEY (genre_key) REFERENCES dbo.dim_genre(genre_key);
  END
END

-- bridge_movie_country
IF OBJECT_ID('dbo.bridge_movie_country','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name='FK_bridge_movie_country_dim_movie')
  BEGIN
    ALTER TABLE dbo.bridge_movie_country
      ADD CONSTRAINT FK_bridge_movie_country_dim_movie FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
  END
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name='FK_bridge_movie_country_dim_country')
  BEGIN
    ALTER TABLE dbo.bridge_movie_country
      ADD CONSTRAINT FK_bridge_movie_country_dim_country FOREIGN KEY (country_key) REFERENCES dbo.dim_country(country_key);
  END
END

-- fact_movie_metrics -> dim_movie & dim_date (if not exists)
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name='FK_fact_movie_metrics_dim_movie')
BEGIN
  ALTER TABLE dbo.fact_movie_metrics
    ADD CONSTRAINT FK_fact_movie_metrics_dim_movie FOREIGN KEY (movie_key) REFERENCES dbo.dim_movie(movie_key);
END
IF COL_LENGTH('dbo.fact_movie_metrics','release_date_key') IS NOT NULL
AND OBJECT_ID('dbo.dim_date','U') IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name='FK_fact_movie_metrics_dim_date')
BEGIN
  ALTER TABLE dbo.fact_movie_metrics
    ADD CONSTRAINT FK_fact_movie_metrics_dim_date FOREIGN KEY (release_date_key) REFERENCES dbo.dim_date(date_key);
END

----****
USE movies_db;
GO

IF COL_LENGTH('dbo.dim_movie','original_language_key') IS NOT NULL
AND OBJECT_ID('dbo.dim_language','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_dim_movie_dim_language')
  BEGIN
    -- تحقق من orphans
    IF EXISTS (
      SELECT 1 FROM dbo.dim_movie m
      LEFT JOIN dbo.dim_language l ON l.language_key = m.original_language_key
      WHERE m.original_language_key IS NOT NULL AND l.language_key IS NULL
    )
    BEGIN
      RAISERROR('There are orphan original_language_key values in dim_movie. Clean them before adding FK.',16,1);
    END
    ELSE
    BEGIN
      ALTER TABLE dbo.dim_movie
      ADD CONSTRAINT FK_dim_movie_dim_language
        FOREIGN KEY (original_language_key) REFERENCES dbo.dim_language(language_key);
      PRINT 'FK_dim_movie_dim_language created.';
    END
  END
  ELSE
    PRINT 'FK_dim_movie_dim_language already exists.';
END
ELSE
  PRINT 'Either dim_movie.original_language_key or dim_language does not exist.';
GO
-------*--------*-
-- عرض كل الـ FKs النهائية
SELECT fk.name AS fk_name,
       OBJECT_NAME(fk.parent_object_id) AS child_table,
       OBJECT_NAME(fk.referenced_object_id) AS parent_table,
       fk.is_disabled, fk.is_not_trusted
FROM sys.foreign_keys fk
WHERE OBJECT_NAME(fk.parent_object_id) IN (
  'fact_movie_metrics','bridge_movie_company','bridge_movie_genre','bridge_movie_country','dim_movie'
)
ORDER BY fk.name;

-- تأكد من عدم وجود orphans بعد أي تغييرات
SELECT 
 (SELECT COUNT(*) FROM dbo.fact_movie_metrics f LEFT JOIN dbo.dim_movie m ON m.movie_key = f.movie_key WHERE m.movie_key IS NULL) AS orphans_fact_movie_metrics,
 (SELECT COUNT(*) FROM dbo.bridge_movie_company b LEFT JOIN dbo.dim_movie m ON m.movie_key = b.movie_key LEFT JOIN dbo.dim_company c ON c.company_key = b.company_key WHERE m.movie_key IS NULL OR c.company_key IS NULL) AS orphans_bridge_movie_company;
 ---------***
