USE painterpalette;

-- We build on the PaintData normalized analytical table.

-- However, it has multiple instances for each painting that has more than one styles or its artist has multiple institutions.
-- As a first step I combine instances, with distinct styles and institutions concatenated into one string, separated by commas in the "Institutions" and "Styles" column.

SET SESSION sort_buffer_size = 1024 * 1024 * 16; -- 16MB, MySQL limits this to 256kB by default
SET SESSION group_concat_max_len = 1024 * 1024 * 16;

DROP VIEW IF EXISTS GroupedPaintData;
CREATE VIEW GroupedPaintData AS
SELECT  PaintingID,
        Artist,
        Gender,
        BirthYear,
        Nationality,
        Citizenship,
        Movement,
        EarliestYearOfMovement,
        MovementOrigin,
        GROUP_CONCAT(DISTINCT Institution ORDER BY Institution SEPARATOR ', ') as Institutions, -- Institution1, Institution2, ...
        GROUP_CONCAT(DISTINCT InstitutionLocation ORDER BY InstitutionLocation SEPARATOR ', ') as InstitutionLocations,
        GROUP_CONCAT(DISTINCT Style ORDER BY Style SEPARATOR ', ') as Styles,
        GROUP_CONCAT(DISTINCT StyleOrigin ORDER BY StyleOrigin SEPARATOR ', ') as StyleOrigins,
        TagsOfPainting
FROM PaintData
-- Group by everything except institutions and styles, those are used for concatenation above
GROUP BY PaintingID, Artist, Gender, BirthYear, Nationality, Citizenship, Movement, EarliestYearOfMovement, MovementOrigin, TagsOfPainting
ORDER BY PaintingID;

-- We can see the results:
SELECT * FROM GroupedPaintData WHERE Styles LIKE "%,%" LIMIT 20;

-- For analysis of institutions, building on this view is easier.

-- ---------------------------- Specific Data Marts for analysis ----------------------------

-- Most common styles per institution
DROP VIEW IF EXISTS StylesPerInstitution;
CREATE VIEW StylesPerInstitution AS
SELECT  Institution,
        Style,
        COUNT(*) as Count
FROM PaintData
WHERE Style IS NOT NULL AND Institution IS NOT NULL
GROUP BY Institution, Style
ORDER BY Institution, Count DESC;

-- Most common styles per movement and most common movements per style (excluding cases when style and movement are the same)

DROP VIEW IF EXISTS StylesPerMovement;
CREATE VIEW StylesPerMovement AS
SELECT  Movement,
        Style,
        COUNT(*) as Amount
FROM PaintData
WHERE Movement != Style
AND Style IS NOT NULL and Movement IS NOT NULL
GROUP BY Movement, Style
ORDER BY Movement, Amount DESC;

DROP VIEW IF EXISTS MovementsPerStyle;
CREATE VIEW MovementsPerStyle AS
SELECT  Movement,
        Style,
        COUNT(*) as Amount
FROM PaintData
WHERE Movement != Style
AND Style IS NOT NULL and Movement IS NOT NULL
GROUP BY Movement, Style
ORDER BY Style, Amount DESC;

DROP VIEW IF EXISTS MovementStylePairsOrdered;
CREATE VIEW MovementStylePairsOrdered AS
SELECT  Style,
		Movement,
        COUNT(*) as Amount
FROM PaintData
WHERE Movement != Style
AND Style IS NOT NULL and Movement IS NOT NULL
GROUP BY Movement, Style
ORDER BY Amount DESC;

-- Which painters have the most paintings preserved (assuming most paintings of famous painters are stored in the database)?

-- We need to use the grouped data view, as we want to count each painting only once.
DROP VIEW IF EXISTS PaintingsPerArtist;
CREATE VIEW PaintingsPerArtist AS
SELECT  Artist,
        COUNT(*) as Amount
FROM GroupedPaintData
WHERE Artist IS NOT NULL
GROUP BY Artist
ORDER BY Amount DESC;

-- Which movements have the least paintings?

DROP VIEW IF EXISTS PaintingsPerMovement;
CREATE VIEW PaintingsPerMovement AS
SELECT  Movement,
        COUNT(*) as Amount
FROM GroupedPaintData
WHERE Movement IS NOT NULL
GROUP BY Movement
ORDER BY Amount ASC;


SELECT * FROM StylesPerInstitution LIMIT 35, 20;
SELECT * FROM StylesPerMovement LIMIT 20;
SELECT * FROM MovementsPerStyle LIMIT 20;
SELECT * FROM MovementStylePairsOrdered LIMIT 20;
SELECT * FROM PaintingsPerArtist LIMIT 20;
SELECT * FROM PaintingsPerMovement LIMIT 20;