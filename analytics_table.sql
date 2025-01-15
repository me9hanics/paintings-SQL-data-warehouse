-- We already filled up the separate tables with "mined" data (e.g. finding years from the style string, comma separations),
-- so most of the preparation is done
-- Here we create a table for analytics, with various dimensions to support analysis


-- Analytics table: All analytics should run on one normalized table, the "analytical layer" table.
-- In this case, every instance in the table correspond to a painting, a painting added is the "fact" - a new painting is the source of a new instance.

-- These are the other dimensions of information:
-- painter: name, gender, birthyear, nationality, citizenship,
-- Movement (of painter): earliest appearance of a work from the movement, origin location
-- Institutions (of painter): names, locations
-- Styles (of painting): origin locations

USE painterpalette;

DROP TABLE IF EXISTS PaintData;
CREATE TABLE PaintData AS
SELECT  p.paintingId AS PaintingID,
        p.dateYear AS Year,
        a.artistName AS Artist,
        a.gender AS Gender,
        a.birthYear as BirthYear,
        a.nationality as Nationality,
        a.citizenship as Citizenship,
        m.movementName as Movement,
        m.periodStart as EarliestYearOfMovement,
        m.majorLocation as MovementOrigin,
        i.institutionName as Institution,
        i.institutionLocation as InstitutionLocation,
        s.styleName as Style,
        s.firstDate as EarliestYearOfStyle,
        s.majorLocation as StyleOrigin,
        p.tags as TagsOfPainting        
FROM Paintings p
LEFT JOIN Artists a
ON p.artist_artistId = a.artistId
LEFT JOIN ArtistInstitutions ai
ON a.artistId = ai.artistId
LEFT JOIN Institutions i
ON ai.institutionId = i.institutionId
LEFT JOIN Movements m
ON a.movementId = m.movementId
LEFT JOIN PaintingStyles ps
ON p.paintingId = ps.paintingId
LEFT JOIN Styles s
ON ps.styleId = s.styleId
ORDER BY p.paintingId;

SELECT * FROM PaintData LIMIT 20;

-- This is a table that works universally for various queries; but has multiple instances for a painting if it has more than one styles or its artist has multiple institutions.

-- It's more logical to not have separate instances for a painting per style or artist institution, just store one instance per painting and concatenate the information.
-- In a view I combine instances, with distinct styles and institutions concatenated into one string, separated by commas in the "Institutions" and "Styles" column.

-- This is done in the data_marts_analysis.sql file.