USE painterpalette;

-- ---------------------------- New painting added to the Paintings table process ----------------------------

-- Steps:
-- Before insert (as table modification is only allowed in before triggers):
-- 1. If the artistName is not null, check if the artist exists. If not, update the Artists table.
-- 2. Foreign key update (Paintings table)

-- After insert:
-- 3. Add the styles if needed to the Styles table
-- 4. Update the PaintingStyles table
-- 5. Add the new instance into the analytical table

DROP TRIGGER IF EXISTS before_painting_insert;
DELIMITER //
CREATE TRIGGER before_painting_insert BEFORE INSERT ON Paintings FOR EACH ROW
BEGIN
    DECLARE artistId_ INT;

    -- 1)
    IF NEW.artistName IS NOT NULL THEN
        SELECT artistId INTO artistId_ FROM Artists WHERE artistName = NEW.artistName LIMIT 1;
        IF artistId_ IS NULL THEN
            INSERT INTO Artists (artistName, nationality, movement, styles, firstYear, lastYear, paintingSchool)
            VALUES (NEW.artistName, NEW.nationality, NEW.movement, NEW.style, NEW.dateYear, NEW.dateYear, NEW.paintingSchool);
            SET artistId_ = LAST_INSERT_ID();
        END IF;

        -- 2)
        SET NEW.artist_artistId = artistId_;
    END IF;
END;
//
DELIMITER ;

DROP TRIGGER IF EXISTS after_painting_insert;
DELIMITER //
CREATE TRIGGER after_painting_insert
AFTER INSERT ON Paintings
FOR EACH ROW
BEGIN
    DECLARE styleName_ VARCHAR(255);
    DECLARE styleId_ INT;
    DECLARE styleCopy VARCHAR(255);
    SET styleCopy = NEW.style;

    -- Handle comma-separated styles
    IF styleCopy IS NOT NULL THEN
        WHILE LOCATE(',', styleCopy) > 0 DO
            SET styleName_ = TRIM(SUBSTRING_INDEX(styleCopy, ',', 1));
            SET styleCopy = TRIM(SUBSTRING(styleCopy FROM LOCATE(',', styleCopy) + 1)); -- Remove the first style from the string
            
            SELECT styleId INTO styleId_
            FROM Styles
            WHERE styleName = styleName_ LIMIT 1; 

            -- 3)
            IF styleId_ IS NULL THEN
                INSERT INTO Styles (styleName)
                VALUES (styleName_);
                SET styleId_ = LAST_INSERT_ID();
            END IF;

            -- 4)
            IF NOT EXISTS (SELECT 1 FROM PaintingStyles WHERE paintingId = NEW.paintingId AND styleId = styleId_) THEN
                INSERT INTO PaintingStyles (paintingId, styleId)
                VALUES (NEW.paintingId, styleId_);
            END IF;
        END WHILE;

        -- Handle the last (or only) style
        SET styleName_ = TRIM(styleCopy);
        SELECT styleId INTO styleId_
        FROM Styles
        WHERE styleName = styleName_ LIMIT 1;
        IF styleId_ IS NULL THEN
            INSERT INTO Styles (styleName)
            VALUES (styleName_);
            SET styleId_ = LAST_INSERT_ID();
        END IF;
        IF NOT EXISTS (SELECT 1 FROM PaintingStyles WHERE paintingId = NEW.paintingId AND styleId = styleId_) THEN -- can happen that it was already added in the loop
            INSERT INTO PaintingStyles (paintingId, styleId)
            VALUES (NEW.paintingId, styleId_);
        END IF;
    END IF;

    -- 5) Update PaintData analytical table
    INSERT INTO PaintData (PaintingID, Year, Artist, Gender, BirthYear, Nationality, Citizenship, Movement, EarliestYearOfMovement, MovementOrigin, Institution, InstitutionLocation, Style, EarliestYearOfStyle, StyleOrigin, TagsOfPainting)
    SELECT  NEW.paintingId AS PaintingID,
            NEW.dateYear AS Year,
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
            NEW.tags as TagsOfPainting
    FROM Paintings p
    LEFT JOIN Artists a ON p.artist_artistId = a.artistId
    LEFT JOIN ArtistInstitutions ai ON a.artistId = ai.artistId
    LEFT JOIN Institutions i ON ai.institutionId = i.institutionId
    LEFT JOIN Movements m ON a.movementId = m.movementId
    LEFT JOIN PaintingStyles ps ON p.paintingId = ps.paintingId
    LEFT JOIN Styles s ON ps.styleId = s.styleId
    WHERE p.paintingId = NEW.paintingId;
END;
//
DELIMITER ;

-- ---------------------------- New painter added to the Paintings table process ----------------------------

-- Trigger steps:

-- Before insert (as table modification is only allowed in before triggers):
-- 1) If movementName is not null, check if the movement exists. If not, add it to the Movements table.
-- 2) movementId foreign key update

-- After insert:
-- 3) If institutionName is not null, check if the institution exists. If not, add it to the Institutions table.
-- 4) Update the ArtistInstitutions table

DROP TRIGGER IF EXISTS before_artist_insert;
DELIMITER //
CREATE TRIGGER before_artist_insert
BEFORE INSERT ON Artists
FOR EACH ROW
BEGIN
    DECLARE movementId_ INT;

    IF NEW.movement IS NOT NULL THEN
        SELECT movementId INTO movementId_
        FROM Movements
        WHERE movementName = NEW.movement;
        
        -- 1)
        IF movementId_ IS NULL THEN
            INSERT INTO Movements (movementName)
            VALUES (NEW.movement);
            SET movementId_ = LAST_INSERT_ID();
        END IF;

        -- 2)
        SET NEW.movementId = movementId_;
    END IF;
END;
//
DELIMITER ;

DROP TRIGGER IF EXISTS after_artist_insert;
DELIMITER //
CREATE TRIGGER after_artist_insert
AFTER INSERT ON Artists
FOR EACH ROW
BEGIN
    DECLARE institutionName_ VARCHAR(255);
    DECLARE institutionId_ INT;
    DECLARE paintingSchoolCopy VARCHAR(255);
    SET paintingSchoolCopy = NEW.paintingSchool;

    -- Handle comma-separated institutions
    IF paintingSchoolCopy IS NOT NULL THEN
        WHILE LOCATE(',', paintingSchoolCopy) > 0 DO
            SET institutionName_ = TRIM(SUBSTRING_INDEX(paintingSchoolCopy, ',', 1));
            SET paintingSchoolCopy = TRIM(SUBSTRING(paintingSchoolCopy FROM LOCATE(',', paintingSchoolCopy) + 1)); -- Remove the first institution from the string
            
            SELECT institutionId INTO institutionId_
            FROM Institutions
            WHERE institutionName = institutionName_;

            -- 3)
            IF institutionId_ IS NULL THEN
                INSERT INTO Institutions (institutionName)
                VALUES (institutionName_);
                SET institutionId_ = LAST_INSERT_ID();
            END IF;

            -- 4)
            INSERT INTO ArtistInstitutions (artistId, institutionId)
            VALUES (NEW.artistId, institutionId_);
        END WHILE;

        -- Handle the last (or only) institution
        SET institutionName_ = TRIM(paintingSchoolCopy);
        SELECT institutionId INTO institutionId_
        FROM Institutions
        WHERE institutionName = institutionName_;
        IF institutionId_ IS NULL THEN
            INSERT INTO Institutions (institutionName)
            VALUES (institutionName_);
            SET institutionId_ = LAST_INSERT_ID();
        END IF;
        INSERT INTO ArtistInstitutions (artistId, institutionId)
        VALUES (NEW.artistId, institutionId_);
    END IF;
END;
//
DELIMITER ;

-- Test the triggers
INSERT INTO Artists (artistName, movement, birthYear, paintingSchool)
VALUES ('Test Artist', 'Test Movement', 1990, 'Test Institution, St Ives School');

INSERT INTO Paintings (artistName, dateYear, style, tags)
VALUES ('Test Artist', 1920, 'teststyle', '[test, impressionism]');

INSERT INTO Paintings (artistName, dateYear, style, tags, nationality, movement, paintingSchool)
VALUES ('Test Artist2', 1900, 'Surrealism, Art Deco, teststyle', '[basic text]', "Czech", "NewMovementFromPaintingInstance", "NewInstitutionFromPaintingInstance");

SELECT * FROM Artists ORDER BY artistId DESC LIMIT 5;
SELECT * FROM Paintings ORDER BY paintingId DESC LIMIT 5;
SELECT * FROM PaintData ORDER BY PaintingID DESC LIMIT 5;
SELECT * FROM Movements ORDER BY movementId DESC LIMIT 5;
SELECT * FROM Institutions ORDER BY institutionId DESC LIMIT 5;
SELECT * FROM Styles ORDER BY styleId DESC LIMIT 5;
SELECT * FROM ArtistInstitutions ORDER BY artistId DESC LIMIT 5;
SELECT * FROM PaintingStyles ORDER BY paintingId DESC LIMIT 5;