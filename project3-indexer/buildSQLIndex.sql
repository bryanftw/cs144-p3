CREATE TABLE IF NOT EXISTS ItemCoordinates
(   ItemID INT(11) NOT NULL,
    Coordinates POINT NOT NULL,
    SPATIAL INDEX(Coordinates),
    PRIMARY KEY (ItemID)
)   ENGINE=MyISAM;

INSERT INTO ItemCoordinates(ItemID, Coordinates)
    SELECT DISTINCT ItemID, POINT(Latitude, Longitude)
    FROM Items INNER JOIN Sellers ON Items.SellerID = Sellers.UserID
    WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL;
