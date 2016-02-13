CREATE TABLE ItemCoordinates
(   ItemID INT(11) NOT NULL,
    Coordinates POINT NOT NULL,
    PRIMARY KEY (ItemID)
)   ENGINE=MyISAM;

INSERT INTO ItemCoordinates(ItemID, Coordinates)
    SELECT ItemID, Point(Latitude, Longitude)
    FROM Items INNER JOIN Sellers ON Items.SellerID = Sellers.UserID
    WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL;

CREATE SPATIAL INDEX sp_index ON ItemCoordinates(Coordinates);