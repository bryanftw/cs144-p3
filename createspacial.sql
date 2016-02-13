CREATE TABLE ItemCoordinates
    AS (SELECT ItemID, Point(Latitude, Longitude) AS Coordinates
        FROM Items INNER JOIN Sellers ON Items.SellerID = Sellers.UserID);

CREATE SPATIAL INDEX sp_index ON ItemCoordinates (g);