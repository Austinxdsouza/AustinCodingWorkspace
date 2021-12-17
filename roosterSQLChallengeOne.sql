--  JOIN - MARKETING + ACTIVITY TABLES
	CREATE VIEW merged_view_a AS
		SELECT activity.["user_id"], activity.["date"], activity.["revenue"], players.["country"],players.["dob"],players.["reg_date"],players.["gender"]
		FROM activity LEFT JOIN players ON activity.["user_id"] = players.["user_id"];
	GO
-- QUESTION NO 1.1 :: HOW MANY MALE USERS IN THE DATASET - 10,120 rows --
	CREATE VIEW merged_view_m AS
		SELECT * FROM merged_view_a WHERE merged_view_a.["gender"] = '"M"'
	GO
	SELECT * FROM merged_view_m;
	GO
-- QUESTION NO 1.2 :: Female users are in the dataset? - 6,918 rows --
	CREATE VIEW merged_view_f AS
		SELECT * FROM merged_view_a WHERE merged_view_a.["gender"] = '"F"'
	GO
	SELECT * FROM merged_view_f;
	GO
-- QUESTION NO 1.3 :: For how many users is no gender information available? -- 5,585 --
	CREATE VIEW merged_view_n AS
		SELECT * FROM merged_view_a WHERE merged_view_a.["gender"] = '" "'
	GO
	SELECT * FROM merged_view_n;
	GO
--  ASSUMING GENDER AS MALE (“M”) for all NULLS OR " "
	CREATE VIEW merged_view_b AS 
	SELECT *, gender_new =
	CASE	WHEN merged_view_a.["gender"] IS NULL THEN '"M"'
			WHEN merged_view_a.["gender"] = '" "' THEN '"M"'
			ELSE merged_view_a.["gender"]
			END
			FROM merged_view_a
	GO
	SELECT * FROM merged_view_b;
	GO
-- QUESTION NO 2.1 :: What is the total all time revenue? - 2972050.4362154
	SELECT SUM(merged_view_b.["revenue"]) AS totalRevinue
	FROM merged_view_b;
	GO
-- QUESTION NO 2.2 :: What is the average revenue per user?
	SELECT merged_view_b.["user_id"], AVG(merged_view_b.["revenue"]) averageRevPerUser
	FROM merged_view_b
	GROUP BY merged_view_b.["user_id"]
	ORDER BY merged_view_b.["user_id"]
	GO
	--QUESTION NO 2.3 :: What is the total number of users in each country?
	SELECT SUM(CAST(merged_view_b.["user_id"] AS bigint)) totalUsers ,merged_view_b.["country"] 
	FROM merged_view_b
	GROUP BY merged_view_b.["country"]
	ORDER BY merged_view_b.["country"]

-- QUESTION NO 2.4 :: What is the average revenue per user in each country?
	SELECT merged_view_b.["user_id"], AVG(merged_view_b.["revenue"]) TotalRevinue,merged_view_b.["country"] country
	FROM merged_view_b
	GROUP BY merged_view_b.["country"],merged_view_b.["user_id"]
	GO
-- QUESTION NO 3.1 :: What is the total revenue for all users in their first week from registration?
	SELECT (merged_view_b.["revenue"]/4) firstweeksRevenue,merged_view_b.["user_id"],month(merged_view_b.["reg_date"])
	FROM merged_view_b
	GROUP BY merged_view_b.["user_id"],merged_view_b.["revenue"], month(merged_view_b.["reg_date"])
	ORDER BY merged_view_b.["revenue"]/4
-- QUESTION NO 3.2 What is the average first week revenue per user, ie in the first 7 days from registration?
	SELECT avg(merged_view_b.["revenue"]/4) avgFirstweeksRevenue,merged_view_b.["user_id"],month(merged_view_b.["reg_date"])
	FROM merged_view_b
	GROUP BY merged_view_b.["user_id"],merged_view_b.["revenue"], month(merged_view_b.["reg_date"])
	ORDER BY merged_view_b.["revenue"]/4
-- QUESTION NO 3.3 :: What is the average revenue in each country, in the first week from registration for each user?
	SELECT avg(merged_view_b.["revenue"]/4) avgFirstweeksRevenue,merged_view_b.["user_id"],merged_view_b.["country"]
	FROM merged_view_b
	GROUP BY merged_view_b.["user_id"],merged_view_b.["revenue"], merged_view_b.["country"]
	ORDER BY merged_view_b.["revenue"]/4

--TO CAST INT TO BIGINT https://stackoverflow.com/questions/15950580/sql-server-arithmetic-overflow-error-converting-expression-to-data-type-int
--TO REPLACE NULLS WITH VALUES LIKE 'MALES'https://www.codeproject.com/Questions/821520/How-to-replace-null-and-blank-with-some-value-usin
--AMAZING CHEAT SHEET https://learnsql.com/blog/sql-basics-cheat-sheet/
--FYI https://dba.stackexchange.com/questions/221805/how-to-set-the-first-row-of-csv-data-as-column-name-in-sql-server
--AMAZING SQL GUIDE https://www.tutorialgateway.org/sql/


DROP VIEW merged_view_a;
DROP VIEW merged_view_b
DROP VIEW merged_view_m
DROP VIEW merged_view_f
DROP VIEW merged_view_n
