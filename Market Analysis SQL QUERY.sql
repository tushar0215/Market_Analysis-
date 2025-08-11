SELECT * 
FROM dbo.products

--******************************************************************************************************************************
--*******************************************************************************************************************************

--Query to categorize products based on their price 
Select 
    ProductID, --Selects the unique identifier for each product 
	ProductName, -- Selects the name of each product 
	Price, -- Selects the price of each Product
	-- Category, -- Selects the product category for each product
	
	CASE -- Categories the product  into prices categories : Low, Medium, or High 
	       WHEN Price < 50 THEN 'Low' -- If the price is less than 50, categorize as 'Low'
		   WHEN Price BETWEEN 50 AND 200 THEN 'Medium' -- If the price is between 50 and 200 (inclusive),categorize as 'Medium'
		   ELSE 'High' -- if the price is greater than 200, categorize as 'High'
		END AS PriceCategory --Names the new column as priceCategory 

	FROM 
	dbo.products; --Specifices the source table from which to select the data 




Select 
*
FROM dbo.geography

--********************************************************************************************************************
--********************************************************************************************************************

--SQL Statement to join dim_customers with dim_geography to enrich customer data with geographic information 
SELECT 
      c.CustomerID, --Select the unique identifier for each customer 
	  c.CustomerName, --Selects the name of each customer 
	  c.Email, --Selects the email of each customer 
	  c.Gender, --Selects the gender of each customer 
	  c.Age, --Selects the age of each customer 
	  g.Country, --Selects the country from the geography tables to enrich customer data 
	  g.City --selects the city from the geography table to enrich custumer data 
FROM
   dbo.customers as c --Specifies the alias 'c' for the dim_customers table 
LEFT JOIN 
-- RIGHT JOIN
-- INNER JOIN
-- FULL OUTER JOIN 
   dbo.geography as g  
ON 
  c.GeographyID = g.GeographyID;   -- join two tables 

--******************************************************************************************************************************
--******************************************************************************************************************************
Select *
FROM dbo.customer_reviews


--Query to clean whitespaces issues in the ReveiwText column 

SELECT 
      ReviewID, --Selects the unique identifier for each Review 
	  CustomerID,--Selects the unique identifier for each customer 
	  ProductID, -- Selects the unique identifier for each product 
	  ReviewDate, -- Selects the data when the review was written 
	  Rating, --Selects the numerical rating given by the customer (e.g., 1 to 5 stars)
	  -- Clean up the ReviewText by replacing doubles spaces with single spaces to ensure the text is more readable and standardized 
	  REPLACE (ReviewText,'  ',' ') AS ReviewText 
	FROM
	    dbo.customer_reviews; -- Specifies the source table from which to select the data 

--*******************************************************************************************************************************
--*******************************************************************************************************************************
Select *
FROM  dbo.engagement_data


--Query to clean and normalize the engagement_data table 

SELECT 
     EngagementID, --Select the unique identifier for each engagement record 
	 ContentID, --Selects The unique identifier for each piece of content 
	 CampaignID, --Selects the unique identifier for each marketing campaign
	 ProductID, -- selects the unique identifier for each product 
	 UPPER(REPLACE(Contenttype,'Socialmedia','Social Media')) AS ContentType, --Replaces "Socialmedia" with "Social Media" 
	 LEFT(ViewsClicksCombined, LEN(ViewsClicksCombined) - CHARINDEX('-',ViewsClicksCombined)-1) AS Clicks,
	 Likes, --Select the number likes the content received 
	 --converts the engagementDate to the dd.mm.yyyy format 
	 FORMAT(CONVERT(DATE,EngagementDate),'dd-mm-yyyy') AS EngagementDate --Convert and formats the date as dd-mm-yyyy
FROM
    dbo.engagement_data -- Specifies the source table from which to select the data 
WHERE 
     ContentType !='Newsletter'; --Filter out rows where ContentType is ' NewsLetter' as these are not revelant for our analysis
	 



SELECT 
*
FROM dbo.customer_journey

--***********************************************************************************************************************
--***********************************************************************************************************************

--Common Table Expression (CTE) to identify and tag duplicate records 
WITH DuplicateRecords AS (
     SELECT 
	      JourneyID, --Select the unique identifier for each journey 
		  CustomerID, --Select the unique identifier for each customer
		  ProductID, -- Select the unique identifier for each product 
		  VisitDate, -- Select the date of the visit, which helps in determining the timeline of customer interaction
		  Stage, --Select the stage of the customer journey (e.g., Awarness, Consideration,etc.)
		  Action, --Select the action taken by the customer 
		  Duration, -- Select the duration of the action or interaction 
		  -- Use ROW_NUMBER() to assign a unique row number to each record within the partition defined below 
		  ROW_NUMBER() OVER(
		        --PARTITION BY groups the rows based on the specified columns that should be unique 
				PARTITION BY CustomerID, ProductID, VisitDate, Stage, Action
				-- ODER BY defines how to order the rows within each partition 
				ORDER BY JourneyID 
				) AS row_num -- This  creates a new column 'row_num' that numbers each row within its partition 
			FROM
			dbo.customer_journey --Specifies the source table from which to select the data 

) 

-- Select all records from the CTE where row_num > 1, which indicates duplicate entries 
SELECT *
FROM DuplicateRecords
WHERE row_num > 1 --Filter out the first occurence from num =1) and only shows the duplicate (row num > 1)
ORDER BY JourneyID

WITH DuplicateRecords AS (
    SELECT 
        JourneyID,
        CustomerID,
        ProductID,
        VisitDate,
        Stage,
        Action,
        Duration,
        ROW_NUMBER() OVER(
            PARTITION BY CustomerID, ProductID, VisitDate, Stage, Action
            ORDER BY JourneyID
        ) AS row_num
    FROM dbo.customer_journey
)
SELECT *
FROM DuplicateRecords
WHERE row_num > 1
ORDER BY JourneyID;


-- Outer query selects the final cleaned and standardized data 
SELECT 
     JourneyID, -- Selects the unique identifier for each journey to ensure data tracebility 
	 CustomerID, -- Selects the unique identifier for each customer to link journey to specific customer 
	 ProductID, --Select the unique identifier for each product to analyse customer interaction with different 
	 VisitDate, -- selects data of the visit to understand the timeline of customer interactions 
	 Stage, --Uses the uppercased stage value from the subquerry for consistency in analysis 
	 Action, -- Selects the action taken by the customer (e.g. , view , click , Purchase)
	 COALESCE(Duration, avg_duration) AS Duration --Replaces missing durations with the average duration for the corresponding customer 
FROM
    (  
	   --Subquery to process and clean the data 
	   SELECT 
	         JourneyID, --Selects the unique identifier for each journey to ensure data tracebility 
			 CustomerID, --Selects the unique identifier for each customer to link journeys to specific customers
			 ProductID, --Selects the unique identifier for each product to analyze customer interaction withdifferent product 
			 VisitDate, --Selects the date of the visit to understand the timeline of customer interactions
			 UPPER(Stage) AS Stage, --Converts Stage values to uppercase for consistency in data analysis 
			 Action,
			 Duration, -- Uses Duration directly, assuming its already a numeric type 
			 AVG(Duration) OVER (PARTITION BY VisitDate) AS avg_duration, -- Calculates the average duration for each data 
			 ROW_NUMBER() OVER(
			          PARTITION BY CustomerID, ProductID, VisitDate, UPPER(Stage), Action --Groups by these columns to identify duplicates 
					  ORDER BY JourneyID --Orders by JourneyID to keep the first occurence of each duplicate 
				) AS row_num --Assigns a row number to each row within the partition to identify duplicates 
               FROM 
	               dbo.customer_journey  --Specifies the source table from which to select the data 
	) AS subquery -- Names the subquery for reference in the outer query 
WHERE
   row_num = 1; --Keeps only the first occurence of each duplicate group identified  in the subquery 




SELECT 
     JourneyID,  -- Unique journey ID
     CustomerID, -- Customer ID
     ProductID,  -- Product ID
     VisitDate,  -- Date of visit
     Stage,      -- Uppercased stage
     Action,     -- Customer action
     COALESCE(Duration, avg_duration) AS Duration -- Replace NULL duration with average for that date
FROM
(
    SELECT 
         JourneyID,
         CustomerID,
         ProductID,
         VisitDate,
         UPPER(Stage) AS Stage, -- Consistency
         Action,
         Duration,
         AVG(Duration) OVER (PARTITION BY VisitDate) AS avg_duration, -- Avg duration for that date
         ROW_NUMBER() OVER(
              PARTITION BY CustomerID, ProductID, VisitDate, UPPER(Stage), Action
              ORDER BY JourneyID
         ) AS row_num
    FROM dbo.customer_journey
) AS subquery
WHERE row_num = 1  -- Keep only the first row in each duplicate group
ORDER BY JourneyID;

