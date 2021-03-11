immigration_sql = """
        SELECT 
      arrival_date,
      i94cit AS origin_country,
      i94res AS residency_country,
      i94port AS arrival_city_code,
      i94mode AS travel_mode,
      i94bir AS age,
      i94visa AS reason,
      gender,
      airline,
      visatype AS visa_type,
      COUNT(*) AS count
    FROM i94 
    GROUP BY 1,2,3,4,5,6,7,8,9,10
        """


time_sql = """
        SELECT
          arrival_date AS date,
          year(arrival_date) AS year,
          month(arrival_date) AS month,
          dayofmonth(arrival_date) AS day
        FROM i94
        GROUP BY 1,2,3,4
        ORDER BY date DESC
    """

us_cities_sql = """
    WITH city_pop AS (
        SELECT 
          state_code,
          city_name,
          SUM(male_population) AS male_population ,
          SUM(female_population) AS female_population,
          SUM(total_population) AS total_population,
          SUM(foreign_born) AS foreign_born
        FROM cities
        GROUP BY 1,2
    )
    SELECT 
      city_code,
      city_name,
      state_code,
      state_name,
      male_population,
      female_population,
      total_population,
      foreign_born
    FROM city_pop 
    LEFT JOIN city_map
    USING(city_name,state_code)
    WHERE city_code IS NOT NULL
    """


temp_sql = """
      SELECT
        country_name AS country,
        date,
        AVG(average_temperature) AS average_temperature
        FROM temp
        WHERE year(date) >= 1900
        GROUP BY 1,2
        ORDER BY date DESC, country
    """