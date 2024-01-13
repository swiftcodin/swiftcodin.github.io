@@ -5,48 +5,69 @@ DROP PROCEDURE IF EXISTS calculate_scenario_close_open //
/*
Sort the close-open scenarios by a Bayesian rating.
Bayesian formula:
	weighted rating (WR) = (v ÷ (v+m)) × R + (m ÷ (v+m)) × C
	
	where:
	* R = average for the movie (mean) = (Rating)
	* v = number of votes for the movie = (votes)
	* m = minimum votes required to be listed in the Top 250 (currently 1300)
	* C = the mean vote across the whole report (currently 6.8)
TODO:
* Eliminate duplication between multiple statements
* Okay parameters?
* Clean up variable names
Example:
CALL calculate_scenario_close_open
*/
CREATE PROCEDURE calculate_scenario_close_open()
BEGIN
	/* todo: run this to get average 'percent up'
	select avg(r) from (
		select a.stock_id,
			sum(a.percent > 0) / count(a.stock_id) r, -- percent of 'percents up'
			count(*) v -- num of releases
		from (SELECT sco.earnings_id,
				sco.percent,
				er.stock_id
			from scenario_close_open sco
			inner join earnings_release er
				on er.earnings_id = sco.earnings_id
		) a
		group by a.stock_id
		order by r desc
	) b
	*/

	select s.stock_id,
	SET @min_releases = 5; -- minimum number of release in order be be considered

	-- calculate the average number of times that stocks are up after a release
	SET @average_up = (
		SELECT AVG(r)
		FROM (
			SELECT a.stock_id,
				SUM(a.percent > 0) / COUNT(a.stock_id) r, -- percent of 'percents up'
				COUNT(*) v -- num of releases
			FROM (
				SELECT sco.earnings_id,
					sco.percent,
					er.stock_id
				FROM scenario_close_open sco
				INNER JOIN earnings_release er
					ON er.earnings_id = sco.earnings_id
			) a
			GROUP BY a.stock_id
			ORDER BY r DESC
		) b
	);

	SELECT s.stock_id,
		s.symbol,
		s.company_name,
		(v / (v+5)) * r + (5 / (v + 5)) * 0.5789 bay
	from (
		select a.stock_id,
			sum(a.percent > 0) / count(a.stock_id) r, -- percent of 'percents up'
			count(*) v -- num of releases
		from (SELECT sco.earnings_id,
		(v / (v+5)) * r + (@min_releases / (v + @min_releases)) * @average_up AS bay_rating
	FROM (
		SELECT a.stock_id,
			SUM(a.percent > 0) / COUNT(a.stock_id) r, -- percent of 'percents up'
			COUNT(*) v -- num of releases
		FROM (
			SELECT sco.earnings_id,
				sco.percent,
				er.stock_id
			from scenario_close_open sco
			inner join earnings_release er
				on er.earnings_id = sco.earnings_id
			FROM scenario_close_open sco
			INNER JOIN earnings_release er
				ON er.earnings_id = sco.earnings_id
		) a
		group by a.stock_id
		order by r desc
		GROUP BY  a.stock_id
		ORDER BY r DESC
	) b
	inner join stock s
		on s.stock_id = b.stock_id
	order by bay desc;
	INNER JOIN stock s
		ON s.stock_id = b.stock_id
	ORDER BY bay_rating DESC;
END //
DELIMITER ;
  2 changes: 1 addition & 1 deletion2  
procedures/get_company_scenario_close_open.sql
@@ -6,7 +6,7 @@ DROP PROCEDURE IF EXISTS get_company_scenario_close_open //
Gets the prior close and after open and percent change given a stock_id
Example:
CALL get_company_scenario_close_open(6820)
CALL get_company_scenario_close_open(3568)
*/
CREATE PROCEDURE get_company_scenario_close_open
(
  51 changes: 26 additions & 25 deletions51  
procedures/insert_scenario_close_open.sql
@@ -6,53 +6,54 @@ DROP PROCEDURE IF EXISTS insert_scenario_close_open //
Inserts historical earnings scenarios into the cordurn.scenario_close_open table given
a stock_id. A scenario is defined as buying the close price before an earnings release
and selling the open prices after an earnings release.
TODO:
* Releases with specific times are incorrectly delt with
* Releases between 9 AM and 4 PM are not delt with
* Earnings releases with 'Time Not Supplied' are ignored for now.
Example:
CALL insert_scenario_close_open(6820)
CALL insert_scenario_close_open(3568)
*/
CREATE PROCEDURE insert_scenario_close_open
(
	IN p_stock_id INT
)
BEGIN
	DROP TEMPORARY TABLE IF EXISTS before_dates;
	create temporary table before_dates (date DATE);
	insert into before_dates(date)
	select date FROM ohlc
	CREATE TEMPORARY TABLE before_dates (date DATE);
	INSERT INTO before_dates(date)
	SELECT date FROM ohlc
	WHERE ohlc.stock_id = p_stock_id
	ORDER BY date ASC;

	DROP TEMPORARY TABLE IF EXISTS after_dates;
	create temporary table after_dates (date DATE);
	insert into after_dates(date)
	select date FROM ohlc
	CREATE TEMPORARY TABLE after_dates (date DATE);
	INSERT INTO after_dates(date)
	SELECT date FROM ohlc
	WHERE ohlc.stock_id = p_stock_id
	ORDER BY date ASC;

	insert into scenario_close_open(earnings_id, before_close_id, after_open_id, percent)
	select earnings_id,
	INSERT INTO scenario_close_open(earnings_id, before_close_id, after_open_id, percent)
	SELECT earnings_id,
		before_close_id,
		after_open_id,
		CAST((after_open - before_close) / before_close as DECIMAL(10,6)) AS percent
	from 
	FROM 
		(SELECT er.earnings_id,
			er.release_date,
			before_close.ohlc_id as before_close_id,
			after_open.ohlc_id as after_open_id,
			before_close.close as before_close,
			after_open.high as after_open
			before_close.ohlc_id AS before_close_id,
			after_open.ohlc_id AS after_open_id,
			before_close.close AS before_close,
			after_open.open AS after_open
		FROM earnings_release er
		INNER JOIN ohlc after_open
			ON er.stock_id = after_open.stock_id
			AND er.release_time != 'After Market Close'
			AND er.release_time != 'Time Not Supplied'
			AND (er.release_time = 'Before Market Open'
				 or TIME(STR_TO_DATE(er.release_time, '%T')) <= CAST('12:00:00' AS TIME))
				 OR TIME(STR_TO_DATE(er.release_time, '%T')) <= CAST('9:00:00' AS TIME))
			AND after_open.date = er.release_date
		inner join ohlc before_close
		INNER JOIN ohlc before_close
			on er.stock_id = before_close.stock_id
			AND before_close.date = (SELECT date -- previous trade date
									FROM before_dates bd
@@ -63,27 +64,27 @@ BEGIN
		UNION ALL
		SELECT er.earnings_id,
			er.release_date,
			before_close.ohlc_id as before_close_id,
			after_open.ohlc_id as after_open_id,
			before_close.close as before_close,
			after_open.open as after_open
			before_close.ohlc_id AS before_close_id,
			after_open.ohlc_id AS after_open_id,
			before_close.close AS before_close,
			after_open.open AS after_open
		FROM earnings_release er
		INNER JOIN ohlc before_close
			ON er.stock_id = before_close.stock_id
			AND er.release_time != 'Before Market Open'
			AND er.release_time != 'Time Not Supplied'
			AND (er.release_time = 'After Market Close'
			 	or TIME(STR_TO_DATE(er.release_time, '%T')) > CAST('12:00:00' AS TIME))
			 	OR TIME(STR_TO_DATE(er.release_time, '%T')) >= CAST('16:00:00' AS TIME))
			AND before_close.date = er.release_date
		inner join ohlc after_open
			on er.stock_id = after_open.stock_id
		INNER JOIN ohlc after_open
			ON er.stock_id = after_open.stock_id
			AND after_open.date = (SELECT date -- next trade date
									FROM after_dates ad
									WHERE ad.date > er.release_date
									ORDER BY ad.date ASC 
									LIMIT 1)
		WHERE er.stock_id = p_stock_id
		order by release_date
		ORDER BY release_date
	) AS scenario;
END //
DELIMITER ;
