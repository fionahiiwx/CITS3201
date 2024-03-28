DROP TABLE IF EXISTS FactMedalWins;
-- DimCountry
DROP TABLE IF EXISTS DimCountry;
CREATE TABLE dimcountry (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(255) NOT NULL,
    country_code VARCHAR(3),
    region VARCHAR(255)
);

-- DimAthlete
DROP TABLE IF EXISTS DimAthlete;
CREATE TABLE dimathlete (
    athlete_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    gender VARCHAR(50)
);

-- DimEvent
DROP TABLE IF EXISTS DimEvent;
CREATE TABLE dimevent (
    event_id SERIAL PRIMARY KEY,
    discipline VARCHAR(255) NOT NULL,
    event_title VARCHAR(255) NOT NULL
);

-- DimTime
DROP TABLE IF EXISTS DimTime;
CREATE TABLE dimtime (
    time_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    game_season VARCHAR(50)
);

-- DimHost
DROP TABLE IF EXISTS DimHost;
CREATE TABLE DimHost (
    host_id SERIAL PRIMARY KEY,
    game_slug VARCHAR(255) NOT NULL,
    game_location VARCHAR(255) NOT NULL
);

-- FactMedalWins
CREATE TABLE factmedalwins (
    medal_win_id SERIAL PRIMARY KEY,
    country_id INT REFERENCES DimCountry(country_id),
    athlete_id INT REFERENCES DimAthlete(athlete_id),
    event_id INT REFERENCES DimEvent(event_id),
    time_id INT REFERENCES DimTime(time_id),
	host_id INT REFERENCES DimHost(host_id),
    medal_type VARCHAR(50)
);