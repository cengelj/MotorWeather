-- Weather Tables

-- Avoid duplicates when loading data into tables
DROP TABLE IF EXISTS Weather CASCADE;
DROP TABLE IF EXISTS Wind CASCADE;
DROP TABLE IF EXISTS Precipitation CASCADE;
DROP TABLE IF EXISTS Temperature CASCADE;
DROP TABLE IF EXISTS Wtype CASCADE;

CREATE TABLE Weather
(
    station VARCHAR(15),
    "date" DATE PRIMARY KEY
    -- TSUN doesnt have any information
);

CREATE TABLE Wind
(
    "date" DATE REFERENCES Weather,
    avgwind NUMERIC(3,2)
    -- peakgusttime never has information
);

CREATE TABLE Precipitation
(
    "date" DATE REFERENCES Weather,
    precip NUMERIC(4,2),
    snow NUMERIC(4,2),
    snowdepth NUMERIC(4,2)
);

CREATE TABLE Temperature
(
    "date" DATE REFERENCES Weather,
    -- avgtemp never has any information
    maxtemp SMALLINT,
    mintemp SMALLINT
);

CREATE TABLE Wtype
(
    code VARCHAR(7) PRIMARY KEY,
    meaning VARCHAR(127)
);



-- Collision Tables

DROP TABLE IF EXISTS Crash CASCADE;
DROP TABLE IF EXISTS Location CASCADE;
DROP TABLE IF EXISTS Injuries CASCADE;
DROP TABLE IF EXISTS Deaths CASCADE;
DROP TABLE IF EXISTS VehiclesFactors CASCADE;

CREATE TABLE Crash
(
    "date" DATE PRIMARY KEY,
    time TIMESTAMP,
    id VARCHAR(15)
);

CREATE TABLE Location
(
    id VARCHAR(15) REFERENCES Crash,
    borough VARCHAR(31),
    zip VARCHAR(7),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    location POINT,
    on_st VARCHAR(63),
    cross_st VARCHAR(63),
    off_st VARCHAR(63)
);

CREATE TABLE Injuries
(
    id VARCHAR(15) REFERENCES Crash,
    total SMALLINT,
    pedestrians SMALLINT,
    cyclists SMALLINT,
    motorists SMALLINT
);

CREATE TABLE Deaths
(
    id VARCHAR(15) REFERENCES Crash,
    total SMALLINT,
    pedestrians SMALLINT,
    cyclists SMALLINT,
    motorists SMALLINT
);

CREATE TABLE VehiclesFactors
(
    id VARCHAR(15) REFERENCES Crash,
    type_vehicle1 VARCHAR(63),
    type_vehicle2 VARCHAR(63),
    type_vehicle3 VARCHAR(63),
    type_vehicle4 VARCHAR(63),
    type_vehicle5 VARCHAR(63),
    contrib_factor1 VARCHAR(63),
    contrib_factor2 VARCHAR(63),
    contrib_factor3 VARCHAR(63),
    contrib_factor4 VARCHAR(63),
    contrib_factor5 VARCHAR(63)
);