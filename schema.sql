-- Weather Tables

CREATE TABLE Weather
(
    station VARCHAR(15),
    date DATE PRIMARY KEY
    -- TSUN doesnt have any information
);

CREATE TABLE Wind
(
    date DATE REFERENCES Weather,
    avgwind NUMERIC(3,2)
    -- peakgusttime never has information
);

CREATE TABLE Precipitation
(
    date DATE REFERENCES Weather,
    precip NUMERIC(4,2),
    snow NUMERIC(4,2),
    snowdepth NUMERIC(4,2)
);

CREATE TABLE Temperature
(
    date DATE REFERENCES Weather,
    -- avgtemp never has any information
    maxtemp SMALLINT,
    mintemp SMALLINT
);

CREATE TABLE Wtype
(
    code VARCHAR(7),
    meaning VARCHAR(127)
);




-- Collision Tables

CREATE TABLE Crash
(
    date DATE PRIMARY KEY,
    time TIMESTAMP,
    id VARCHAR(31) -- NOT SURE ABOUT TYPE HERE
);

CREATE TABLE Location
(
    borough VARCHAR(31),
    zip VARCHAR(7),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    location POINT,
    on_st VARCHAR(64),
    cross_st VARCHAR(64),
    off_st VARCHAR(64)
);

CREATE TABLE Injuries
(
    num_injured SMALLINT,
    ped_injured SMALLINT,
    cyc_injured SMALLINT,
    mot_injured SMALLINT
);

CREATE TABLE Deaths
(
    num_killed SMALLINT,
    ped_killed SMALLINT,    -- Maybe num_pedestrians or something in both Injuries and Deaths
    cyc_killed SMALLINT,
    mot_killed SMALLINT
);

CREATE TABLE VehiclesFactors
(
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