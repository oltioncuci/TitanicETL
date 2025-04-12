CREATE TABLE IF NOT EXISTS train (
    passenger_id INT PRIMARY KEY NOT NULL,
    survived INT,
    pclass INT,
    name VARCHAR(100),
    sex VARCHAR(100),
    age INT,
    sibSp INT,
    parch INT,
    ticket VARCHAR(100),
    fare FLOAT,
    cabin VARCHAR(100),
    embarked VARCHAR(100)
)