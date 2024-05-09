--Script that also creates table but with an extra column for country.
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255),
    country CHECK (country IN ('US', 'CO', 'TN')) NOT NULL DEFAULT 'US'
);