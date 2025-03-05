// db-setup.js
const { Client } = require('pg');

// Database connection configuration
const dbConfig = {
  host: 'localhost',
  port: 5432, // Default PostgreSQL port
  user: 'ariana', // Your macOS username
  password: '', // Empty password for local development
  database: 'postgres' // Default database to connect initially
};

// Create a client to connect to the default database
const client = new Client(dbConfig);

// SQL statements for creating tables
const createTableQueries = [
  `CREATE TABLE IF NOT EXISTS Companies (
    companyId VARCHAR(20) PRIMARY KEY,
    companyName VARCHAR(255),
    score FLOAT,
    status VARCHAR(50),
    liqStatus VARCHAR(50),
    industryCode VARCHAR(10),
    industryName VARCHAR(255),
    address VARCHAR(255),
    location VARCHAR(50),
    incorporatedDate DATE,
    country VARCHAR(50),
    activity VARCHAR(255),
    watchListTitles TEXT
  )`,
  
  `CREATE TABLE IF NOT EXISTS NetAssets (
    id SERIAL PRIMARY KEY,
    companyId VARCHAR(20) REFERENCES Companies(companyId),
    value FLOAT,
    type VARCHAR(50)
  )`,
  
  `CREATE TABLE IF NOT EXISTS ProfitAfterTax (
    id SERIAL PRIMARY KEY,
    companyId VARCHAR(20) REFERENCES Companies(companyId),
    value FLOAT,
    type VARCHAR(50)
  )`,
  
  `CREATE TABLE IF NOT EXISTS Turnover (
    id SERIAL PRIMARY KEY,
    companyId VARCHAR(20) REFERENCES Companies(companyId),
    value FLOAT,
    type VARCHAR(50)
  )`,
  
  `CREATE TABLE IF NOT EXISTS TradAreas (
    tradAreaId SERIAL PRIMARY KEY,
    companyId VARCHAR(20) REFERENCES Companies(companyId),
    tradArea VARCHAR(255),
    tradAddress VARCHAR(255),
    tradPostShort VARCHAR(50),
    tradRegionID INTEGER,
    tradRegion VARCHAR(255),
    tradPost VARCHAR(50),
    tradDistrictID INTEGER,
    tradLatitude FLOAT,
    tradDistrict VARCHAR(255),
    tradLongitude FLOAT,
    tradLocation VARCHAR(255)
  )`,
  
  `CREATE TABLE IF NOT EXISTS IndustryMix (
    id SERIAL PRIMARY KEY,
    companyId VARCHAR(20) REFERENCES Companies(companyId),
    SICDesc VARCHAR(255),
    SICGroup VARCHAR(10),
    SIC2007Weight FLOAT,
    SIC2007 VARCHAR(10)
  )`,
  
  `CREATE TABLE IF NOT EXISTS MultipleIndustryMixTO (
    id SERIAL PRIMARY KEY,
    companyId VARCHAR(20) REFERENCES Companies(companyId),
    value FLOAT,
    type VARCHAR(50)
  )`,
  
  `CREATE TABLE IF NOT EXISTS Activity (
    id SERIAL PRIMARY KEY,
    companyId VARCHAR(20) REFERENCES Companies(companyId),
    value VARCHAR(255),
    type VARCHAR(50)
  )`
];

// Function to create the database and tables
async function setupDatabase() {
  try {
    // Connect to the default postgres database
    await client.connect();
    console.log('Connected to PostgreSQL server');
    
    // Check if our database exists
    const checkDbResult = await client.query(
      "SELECT 1 FROM pg_database WHERE datname = 'scraping_db'"
    );
    
    // Create our database if it doesn't exist
    if (checkDbResult.rows.length === 0) {
      console.log('Creating database "scraping_db"...');
      await client.query('CREATE DATABASE scraping_db');
      console.log('Database created successfully');
    } else {
      console.log('Database "scraping_db" already exists');
    }
    
    // Close connection to default database
    await client.end();
    
    // Connect to our new database
    const dbClient = new Client({
      ...dbConfig,
      database: 'scraping_db'
    });
    
    await dbClient.connect();
    console.log('Connected to "scraping_db" database');
    
    // Create all tables
    for (const query of createTableQueries) {
      await dbClient.query(query);
    }
    console.log('All tables created successfully');
    
    // Close the connection
    await dbClient.end();
    console.log('Database setup completed');
    
  } catch (error) {
    console.error('Error setting up database:', error);
    process.exit(1);
  }
}

// Run the setup
setupDatabase();