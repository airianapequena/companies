// reset-database.mjs
import pg from 'pg';
const { Pool } = pg;

// Database connection configuration - same as in your scraper
const dbConfig = {
  host: 'localhost',
  port: 5432,
  user: 'ariana',
  password: '',
  database: 'scraping_db'
};

// Create a connection pool
const pool = new Pool(dbConfig);

/**
 * Clear all data from the database tables but keep the structure
 */
async function resetDatabase() {
  const client = await pool.connect();
  
  try {
    console.log('Starting database reset...');
    
    // Begin transaction
    await client.query('BEGIN');
    
    // Disable foreign key constraints temporarily
    await client.query('SET CONSTRAINTS ALL DEFERRED');
    
    // Clear all child tables first
    console.log('Clearing TradAreas table...');
    await client.query('TRUNCATE TABLE TradAreas CASCADE');
    
    console.log('Clearing IndustryMix table...');
    await client.query('TRUNCATE TABLE IndustryMix CASCADE');
    
    console.log('Clearing financial data tables...');
    await client.query('TRUNCATE TABLE NetAssets CASCADE');
    await client.query('TRUNCATE TABLE ProfitAfterTax CASCADE');
    await client.query('TRUNCATE TABLE Turnover CASCADE');
    await client.query('TRUNCATE TABLE MultipleIndustryMixTO CASCADE');
    await client.query('TRUNCATE TABLE Activity CASCADE');
    
    // Finally clear the main Companies table
    console.log('Clearing Companies table...');
    await client.query('TRUNCATE TABLE Companies CASCADE');
    
    // Re-enable foreign key constraints
    await client.query('SET CONSTRAINTS ALL IMMEDIATE');
    
    // Commit transaction
    await client.query('COMMIT');
    
    console.log('Database reset completed successfully!');
  } catch (error) {
    // Rollback in case of error
    await client.query('ROLLBACK');
    console.error('Error resetting database:', error);
    throw error;
  } finally {
    // Release the client back to the pool
    client.release();
    await pool.end();
  }
}

// Run the reset function
resetDatabase()
  .then(() => {
    console.log('Database cleanup complete');
    process.exit(0);
  })
  .catch(err => {
    console.error('Failed to reset database:', err);
    process.exit(1);
  });