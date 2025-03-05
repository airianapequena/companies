const { Client } = require('pg');

const client = new Client({
  host: 'localhost',
  port: 5432,
  user: 'ariana',
  password: '',
  database: 'scraping_db'
});

async function insertTestData() {
  try {
    await client.connect();
    
    // Insert into Companies
    await client.query(`
      INSERT INTO Companies (companyId, companyName, score, status, liqStatus, industryCode, industryName, address, location, incorporatedDate, country, activity, watchListTitles)
      VALUES ('08075408', 'VAUX BREWERY (SUNDERLAND) LTD', NULL, 'Live', NULL, '1105', 'Manufacture of beer', 'UNIT 2 MONK STREET, SUNDERLAND, SR6 0DB.', 'SR6 0DB', '2012-05-18', 'UK', 'Independent craft brewery', NULL)
    `);
    
    // Insert into NetAssets
    await client.query(`
      INSERT INTO NetAssets (companyId, value, type)
      VALUES ('08075408', 21648, 'COMPANYHOUSE')
    `);
    
    // Insert into ProfitAfterTax
    await client.query(`
      INSERT INTO ProfitAfterTax (companyId, value, type)
      VALUES ('08075408', 69536, 'ESTIMATE')
    `);
    
    // Insert into Turnover
    await client.query(`
      INSERT INTO Turnover (companyId, value, type)
      VALUES ('08075408', 312757, 'ESTIMATE')
    `);
    
    // Insert into TradAreas
    await client.query(`
      INSERT INTO TradAreas (companyId, tradArea, tradAddress, tradPostShort, tradRegionID, tradRegion, tradPost, tradDistrictID, tradLatitude, tradDistrict, tradLongitude, tradLocation)
      VALUES 
      ('08075408', 'Sunderland', 'North East Business And Innovati, Wearfield, Sunderland, Tyne and Wear, SR5 2TA.', 'SR5', 6, 'North East England', 'SR5 2TA', 776, 54.9217, 'Carley Hill, Castletown, Downhill, Fulwell, Hylton Castle, Hylton Red House, Marley Pots, Monkwearmouth, Sheepfolds, Southwick, Town End Farm, Witherwack', -1.42208, 'Tyne and Wear')
    `);
    
    console.log('Test data inserted successfully');
    
  } catch (error) {
    console.error('Error inserting test data:', error);
  } finally {
    await client.end();
  }
}

// To delete the test data (run after testing)
async function removeTestData() {
  try {
    await client.connect();
    
    // Delete in reverse order due to foreign key constraints
    await client.query(`DELETE FROM TradAreas WHERE companyId = '08075408'`);
    await client.query(`DELETE FROM Turnover WHERE companyId = '08075408'`);
    await client.query(`DELETE FROM ProfitAfterTax WHERE companyId = '08075408'`);
    await client.query(`DELETE FROM NetAssets WHERE companyId = '08075408'`);
    await client.query(`DELETE FROM Companies WHERE companyId = '08075408'`);
    
    console.log('Test data removed successfully');
    
  } catch (error) {
    console.error('Error removing test data:', error);
  } finally {
    await client.end();
  }
}

// Uncomment the function you want to run
// insertTestData();
removeTestData();