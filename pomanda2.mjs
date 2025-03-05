// esm-brewery-loader.mjs
// This file uses ES modules syntax to import got-scraping properly
// Note the .mjs extension which forces Node.js to treat it as an ES module

import { gotScraping } from 'got-scraping';
import pg from 'pg';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Import your proxy utility (assuming it's also an ES module)
// If your proxies.js is CommonJS, you'll need to adapt this part
import { formatProxyUrl } from './proxies.mjs';

const { Pool } = pg;
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Database connection configuration
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
 * Fetch brewery data from the Pomanda API
 */
async function fetchBreweryData(page = 0, size = 48) {
  try {
    const proxyUrl = formatProxyUrl();
    const url = "https://restapi.pomanda.com/powerSearch/getCompanySearchData";
    
    console.log(`Fetching brewery data (page: ${page}, size: ${size})...`);
    
    const response = await gotScraping({
      url, 
      proxyUrl,
      method: "POST",
      headers: {
        "content-type": "application/json",
        "resttoken": "53a72850b6156b738ef6656a40813bcd24c9e1d7df27c59b4e1bcfb181706869241d05741f43e3611fb71e4f2c90f1f94a9944f40ed7731bed978b9568bb25f952bb4a99312321670ab61cd0599b6411b2ce7cc65c09ddfabbf6edd8a4fa42c5ef7be12ac7ca637c6c0b80d1b5b571887b4c724a5dfc6fcdaa6af984ef49d7b7f8adb3681447d8ae2985d1a3513083193f035b87e6d28d5ea1f229b3f579a3c4367f8b27cf330a4239d426805458b400d7ff202c6cfa164b2939f04fb84b427452dede013d1d516d750a57789148bbac9e02c9651254c823af14ae592f0f572e8b48371cd4faa4509ce5ae34b08cb5065e09b8f425b294fbd59d1512c482c7b438fbc69b114fceaa6afb8d4a76c03551479dac1f55742e0164452df8a3f83431aa93828b57685797d3cb84c1f10f5a3f6698338ef206261184ed11e301bf9acbd3c33db28f9c8cc84e597e79715ac63ca6f996a194421852b99e99f9a9b8a61776e6fcc9d9b9de4b5521efcc3f72a61a",
        "sessionid": "67c6e26cd207779095385a72",
        "trackerid": "67c6e26cd207779095385a70",
        "Referer": "https://pomanda.com/",
        "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Google Chrome\";v=\"133\", \"Chromium\";v=\"133\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
        "Referrer-Policy": "strict-origin-when-cross-origin"
      },
      body: JSON.stringify({
        "searchText": "",
        "from": page * size,
        "size": size,
        "sortField": "companyName",
        "sortType": "asc",
        "filters": {
          "company status": {
            "name": "company status",
            "type": "check",
            "valueType": null,
            "fields": {
              "companyStatusList": {
                "value": [{
                  "displayName": "live",
                  "fieldNames": ["Dissolved"],
                  "value": "L",
                  "shareKey": "liv"
                }],
                "type": "check",
                "fieldNames": []
              }
            }
          },
          "industry": {
            "name": "industry",
            "type": "industry",
            "fields": {
              "industry": {
                "value": ["1105_SICGroup"],
                "type": "industry"
              }
            }
          },
          "Trading Address": {
            "name": "Trading Address",
            "type": "location",
            "fields": {
              "locationTradingAddress": {
                "value": ["6_Region"],
                "type": "trading"
              }
            }
          },
          "Turnover": {
            "name": "Turnover",
            "type": "financialrange",
            "valueType": null,
            "fields": {
              "ProfitAndLossTurnoverMin": {
                "value": "100000",
                "type": "min",
                "fieldNames": ["Turnover", "Est_Turnover"]
              }
            }
          }
        },
        "isEstimateOn": true,
        "trackerId": "67c6e26cd207779095385a70",
        "userId": 0
      }),
      retry: {
        limit: 3,
        methods: ['GET', 'POST'],
        statusCodes: [408, 413, 429, 500, 502, 503, 504, 521, 522, 524],
        errorCodes: ['ETIMEDOUT', 'ECONNRESET', 'EADDRINUSE', 'ECONNREFUSED', 'EPIPE', 'ENOTFOUND', 'ENETUNREACH', 'EAI_AGAIN']
      },
      timeout: {
        request: 15000,
      },
      https: {
        rejectUnauthorized: false
      }
    });
    
    try {
      const data = JSON.parse(response.body);
      return data;
    } catch (e) {
      console.error("Response is not valid JSON:", e.message);
      await saveRawResponse(response.body, `error-response-${Date.now()}.txt`);
      throw new Error("Failed to parse API response");
    }
  } catch (error) {
    console.error("Error fetching data:", error.message);
    
    if (error.response) {
      console.error("Status code:", error.response.statusCode);
      console.error("Headers:", JSON.stringify(error.response.headers));
      
      // Save error response to file for debugging
      await saveRawResponse(
        JSON.stringify({
          status: error.response.statusCode,
          headers: error.response.headers,
          body: error.response.body
        }, null, 2),
        `error-details-${Date.now()}.json`
      );
    }
    
    throw error;
  }
}

/**
 * Save raw API response to file for debugging
 */
async function saveRawResponse(data, filename) {
  try {
    const logsDir = path.join(__dirname, 'logs');
    
    // Create logs directory if it doesn't exist
    await fs.mkdir(logsDir, { recursive: true });
    
    const filePath = path.join(logsDir, filename);
    await fs.writeFile(filePath, data);
    console.log(`Raw response saved to ${filePath}`);
  } catch (err) {
    console.error('Error saving raw response:', err);
  }
}

/**
 * Examine the structure of the API response to understand where the data is located
 */
async function examineApiResponse(responseData) {
  console.log('Examining API response structure...');
  console.log('Top-level keys:', Object.keys(responseData));
  
  // Check for common data container fields
  const possibleDataFields = ['records', 'data', 'companies', 'results', 'items', 'content'];
  
  for (const field of possibleDataFields) {
    if (responseData[field] && Array.isArray(responseData[field])) {
      console.log(`Found data array in field '${field}' with ${responseData[field].length} items`);
      if (responseData[field].length > 0) {
        console.log('Sample item keys:', Object.keys(responseData[field][0]));
      }
    }
  }
  
  // If none of the common fields exist, look for any arrays in the response
  for (const key of Object.keys(responseData)) {
    const value = responseData[key];
    if (Array.isArray(value) && value.length > 0) {
      console.log(`Found array in field '${key}' with ${value.length} items`);
      console.log('Sample item keys:', Object.keys(value[0]));
    } else if (typeof value === 'object' && value !== null) {
      // Check one level deeper for nested arrays
      for (const nestedKey of Object.keys(value)) {
        const nestedValue = value[nestedKey];
        if (Array.isArray(nestedValue) && nestedValue.length > 0) {
          console.log(`Found nested array in '${key}.${nestedKey}' with ${nestedValue.length} items`);
          console.log('Sample nested item keys:', Object.keys(nestedValue[0]));
        }
      }
    }
  }
  
  // Check if the API returned an error response
  if (responseData.error || responseData.errorMessage || responseData.message) {
    console.error('API error response:', 
      responseData.error || responseData.errorMessage || responseData.message);
  }
  
  // Check if there's a status field indicating success/failure
  if (responseData.status || responseData.statusCode) {
    console.log('API status:', responseData.status || responseData.statusCode);
  }
}

/**
 * Format date string to PostgreSQL date format (YYYY-MM-DD)
 */
function formatDate(dateStr) {
  if (!dateStr) return null;
  
  try {
    // If date is in format DD/MM/YYYY
    const parts = dateStr.split('/');
    if (parts.length === 3) {
      return `${parts[2]}-${parts[1]}-${parts[0]}`;
    }
    return dateStr; // Return as is if not in expected format
  } catch (e) {
    console.error('Error formatting date:', e);
    return null;
  }
}

/**
 * Extract value safely from a potentially complex or missing field
 */
function safeExtract(obj, field, defaultValue = null) {
  if (!obj) return defaultValue;
  return obj[field] !== undefined ? obj[field] : defaultValue;
}

/**
 * Insert company data into the database with improved error handling for text fields
 */
async function insertCompanyData(client, company) {
  try {
    // Verify we have the minimum required fields
    if (!company.companyId && !company.id) {
      console.error('Company is missing ID field:', company);
      return null;
    }
    
    // Extract companyId from different possible field names
    const companyId = company.companyId || company.id;
    
    // Normalize company data structure
    const normalizedCompany = {
      companyId: companyId,
      companyName: safeExtract(company, 'companyName', safeExtract(company, 'name', '')),
      score: safeExtract(company, 'score', 0),
      status: safeExtract(company, 'status', safeExtract(company, 'companyStatus', '')),
      liqStatus: safeExtract(company, 'liqStatus', ''),
      industryCode: safeExtract(company, 'industryCode', safeExtract(company, 'SIC', '')),
      industryName: safeExtract(company, 'industryName', safeExtract(company, 'industry', '')),
      address: safeExtract(company, 'address', safeExtract(company, 'companyAddress', '')),
      location: safeExtract(company, 'location', safeExtract(company, 'companyLocation', '')),
      incorporatedDate: safeExtract(company, 'incorporatedDate', safeExtract(company, 'dateOfIncorporation', null)),
      country: safeExtract(company, 'country', ''),
      watchListTitles: safeExtract(company, 'watchListTitles', '')
    };
    
    // Insert into Companies table
    const companyQuery = `
      INSERT INTO Companies(
        companyId, companyName, score, status, liqStatus, 
        industryCode, industryName, address, location, 
        incorporatedDate, country, watchListTitles
      ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      ON CONFLICT (companyId) DO UPDATE SET
        companyName = EXCLUDED.companyName,
        score = EXCLUDED.score,
        status = EXCLUDED.status,
        liqStatus = EXCLUDED.liqStatus,
        industryCode = EXCLUDED.industryCode,
        industryName = EXCLUDED.industryName,
        address = EXCLUDED.address,
        location = EXCLUDED.location,
        incorporatedDate = EXCLUDED.incorporatedDate,
        country = EXCLUDED.country,
        watchListTitles = EXCLUDED.watchListTitles
      RETURNING companyId;
    `;
    
    const companyValues = [
      normalizedCompany.companyId,
      normalizedCompany.companyName,
      normalizedCompany.score,
      normalizedCompany.status,
      normalizedCompany.liqStatus,
      normalizedCompany.industryCode,
      normalizedCompany.industryName,
      normalizedCompany.address,
      normalizedCompany.location,
      normalizedCompany.incorporatedDate ? formatDate(normalizedCompany.incorporatedDate) : null,
      normalizedCompany.country,
      normalizedCompany.watchListTitles
    ];
    
    const companyResult = await client.query(companyQuery, companyValues);
    const insertedCompanyId = companyResult.rows[0].companyid;
    
    // Helper function to handle financial data insertion
    const insertFinancialData = async (tableName, dataObj, valueType = null) => {
      if (!dataObj) return;
      
      // Handle different data formats
      let value, type;
      
      if (typeof dataObj === 'object' && dataObj !== null) {
        // Standard format: { value: 1000, type: 'actual' }
        value = dataObj.value;
        type = dataObj.type;
      } else if (!isNaN(dataObj)) {
        // Simple numeric value
        value = dataObj;
        type = valueType || 'unknown';
      } else {
        // Skip invalid data
        return;
      }
      
      if (value === undefined || value === null) return;
      
      try {
        await client.query(`
          INSERT INTO ${tableName}(companyId, value, type)
          VALUES($1, $2, $3)
          ON CONFLICT DO NOTHING;
        `, [insertedCompanyId, value, type]);
      } catch (err) {
        console.error(`Error inserting into ${tableName}:`, err.message);
        // Continue with other data - don't throw
      }
    };
    
    // Insert financial data with improved handling for different formats
    await insertFinancialData('NetAssets', company.netAssets || company.NetAssets);
    await insertFinancialData('ProfitAfterTax', company.profitAfterTax || company.ProfitAfterTax);
    await insertFinancialData('Turnover', company.turnover || company.Turnover);
    await insertFinancialData('MultipleIndustryMixTO', company.multipleIndustryMixTO || company.MultipleIndustryMixTO);
    
    // Handle Activity separately - some descriptions are long
    const activity = company.activity || company.Activity;
    if (activity) {
      try {
        let value, type;
        
        if (typeof activity === 'object' && activity !== null) {
          value = activity.value;
          type = activity.type;
        } else {
          value = activity;
          type = 'unknown';
        }
        
        if (value !== undefined && value !== null) {
          await client.query(`
            INSERT INTO Activity(companyId, value, type)
            VALUES($1, $2, $3)
            ON CONFLICT DO NOTHING;
          `, [insertedCompanyId, value, type]);
        }
      } catch (err) {
        console.error(`Error inserting into Activity:`, err.message);
        // Continue with other data - don't throw
      }
    }
    
    // Insert TradAreas with better error handling
    const tradArray = company.trad || company.Trad || company.tradingAddresses || company.TradingAddresses || [];
    if (Array.isArray(tradArray) && tradArray.length > 0) {
      for (const trad of tradArray) {
        try {
          await client.query(`
            INSERT INTO TradAreas(
              companyId, tradArea, tradAddress, tradPostShort, 
              tradRegionID, tradRegion, tradPost, tradDistrictID, 
              tradLatitude, tradDistrict, tradLongitude, tradLocation
            ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT DO NOTHING;
          `, [
            insertedCompanyId,
            safeExtract(trad, 'TradArea', safeExtract(trad, 'area', '')),
            safeExtract(trad, 'TradAddress', safeExtract(trad, 'address', '')),
            safeExtract(trad, 'TradPostShort', safeExtract(trad, 'postCodeShort', '')),
            safeExtract(trad, 'TradRegionID', safeExtract(trad, 'regionId', '')),
            safeExtract(trad, 'TradRegion', safeExtract(trad, 'region', '')),
            safeExtract(trad, 'TradPost', safeExtract(trad, 'postCode', '')),
            safeExtract(trad, 'TradDistrictID', safeExtract(trad, 'districtId', '')),
            safeExtract(trad, 'TradLatitude', safeExtract(trad, 'latitude', null)),
            safeExtract(trad, 'TradDistrict', safeExtract(trad, 'district', '')),
            safeExtract(trad, 'TradLongitude', safeExtract(trad, 'longitude', null)),
            safeExtract(trad, 'TradLocation', safeExtract(trad, 'location', ''))
          ]);
        } catch (err) {
          console.error('Error inserting trading area:', err.message);
          // Continue with other trading areas - don't throw
        }
      }
    }
    
    // Insert IndustryMix with better error handling
    const indMixArray = company.indMix || company.IndustryMix || company.industries || company.Industries || [];
    if (Array.isArray(indMixArray) && indMixArray.length > 0) {
      for (const ind of indMixArray) {
        try {
          await client.query(`
            INSERT INTO IndustryMix(
              companyId, SICDesc, SICGroup, SIC2007Weight, SIC2007
            ) VALUES($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING;
          `, [
            insertedCompanyId,
            safeExtract(ind, 'SICDesc', safeExtract(ind, 'description', '')),
            safeExtract(ind, 'SICGroup', safeExtract(ind, 'group', '')),
            safeExtract(ind, 'SIC2007Weight', safeExtract(ind, 'weight', 0)),
            safeExtract(ind, 'SIC2007', safeExtract(ind, 'code', ''))
          ]);
        } catch (err) {
          console.error('Error inserting industry mix:', err.message);
          // Continue with other industry data - don't throw
        }
      }
    }
    
    return insertedCompanyId;
  } catch (error) {
    console.error('Error in insertCompanyData:', error.message);
    console.error('Problem company:', JSON.stringify(company).substring(0, 200) + '...');
    throw error; // Rethrow to trigger transaction rollback
  }
}

/**
 * Process a batch of company data from the Pomanda API response with improved transaction handling
 */
async function processCompanyData(companies) {
  let successCount = 0;
  
  for (const company of companies) {
    // Use a separate client and transaction for each company to prevent one error from affecting all
    const client = await pool.connect();
    
    try {
      console.log(`Processing company: ${company.companyName || company.name || 'Unknown'}`);
      
      await client.query('BEGIN');
      const id = await insertCompanyData(client, company);
      await client.query('COMMIT');
      
      if (id) successCount++;
    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`Error processing company ${company.companyName || company.name || 'Unknown'}:`, error.message);
    } finally {
      client.release();
    }
  }
  
  console.log(`Successfully processed ${successCount} out of ${companies.length} companies`);
  return successCount;
}

/**
 * Fetch multiple pages of data from the API with improved error handling and data structure detection
 */
async function fetchAllPages(startPage = 0, pageSize = 48, maxPages = 10) {
  let currentPage = startPage;
  let totalProcessed = 0;
  let hasMoreData = true;
  let consecutiveErrors = 0;
  const MAX_CONSECUTIVE_ERRORS = 3;
  
  try {
    while (hasMoreData && currentPage < startPage + maxPages && consecutiveErrors < MAX_CONSECUTIVE_ERRORS) {
      try {
        const data = await fetchBreweryData(currentPage, pageSize);
        
        // Reset consecutive errors counter on successful fetch
        consecutiveErrors = 0;
        
        // Save raw data for backup/debugging
        await saveRawResponse(
          JSON.stringify(data, null, 2),
          `brewery-data-page-${currentPage}.json`
        );
        
        // Examine the API response structure
        await examineApiResponse(data);
        
        // Check various possible data fields
        let records = null;
        
        // Check for the specific structure we found in the logs: data.searchData
        if (data.data && data.data.searchData && Array.isArray(data.data.searchData)) {
          console.log("Found data in 'data.searchData' field!");
          records = data.data.searchData;
        } else if (data.records && Array.isArray(data.records)) {
          records = data.records;
        } else if (data.data && Array.isArray(data.data)) {
          records = data.data;
        } else if (data.companies && Array.isArray(data.companies)) {
          records = data.companies;
        } else if (data.results && Array.isArray(data.results)) {
          records = data.results;
        } else if (data.items && Array.isArray(data.items)) {
          records = data.items;
        } else if (data.content && Array.isArray(data.content)) {
          records = data.content;
        } else if (data.hits && data.hits.hits && Array.isArray(data.hits.hits)) {
          // Elasticsearch-style response
          records = data.hits.hits.map(hit => hit._source || hit);
        } else {
          // Look for any array field that might contain company data
          for (const key of Object.keys(data)) {
            if (Array.isArray(data[key]) && data[key].length > 0 && 
                typeof data[key][0] === 'object' && data[key][0] !== null) {
              console.log(`Using data from field '${key}' as records`);
              records = data[key];
              break;
            }
            
            // Check one level deeper for nested arrays
            if (typeof data[key] === 'object' && data[key] !== null) {
              for (const nestedKey of Object.keys(data[key])) {
                if (Array.isArray(data[key][nestedKey]) && data[key][nestedKey].length > 0 &&
                    typeof data[key][nestedKey][0] === 'object' && data[key][nestedKey][0] !== null) {
                  console.log(`Using nested data from field '${key}.${nestedKey}' as records`);
                  records = data[key][nestedKey];
                  break;
                }
              }
              if (records) break;
            }
          }
        }
        
        if (records && records.length > 0) {
          console.log(`Found ${records.length} records to process`);
          const processed = await processCompanyData(records);
          totalProcessed += processed;
          
          // Check if we've received fewer records than requested (end of data)
          if (records.length < pageSize) {
            hasMoreData = false;
            console.log('Received fewer records than requested. End of data reached.');
          }
        } else {
          console.error('No valid data records found in API response');
          
          // Print a sample of the response for debugging
          console.log('Response sample:', JSON.stringify(data).substring(0, 500) + '...');
          
          // Increment consecutive errors counter
          consecutiveErrors++;
          
          if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
            console.error(`Reached maximum consecutive errors (${MAX_CONSECUTIVE_ERRORS}). Stopping.`);
            hasMoreData = false;
          } else {
            console.log(`No records found on page ${currentPage}. Trying next page...`);
          }
        }
      } catch (error) {
        console.error(`Error processing page ${currentPage}:`, error.message);
        
        // Increment consecutive errors counter
        consecutiveErrors++;
        
        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          console.error(`Reached maximum consecutive errors (${MAX_CONSECUTIVE_ERRORS}). Stopping.`);
          hasMoreData = false;
        } else {
          console.log(`Error on page ${currentPage}. Trying next page...`);
        }
      }
      
      currentPage++;
    }
    
    console.log(`Total companies processed: ${totalProcessed}`);
    return totalProcessed;
  } catch (error) {
    console.error('Error in fetch all pages:', error);
    throw error;
  }
}

/**
 * Main function to run the ETL process
 */
async function runETL() {
  try {
    console.log('Starting ETL process...');
    
    // Fetch and process data from the API
    await fetchAllPages(0, 48, 5); // Start at page 0, 48 records per page, fetch up to 5 pages
    
    console.log('ETL process completed successfully');
  } catch (error) {
    console.error('ETL process failed:', error);
  } finally {
    // Close the pool
    await pool.end();
  }
}

// Run the ETL process
runETL().catch(err => {
  console.error('Error in main ETL process:', err);
  process.exit(1);
});