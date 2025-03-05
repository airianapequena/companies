// pomanda-scroll-scraper.mjs
import { gotScraping } from 'got-scraping';
import pg from 'pg';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
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

// Configuration for scraping
const BATCH_SIZE = 100;  // Number of records to fetch per request
const DELAY_BETWEEN_REQUESTS = 2000;  // Delay between requests in ms
const MAX_CONSECUTIVE_ERRORS = 5;  // Max consecutive errors before aborting

// Session state object to track pagination and auth
const session = {
  // Authentication tokens
  restToken: "53a72850b6156b738ef6656a40813bcd24c9e1d7df27c59b4e1bcfb181706869241d05741f43e3611fb71e4f2c90f1f94a9944f40ed7731bed978b9568bb25f952bb4a99312321670ab61cd0599b6411b2ce7cc65c09ddfabbf6edd8a4fa42c5ef7be12ac7ca637c6c0b80d1b5b571887b4c724a5dfc6fcdaa6af984ef49d7b7f8adb3681447d8ae2985d1a3513083193f035b87e6d28d5ea1f229b3f579a3c4367f8b27cf330a4239d426805458b400d7ff202c6cfa164b2939f04fb84b427452dede013d1d516d750a57789148bbac9e02c9651254c823af14ae592f0f572e8b48371cd4faa4509ce5ae34b08cb5065e09b8f425b294fbd59d1512c482c7b438fbc69b114fceaa6afb8d4a76c03551479dac1f55742e0164452df8a3f83431aa93828b57685797d3cb84c1f10f5a3f6698338ef206261184ed11e301bf9acbd3c33db28f9c8cc84e597e79715ac63ca6f996a194421852b99e99f9a9b8a61776e6fcc9d9b9de4b5521efcc3f72a61a",
  sessionId: "67c6e26cd207779095385a72",
  trackerId: "67c6e26cd207779095385a70",
  
  // Pagination tracking - simulate browser scroll
  currentOffset: 0,
  scrollPosition: 0,
  
  // Cookies storage
  cookies: {},
  
  // Stats
  totalRecords: 0,
  processedRecords: 0
};

/**
 * Sleep function to introduce delay between requests
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Save raw API response to file for debugging
 */
async function saveRawResponse(data, filename) {
  try {
    const logsDir = path.join(__dirname, 'logs');
    await fs.mkdir(logsDir, { recursive: true }).catch(() => {});
    
    const filePath = path.join(logsDir, filename);
    await fs.writeFile(filePath, typeof data === 'string' ? data : JSON.stringify(data, null, 2));
    console.log(`Saved to ${filePath}`);
  } catch (err) {
    console.error('Error saving response:', err);
  }
}

/**
 * Save checkpoint with session state
 */
async function saveCheckpoint() {
  try {
    await saveRawResponse(session, 'checkpoint.json');
    console.log(`Checkpoint saved: ${session.processedRecords}/${session.totalRecords} records processed`);
  } catch (err) {
    console.error('Error saving checkpoint:', err);
  }
}

/**
 * Load checkpoint if exists
 */
async function loadCheckpoint() {
  try {
    const checkpointPath = path.join(__dirname, 'logs', 'checkpoint.json');
    const exists = await fs.access(checkpointPath).then(() => true).catch(() => false);
    
    if (exists) {
      const data = await fs.readFile(checkpointPath, 'utf8');
      const checkpoint = JSON.parse(data);
      
      // Restore session state
      Object.assign(session, checkpoint);
      
      console.log(`Checkpoint loaded: resuming from offset ${session.currentOffset}`);
      console.log(`${session.processedRecords}/${session.totalRecords} records already processed`);
      
      return true;
    }
  } catch (err) {
    console.error('Error loading checkpoint:', err);
  }
  
  return false;
}

/**
 * Extract cookies from response headers
 */
function extractCookies(headers) {
  const cookies = {};
  
  if (headers && headers['set-cookie']) {
    headers['set-cookie'].forEach(cookie => {
      const parts = cookie.split(';')[0].split('=');
      if (parts.length === 2) {
        cookies[parts[0].trim()] = parts[1].trim();
      }
    });
  }
  
  // Update session cookies
  Object.assign(session.cookies, cookies);
  
  return cookies;
}

/**
 * Format cookies for request
 */
function formatCookies() {
  return Object.entries(session.cookies)
    .map(([key, value]) => `${key}=${value}`)
    .join('; ');
}

/**
 * Fetch companies using exact request structure from browser
 */
async function fetchCompanies(offset, size) {
  try {
    console.log(`Fetching companies: offset=${offset}, size=${size}, scroll=${session.scrollPosition}`);
    
    const proxyUrl = formatProxyUrl();
    const url = "https://restapi.pomanda.com/powerSearch/getCompanySearchData";
    
    // Calculate a realistic scroll position based on offset
    // In a real browser, scroll position would increase as you go down the list
    session.scrollPosition = offset * 50; // Approximate height per row
    
    const response = await gotScraping({
      url,
      proxyUrl,
      method: "POST",
      headers: {
        "accept": "*/*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type": "application/json",
        "priority": "u=1, i",
        "resttoken": session.restToken,
        "sessionid": session.sessionId,
        "trackerid": session.trackerId,
        "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Google Chrome\";v=\"133\", \"Chromium\";v=\"133\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "Referer": "https://pomanda.com/",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "cookie": formatCookies()
      },
      body: JSON.stringify({
        "searchText": "",
        "from": offset,
        "size": 222, // Use larger batch size to match browser request
        "sortField": "companyName",
        "sortType": "asc",
        "filters": {
          "company status": {
            "name": "company status",
            "type": "check",
            "valueType": null,
            "fields": {
              "companyStatusList": {
                "value": [
                  {
                    "displayName": "live",
                    "fieldNames": ["Dissolved"],
                    "value": "L",
                    "shareKey": "liv"
                  },
                  {
                    "displayName": "Exclude Dormant Companies",
                    "fieldNames": ["Dormant"],
                    "value": "false",
                    "shareKey": "edc"
                  }
                ],
                "type": "check",
                "fieldNames": []
              }
            }
          },
          "Has a Subsidiary": {
            "name": "Has a Subsidiary",
            "type": "radio",
            "valueType": null,
            "fields": {
              "hasSubsidiary": {
                "value": "No",
                "type": "radio",
                "fieldNames": ["HasSubs"]
              }
            }
          },
          "Registered Address": {
            "name": "Registered Address",
            "type": "location",
            "fields": {
              "locationRegisteredAddress": {
                "value": ["5_Region"],
                "type": "register"
              }
            }
          },
          "Trading Address": {
            "name": "Trading Address",
            "type": "location",
            "fields": {
              "locationTradingAddress": {
                "value": ["5_Region"],
                "type": "trading"
              }
            }
          },
          "tags": {
            "name": "tags",
            "type": "tags",
            "valueType": null,
            "fields": {
              "companyTagsGrowth": {
                "value": [
                  {
                    "displayName": "High",
                    "fieldNames": ["GrowthTag"],
                    "value": "High",
                    "shareKey": "high"
                  },
                  {
                    "displayName": "Rapid",
                    "fieldNames": ["GrowthTag"],
                    "value": "Rapid",
                    "shareKey": "rapid"
                  }
                ],
                "type": "tags",
                "fieldNames": []
              }
            }
          },
          "Company Name": {
            "name": "Company Name",
            "type": "text",
            "valueType": "linkedFilter",
            "fields": {
              "companyName": {
                "value": "aa",
                "type": "text",
                "fieldNames": ["CompanyName"]
              }
            }
          }
        },
        "captchaToken": null,
        "paginationType": "onscroll",
        "isEstimateOn": true,
        "trackerId": session.trackerId,
        "userId": 0
      }),
      timeout: {
        request: 15000,
      },
      https: {
        rejectUnauthorized: false
      }
    });
    
    // Extract and save cookies from response
    extractCookies(response.headers);
    
    // Generate filename with batch info
    const batchId = Date.now();
    const filename = `batch-${offset}-${size}-${batchId}.json`;
    
    // Parse response body
    const data = JSON.parse(response.body);
    
    // Save response for debugging
    await saveRawResponse(data, filename);
    
    // Update total records count if available
    if (data.data && data.data.total !== undefined) {
      session.totalRecords = data.data.total;
    }
    
    return data;
  } catch (error) {
    console.error(`Error fetching companies (offset=${offset}):`, error.message);
    
    if (error.response) {
      console.error('Status code:', error.response.statusCode);
      await saveRawResponse(
        {
          error: error.message,
          statusCode: error.response.statusCode,
          body: error.response.body
        },
        `error-${offset}-${Date.now()}.json`
      );
    }
    
    return null;
  }
}

/**
 * Extract company records from the API response
 */
function extractCompanyRecords(data) {
  if (!data) return null;
  
  if (data.data && data.data.searchData && Array.isArray(data.data.searchData)) {
    return data.data.searchData;
  }
  
  // Check other possible locations (as in your existing code)
  if (data.records && Array.isArray(data.records)) {
    return data.records;
  } else if (data.data && Array.isArray(data.data)) {
    return data.data;
  } else if (data.companies && Array.isArray(data.companies)) {
    return data.companies;
  } else if (data.results && Array.isArray(data.results)) {
    return data.results;
  } else if (data.items && Array.isArray(data.items)) {
    return data.items;
  } else if (data.content && Array.isArray(data.content)) {
    return data.content;
  }
  
  return null;
}

/**
 * Insert company data into the database
 */
async function insertCompanyData(client, company) {
  try {
    // Simple example - in practice, use your full implementation
    if (!company.companyId && !company.id) {
      console.error('Company is missing ID field:', company);
      return null;
    }
    
    const companyId = company.companyId || company.id;
    const companyName = company.companyName || company.name || 'Unknown';
    
    const result = await client.query(`
      INSERT INTO Companies(companyId, companyName) 
      VALUES($1, $2) 
      ON CONFLICT (companyId) DO UPDATE SET
        companyName = EXCLUDED.companyName
      RETURNING companyId;
    `, [companyId, companyName]);
    
    return result.rows[0].companyid;
  } catch (error) {
    console.error('Error inserting company:', error.message);
    throw error;
  }
}

/**
 * Process company data and save to database
 */
async function processCompanyData(companies) {
  if (!companies || companies.length === 0) {
    console.log('No companies to process');
    return 0;
  }

  let successCount = 0;
  
  for (const company of companies) {
    const client = await pool.connect();
    
    try {
      console.log(`Processing company: ${company.companyName || company.name || 'Unknown'} (${successCount + 1}/${companies.length})`);
      
      await client.query('BEGIN');
      const id = await insertCompanyData(client, company);
      await client.query('COMMIT');
      
      if (id) successCount++;
    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`Error processing company:`, error.message);
    } finally {
      client.release();
    }
  }
  
  console.log(`Successfully processed ${successCount} out of ${companies.length} companies`);
  return successCount;
}

/**
 * Main function to fetch all companies using sequential scroll-based pagination
 */
async function fetchAllCompanies() {
  let consecutiveErrors = 0;
  let hasMoreData = true;
  
  // Record start time for rate calculation
  const startTime = Date.now();
  
  try {
    // Load checkpoint if exists
    await loadCheckpoint();
    
    // Continue fetching until we run out of data or hit too many errors
    while (hasMoreData && consecutiveErrors < MAX_CONSECUTIVE_ERRORS) {
      try {
        // Fetch the next batch
        const offset = session.currentOffset;
        const data = await fetchCompanies(offset, BATCH_SIZE);
        
        if (!data) {
          console.error(`Failed to fetch data at offset ${offset}`);
          consecutiveErrors++;
          continue;
        }
        
        // Extract records from the response
        const records = extractCompanyRecords(data);
        
        if (!records || records.length === 0) {
          console.log(`No records found at offset ${offset}`);
          
          // Two possibilities:
          // 1. We've reached the end of the data
          // 2. There was an error (maybe due to pagination)
          
          if (session.processedRecords >= session.totalRecords) {
            // We've processed all records
            console.log('All records processed!');
            hasMoreData = false;
          } else {
            // Potential error, count as consecutive error
            consecutiveErrors++;
            
            // If we've hit too many errors, stop
            if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
              console.error(`Reached maximum consecutive errors (${MAX_CONSECUTIVE_ERRORS}). Stopping.`);
              hasMoreData = false;
            } else {
              // Try to recover by incrementing offset and trying again
              console.log(`Trying to recover by incrementing offset`);
              session.currentOffset += BATCH_SIZE;
              await saveCheckpoint();
            }
          }
        } else {
          // Reset consecutive errors counter on success
          consecutiveErrors = 0;
          
          // Process the records
          const processed = await processCompanyData(records);
          session.processedRecords += processed;
          
          // Calculate and log progress
          const progress = Math.round((session.processedRecords / session.totalRecords) * 100);
          const elapsedMinutes = (Date.now() - startTime) / 60000;
          const recordsPerMinute = session.processedRecords / elapsedMinutes;
          const estimatedTimeRemaining = (session.totalRecords - session.processedRecords) / recordsPerMinute;
          
          console.log(`Progress: ${session.processedRecords}/${session.totalRecords} records (${progress}%)`);
          console.log(`Processing rate: ${recordsPerMinute.toFixed(2)} records/min`);
          console.log(`Est. time remaining: ${estimatedTimeRemaining.toFixed(2)} minutes`);
          
          // Increment offset for next batch (simulate scrolling)
          session.currentOffset += records.length;
          
          // Save checkpoint
          await saveCheckpoint();
          
          // Check if we've reached the end
          if (session.processedRecords >= session.totalRecords) {
            console.log('All records processed!');
            hasMoreData = false;
          } else {
            // Add delay between requests
            console.log(`Waiting ${DELAY_BETWEEN_REQUESTS}ms before next request...`);
            await sleep(DELAY_BETWEEN_REQUESTS);
          }
        }
      } catch (error) {
        console.error('Error in fetch loop:', error.message);
        consecutiveErrors++;
        
        // Longer delay on error
        await sleep(DELAY_BETWEEN_REQUESTS * 2);
      }
    }
    
    console.log(`Total companies processed: ${session.processedRecords}`);
    return session.processedRecords;
  } catch (error) {
    console.error('Error in fetchAllCompanies:', error);
    throw error;
  }
}

/**
 * Main function to run the ETL process
 */
async function runETL() {
  try {
    console.log('Starting ETL process with scroll-based pagination...');
    
    // Fetch all companies using sequential pagination
    await fetchAllCompanies();
    
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