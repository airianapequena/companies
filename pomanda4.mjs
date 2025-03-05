// pomanda-parallel-scraper-optimized.mjs
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
const BATCH_SIZE = 222;  // Number of records to fetch per request
const BASE_DELAY = 1000;  // Base delay between requests in ms
const MAX_CONSECUTIVE_ERRORS = 5;  // Max consecutive errors before aborting

// Create logs directory
const logsDir = path.join(__dirname, 'logs');
fs.mkdir(logsDir, { recursive: true }).catch(() => {});

// Alphabet-based filtering configuration
const ALPHABET = 'abcdefghijklmnopqrstuvwxyz'.split('');
const TRACKER_FILE = path.join(logsDir, 'alphabet_progress.json');

// List of high, medium, and low volume prefixes based on company distribution data
const HIGH_VOLUME_PREFIXES = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'l', 'm', 'p', 'r', 's', 't'];
const MEDIUM_VOLUME_PREFIXES = ['j', 'k', 'n', 'o', 'u', 'v', 'w'];
const LOW_VOLUME_PREFIXES = ['q', 'x', 'y', 'z'];

// Use the same restToken for all requests since that seems to be constant
const REST_TOKEN = "53a72850b6156b738ef6656a40813bcd24c9e1d7df27c59b4e1bcfb181706869241d05741f43e3611fb71e4f2c90f1f94a9944f40ed7731bed978b9568bb25f952bb4a99312321670ab61cd0599b6411b2ce7cc65c09ddfabbf6edd8a4fa42c5ef7be12ac7ca637c6c0b80d1b5b571887b4c724a5dfc6fcdaa6af984ef49d7b7f8adb3681447d8ae2985d1a3513083193f035b87e6d28d5ea1f229b3f579a3c4367f8b27cf330a4239d426805458b400d7ff202c6cfa164b2939f04fb84b427452dede013d1d516d750a57789148bbac9e02c9651254c823af14ae592f0f572e8b48371cd4faa4509ce5ae34b08cb5065e09b8f425b294fbd59d1512c482c7b438fbc69b114fceaa6afb8d4a76c03551479dac1f55742e0164452df8a3f83431aa93828b57685797d3cb84c1f10f5a3f";

// Session state object to track pagination and auth
const session = {
  // Authentication tokens
  restToken: REST_TOKEN,
  sessionId: "", // Will be set by getNextTrackerSession()
  trackerId: "", // Will be set by getNextTrackerSession()
  
  // Pagination tracking
  currentOffset: 0,
  scrollPosition: 0,
  
  // Cookies storage
  cookies: {},
  
  // Current search parameters
  searchPrefix: "",
  
  // Stats
  totalRecords: 0,
  processedRecords: 0,
  batchStartTime: 0
};

/**
 * Progress tracker for alphabet-based filtering
 */
const progressTracker = {
  completed: [], // Completed prefixes
  inProgress: null, // Currently processing prefix
  pending: [], // Prefixes to be processed
  startTime: null, // Overall start time
  subdivisionAnalysis: {}, // Track which letters were subdivided
  
  // Statistics
  totalCompanies: 0,
  processingRates: []
};

/**
 * Function to rotate tracker and session IDs with only the new IDs
 */
function getNextTrackerSession() {
  // Define arrays of tracker IDs and session IDs to rotate through
  const trackerIds = [
    "67c8783deaaf15867fa84fa9",     // New ID 1
    "67c87517610c1e527a9b74aa",     // New ID 2
    "67c8789cd2077790953dcf00",     // New ID 3
    "67c878bf01d79e3fd4330915",     // New ID 4
    "67c879ee01d79e3fd4330d8d"      // New ID 5 (just added)
  ];
  
  const sessionIds = [
    "67c8783deaaf15867fa84fab",     // New ID 1
    "67c87517610c1e527a9b74ac",     // New ID 2
    "67c8789cd2077790953dcf02",     // New ID 3
    "67c878bf01d79e3fd4330917",     // New ID 4
    "67c879ee01d79e3fd4330d8f"      // New ID 5 (just added)
  ];
  
  // Static variable to keep track of which ID pair to use next
  if (typeof getNextTrackerSession.currentIndex === 'undefined') {
    getNextTrackerSession.currentIndex = 0;
  }
  
  // Get the next ID pair and increment counter
  const trackerId = trackerIds[getNextTrackerSession.currentIndex];
  const sessionId = sessionIds[getNextTrackerSession.currentIndex];
  
  // Rotate to next pair
  getNextTrackerSession.currentIndex = (getNextTrackerSession.currentIndex + 1) % trackerIds.length;
  
  console.log(`Using tracker ID: ${trackerId}, session ID: ${sessionId}`);
  return { trackerId, sessionId };
}

// Initialize session with first tracker/session ID pair
const initialIds = getNextTrackerSession();
session.trackerId = initialIds.trackerId;
session.sessionId = initialIds.sessionId;

/**
 * Sleep function to introduce delay between requests
 * Adjusts delay based on prefix volume
 */
const sleep = async (prefix) => {
  // Get the base letter from the prefix
  const baseLetter = prefix.charAt(0);
  
  let delay = BASE_DELAY;
  
  // Adjust delay based on letter volume
  if (HIGH_VOLUME_PREFIXES.includes(baseLetter)) {
    // High volume letters get longer delays
    delay = BASE_DELAY * 3;
  } else if (MEDIUM_VOLUME_PREFIXES.includes(baseLetter)) {
    // Medium volume letters get moderate delays
    delay = BASE_DELAY * 1.5;
  } else if (LOW_VOLUME_PREFIXES.includes(baseLetter)) {
    // Low volume letters get shorter delays
    delay = BASE_DELAY;
  }
  
  // Add a small random factor to avoid predictable patterns
  const randomFactor = 0.75 + (Math.random() * 0.5); // Between 0.75 and 1.25
  const finalDelay = Math.round(delay * randomFactor);
  
  console.log(`Waiting ${finalDelay}ms before next request for prefix '${prefix}'...`);
  return new Promise(resolve => setTimeout(resolve, finalDelay));
};

/**
 * Save raw API response to file for debugging
 */
async function saveRawResponse(data, filename) {
  try {
    const filePath = path.join(logsDir, filename);
    await fs.writeFile(filePath, typeof data === 'string' ? data : JSON.stringify(data, null, 2));
    console.log(`Saved to ${filePath}`);
  } catch (err) {
    console.error('Error saving response:', err);
  }
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
 * Initialize custom prefix list based on known distribution
 */
function initializeCustomPrefixList() {
  console.log('Initializing custom prefix strategy based on known distribution...');
  progressTracker.pending = [];
  progressTracker.subdivisionAnalysis = {};
  
  // For high volume letters, use double-letter combinations
  for (const letter of HIGH_VOLUME_PREFIXES) {
    progressTracker.subdivisionAnalysis[letter] = true; // Mark as subdivided
    
    for (const secondLetter of ALPHABET) {
      progressTracker.pending.push(letter + secondLetter);
    }
  }
  
  // For medium and low volume letters, use single letters
  for (const letter of [...MEDIUM_VOLUME_PREFIXES, ...LOW_VOLUME_PREFIXES]) {
    progressTracker.subdivisionAnalysis[letter] = false; // Mark as not subdivided
    progressTracker.pending.push(letter);
  }
  
  console.log(`Custom prefix strategy initialized with ${progressTracker.pending.length} search prefixes`);
  console.log('Subdivision analysis:', progressTracker.subdivisionAnalysis);
  
  // Save the subdivision analysis to a file for reference
  return saveRawResponse(progressTracker.subdivisionAnalysis, 'subdivision_analysis.json');
}

/**
 * Load progress tracker from file if exists
 */
async function loadProgressTracker() {
  try {
    const exists = await fs.access(TRACKER_FILE).then(() => true).catch(() => false);
    
    if (exists) {
      const data = await fs.readFile(TRACKER_FILE, 'utf8');
      const savedTracker = JSON.parse(data);
      
      // Restore tracker state
      Object.assign(progressTracker, savedTracker);
      
      console.log(`Progress tracker loaded:`);
      console.log(`- Completed: ${progressTracker.completed.length} prefixes`);
      console.log(`- Pending: ${progressTracker.pending.length} prefixes`);
      console.log(`- Total companies so far: ${progressTracker.totalCompanies}`);
      
      if (progressTracker.inProgress) {
        console.log(`- Resuming in-progress prefix: ${progressTracker.inProgress}`);
      }
      
      return true;
    }
  } catch (err) {
    console.error('Error loading progress tracker:', err);
  }
  
  return false;
}

/**
 * Save progress tracker to file
 */
async function saveProgressTracker() {
  try {
    await saveRawResponse(progressTracker, 'alphabet_progress.json');
    console.log(`Progress tracker saved: ${progressTracker.completed.length}/${progressTracker.completed.length + progressTracker.pending.length} prefixes completed`);
  } catch (err) {
    console.error('Error saving progress tracker:', err);
  }
}

/**
 * Save checkpoint with session state for current prefix
 */
async function saveCheckpoint() {
  try {
    const checkpointFile = `checkpoint_${session.searchPrefix}.json`;
    await saveRawResponse(session, checkpointFile);
    console.log(`Checkpoint saved for prefix '${session.searchPrefix}': ${session.processedRecords}/${session.totalRecords} records processed`);
  } catch (err) {
    console.error('Error saving checkpoint:', err);
  }
}

/**
 * Load checkpoint for specified prefix if exists
 */
async function loadCheckpoint(prefix) {
  try {
    const checkpointFile = path.join(logsDir, `checkpoint_${prefix}.json`);
    const exists = await fs.access(checkpointFile).then(() => true).catch(() => false);
    
    if (exists) {
      const data = await fs.readFile(checkpointFile, 'utf8');
      const checkpoint = JSON.parse(data);
      
      // Restore session state
      Object.assign(session, checkpoint);
      
      console.log(`Checkpoint loaded for prefix '${prefix}':`);
      console.log(`- Resuming from offset ${session.currentOffset}`);
      console.log(`- ${session.processedRecords}/${session.totalRecords} records already processed`);
      
      return true;
    }
  } catch (err) {
    console.error(`Error loading checkpoint for prefix '${prefix}':`, err);
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
 * Fetch companies using exact request structure from browser
 * and apply alphabet-based filtering
 */
async function fetchCompanies(prefix, offset = 0) {
  try {
    console.log(`Fetching companies with prefix '${prefix}', offset=${offset}, size=${BATCH_SIZE}`);
    
    // Rotate tracker ID and session ID for each request
    const { trackerId, sessionId } = getNextTrackerSession();
    session.trackerId = trackerId;
    session.sessionId = sessionId;
    
    const proxyUrl = formatProxyUrl();
    const url = "https://restapi.pomanda.com/powerSearch/getCompanySearchData";
    
    // Update session info
    session.searchPrefix = prefix;
    session.currentOffset = offset;
    session.scrollPosition = offset * 50; // Approximate height per row
    session.batchStartTime = Date.now();
    
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
        "size": BATCH_SIZE,
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
          "Company Name": {
            "name": "Company Name",
            "type": "text",
            "valueType": "linkedFilter",
            "fields": {
              "companyName": {
                "value": prefix,
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
        request: 30000, // Increased timeout
      },
      https: {
        rejectUnauthorized: false
      }
    });
    
    // Extract and save cookies from response
    extractCookies(response.headers);
    
    // Generate unique filename with prefix and batch info
    const batchId = Date.now();
    const filename = `${prefix}_batch-${offset}-${batchId}.json`;
    
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
    console.error(`Error fetching companies with prefix '${prefix}' (offset=${offset}):`, error.message);
    
    if (error.response) {
      console.error('Status code:', error.response.statusCode);
      await saveRawResponse(
        {
          error: error.message,
          statusCode: error.response.statusCode,
          body: error.response.body
        },
        `error-${prefix}-${offset}-${Date.now()}.json`
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
  
  // Check other possible locations
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
 * Process companies for a single alphabet prefix
 */
async function processAlphabetPrefix(prefix) {
  // Reset session state for new prefix
  session.currentOffset = 0;
  session.scrollPosition = 0;
  session.processedRecords = 0;
  session.totalRecords = 0;
  
  // Update progress tracker
  progressTracker.inProgress = prefix;
  await saveProgressTracker();
  
  // Try to load checkpoint for this prefix
  const hasCheckpoint = await loadCheckpoint(prefix);
  if (!hasCheckpoint) {
    console.log(`Starting new search for prefix '${prefix}'`);
  }
  
  let consecutiveErrors = 0;
  let hasMoreData = true;
  
  try {
    // Continue fetching until we run out of data or hit too many errors
    while (hasMoreData && consecutiveErrors < MAX_CONSECUTIVE_ERRORS) {
      try {
        // Fetch the next batch
        const offset = session.currentOffset;
        const data = await fetchCompanies(prefix, offset);
        
        if (!data) {
          console.error(`Failed to fetch data for prefix '${prefix}' at offset ${offset}`);
          consecutiveErrors++;
          continue;
        }
        
        // Extract records from the response
        const records = extractCompanyRecords(data);
        
        if (!records || records.length === 0) {
          console.log(`No records found for prefix '${prefix}' at offset ${offset}`);
          
          if (session.processedRecords >= session.totalRecords || session.totalRecords === 0) {
            // We've processed all records for this prefix
            console.log(`All records processed for prefix '${prefix}'!`);
            hasMoreData = false;
          } else {
            // Potential error, count as consecutive error
            consecutiveErrors++;
            
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
          progressTracker.totalCompanies += processed;
          
          // Calculate and log progress
          const progress = session.totalRecords ? 
            Math.round((session.processedRecords / session.totalRecords) * 100) : 0;
          
          const batchTime = (Date.now() - session.batchStartTime) / 60000; // minutes
          const recordsPerMinute = processed / batchTime;
          
          // Store processing rate for estimating remaining time
          progressTracker.processingRates.push(recordsPerMinute);
          
          // Calculate average processing rate from last 5 batches
          const recentRates = progressTracker.processingRates.slice(-5);
          const avgRate = recentRates.reduce((sum, rate) => sum + rate, 0) / recentRates.length;
          
          const estimatedTimeRemaining = session.totalRecords ? 
            ((session.totalRecords - session.processedRecords) / avgRate) : 0;
          
          console.log(`Progress for '${prefix}': ${session.processedRecords}/${session.totalRecords} records (${progress}%)`);
          console.log(`Processing rate: ${recordsPerMinute.toFixed(2)} records/min`);
          console.log(`Est. time remaining for this prefix: ${estimatedTimeRemaining.toFixed(2)} minutes`);
          
          // Increment offset for next batch
          session.currentOffset += records.length;
          
          // Save checkpoint
          await saveCheckpoint();
          
          // Update progress tracker periodically
          await saveProgressTracker();
          
          // Check if we've reached the end
          if (session.processedRecords >= session.totalRecords) {
            console.log(`All records processed for prefix '${prefix}'!`);
            hasMoreData = false;
          } else {
            // Add variable delay between requests based on prefix
            await sleep(prefix);
          }
        }
      } catch (error) {
        console.error(`Error in fetch loop for prefix '${prefix}':`, error.message);
        consecutiveErrors++;
        
        // Implement exponential backoff on error
        const backoffDelay = BASE_DELAY * Math.pow(2, consecutiveErrors);
        console.log(`Backing off for ${backoffDelay}ms after error...`);
        await new Promise(resolve => setTimeout(resolve, backoffDelay));
      }
    }
    
    // Mark prefix as completed
    progressTracker.completed.push(prefix);
    progressTracker.inProgress = null;
    const indexInPending = progressTracker.pending.indexOf(prefix);
    if (indexInPending !== -1) {
      progressTracker.pending.splice(indexInPending, 1);
    }
    
    // Save updated progress
    await saveProgressTracker();
    
    console.log(`Completed processing prefix '${prefix}': ${session.processedRecords} companies processed`);
    return session.processedRecords;
  } catch (error) {
    console.error(`Error processing prefix '${prefix}':`, error);
    throw error;
  }
}

/**
 * Process all alphabet prefixes sequentially
 */
async function processAllPrefixes() {
  try {
    // Initialize or load prefix list and progress tracker
    const trackerExists = await loadProgressTracker();
    if (!trackerExists) {
      // Use custom strategy based on known distribution
      await initializeCustomPrefixList();
      progressTracker.startTime = Date.now();
      await saveProgressTracker();
    }
    
    // If we have an in-progress prefix, start with that
    if (progressTracker.inProgress) {
      console.log(`Resuming in-progress prefix: '${progressTracker.inProgress}'`);
      await processAlphabetPrefix(progressTracker.inProgress);
    }
    
    // Process remaining prefixes
    while (progressTracker.pending.length > 0) {
      // Get next prefix
      const prefix = progressTracker.pending[0];
      console.log(`Starting prefix '${prefix}' (${progressTracker.pending.length} remaining prefixes)`);
      
      // Process this prefix
      await processAlphabetPrefix(prefix);
      
      // Calculate and log overall progress
      const totalPrefixes = progressTracker.completed.length + progressTracker.pending.length;
      const overallProgress = Math.round((progressTracker.completed.length / totalPrefixes) * 100);
      
      const elapsedMinutes = (Date.now() - progressTracker.startTime) / 60000;
      const prefixesPerMinute = progressTracker.completed.length / elapsedMinutes;
      const estimatedMinutesRemaining = progressTracker.pending.length / prefixesPerMinute;
      
      console.log(`Overall progress: ${progressTracker.completed.length}/${totalPrefixes} prefixes (${overallProgress}%)`);
      console.log(`Total companies processed: ${progressTracker.totalCompanies}`);
      console.log(`Est. time remaining: ${estimatedMinutesRemaining.toFixed(2)} minutes`);
    }
    
    console.log(`All prefixes processed! Total companies: ${progressTracker.totalCompanies}`);
    return progressTracker.totalCompanies;
  } catch (error) {
    console.error('Error in processAllPrefixes:', error);
    throw error;
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
 * Insert company data into the database with full schema support
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
    const insertedCompanyId = companyResult.rows[0].company

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
 * Main function to run the ETL process with alphabet-based filtering
 */
async function runETL() {
  try {
    console.log('Starting ETL process with alphabet-based filtering...');
    
    // Process all alphabet prefixes
    const totalCompanies = await processAllPrefixes();
    
    console.log(`ETL process completed successfully: ${totalCompanies} total companies processed`);
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