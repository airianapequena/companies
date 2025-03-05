// // proxies.js
export const formatProxyUrl = () => {
  const proxyHost = 'core-residential.evomi.com';
  const proxyPort = '1000';
  const proxyUser = 'arianawpil8';
  const proxyPass = 'AipD3BkYyPu4t3EUkKBY_country-GB_city-london';
  
  // Format: http://username:password@hostname:port
  return `http://${proxyUser}:${proxyPass}@${proxyHost}:${proxyPort}`;
};


