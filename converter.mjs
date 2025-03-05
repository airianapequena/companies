// inspect-package.js
// Run this script to check how got-scraping is structured in your project

const fs = require('fs');
const path = require('path');

// Path to got-scraping package.json
const packageJsonPath = path.join(__dirname, 'node_modules', 'got-scraping', 'package.json');

try {
  // Read and parse package.json
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
  
  console.log('Package structure:');
  console.log('------------------');
  console.log('Name:', packageJson.name);
  console.log('Version:', packageJson.version);
  console.log('Main entry point:', packageJson.main);
  console.log('Module entry point:', packageJson.module);
  console.log('Exports:', JSON.stringify(packageJson.exports, null, 2));
  console.log('Type:', packageJson.type);
  
  // Check what files exist in the package
  const packageDir = path.join(__dirname, 'node_modules', 'got-scraping');
  const files = fs.readdirSync(packageDir);
  
  console.log('\nFiles in package directory:');
  console.log('--------------------------');
  files.forEach(file => {
    console.log(file);
  });
  
  // Try to find actual module file
  const potentialEntryPoints = [
    'index.js',
    'index.mjs',
    'index.cjs',
    'dist/index.js',
    'dist/index.mjs',
    'dist/index.cjs',
    'src/index.js'
  ];
  
  console.log('\nChecking potential entry points:');
  console.log('------------------------------');
  potentialEntryPoints.forEach(entryPoint => {
    const entryPointPath = path.join(packageDir, entryPoint);
    if (fs.existsSync(entryPointPath)) {
      console.log(`✅ ${entryPoint} exists`);
      
      // Peek at the file content
      const content = fs.readFileSync(entryPointPath, 'utf8');
      const lines = content.split('\n').slice(0, 5).join('\n'); // First 5 lines
      console.log('First few lines:');
      console.log(lines);
      console.log('...');
    } else {
      console.log(`❌ ${entryPoint} does not exist`);
    }
  });
  
} catch (error) {
  console.error('Error inspecting package:', error.message);
}