// build-index.js
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { createGzip } = require('zlib');

const CSV_FILE = 'collection.csv';
const OUTPUT_DIR = 'dist';

// Ensure output directory exists
if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR);
}

console.log('Starting to build search index...');

// Create indexes
const indexes = {
    // Full data stored in chunks
    chunks: [],
    
    // Inverted indexes for quick searching
    family: new Map(),
    genus: new Map(),
    species: new Map(),
    subspecies: new Map(),
    location: new Map(),
    drawerNumber: new Map(),
    preparation: new Map(),
    
    // Full-text search index (trigram based)
    fullText: new Map(),
    
    // Metadata
    totalRows: 0,
    headers: []
};

// Process CSV line by line (memory efficient)
async function processCSV() {
    const fileStream = fs.createReadStream(CSV_FILE);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let isFirstLine = true;
    let headers = [];
    let currentChunk = [];
    const CHUNK_SIZE = 1000; // Store 1000 rows per chunk
    let rowIndex = 0;

    for await (const line of rl) {
        if (isFirstLine) {
            // Parse headers
            headers = line.split(',').map(h => h.trim());
            indexes.headers = headers;
            isFirstLine = false;
            continue;
        }

        // Parse CSV line (simple parsing - assumes no quoted commas)
        const values = line.split(',').map(v => v.trim());
        const row = {};
        headers.forEach((header, i) => {
            row[header] = values[i] || '';
        });

        // Add to current chunk
        currentChunk.push(row);
        
        // Index the row
        indexRow(row, rowIndex);
        
        rowIndex++;

        // Save chunk when full
        if (currentChunk.length >= CHUNK_SIZE) {
            saveChunk(currentChunk);
            currentChunk = [];
        }

        // Progress indicator
        if (rowIndex % 10000 === 0) {
            console.log(`Processed ${rowIndex} rows...`);
        }
    }

    // Save final chunk
    if (currentChunk.length > 0) {
        saveChunk(currentChunk);
    }

    indexes.totalRows = rowIndex;
    console.log(`Total rows processed: ${rowIndex}`);

    // Save indexes
    await saveIndexes();
}

function indexRow(row, rowIndex) {
    // Index each field
    Object.entries(row).forEach(([field, value]) => {
        if (!value) return;
        
        value = String(value).toLowerCase();
        
        // Add to field-specific index
        if (indexes[field]) {
            if (!indexes[field].has(value)) {
                indexes[field].set(value, []);
            }
            indexes[field].get(value).push(rowIndex);
        }
        
        // Add to full-text index (using trigrams)
        const trigrams = generateTrigrams(value);
        trigrams.forEach(trigram => {
            if (!indexes.fullText.has(trigram)) {
                indexes.fullText.set(trigram, new Set());
            }
            indexes.fullText.get(trigram).add(rowIndex);
        });
    });
}

function generateTrigrams(text) {
    const trigrams = new Set();
    const normalized = `__${text}__`; // Pad with underscores for prefix/suffix matching
    for (let i = 0; i < normalized.length - 2; i++) {
        trigrams.add(normalized.substr(i, 3));
    }
    return trigrams;
}

function saveChunk(chunk) {
    const chunkId = indexes.chunks.length;
    const chunkFile = path.join(OUTPUT_DIR, `chunk_${chunkId}.json.gz`);
    const compressed = createGzip();
    const writeStream = fs.createWriteStream(chunkFile);
    
    compressed.pipe(writeStream);
    compressed.write(JSON.stringify(chunk));
    compressed.end();
    
    indexes.chunks.push({
        id: chunkId,
        file: `chunk_${chunkId}.json.gz`,
        count: chunk.length
    });
}

async function saveIndexes() {
    console.log('Saving indexes...');
    
    // Convert Maps to objects for JSON serialization
    const serializedIndexes = {
        chunks: indexes.chunks,
        totalRows: indexes.totalRows,
        headers: indexes.headers,
        fieldIndexes: {},
        fullTextIndex: {}
    };
    
    // Convert field indexes
    ['family', 'genus', 'species', 'subspecies', 'location', 'drawerNumber', 'preparation'].forEach(field => {
        if (indexes[field]) {
            serializedIndexes.fieldIndexes[field] = Array.from(indexes[field].entries());
        }
    });
    
    // Convert full-text index (limit size by using array of entries)
    const fullTextArray = Array.from(indexes.fullText.entries())
        .map(([trigram, rowSet]) => [trigram, Array.from(rowSet)]);
    serializedIndexes.fullTextIndex = fullTextArray;
    
    // Save main index
    fs.writeFileSync(
        path.join(OUTPUT_DIR, 'index.json'),
        JSON.stringify(serializedIndexes)
    );
    
    // Create a smaller version for quick loading
    const quickIndex = {
        totalRows: indexes.totalRows,
        chunks: indexes.chunks.length,
        headers: indexes.headers,
        fieldCounts: {}
    };
    
    ['family', 'genus', 'species', 'location'].forEach(field => {
        if (indexes[field]) {
            quickIndex.fieldCounts[field] = indexes[field].size;
        }
    });
    
    fs.writeFileSync(
        path.join(OUTPUT_DIR, 'quick-index.json'),
        JSON.stringify(quickIndex)
    );
    
    console.log('Indexes saved successfully');
}

// Run the build
processCSV().catch(console.error);