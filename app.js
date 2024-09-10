const fs = require('fs');
const path = require('path');
const readline = require('readline');

const sizeMB = 1;
const maxChunkSize = sizeMB*1024*1024;  // in bytes
const inputFile = 'largefile.txt';
const outputFile = 'sorted_largefile.txt'
const outputDir = './result';
const tmpDir = path.join('./', 'sorted_chunks');
if (!fs.existsSync(tmpDir)) {
    fs.mkdirSync(tmpDir);
}

async function splitAndSortFile(inputFile, maxChunkSize) {
    const fileStream = fs.createReadStream(inputFile);
    const rl = readline.createInterface({input: fileStream});

    let chunk = [];
    let chunkIndex = 0;
    let chunkSize = 0;

    for await (const line of rl) {
        chunk.push(line);
        chunkSize += Buffer.byteLength(line, 'utf8');

        if (chunkSize >= maxChunkSize) {
            await sortAndSaveChunk(chunk, chunkIndex);
            chunk = [];
            chunkSize = 0;
            chunkIndex++;
        }
    }

    if (chunk.length > 0) {
        await sortAndSaveChunk(chunk, chunkIndex);
    }

    fileStream.close();
}

async function sortAndSaveChunk(chunk, chunkIndex) {
    chunk.sort();
    const sortedChunkFileName = path.join(tmpDir, `${chunkIndex}.txt`);
    fs.writeFileSync(sortedChunkFileName, chunk.join('\n'), 'utf-8')
}



splitAndSortFile(inputFile, maxChunkSize);