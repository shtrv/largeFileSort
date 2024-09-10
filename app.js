const fs = require('fs');
const path = require('path');
const readline = require('readline');

const sizeMB = 1;
const maxChunkSize = sizeMB*1024*1024;  // in bytes
const inputFile = 'largefile.txt';
const outputFile = 'sorted_largefile.txt'
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

async function mergeChunks(outputFile) {
    const chunkFiles = fs.readdirSync(tmpDir).map(file => path.join(tmpDir, file));
    const outputStream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

    let chunkStreams = chunkFiles.map(file => readline.createInterface({ input: fs.createReadStream(file, { encoding: 'utf8' }) }));
    let chunkLines = await Promise.all(chunkStreams.map(async stream => (await stream[Symbol.asyncIterator]().next()).value));
    
    while (chunkStreams.length > 0) {
        let minIndex = 0;
        for (let i = 1; i < chunkLines.length; i++) {
            if (chunkLines[i] < chunkLines[minIndex]) {
                minIndex = i;
            }
        }

        outputStream.write(chunkLines[minIndex] + '\n');
        const next = await chunkStreams[minIndex][Symbol.asyncIterator]().next();

        if (next.done) {
            chunkStreams.splice(minIndex, 1);
            chunkLines.splice(minIndex, 1);
        } else {
            chunkLines[minIndex] = next.value;
        }
    }

    outputStream.end();
}

(async function main() {
    console.log('Разделение и сортировка кусочков...');
    await splitAndSortFile(inputFile, maxChunkSize);

    console.log('Соединение кусочков...');
    await mergeChunks(outputFile);

    console.log('Очистка временных файлов...');
    fs.rmSync(tmpDir, { recursive: true, force: true });

    console.log('Готово.');
})();