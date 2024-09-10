const fs = require('fs');
const path = require('path');
const readline = require('readline');

const sizeMB = 1;
const maxChunkSize = sizeMB * 1024 * 1024;  // in bytes
const inputFile = 'largefile.txt';
const outputFile = 'sorted_largefile.txt'
const tmpDir = path.join('./', 'sorted_chunks');
if (!fs.existsSync(tmpDir)) {
    fs.mkdirSync(tmpDir);
}

async function splitAndSortFile(inputFile, maxChunkSize) {
    const fileStream = fs.createReadStream(inputFile);
    const rl = readline.createInterface({ input: fileStream });

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

async function mergeTwoFiles(file1, file2, outputFile) {
    const rl1 = readline.createInterface({
        input: fs.createReadStream(file1, { encoding: 'utf8' }),
    });
    const rl2 = readline.createInterface({
        input: fs.createReadStream(file2, { encoding: 'utf8' }),
    });
    const outputStream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

    let iterator1 = rl1[Symbol.asyncIterator]();
    let iterator2 = rl2[Symbol.asyncIterator]();

    let line1 = (await iterator1.next()).value;
    let line2 = (await iterator2.next()).value;

    while (line1 !== undefined || line2 !== undefined) {
        if (line1 === undefined) {
            outputStream.write(line2 + '\n');
            line2 = (await iterator2.next()).value;
        } else if (line2 === undefined) {
            outputStream.write(line1 + '\n');
            line1 = (await iterator1.next()).value;
        } else if (line1 < line2) {
            outputStream.write(line1 + '\n');
            line1 = (await iterator1.next()).value;
        } else {
            outputStream.write(line2 + '\n');
            line2 = (await iterator2.next()).value;
        }
    }

    rl1.close();
    rl2.close();
    outputStream.end();
}

async function recursiveMerge(chunkFiles, level = 0) {
    if (chunkFiles.length === 1) {
        return chunkFiles[0];
    }

    let mergedFiles = [];

    for (let i = 0; i < chunkFiles.length; i += 2) {
        if (i + 1 < chunkFiles.length) {
            const mergedFile = path.join(tmpDir, `merged_${level}_${i}.txt`);
            await mergeTwoFiles(chunkFiles[i], chunkFiles[i + 1], mergedFile);
            mergedFiles.push(mergedFile);
        } else {
            mergedFiles.push(chunkFiles[i]);
        }
    }

    return await recursiveMerge(mergedFiles, level + 1);
}

(async function main() {
    console.log('Разделение и сортировка кусочков...');
    await splitAndSortFile(inputFile, maxChunkSize);

    console.log('Соединение кусочков...');
    const chunkFiles = fs.readdirSync(tmpDir).map(file => path.join(tmpDir, file));
    const finalSortedFile = await recursiveMerge(chunkFiles);

    // Переименовываем окончательный файл в выходное имя
    fs.renameSync(finalSortedFile, outputFile);

    console.log('Очистка временных файлов...');
    fs.rmSync(tmpDir, { recursive: true, force: true });

    console.log('Готово.');
})();