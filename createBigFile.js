const fs = require('fs');
const path = require('path');

const targetGB = 1;
const filePath = path.join(__dirname, 'largefile.txt');
const targetSizeBytes = targetGB * 1024 * 1024 * 1024;

let numberOfBytesWritten = 0;
const writableStream = fs.createWriteStream(filePath, { flags: 'w' })

function getRandomNumber() {
    return Math.floor(Math.random() * 1000000); // Случайное число до 999999
}

function writeRandomNumbers() {
    if (numberOfBytesWritten >= targetSizeBytes) {
        writableStream.end();
        console.log(`Файл заполнен до ${targetGB} ГБ.`);
        return;
    }

    const number = getRandomNumber();
    const line = `${number}\n`;

    if (numberOfBytesWritten + Buffer.byteLength(line, 'utf8') > targetSizeBytes) {
        writableStream.write(line, () => {
            numberOfBytesWritten += Buffer.byteLength(line, 'utf8');
            writableStream.end();
            console.log(`Файл заполнен до ${targetGB} ГБ.`);
        });
    } else {
        writableStream.write(line);
        numberOfBytesWritten += Buffer.byteLength(line, 'utf8');
    }

    setImmediate(writeRandomNumbers);
}

writeRandomNumbers();