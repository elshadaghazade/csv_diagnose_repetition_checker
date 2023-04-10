const csv = require('csv-parser');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { fork } = require('child_process');
const {sizeOfMemory} = require('./memory');
const inquirer = require('inquirer');

let results = [];
let resultsAfterSubProcesses = [];

const FILES_PATH = path.join(__dirname, 'files');


function checkIfInterrupted () {
    const files = fs.readdirSync(FILES_PATH).filter(file => file.toLowerCase().endsWith('.csv'));
    let ids = [];
    
    for(let file of files) {
        const data = fs.readFileSync(FILES_PATH + '/' + file, 'utf-8').split(',');
        ids = ids.concat(data.map(el => parseInt(el.trim())).filter(el => !isNaN(el)));
    }

    return ids;
}



async function processDiagnose (fromFile, similarity, idColumn, comparisonColumn) {

    const idsToRemove = checkIfInterrupted();
    let continueFromLastPoint = false;

    if (idsToRemove?.length) {
        const prompt = inquirer.createPromptModule();
        const answers = await prompt([
            {
                name: 'continueFromLastPoint',
                type: 'confirm',
                message: 'Əvvəl qaldığınız yerdən davam etmək istəyirsinizmi?:',
                default: true
            }
        ]);

        continueFromLastPoint = answers.continueFromLastPoint;
    }

    if (!continueFromLastPoint) {
        removeFiles();
    }

    fs.createReadStream(fromFile)
    .pipe(csv())
    .on('data', (data) => {
        const line = Object.entries(data);
        for(let col of line) {
            col[0] = col[0]?.trim();
            col[1] = col[1]?.trim();
        }
        results.push(Object.fromEntries(line));
    })
    .on('end', () => {
        
        if (idsToRemove?.length && continueFromLastPoint) {
            results = results.filter(result => !idsToRemove.includes(parseInt(result[idColumn]?.trim())));
        }

        const cpuCount = os.cpus().length;
        const dataPortionCount = Math.ceil(results.length / cpuCount);
        let nameInd = 1;
        let returnedSubProcesses = 0;
        for(let i = 0; i < results.length; i += dataPortionCount) {
            const portion = results.slice(i, dataPortionCount + i);
            subProcess = fork('./findSimilarity.js');
            subProcess.send({ 
                portion, 
                results: portion, 
                similarity: parseFloat(similarity),
                idColumn,
                comparisonColumn,
                continueFromLastPoint,
                file: `${FILES_PATH}/new${nameInd++}.csv`
            });
            
            subProcess.on('message', ({results}) => {
                resultsAfterSubProcesses = resultsAfterSubProcesses.concat(results);
                returnedSubProcesses++;
                if (returnedSubProcesses === cpuCount) {
                    resultsAfterSubProcesses = resultsAfterSubProcesses.filter(el => !!el);
                    subProcess = fork('./findSimilarity.js');
                    subProcess.send({ 
                        portion: resultsAfterSubProcesses, 
                        results: resultsAfterSubProcesses, 
                        similarity: parseFloat(similarity),
                        file: `${FILES_PATH}/new${nameInd++}.csv`
                    });
                    
                    subProcess.on('message', ({results}) => {
                        console.log(results.length);
                    });
                }
            });

        }
    });
}

function readFirstLine (fromFile, cb) {
    const stream = fs.createReadStream(fromFile);
    stream.setEncoding('utf-8');
    
    stream.on('data', data => {
        const splittedFirstLine = data?.split('\n')?.[0]?.trim()?.split(',');
        stream.close();
        cb(splittedFirstLine);
    });
}

function fixMissingThings () {
    if (!fs.existsSync(FILES_PATH)) {
        fs.mkdirSync(FILES_PATH);
    }
}

function removeFiles () {
    if (!fs.existsSync(FILES_PATH)) {
        return;
    }

    const files = fs.readdirSync(FILES_PATH).filter(file => file.toLowerCase().endsWith('.csv'));
    for(let file of files) {
        fs.unlinkSync(FILES_PATH + '/' + file);
    }
}


async function main () {
    fixMissingThings();
    const prompt = inquirer.createPromptModule();
    const answers = await prompt([
        {
            name: 'fromFile',
            type: 'input',
            message: 'CSV faylın adını yazın:'
        },
        {
            name: 'similarity',
            type: 'number',
            message: 'Oxşarlıq dərəcəsini 0-1 arasında kəsr ədədi ilə qeyd edin (default 0.8):',
            default: 0.8
        }
    ]);

    readFirstLine(answers.fromFile, async splittedFirstLine => {
        const prompt = inquirer.createPromptModule();
        const answers2 = await prompt([
            {
                name: "idColumn",
                type: "list",
                message: "İD sütununu seçin",
                choices: splittedFirstLine
            },
            {
                name: 'comparisonColumn',
                type: 'list',
                message: 'Müqayisə edilməli sütunun adını seçin:',
                choices: splittedFirstLine
            }
        ]);

        processDiagnose(
            answers.fromFile, 
            answers.similarity, 
            answers2.idColumn,
            answers2.comparisonColumn
        );
    });
}

main();