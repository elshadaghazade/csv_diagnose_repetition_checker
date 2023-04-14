const stringSimilarity = require("string-similarity");
const fs = require('fs');
const path = require('path');

let totalDeleted = 0;

function findBestMatches (i, line1, results, file, sim=0.8, idColumn, comparisonColumn) {
    for(let j = i+1; j < results.length; j++) {
        const line2 = results[j];
        if (!line2) {
            continue;
        }

        if (line1[idColumn] === line2[idColumn]) {
            continue;
        }

        const similarity = stringSimilarity.compareTwoStrings(line1[comparisonColumn], line2[comparisonColumn]);
        if (similarity >= sim) {
            // line1.similarities.push(line2[idColumn])
            fs.appendFileSync(file, line1[idColumn] + ',' + line2[idColumn] + ',' + line2[comparisonColumn].replace(/,+/gi, ' ').replace(/[\n\r]+/gi, '. ') + '\n', 'utf-8');
            delete results[j];
            ++totalDeleted;
        }
    }

    // console.log(file, 'total deleted:', totalDeleted);
}

function findSimilarity (
    portion, 
    results, 
    file, 
    similarity, 
    idColumn, 
    comparisonColumn, 
    continueFromLastPoint
) {
    if (!continueFromLastPoint) {
        fs.writeFileSync(file, `müqayisə edilən id,oxşarı tapılan id,${comparisonColumn}\n`, 'utf-8');
    }

    for(let i = 0; i < portion.length; i++) {
        const line1 = portion[i];
        if (!line1) {
            continue;
        }

        // fs.appendFileSync(file, line1[idColumn] + ',', 'utf-8');

        if (line1.similarities === undefined) {
            line1.similarities = [];
        }

        const t1 = Date.now();
        findBestMatches(i, line1, results, file, similarity, idColumn, comparisonColumn);
        const t2 = Date.now();

        console.log(path.basename(file), (t2 - t1) / 1000, 'sec.');

        fs.appendFileSync(file, '\n', 'utf-8');

        // console.log(line1[idColumn], line1.similarities.length);

        // line1.similarities = line1.similarities.join(',');
    }

    process.send({results});
}

process.on('message', ({
    portion, 
    results, 
    file, 
    similarity, 
    idColumn,
    comparisonColumn, 
    continueFromLastPoint
}) => {
    findSimilarity(
        portion, 
        results, 
        file, 
        similarity, 
        idColumn,
        comparisonColumn, 
        continueFromLastPoint
    );
})