const stringSimilarity = require("string-similarity");
const fs = require('fs');

let totalDeleted = 0;

function findBestMatches (i, line1, results, file, sim=0.8, comparisonColumn) {
    for(let j = i+1; j < results.length; j++) {
        const line2 = results[j];
        if (!line2) {
            continue;
        }

        if (line1['S/S'] === line2['S/S']) {
            continue;
        }

        const similarity = stringSimilarity.compareTwoStrings(line1[comparisonColumn], line2[comparisonColumn]);
        if (similarity >= sim) {
            // line1.similarities.push(line2['S/S'])
            fs.appendFileSync(file, line2['S/S'] + ',', 'utf-8');
            delete results[j];
            ++totalDeleted;
        }
    }

    console.log(file, 'total deleted:', totalDeleted);
}

function findSimilarity (portion, results, file, similarity, comparisonColumn, continueFromLastPoint) {
    if (!continueFromLastPoint) {
        fs.writeFileSync(file, 'S/S,similarities\n', 'utf-8');
    }

    for(let i = 0; i < portion.length; i++) {
        const line1 = portion[i];
        if (!line1) {
            continue;
        }

        fs.appendFileSync(file, line1['S/S'] + ',', 'utf-8');

        if (line1.similarities === undefined) {
            line1.similarities = [];
        }

        const t1 = Date.now();
        findBestMatches(i, line1, results, file, similarity, comparisonColumn);
        const t2 = Date.now();

        console.log(file, i, (t2 - t1) / 1000);

        fs.appendFileSync(file, '\n', 'utf-8');

        // console.log(line1['S/S'], line1.similarities.length);

        // line1.similarities = line1.similarities.join(',');
    }

    process.send({results});
}

process.on('message', ({portion, results, file, similarity, comparisonColumn, continueFromLastPoint}) => {
    findSimilarity(portion, results, file, similarity, comparisonColumn, continueFromLastPoint);
})