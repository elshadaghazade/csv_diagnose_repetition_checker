function sizeOfMemory () {
    if (!['true', '1'].includes(process.env.DEBUG?.toLowerCase())) {
        return;
    }

    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    console.log(`The script uses approximately ${Math.round(used * 100) / 100} MB`);
}

module.exports = {
    sizeOfMemory
};