
const readline = require('readline').createInterface({
  input: process.stdin,
  output: process.stdout
});


(async function () {
    for await(const line of readline){
        console.log(line);
    }
})();
