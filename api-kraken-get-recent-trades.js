const fetch = require('isomorphic-fetch');
const fs = require('fs');

const BASE_URL = 'https://api.kraken.com/0/public/Trades';
const NOW = new Date().getTime() * 1000000;
const TIMEOUT = 10000;

let delay = TIMEOUT;

process.on('unhandledRejection', error => {
    console.error('Ooooooooooops!', error);
});

const wait = () =>
    new Promise(resolve => {
        console.log('Waiting ', delay, ' ms');
        setTimeout(resolve, delay);
    });

const listToCsv = list => list.map(row => row.join(',')).join('\n') + '\n';

const getTradesPage = (pair, since = 0) =>
    fetch(`${BASE_URL}?pair=${pair}&since=${since}`).then(async response => {
        const {error, result} = await response.json();

        if (error && error.length) {
            if (error.includes('EAPI:Rate limit exceeded')) {
                // delay = delay + 100;
                await wait();
                return getTradesPage(pair, since);
            } else {
                throw new Error(JSON.stringify(error));
            }
        }

        return result;
    });

const getAllTrades = async (pair, since = 0) => {
    let trades = [];
    const file = fs.createWriteStream(`/Users/wzoom/Desktop/CryptoMata/${pair}-since-${since}.csv`);
    do {
        const {[pair]: currentPageTrades, last} = await getTradesPage(pair, since);

        // Probably requesting the last possible page again -> Exit.
        if (since === last) break;

        since = last;

        file.write(listToCsv(currentPageTrades));

        trades.push(...currentPageTrades);

        console.log(
            `${pair}: We have ${currentPageTrades.length} new records since ${since} (${new Date(
                since / 1000000
            ).toISOString()}). Total trades: ${trades.length}`
        );
    } while (since < NOW);

    file.end();

    return trades;
};

getAllTrades('XXBTZUSD', 1568378065734994032);
//getAllTrades('XETHZUSD', 1543211298497229230);
//getAllTrades('XXRPZUSD');
//getAllTrades('XLTCZUSD');
