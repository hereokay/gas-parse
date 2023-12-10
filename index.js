const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { MongoClient } = require('mongodb');
const { createObjectCsvWriter } = require('csv-writer');
const csv = require('csv-parser');



/*
시나리오 1 : Bigquery 연결 실패
시나리오 2 : coingecko API 연결 실패
시나리오 3 : MongoDB 연결 실패
*/

const mongoUrl = process.env.MONGODB_URI || 'mongodb://localhost:27017/your-db-name';
const dbName = 'your-db-name';
const collectionName = 'users';

function convertDateFormat(date) {
  const parts = date.split("-");
  if (parts.length !== 3) {
      throw new Error("Invalid date format");
  }
  return `${parts[2]}-${parts[1]}-${parts[0]}`;
}

function getCurrentUtcTime() {
  const now = new Date();
  return now.toISOString();
}

function logWithUtcTime(message) {
  const utcTime = getCurrentUtcTime();
  console.log(`[${utcTime}] ${message}`);
}


async function processDataAndSave(date){
  logWithUtcTime("processDataAndSave called")
  let rows;
  let ethPrice;

  try {
    rows = await fetchQueryData(date);
  } catch (error) {
    console.error('Error fetching query data:', error);
    throw new Error(`fetchQueryData failed for date: ${date}`); // 또는 다른 적절한 에러 처리
  }

  try {
    ethPrice = await getEthereumPriceOnDate(date);
    logWithUtcTime(`ethPrice : ${ethPrice}`);
  } catch (error) {
    console.error(`Error fetching Ethereum price for date: ${date}`, error);
    throw new Error(`getEthereumPriceOnDate failed for date: ${date}`);
  }

  try {
    await saveRowsToCSV(rows, ethPrice,date);
    logWithUtcTime('CSV file saved successfully.');
  } catch (err) {
    console.error('Error saving CSV file:', err);
    throw new Error(`saveRowsToCSV failed for date: ${date}`); // 또는 다른 적절한 에러 처리
  }

  try {
    await updateSpendGasUSDTInMongoDB(mongoUrl, dbName, collectionName, date);
  } catch (err) {
    console.error('Error:', err);
    throw new Error(`updateSpendGasUSDTInMongoDB failed for date: ${date}`);
  }
}


async function saveRowsToCSV(rows, ethPrice, date) {
  logWithUtcTime("saveRowsToCSV called")
  // 각 행에 spendGasUSDT 값을 추가
  const modifiedRows = rows.map(row => ({
    ...row,
    spendGasUSDT: row.gasCost * ethPrice
  }));

  const csvWriter = createObjectCsvWriter({
    path: `gas-${date}.csv`,
    header: [
      { id: 'gasCost', title: 'gasCost' },
      { id: '_id', title: '_id' },
      { id: 'spendGasUSDT', title: 'spendGasUSDT' }
    ]
  });

  try {
    await csvWriter.writeRecords(modifiedRows); // 수정된 rows 배열 사용
    logWithUtcTime(`Data saved to gas-${date}.csv`);
  } catch (err) {
    console.error('Error writing to CSV:', err);
    throw err;
  }
}



async function updateSpendGasUSDTInMongoDB(mongoUrl, dbName, collectionName, date) {
  logWithUtcTime("updateSpendGasUSDTInMongoDB called")
  const client = new MongoClient(mongoUrl);
  const filePath = `gas-${date}.csv`;

  try {
    await client.connect();
    logWithUtcTime("Connected successfully to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const rows = [];

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => rows.push(row))
      .on('end', async () => {
        logWithUtcTime('CSV file successfully processed');

        for (const row of rows) {
          const _id = row._id;
          const spendGasUSDT = parseFloat(row.spendGasUSDT);

          const updateResult = await collection.updateOne(
            { _id: _id },
            { $inc: { spendGasUSDT: spendGasUSDT } },
            { upsert: true }
          );
    
          logWithUtcTime(`_id ${_id}: Document updated: ${updateResult.modifiedCount}, Document inserted: ${updateResult.upsertedCount} spendGasUSDT ${spendGasUSDT}`);
        }

        await client.close();
      });
  } catch (error) {
    console.error('Error:', error);
    await client.close();
  }
}


async function getEthereumPriceOnDate(date) {
  logWithUtcTime("getEthereumPriceOnDate called")
  try {
    const url = `https://api.coingecko.com/api/v3/coins/ethereum/history?date=${convertDateFormat(date)}`;
    const response = await axios.get(url);
    const price = response.data.market_data.current_price.usd;
    return price;
  } catch (error) {
    console.error(`Error fetching Ethereum price for date: ${date}`, error);
    throw error;
  }
}




async function fetchQueryData(date) {
  logWithUtcTime("fetchQueryData called")

  const bigqueryClient = new BigQuery();
  const query = `
    SELECT
      SUM(transactions.receipt_gas_used * (transactions.gas_price / 1000000000000000000)) as gasCost,
      transactions.from_address as _id
    FROM
      \`bigquery-public-data.crypto_ethereum.transactions\` transactions
    WHERE
      DATE(transactions.block_timestamp) = '${date}'
    GROUP BY
      transactions.from_address
  `;

  const options = {
    query: query,
    location: 'US',  // 쿼리를 실행할 위치, 필요에 따라 변경 가능
  };

  const [rows] = await bigqueryClient.query(options);
  return rows;
}

// index.js
const date = process.env.DATE;

if (!date) {
    console.error("ERROR: DATE 환경 변수가 설정되지 않았습니다.");
    process.exit(1); // 비정상 종료
}


if (date !== null) {
  logWithUtcTime('call processDataAndSave');
  processDataAndSave(date);
} else {
  console.error('Error: Date parameter is required.');
  process.exit(1); // 또는 다른 적절한 오류 처리
}