const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { MongoClient } = require('mongodb');
const { createObjectCsvWriter } = require('csv-writer');


/*
시나리오 1 : Bigquery 연결 실패
시나리오 2 : coingecko API 연결 실패
시나리오 3 : MongoDB 연결 실패
*/

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/your-db-name';
const dbName = 'your-db-name';
const collectionName = 'users';

function convertDateFormat(date) {
  const parts = date.split("-");
  if (parts.length !== 3) {
      throw new Error("Invalid date format");
  }
  return `${parts[2]}-${parts[1]}-${parts[0]}`;
}



async function processDataAndSave(date){
  let rows;
  let ethPrice;

  try {
    rows = await fetchQueryData(date);
  } catch (error) {
    console.error('Error fetching query data:', error);
    throw new Error(`fetchQueryData failed for date: ${date}`); // 또는 다른 적절한 에러 처리
  }

  try {
    await saveRowsToCSV(rows, date);
    console.log('CSV file saved successfully.');
  } catch (err) {
    console.error('Error saving CSV file:', err);
    throw new Error(`saveRowsToCSV failed for date: ${date}`); // 또는 다른 적절한 에러 처리
  }

  try {
    ethPrice = await getEthereumPriceOnDate(date);
  } catch (error) {
    console.error(`Error fetching Ethereum price for date: ${date}`, error);
    throw new Error(`getEthereumPriceOnDate failed for date: ${date}`);
  }

  try {
    await updateSpendGasUSDTInMongoDB(mongoUrl, dbName, collectionName, rows, ethPrice);
  } catch (err) {
    console.error('Error:', err);
    throw new Error(`updateSpendGasUSDTInMongoDB failed for date: ${date}`);
  }
}


async function saveRowsToCSV(rows, date) {
  const csvWriter = createObjectCsvWriter({
      path: `gas-${date}.csv`,
      header: Object.keys(rows[0]).map(key => ({ id: key, title: key }))
  });

  try {
      await csvWriter.writeRecords(rows);
      console.log(`Data saved to gas-${date}.csv`);
  } catch (err) {
      console.error('Error writing to CSV:', err);
      throw err;
  }
}

async function updateSpendGasUSDTInMongoDB(mongoUrl, dbName, collectionName, rows, ethPrice) {
  const client = new MongoClient(mongoUrl);

  try {
    await client.connect();
    console.log("Connected successfully to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    for (const row of rows) {
      const _id = row._id;
      const gasCost = row.gasCost;
      const spendGasUSDT = gasCost * ethPrice;

      const updateResult = await collection.updateOne(
        { _id: _id },
        { $inc: { spendGasUSDT: spendGasUSDT } },
        { upsert: true }
      );

      console.log(`_id ${_id}: Document updated: ${updateResult.modifiedCount}, Document inserted: ${updateResult.upsertedCount}`);
    }
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.close();
  }
}

async function getEthereumPriceOnDate(date) {
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


const bigqueryClient = new BigQuery();

async function fetchQueryData(date) {
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


const args = process.argv.slice(2);
const dateArg = args.find(arg => arg.startsWith('--date='));
const date = dateArg ? dateArg.split('=')[1] : null;


if (date !== null) {
  processDataAndSave(date);
} else {
  console.error('Error: Date parameter is required.');
  process.exit(1); // 또는 다른 적절한 오류 처리
}