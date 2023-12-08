import { BigQuery } from '@google-cloud/bigquery';
import fs from 'fs';
import path from 'path';



function processDataAndSave(date){
  rows = fetchQueryData(date);
  console.log(rows);
}

const { BigQuery } = require('@google-cloud/bigquery');

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