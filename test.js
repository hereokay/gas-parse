import fs from 'fs';
import path from 'path';
import saveRowsToCSV from './index'; // saveRowsToCSV 함수를 가져옵니다.

describe('saveRowsToCSV', () => {
  const rows = [{
    gasCost: 0.6443391295622358,
    _id: '0x348c38effd3b6ae4694e4f7460cc117917112654'
  }];
  const date = '2023-11-21';
  const filePath = path.join(__dirname, `gas-${date}.csv`);

  afterAll(() => {
    // 테스트 후 생성된 파일을 정리합니다.
    fs.unlinkSync(filePath);
  });

  test('should create a CSV file', async () => {
    await saveRowsToCSV(rows, date);

    // 파일이 생성되었는지 확인합니다.
    expect(fs.existsSync(filePath)).toBe(true);
  });
});
