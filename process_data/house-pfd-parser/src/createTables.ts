import path from 'path';
import { csvFormat } from 'd3-dsv';
import fs from 'fs';
import { Table } from './parsePage';

import { reformatHeaders } from './reformatHeaders';
import { fixRowsThatCrossPages } from './fixRowsThatCrossPages';

export const createTables = (
  tables: Table[],
  pdfPath: string,
  outPath: string,
  skipHeaders: Record<string, boolean>,
): Promise<unknown> => {
  const fileName = path.basename(pdfPath, '.pdf').replace('.PDF', '');
  console.log('TCL: createTables -> fileName', fileName);
  return new Promise((resolve) => {
    tables.forEach((table) => {
      const csvFile = `${
        outPath +
        table.name
          .toLowerCase()
          .replace(/[ ,']+/g, '-')
          .replace(/"/g, '')
      }.csv`;

      table = fixRowsThatCrossPages(table, fileName);

      if (table.cols.length > 1) {
        const columns = ['file'].concat(table.cols.map((col) => col.slug));
        let csvString = csvFormat(table.rows, columns);
        csvString = reformatHeaders(csvString);
        if (skipHeaders[table.name]) {
          csvString = csvString.substring(csvString.indexOf('\n') + 1);
        } else {
          skipHeaders[table.name] = true;
        }
        fs.appendFileSync(csvFile, `${csvString}\n`);
      } else {
        const csvString = `${fileName} - None disclosed`;
        fs.appendFileSync(csvFile, `${csvString}\n`);
      }
    });

    resolve(tables);
  });
};
