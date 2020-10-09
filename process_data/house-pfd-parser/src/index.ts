// jshint esnext:true
// Started as a PDF.js example file, originally licensed public domain
import fs from 'fs';

/*global.DOMParser = require('../lib/domparsermock.js').DOMParserMock;*/
// import { DOMParserMock } from '../lib/domparsermock';

import pdfjsLib from 'pdfjs-dist';
import { createTables } from './createTables';
import { parsePage, Table } from './parsePage';

const skipHeaders = {};

async function processFiling(pdfPath: string, outPath: string) {
  const data = new Uint8Array(fs.readFileSync(pdfPath));
  let tables: Table[] = [];
  const doc = await pdfjsLib.getDocument(data).promise;

  const numPages = doc.numPages;
  const ignoreRest = false; // Used to ignore the rest of the document
  /* TODO: should ignoreRest be modified each time? */
  for (let i = 1; i <= numPages; i++) {
    tables = await parsePage(i, doc, ignoreRest, tables, outPath, pdfPath);
  }

  return createTables(tables, pdfPath, outPath, skipHeaders);
}

const run = async (filePath: string, outPath: string) => {
  /* Create folders that don't exist. */
  for (const path of [filePath, outPath]) {
    if (!fs.existsSync(path)) {
      /* Hack because TS doesn't have the right type for this function. */
      const options: any = { recursive: true };
      fs.mkdirSync(path, options);
    }
  }

  const noContentCSV = `${outPath}no-content.csv`;
  fs.appendFileSync(noContentCSV, `File\n`);

  const files = fs
    .readdirSync(filePath)
    .filter((file) => file.toLowerCase().includes('.pdf'));

  for (let pos = 0; pos < files.length; pos++) {
    await processFiling(filePath + files[pos], outPath);
  }
  console.log('done');
};

(() => {
  /*process.argv;*/
  run(
    process.argv[2] ||
      `../data/raw_disclosures/` /* `${__dirname}/data/input/` */,
    process.argv[3] || `../data/parsed_disclosures/`,
  ).then(() => {
    console.log('done');
  }, console.error);
})();
