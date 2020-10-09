import { Table } from './parsePage';

export const fixRowsThatCrossPages = (
  table: Table,
  fileName: string,
): Table => {
  const rowsToDelete: number[] = [];

  table.rows.forEach((row, index) => {
    const keys = Object.keys(row);
    if (keys.indexOf('page') !== -1) {
      keys.forEach((key) => {
        const valueLength = row[key].length;
        const lastCharacter = row[key][valueLength - 1];
        // Find values that end in "-", which means they got cut off
        if (lastCharacter !== undefined && lastCharacter === '-') {
          // Append all values of next row
          if (row['page'] !== table.rows[index + 1]['page']) {
            keys.forEach((k) => {
              const nextRowValue = table.rows[index + 1][k];
              row[k] += nextRowValue !== undefined ? ` ${nextRowValue}` : '';
            });
            //  Mark next row for deletion
            rowsToDelete.push(index + 1);
          }
        }
      });
      // Find rows with two or less values, including "page"
      if (keys.length <= 2 && rowsToDelete.indexOf(index) === -1) {
        const previousRow = table.rows[index - 1];
        if (row['page'] !== previousRow['page']) {
          keys.forEach((k) => {
            previousRow[k] += row[k] !== undefined ? ` ${row[k]}` : '';
          });
          //  Mark next row for deletion
          if (rowsToDelete.indexOf(index) < 0) {
            rowsToDelete.push(index);
          }
        }
      }
    }

    row.file = fileName;
  });

  /* Now remove all of the rows that we decided to delete. */
  const filteredRows = table.rows.filter((row, index) => {
    const uniqueRowsToDelete = Array.from(new Set(rowsToDelete));
    return uniqueRowsToDelete.indexOf(index) === -1;
  });

  table.rows = filteredRows;
  return table;
};
