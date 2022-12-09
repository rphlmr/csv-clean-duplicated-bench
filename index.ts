import fs from "node:fs";
import csvParser from "csv-parser";
import languageEncoding from "detect-file-encoding-and-language";

type Product = {
  EAN: string;
};

function parseWithMap(array: Product[]) {
  console.time("parseWithMap");

  const result: Product[] = [
    ...new Map(array.map((item) => [item.EAN, item])).values(),
  ];

  console.log("parseWithMap count", result.length);
  console.timeEnd("parseWithMap");

  return result;
}

function parseWithReduce(array: Product[]) {
  console.time("parseWithReduce");

  const result: Product[] = Object.values(
    array.reduce((acc, current) => ({ ...acc, [current.EAN]: current }), {})
  );

  console.log("parseWithReduce count", result.length);
  console.timeEnd("parseWithReduce");

  return result;
}

function parseWithReduceRight(array: Product[]) {
  console.time("parseWithReduceRight");

  const result: Product[] = Object.values(
    array.reduceRight(
      (acc, current) => ({ ...acc, [current.EAN]: current }),
      {}
    )
  );

  console.log("parseWithReduceRight count", result.length);
  console.timeEnd("parseWithReduceRight");

  return result;
}

async function removeDuplicates(parser: (array: Product[]) => Product[]) {
  console.time("csv parse");

  const { encoding } = await languageEncoding(`products.csv`);
  if (encoding !== "UTF-8") throw new Error("Encoding is not UTF-8");

  const products = await new Promise<Product[]>((resolve) => {
    const products: Product[] = [];
    const errors: [unknown[]?] = [];

    fs.createReadStream(`products.csv`)
      .pipe(
        csvParser({
          separator: ";",
          mapHeaders: ({ header }) => header.trim(),
        })
      )
      .on("data", (data: Product) => {
        products.push(data);
      })
      .on("end", () => {
        return resolve(products);
      });
  });

  const productsParsed = parser(products);

  console.timeEnd("csv parse");

  return productsParsed;
}

async function main() {
  // CSV with 21044 rows
  await removeDuplicates(parseWithMap); // best 5.958ms
  await removeDuplicates(parseWithReduce); // 1:36.498 (m:ss.mmm) bad
  await removeDuplicates(parseWithReduceRight); // 1:38.053 (m:ss.mmm) worst
}

main();

/*
parseWithMap count 21044
parseWithMap: 5.958ms
csv parse: 370.896ms

parseWithReduce count 21044
parseWithReduce: 1:36.498 (m:ss.mmm)
csv parse: 1:36.866 (m:ss.mmm)

parseWithReduceRight count 21044
parseWithReduceRight: 1:38.053 (m:ss.mmm)
csv parse: 1:38.408 (m:ss.mmm)
*/
