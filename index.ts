import fs from "node:fs";
import csvParser from "csv-parser";
import languageEncoding from "detect-file-encoding-and-language";
import Zip from "adm-zip";

type Product = {
  ean: string;
  storeTrigram: string;
};

const outputFolder = "./output";
const chunksFolder = `${outputFolder}/chunks`;

function parseWithMap(array: Product[]) {
  console.time("parseWithMap");

  const result: Product[] = [
    ...new Map(
      array.map((item) => [
        `${item.ean.trim()}${item.storeTrigram.trim()}`.toLowerCase(),
        item,
      ])
    ).values(),
  ];

  console.log("parseWithMap count", result.length);
  console.log("number of products", array.length);
  console.log("number of duplicates", array.length - result.length);
  console.timeEnd("parseWithMap");

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
          mapHeaders: function ({ header }) {
            return header.trim();
          },
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

// create chunked files from array
async function createChunkedFiles(array: Product[]) {
  const chunkSize = 10_000;
  const chunks: Product[][] = [];

  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }

  if (fs.existsSync(outputFolder)) {
    await fs.promises.rm(outputFolder, { recursive: true });
  }

  await fs.promises.mkdir(chunksFolder, { recursive: true });

  const chunkPromises = chunks.map((chunk, index) => {
    fs.promises.writeFile(
      `${chunksFolder}/chunk-${index + 1}.json`,
      JSON.stringify(chunk)
    );
  });

  await Promise.all(chunkPromises);
}

async function zipFiles() {
  const zip = new Zip();
  zip.addLocalFolder(chunksFolder);
  zip.writeZip(`${outputFolder}/products.zip`);
}

async function main() {
  console.log("I'm starting parsing the file and removing duplicates 🔥");
  const payload = await removeDuplicates(parseWithMap);

  console.log(
    "That was cool, now I'm creating chunked files 🚀 in ./output/chunks"
  );
  await createChunkedFiles(payload);

  zipFiles();

  console.log("Done! 🎉");
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

// function parseWithReduce(array: Product[]) {
//   console.time("parseWithReduce");

//   const result: Product[] = Object.values(
//     array.reduce((acc, current) => ({ ...acc, [current.ean]: current }), {})
//   );

//   console.log("parseWithReduce count", result.length);
//   console.timeEnd("parseWithReduce");

//   return result;
// }

// function parseWithReduceRight(array: Product[]) {
//   console.time("parseWithReduceRight");

//   const result: Product[] = Object.values(
//     array.reduceRight(
//       (acc, current) => ({ ...acc, [current.ean]: current }),
//       {}
//     )
//   );

//   console.log("parseWithReduceRight count", result.length);
//   console.timeEnd("parseWithReduceRight");

//   return result;
// }
