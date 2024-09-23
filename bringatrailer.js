// import pLimit from "p-limit";
import puppeteer from "puppeteer";
import fs, { stat } from "fs";
import mongoose from "mongoose";
// import cron from "node-cron";

// mongodb
const mongoUri =
  "mongodb+srv://hammershift1:knhyxrCw0GwEmGQc@cluster0.kpemmst.mongodb.net/hammershift";

// connect to mongodb
try {
  mongoose.connect(mongoUri);
  console.log("MongoDB connected");
} catch (error) {
  console.error("MongoDB connection error:", error);
}

// define model and schema
const auctionSchema = new mongoose.Schema(
  {
    attributes: [
      {
        key: String,
        value: mongoose.Schema.Types.Mixed,
      },
    ],
    auction_id: { type: String, unique: true },
    website: String,
    image: String,
    page_url: String,
    isActive: { type: Boolean, default: true },
    views: { type: Number, default: 0 },
    watchers: { type: Number, default: 0 },
    comments: { type: Number, default: 0 },
    description: [String],
    images_list: [Object],
    listing_details: [String],
    sort: {
      price: Number,
      bids: Number,
      deadline: Date,
    },
    statusAndPriceChecked: { type: Boolean, default: false },
    pot: { type: Number, default: 0 }, // Added pot field
    tournamentID: mongoose.Schema.Types.ObjectId, // Added tournamentID field
  },
  { timestamps: true }
);

const Auction = mongoose.model("Auction", auctionSchema);

const wagerSchema = new mongoose.Schema({
  auctionID: mongoose.Schema.Types.ObjectId,
  amount: Number,
  userID: mongoose.Schema.Types.ObjectId,
  createdAt: { type: Date, default: Date.now },
});

const Wager = mongoose.model("Wager", wagerSchema);

// const cleanUpAuctions = async () => {
//   try {
//     // Fetch all auctions with status 2 (successful) and 3 (unsuccessful)
//     const auctions = await Auction.find({
//       attributes: {
//         $elemMatch: {
//           key: 'status',
//           value: { $in: [2, 3] },
//         },
//       },
//     });

//     let auctionsDeletedCount = 0;

//     for (const auction of auctions) {
//       const statusAttr = auction.attributes.find((attr) => attr.key === 'status');
//       const status = statusAttr ? statusAttr.value : null;
//       const pot = auction.pot || 0;

//       const wagerCountAggregation = await Wager.aggregate([{ $match: { auctionID: auction._id } }, { $count: 'totalWagers' }]).exec(); // Use exec() instead of toArray()

//       const wagerCount = wagerCountAggregation.length > 0 ? wagerCountAggregation[0].totalWagers : 0;

//       let shouldDelete = false;

//       // Do not delete if there are any wagers or if the pot is greater than 0
//       if (wagerCount === 0 && pot === 0) {
//         if (status === 2) {
//           shouldDelete = true; // Delete completed auctions if there's no pot and no wagers
//         } else if (status === 3) {
//           shouldDelete = true; // Delete unsuccessful auctions if there are no wagers
//         }
//       }

//       if (shouldDelete) {
//         await Auction.deleteOne({ _id: auction._id });
//         auctionsDeletedCount++;
//         console.log(`Deleted auction with ID: ${auction._id}, status: ${status}, pot: ${pot}, wagerCount: ${wagerCount}`);
//       } else {
//         console.log(`Retained auction with ID: ${auction._id}, status: ${status}, pot: ${pot}, wagerCount: ${wagerCount}, tournamentID: ${!!auction.tournamentID}`);
//       }
//     }

//     console.log(`${auctionsDeletedCount} auctions deleted (including Completed and Unsuccessful auctions with no wagers and no pot)`);
//     return { deletedCount: auctionsDeletedCount };
//   } catch (error) {
//     console.error('Error in deleting auctions: ', error);
//     throw error;
//   }
// };

const currentAuctionData = [];
const auctionURLList = [];

const website = "https://bringatrailer.com/auctions/";
const batchSize = 20;

// for the dynamic scrolling of the webpage
const scrapeInfiniteScrollItems = async (page) => {
  while (true) {
    const previousHeight = await page.evaluate("document.body.scrollHeight");
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)");
    await page.waitForTimeout(1000);
    const newHeight = await page.evaluate("document.body.scrollHeight");

    // break and exit the loop if the page height did not change
    if (newHeight === previousHeight) {
      break;
    }
  }
};

const getData = async (url, browser) => {
  const page = await browser.newPage();
  await page.goto(url);
  await scrapeInfiniteScrollItems(page);

  const currentAuction = await page.$$eval(
    "a.listing-card.bg-white-transparent",
    (auctions) => {
      return auctions.map((auction) => {
        const url = auction.href;
        return { url };
      });
    }
  );

  auctionURLList.push(...currentAuction);

  // process the data in batches
  for (let i = 0; i < auctionURLList.length; i += batchSize) {
    const batch = auctionURLList.slice(i, i + batchSize);
    console.log(
      `Processing auctions ${i + 1} to ${i + batch.length} out of ${
        auctionURLList.length
      }`
    );

    // tally counters
    let successfulCount = 0;
    let unsuccessfulCount = 0;

    // iterate through each auction URL and extract data
    for (const auction of batch) {
      const isSuccess = await getDataFromPage(auction.url, browser);
      if (isSuccess) {
        successfulCount++;
      } else {
        unsuccessfulCount++;
      }
    }

    console.log(
      `Batch ${
        i + 1
      } processed: ${successfulCount} successful, ${unsuccessfulCount} unsuccessful`
    );
  }

  await page.close();
  // outputData();
};

const getDataFromPage = async (url, browser, retryCount = 0) => {
  const page = await browser.newPage();
  const overallTimeout = 10000;
  const gotoTimeout = 30000;
  const maxRetries = 1;

  const loadData = async () => {
    try {
      await page.goto(url, { waitUntil: "networkidle0", timeout: gotoTimeout });

      // TEST IMPLEMENTATION: Detect auction status
      const currentStatus = await page.evaluate(() => {
        const AuctionStatusEnum = {
          Live: 1,
          Completed: 2,
        };

        const availableInfo = document.querySelector(".listing-available-info");
        if (!availableInfo) return null;

        if (availableInfo.innerHTML.includes("Sold for")) {
          return AuctionStatusEnum.Completed;
        }
        return AuctionStatusEnum.Live;
      });

      const title = await page.$$eval(
        "h1.post-title.listing-post-title",
        (titleElements) => {
          if (titleElements.length === 0) {
            throw new Error("Title element not found");
          }
          return titleElements[0].textContent.trim();
        }
      );

      const titleArray = title.split(" ");

      let year;
      let make;
      let model;

      const REGEX = /[a-zA-Z]/;
      if (REGEX.test(titleArray[0])) {
        if (REGEX.test(titleArray[1])) {
          if (REGEX.test(titleArray[2])) {
            year = titleArray[3];
            make = titleArray[4];
            model = titleArray.slice(5).join(" ");
          } else {
            year = titleArray[2];
            make = titleArray[3];
            model = titleArray.slice(4).join(" ");
          }
        } else {
          year = titleArray[1];
          make = titleArray[2];
          model = titleArray.slice(3).join(" ");
        }
      } else {
        year = titleArray[0];
        make = titleArray[1];
        model = titleArray.slice(2).join(" ");
      }

      // price
      const price = await page.$eval("strong.info-value", (priceElement) => {
        return Number(priceElement.textContent.replace(/[$,]/g, ""));
      });

      // bids
      const bids = await page.$eval(
        ".listing-stats-value.number-bids-value",
        (element) => {
          return parseInt(element.textContent);
        }
      );

      // extract auction deadline
      const deadlineTimestamp = await page.$eval(
        ".listing-available-countdown",
        (element) => element.getAttribute("data-until")
      );
      const deadline = new Date(deadlineTimestamp * 1000);

      // get the car category
      const categoryArray = await page.$$eval("div.group-item-wrap", (wraps) =>
        wraps.reduce((acc, wrap) => {
          const title = wrap.querySelector("button.group-title");
          if (title && title.textContent.includes("Category")) {
            acc.push(title.textContent.split("Category")[1].trim());
          }
          return acc;
        }, [])
      );

      let category = categoryArray.join(", ");
      if (!category || category.trim() === "") {
        category = "Others";
      }

      // get the car era
      const eraArray = await page.$$eval("div.group-item-wrap", (wraps) =>
        wraps.reduce((acc, wrap) => {
          const title = wrap.querySelector("button.group-title");
          if (title && title.textContent.includes("Era")) {
            acc.push(title.textContent.split("Era")[1].trim());
          }
          return acc;
        }, [])
      );

      const era = eraArray.join(", ");

      // image
      const imgSelector =
        "div.listing-intro-image.column-limited-width-full-mobile > img";
      // this is to wait for the image to load using waitForFunction
      await page.waitForFunction(
        (sel) => {
          const image = document.querySelector(sel);
          return image && image.complete && image.naturalHeight !== 0;
        },
        {},
        imgSelector
      );
      // get the image URL
      const imgUrl = await page.$eval(imgSelector, (img) => img.src);

      // car specifications

      // auctionId
      const auction_id = (
        await page.$eval("body > main > div > div.listing-intro", (intro) =>
          intro.getAttribute("data-listing-intro-id")
        )
      ).toString();

      // lot_num
      const lot_num = await page.$eval(
        "body > main > div > div:nth-child(3) > div.column.column-right.column-right-force > div.essentials",
        (element) => {
          const lotElement = Array.from(
            element.querySelectorAll("div.item")
          ).find((item) => item.textContent.includes("Lot #"));
          const match = lotElement
            ? lotElement.textContent.trim().match(/Lot #(\d+)/)
            : null;
          return match ? match[1] : "";
        }
      );

      const chassis = await page.$eval(
        "body > main > div > div:nth-child(3) > div.column.column-right.column-right-force > div.essentials > div:nth-child(5) > ul > li:nth-child(1) > a",
        (element) => element?.textContent || ""
      );

      const seller = await page.$eval(
        "body > main > div > div:nth-child(3) > div.column.column-right.column-right-force > div.essentials > div.item.item-seller > strong + a",
        (element) => element.textContent
      );

      // location
      const location = await page.$eval(
        'div.essentials > a[href^="https://www.google.com/maps/place/"]',
        (element) => element.textContent
      );

      // state
      const extractState = (location) => {
        const array = location.split(", ");
        const stateWithZipCode = array[array.length - 1];
        const state = stateWithZipCode.replace(/\d+/g, "").trim();
        return state;
      };

      const state = extractState(location);

      // description
      const descriptionText = await page.$$(
        "body > main > div > div:nth-child(3) > div.column.column-left > div > div.post-excerpt > p"
      );
      const description = [];
      const images_list = [];
      let placing = 0;

      for (const element of descriptionText) {
        const excerpt = await page.evaluate(
          (el) => el.textContent.trim(),
          element
        );

        if (excerpt !== "" && excerpt !== undefined) {
          description.push(excerpt);
        } else {
          // check if the element contains an image
          const imgElement = await element.$("img");
          if (imgElement) {
            const imgUrl = await page.evaluate(
              (img) => img.getAttribute("src"),
              imgElement
            );
            if (imgUrl !== "" && imgUrl !== undefined) {
              placing += 1;
              const imgUrlClean = imgUrl.split("?")[0];
              images_list.push({ placing, src: imgUrlClean });
            }
          }
        }
      }

      // listing type
      const dealer = await page.$eval(
        "body > main > div > div:nth-child(3) > div.column.column-right.column-right-force > div.essentials > div.item.additional",
        (element) => element.textContent.trim()
      );

      let listing_type;
      if (dealer) {
        listing_type = "Private Property";
      }

      const list = await page.$$(
        "body > main > div > div:nth-child(3) > div.column.column-right.column-right-force > div.essentials > div:nth-child(5) > ul > li"
      );
      const listing_details = [];

      for (const element of list) {
        const detail = await page.evaluate(
          (el) => el.textContent.trim(),
          element
        );
        listing_details.push(detail);
      }

      // TEST IMPLEMENTATION
      // views
      const views = await page.$eval('span[data-stats-item="views"]', (el) =>
        parseInt(el.innerText.replace(/[\D]/g, ""))
      );

      // watchers
      const watchers = await page.$eval(
        'span[data-stats-item="watchers"]',
        (el) => parseInt(el.innerText.replace(/[\D]/g, ""))
      );

      // comments
      const comments = await page.$eval("h2.comments-title", (el) =>
        parseInt(el.innerText.match(/\d+/)[0], 10)
      );

      let attributes = [];

      attributes.push({ key: "price", value: price });
      attributes.push({ key: "year", value: year });
      attributes.push({ key: "make", value: make });
      attributes.push({ key: "model", value: model });
      attributes.push({ key: "category", value: category });
      attributes.push({ key: "era", value: era });
      attributes.push({ key: "chassis", value: chassis });
      attributes.push({ key: "seller", value: seller });
      attributes.push({ key: "location", value: location });
      attributes.push({ key: "state", value: state });
      attributes.push({ key: "lot_num", value: lot_num });
      attributes.push({ key: "listing_type", value: listing_type });
      attributes.push({ key: "deadline", value: deadline });
      attributes.push({ key: "bids", value: bids });
      attributes.push({ key: "status", value: currentStatus });

      const filteredDescription = description.filter(
        (item) => item.trim() !== ""
      );
      const filteredImagesList = images_list.filter(
        (item) => item.src && item.src.trim() !== ""
      );
      const filteredListingDetails = listing_details.filter(
        (item) => item.trim() !== ""
      );

      const extractedData = {
        auction_id,
        website: "Bring A Trailer",
        image: imgUrl,
        description: filteredDescription,
        images_list: filteredImagesList,
        listing_details: filteredListingDetails,
        page_url: url,
        isActive: true,
        views,
        watchers,
        comments,
        attributes,
        sort: {
          price: price,
          bids: bids,
          deadline: deadline,
        },
      };

      const requiredAttributeKeys = [
        "price",
        "year",
        "make",
        "model",
        "category",
        "era",
        "chassis",
        "seller",
        "location",
        "state",
        "lot_num",
        "listing_type",
        "deadline",
        "bids",
        "status",
      ];

      const hasAllRequiredAttributes = requiredAttributeKeys.every((key) =>
        attributes.some((attr) => attr.key === key)
      );

      const hasOtherRequiredFields =
        extractedData.auction_id &&
        extractedData.website &&
        extractedData.description.length > 0 &&
        extractedData.images_list.length > 0 &&
        extractedData.listing_details.length > 0 &&
        extractedData.page_url &&
        extractedData.views != null &&
        extractedData.watchers != null;

      if (hasAllRequiredAttributes && hasOtherRequiredFields) {
        currentAuctionData.push(extractedData);
        console.log(`✔ ${url} is extracted successfully.`);
        return true;
      } else {
        console.error(`✖ Missing required fields for auction at URL: ${url}`);
        return false;
      }
    } catch (error) {
      if (error.message.includes("failed to find element matching selector")) {
        console.error(
          `✖ Skipping auction at URL: ${url} due to element not found`
        );
        return false;
      }
      console.error(`✖ Error processing auction at URL: ${url}, ${error}`);
      throw error;
    } finally {
      await page.close();
    }
  };

  try {
    return await Promise.race([
      loadData(),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Timeout exceeded")), overallTimeout)
      ),
    ]);
  } catch (error) {
    if (error.message === "Timeout exceeded" && retryCount < maxRetries) {
      console.log(`Retrying... Attempt ${retryCount + 1} for URL: ${url}`);
      return getDataFromPage(url, browser, retryCount + 1);
    } else {
      console.log(`Max retries reached for ${url}.`);
      return false;
    }
  }
};

const outputData = async () => {
  const jsonContent = JSON.stringify(currentAuctionData, null, 2);
  fs.writeFile(
    "bringatrailer-current-data.json",
    jsonContent,
    "utf-8",
    (error) => {
      if (error) {
        console.log("Error writing JSON File:", error);
      } else {
        console.log("JSON File written successfully");
      }
    }
  );

  // Counters for updates and inserts
  let updateCount = 0;
  let insertCount = 0;

  // Save to MongoDB
  for (const item of currentAuctionData) {
    try {
      const existingAuction = await Auction.findOne({
        auction_id: item.auction_id,
      });

      if (existingAuction) {
        // Update both attributes and sorting fields
        const updatedAttributes = existingAuction.attributes.map((attr) => {
          if (attr.key === "price") attr.value = item.sort.price;
          if (attr.key === "bids") attr.value = item.sort.bids;
          if (attr.key === "deadline") attr.value = item.sort.deadline;
          // Add more fields if necessary
          return attr;
        });

        await Auction.updateOne(
          { auction_id: item.auction_id },
          {
            $set: {
              attributes: updatedAttributes,
              sort: item.sort,
              views: item.views,
              watchers: item.watchers,
              comments: item.comments,
            },
          }
        );

        console.log(`✔ Updated auction with ID ${item.auction_id}`);
        updateCount++;
      } else {
        await Auction.create(item);
        console.log(`✔ Inserted new auction with ID ${item.auction_id}`);
        insertCount++;
      }
    } catch (error) {
      console.error(
        `✖ Error processing auction with ID ${item.auction_id}: ${error}`
      );
    }
  }

  // Log the total number of updates and inserts
  console.log(`✔ Total auctions updated: ${updateCount}`);
  console.log(`✔ Total auctions inserted: ${insertCount}`);
};

const getFinalPriceFromPage = async (url, page, auctionId, retryCount = 0) => {
  const maxRetries = 3;

  try {
    // Fetch the current auction from the database
    const auction = await Auction.findOne({ auction_id: auctionId });
    const currentPriceAttr = auction
      ? auction.attributes.find((attr) => attr.key === "price")
      : null;
    const currentPrice = currentPriceAttr ? currentPriceAttr.value : null;

    // Log the current price from the database
    console.log(`Current price for auction ID ${auctionId}: ${currentPrice}`);

    await page.goto(url, { waitUntil: "networkidle0", timeout: 90000 });

    const priceSelector =
      ".listing-available-info .info-value strong, strong.info-value";
    const priceIsPresent = await page.$(priceSelector);

    if (!priceIsPresent) {
      console.log(`Price element not found for auction at URL: ${url}`);
      return null;
    }

    const finalPriceText = await page.$eval(
      priceSelector,
      (el) => el.textContent
    );
    const finalPrice = parseFloat(finalPriceText.replace(/[$,]/g, ""));
    console.log(
      `✔ Final price extracted for auction ID ${auctionId}: ${finalPrice}`
    );

    // Log the comparison of current and final prices
    console.log(`Current price: ${currentPrice}, Final price: ${finalPrice}`);

    return finalPrice;
  } catch (error) {
    console.error(
      `✖ Error extracting final price from page for auction ID ${auctionId}: ${url}`
    );

    if (retryCount < maxRetries) {
      console.log(
        `Retrying... Attempt ${retryCount + 1} for auction ID ${auctionId}`
      );
      return getFinalPriceFromPage(url, page, auctionId, retryCount + 1);
    } else {
      console.log(`Max retries reached for ${url}.`);
      return null;
    }
  }
};

const checkAndUpdateAuctionStatus = async (browser) => {
  const auctions = await Auction.find({
    "attributes.key": "status",
    statusAndPriceChecked: { $ne: true },
  });

  console.log(`Fetched ${auctions.length} auctions for status and price check`);

  const batchSize = 20;
  console.log(`Starting status check for ${auctions.length} auctions`);

  const page = await browser.newPage();

  for (let i = 0; i < auctions.length; i += batchSize) {
    const batch = auctions.slice(i, i + batchSize);
    console.log(
      `Processing auctions ${i + 1} to ${i + batch.length} out of ${
        auctions.length
      }`
    );

    // Tally counters
    let successfulCount = 0;
    let unsuccessfulCount = 0;

    for (const auction of batch) {
      try {
        console.log(`Processing auction ID: ${auction.auction_id}`);

        if (!auction.page_url || typeof auction.page_url !== "string") {
          console.error(`✖ Invalid URL for auctionID: ${auction.auction_id}`);
          unsuccessfulCount++;
          continue;
        }

        await page.goto(auction.page_url, {
          waitUntil: "networkidle0",
          timeout: 90000,
        });

        const currentStatus = await page.evaluate(() => {
          const availableInfo = document.querySelector(
            ".listing-available-info"
          );
          if (!availableInfo) return 1; // Assume live if no info is available
          if (availableInfo.innerHTML.includes("Sold for")) return 2; // Completed
          if (
            availableInfo.innerHTML.includes("Bid to") ||
            availableInfo.innerHTML.includes("Withdrawn on")
          )
            return 3; // Unsuccessful
          return 1; // Assume live if none of the above match
        });

        console.log(
          `Current status for auctionID: ${auction.auction_id} is ${currentStatus}`
        );

        // Update auction status if different
        if (
          auction.attributes.some(
            (attr) => attr.key === "status" && attr.value !== currentStatus
          )
        ) {
          await Auction.updateOne(
            { _id: auction._id, "attributes.key": "status" },
            { $set: { "attributes.$.value": currentStatus } }
          );
          console.log(`✔ Updated status for auctionID: ${auction.auction_id}`);
        }

        // Only update final price if the auction is completed or unsuccessful
        if (currentStatus === 2 || currentStatus === 3) {
          const finalPrice = await getFinalPriceFromPage(
            auction.page_url,
            page,
            auction.auction_id
          );
          if (finalPrice !== null) {
            await Auction.updateOne(
              { _id: auction._id, "attributes.key": "price" },
              {
                $set: {
                  "attributes.$.value": finalPrice,
                  "sort.price": finalPrice,
                },
              }
            );
            console.log(
              `✔ Updated final price for auctionID: ${auction.auction_id}`
            );
          }

          // Mark auction as checked if it is completed or unsuccessful
          await Auction.updateOne(
            { _id: auction._id },
            { $set: { statusAndPriceChecked: true } }
          );
          console.log(
            `✔ Marked auction as checked for auctionID: ${auction.auction_id}`
          );
        }

        console.log(
          `✔ Successfully processed auction ID: ${auction.auction_id}`
        );
        successfulCount++;
      } catch (error) {
        console.error(
          `✖ Failed to load or process auction ID: ${auction.auction_id}`
        );
        unsuccessfulCount++;
      }
    }

    console.log(
      `Batch ${
        i + 1
      } processed: ${successfulCount} successful, ${unsuccessfulCount} unsuccessful`
    );
  }

  await page.close();
  console.log(`Completed status check for ${auctions.length} auctions`);
};

async function runScraper() {
  const browser = await puppeteer.launch({ headless: "new" });

  try {
    console.log("Starting the scraping job...");
    const page = await browser.newPage();
    await scrapeInfiniteScrollItems(page);
    await getData(website, browser);
    console.log("Scraping job finished");
  } catch (error) {
    console.error(`✖ An error ocurred: ${error.message}`);
  } finally {
    await outputData();
    await browser.close();
    console.log("Browser closed");
  }
}

const MAX_RETRIES = 5;
const RETRY_DELAY_MS = 6000;

async function restartScraper(retryCount = 0) {
  try {
    await runScraper();
  } catch (error) {
    console.error(`✖ An error ocurred: ${error.message}`);

    if (retryCount < MAX_RETRIES) {
      console.error(`Restarting the scraper. Attempt ${retryCount + 1}`);
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
      await restartScraper(retryCount + 1);
    } else {
      console.error(`Max retries reached. Exiting the scraper.`);
    }
  }
}

// cron job for scraper
// cron.schedule('25 8 * * *', async () => {
//   console.log('Cron job started');
// await restartScraper();
// });

// cron job for checkAndUpdateAuctionStatus
// cron.schedule("4 11 * * *", async () => {
//   console.log("Starting status update job...");
//   const browser = await puppeteer.launch({ headless: "new" });

//   try {
//     await checkAndUpdateAuctionStatus(browser);
//     console.log("Status update job finished");
//   } catch (error) {
//     console.error(`✖ An error ocurred: ${error.message}`);
//   } finally {
//     await browser.close();
//     console.log("Browser closed");
//   }
// });

// Cron job for cleaning up auctions
// cron.schedule('16 10 * * *', async () => {
//   console.log('Starting cleanup job...');
//   await cleanUpAuctions();
//   console.log('Cleanup job finished');
// });

await restartScraper();
