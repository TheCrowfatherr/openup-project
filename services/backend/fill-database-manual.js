const fs = require("fs");
const files = fs.readdirSync("products"); // folder products must be in the same directory as this file
const uuid = require("uuid");
const AWS = require("aws-sdk");

/* This script is for manually inserting products from Voli in the database,
but I left the possibility to insert products from Idea too. */
const voliNamespace = "db03ea1b-1f65-4882-85ed-5b73310b089a";
const ideaNamespace = "8b6eca61-ced0-48a7-b73b-16768e2b8c76";
const productsTable = "dev-infrastructure-dynamodb-Products229621C6-HOUEPHVSHL34";
AWS.config.update({
    region: "eu-central-1",
});

const dynamoDB = new AWS.DynamoDB.DocumentClient();

function buildNutriValues(product) {
    // Form the nutritional values
    let nutriValues = {
        energy: product.energy || "",
        fats: product.fats || 0,
        saturatedFats: product.saturated_fats || 0,
        proteins: product.proteins || 0,
        carbs: product.carbs || 0,
        sugar: product.sugar || 0,
        fibers: product.fiber || 0,
        salt: product.salt || 0,
    };
    return nutriValues;
}

function buildPrice(product) {
    // Form the price information
    let today = new Date();
    let discountAmount,
        discountStartDate,
        discountEndDate,
        discountPrice,
        regularPrice;
    const sliceBackAmount = product.store.toLowerCase() === "voli" ? -1 : -2; // for removing the euro sign

    if ("discount_info" in product) {
        discountAmount = product.discount_info.discount.slice(0, -1); // remove the %
        let dates = product.discount_info.duration.split("-");
        discountStartDate = dates[0]; // start date of discount
        discountEndDate = dates[1]; // end date of discount
        discountPrice = product.price_info.discounted_price.slice(0, -1);
        regularPrice = product.price_info.old_price.slice(0, sliceBackAmount);
        if (product.store.toLowerCase() === "voli") {
            discountStartDate += today.getFullYear() + ".";
            discountEndDate += today.getFullYear() + ".";
        }
    } else {
        discountAmount = discountStartDate = discountEndDate = discountPrice = "";
        regularPrice = product.price_info.current_price.slice(0, sliceBackAmount);
    }
    let price = {
        date: formatDate(today),  // so the date format is consistent throughout the product
        discountAmount: discountAmount,
        discountStartDate: discountStartDate,
        discountEndDate: discountEndDate,
        discountPrice: discountPrice,
        regularPrice: regularPrice,
    };
    return price;
}

function buildDescription(product) {
    // Form the description attribute
    let description;
    if (!product.description) {
        description = {
            maintenance: "",
            countryOfOrigin: "",
            producer: "",
            imports: "",
            ingredients: "",
            expiryDate: "",
            allergens: "",
            alcohol: "",
            additionalInformation: "",
        };
    } else {
        description = {
            maintenance: product.description["Čuvanje"],
            countryOfOrigin: product.description["Zemlja"],
            producer: product.description["Proizvođač"],
            imports: product.description["Uvozi"],
            ingredients: product.description["Sastojci"],
            expiryDate: product.description["Rok upotrebe"],
            allergens: product.description["Alergeni"],
            alcohol: product.description["Alkohol"],
            additionalInformation: product.description["Dodatne informacije"],
        };
    }
    return description;
}

function parseStore(store) {
    switch (store.toLowerCase()) {
        case "voli":
            return voliNamespace;
        case "idea":
            return ideaNamespace;
        default:
            return "0";
    }
}

function parseBarcode(barcodes) {
    if (!barcodes) {
        // voli
        return "";
    }
    // idea, barcodes are a list of strings
    return barcodes[0];
}

function formatDate(date) {
    // Format the date
    // https://stackoverflow.com/a/30272803
    let formatted =
        ("0" + date.getDate()).slice(-2) + "." +
        ("0" + (date.getMonth() + 1)).slice(-2) + "." +
        date.getFullYear() + ".";
    return formatted;
}

const path = __dirname + "\\products\\"; // this is where the JSON files live

const main = async () => {
    for (const file of files) {
        const products = require(path + file); // get the array in the JSON file
        let allParams = [];
        console.log(products.length);

        for (const product of products) {
            let productID = uuid.v4();
            const storeNamespace = parseStore(product.store);
            let params = {
                PutRequest: {
                    Item: {
                        id: productID,
                        name: product.name,
                        category: product.category_name,
                        briefDescription: product.brief_product_description || "",
                        status: "published",
                        nutriScore: "E",
                        images: product.image_urls || [], // just in case
                        // new
                        store: product.store.toLowerCase(),
                        storeID: storeNamespace, // returns the namespace of the store (acts as the ID)
                        productStoreID: uuid.v5(product.product_id.toString(), storeNamespace), // for synchronizing changes later
                        barcode: parseBarcode(product.barcodes),
                        //
                        description: buildDescription(product),
                        nutritionalValues: buildNutriValues(product),
                        currentPrice: buildPrice(product),
                    },
                },
            };
            allParams.push(params);
        }
        // added all the 25 products to the params list, now batch write
        let batch = {
            RequestItems: {},
        };
        batch.RequestItems[productsTable] = allParams;
        const res = await dynamoDB.batchWrite(batch).promise();
        // I have to process the unprocessed items later on
        console.log("Any unprocessed items?", res.UnprocessedItems);
    }
};

main().then((x) => console.log("All done!"));