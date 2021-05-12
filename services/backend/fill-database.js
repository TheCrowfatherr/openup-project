import * as uuid from "uuid";
import constants from "./libs/fill-db-constants";
import AWS from "aws-sdk";

AWS.config.update({
    region: "eu-central-1",
});

const dynamoDB = new AWS.DynamoDB.DocumentClient();

/* Move some of these functions to a separate file,
or "merge" with existing files, now that this is a module */
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
    let discountAmount, discountStartDate, discountEndDate, discountPrice, regularPrice;
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
        date: formatDate(today), // so the date format is consistent throughout the product
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
            return constants.voliNamespace;
        case "idea":
            return constants.ideaNamespace;
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
        ("0" + date.getDate()).slice(-2) +
        "." +
        ("0" + (date.getMonth() + 1)).slice(-2) +
        "." +
        date.getFullYear() +
        ".";
    return formatted;
}

const getProducts = async idList => {
    // Return old products whose productStoreID is in idList
    let filterExpression = "productStoreID IN (";
    let expressionAttributeValues = {};
    for (let i = 0; i < idList.length; i++) {
        const id = `:id${i}`;
        filterExpression += id + ", ";
        expressionAttributeValues[id] = idList[i];
    }
    filterExpression = filterExpression.slice(0, -2) + ")";

    let params = {
        TableName: "PeriodicScrapingTest",
        IndexName: "productStoreID-index",
        FilterExpression: filterExpression,
        ExpressionAttributeValues: expressionAttributeValues,
    };

    let entireResult = []; // all of the old products
    let result = await dynamoDB.scan(params).promise();
    result.Items.forEach(product => entireResult.push(product));

    while (result.LastEvaluatedKey) {
        // while it's not undefined
        // continue scanning from the LastEvaluatedKey
        params["ExclusiveStartKey"] = result.LastEvaluatedKey;
        result = await dynamoDB.scan(params).promise();
        result.Items.forEach(product => entireResult.push(product));
    }
    console.log("The scan returned", entireResult.length, "'relevant' products");
    return entireResult;
};

const returnOldProductWith = (productStoreID, oldProducts) => {
    if (oldProducts.length === 0) {
        return undefined;
    }
    for (const oldProduct of oldProducts) {
        if (oldProduct.productStoreID === productStoreID) {
            return oldProduct;
        }
    }
    return undefined;
};

const merge = (oldProduct, newProduct) => {
    // I couldn't bother with deepmerge, it didn't want to cooperate
    if (!oldProduct) {
        return newProduct;
    }
    const result = {};
    // console.log(oldProduct);
    // console.log("\n\nNEW PRODUCT:", newProduct);
    const keys = Object.keys(oldProduct);
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        //console.log(key);
        if (key === "currentPrice" || key === "nutritionalValues" || key === "description") {
            const subkeys = Object.keys(oldProduct[key]);
            result[key] = {};
            for (let j = 0; j < subkeys.length; j++) {
                const subkey = subkeys[j];
                //console.log("\t", subkey);
                result[key][subkey] = newProduct[key][subkey];
            }
        } else {
            if (key === "id") {
                result[key] = oldProduct[key];
            } else {
                result[key] = newProduct[key];
            }
        }
    }
    //console.log("\n\nRESULT:", result);
    return result;
};

export const writeProducts = async products => {
    let allParams = [];
    let idList = [];

    console.log("Lambda called for", products[0].category_name);
    for (const product of products) {
        // find the productStoreID of the given products
        const storeNamespace = parseStore(product.store);
        const productStoreID = uuid.v5(product.product_id.toString(), storeNamespace);
        idList.push(productStoreID);
    }
    // get the products that already exist
    const oldProducts = await getProducts(idList);

    for (let i = 0; i < products.length; i++) {
        const product = products[i];
        let productID = uuid.v4();
        const storeNamespace = parseStore(product.store);
        const productStoreID = uuid.v5(product.product_id.toString(), storeNamespace);
        const newProduct = {
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
            productStoreID: productStoreID, // for synchronizing changes later
            barcode: parseBarcode(product.barcodes),
            //
            description: buildDescription(product),
            nutritionalValues: buildNutriValues(product),
            currentPrice: buildPrice(product),
        };
        const oldProduct = returnOldProductWith(productStoreID, oldProducts);
        const item = merge(oldProduct, newProduct);

        let params = {
            PutRequest: {
                Item: item,
            },
        };
        allParams.push(params);
    }

    // added all the 25 products to the params list, now batch write
    let batch = {
        RequestItems: {},
    };
    // Don't forget to change the table!!!!!
    batch.RequestItems["PeriodicScrapingTest"] = allParams;
    const res = await dynamoDB.batchWrite(batch).promise();
    console.log("Any unprocessed items?", res.UnprocessedItems);
};
