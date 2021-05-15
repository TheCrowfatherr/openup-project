import { useParams } from "react-router";
import { Link } from "react-router-dom";
import { useState, useEffect } from "react";
import { API, graphqlOperation } from "aws-amplify";
import { getProduct } from "./graphql/queries";
import { Carousel } from "react-responsive-carousel";
import {
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Bar,
} from "recharts";
import "react-responsive-carousel/lib/styles/carousel.min.css"; // required import for Carousel
// image slider.

const ProductDetails = () => {
  const { id } = useParams();

  const [product, setProduct] = useState(undefined);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchProduct();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Render the Carousel image slider
  const RenderImageSlider = () => {
    if (!product.images || product.images.length === 0) {
      return <></>;
    }

    return (
      <div>
        <Carousel
          showThumbs={false}
          showStatus={false}
          className="rounded-xl my-2 ring-amber-500 ring-4 w-80 h-80"
        >
          {product.images.map((image) => (
            <img
              key={image}
              src={image}
              alt="Doest not exist"
              className="my-5 w-72 h-72"
            />
          ))}
        </Carousel>
      </div>
    );
  };

  /*
    Generates data for BarChart component basend on nutritional values of the product, in the
    appropriate form. Resulting data is a list of objects, each of the form:
      {
        dataKey: some value,
        valueKey: another value
      }
    Also, only basic nutrients are taken into consideration.
  */
  const generateBarChartData = () => {
    const nutritionalValues = product.nutritionalValues;
    const pieChartData = [];

    Object.keys(nutritionalValues).forEach((key) => {
      if (
        key !== "energy" &&
        key !== "saturatedFats" &&
        key !== "sugar" &&
        nutritionalValues[key] !== ""
      ) {
        pieChartData.push({
          name: key,
          "ammount on 100 grams": nutritionalValues[key],
        });
      }
    });

    console.log(pieChartData);
    return pieChartData;
  };

  async function fetchProduct() {
    try {
      setLoading(true);
      const productData = await API.graphql(
        graphqlOperation(getProduct, { id: id })
      );

      const prod = productData.data.getProduct;

      // Remove duplicate pictures for products from IDEA
      if (prod.store === "idea") {
        const regex = /_[123456789]\d*[l][.](jpg|jpeg|png)/;
        const newImages = prod.images.filter((image) => regex.test(image));
        prod.images = newImages;
      }

      setProduct(prod);
      setLoading(false);
    } catch (err) {
      console.log(err);
      alert(err.status);
    }
  }

  if (loading) {
    return (
      <h1 className="text-2xl grid justify-items-center mt-48">Loading ...</h1>
    );
  }

  return (
    <div>
      {product && !loading && (
        <div className="grid justify-items-center mb-16 grid-cols-1 ">
          <div className="grid justify-items-center md:flex md:flex-wrap md:mt-10 md:mb-5 md:space-x-10 my-5 md:items-center">
            <div>
              <RenderImageSlider></RenderImageSlider>
            </div>
            {/* <img
              src={product.images ? products.images[0] : null}
              alt="error"
              className="rounded-xl my-2 ring-amber-500 ring-4 w-80 h-80 my-5"
            /> */}
            <div className="w-80 h-60 text-center font-karla font-semibold text-white text-base bg-gradient-to-l rounded-xl from-amber-400 to-red-400 py-5 my-5 ">
              <h1 className="text-3xl">{product.name}</h1>
              <br />
              <ul className="mb-10">
                <li>
                  Brief description:
                  <br />
                  {product.briefDescription}
                </li>
                <br />
                <li>Category: {product.category}</li>
                <br />
                <li>
                  Nutri-score: <span>{product.nutriScore}</span>
                </li>
              </ul>
              <div className="mt-2">
                <Link
                  to={`/edit/${id}`}
                  className="my-5 px-6 rounded-xl focus:outline-none bg-red-500 font-karla hover:ring-2 hover:ring-amber-500 font-semibold text-lg text-white"
                >
                  Edit
                </Link>
              </div>
            </div>
          </div>

          <div className="grid justify-items-center my-10 space-y-10">
            <div className="text-3xl text-amber-500">
              <h1>On 100 g</h1>
            </div>
            <div className="hidden xs:block">
              <table className="table ">
                <tbody className="">
                  <tr className=" border-b-2 solid border-amber-500 border-collapse">
                    <td className="border-r-2 solid border-amber-500 w-44 h-36">
                      <div className="grid justify-items-center px-5">
                        <img
                          className="w-10 h-10 text-white mb-2"
                          src="/images/energy.png"
                          alt=""
                        />
                        <h4 className="font-karla font-semibold text-base">
                          {" "}
                          Energy
                        </h4>
                        <h5>{product.nutritionalValues.energy} (kJ/kcal)</h5>
                      </div>
                    </td>

                    <td className="border-r-2 solid border-amber-500 w-44 h-36">
                      <div className="grid justify-items-center px-5">
                        <img
                          className="w-10 h-10 text-white mb-2"
                          src="/images/fats.png"
                          alt=""
                        />
                        <h4 className="font-karla font-semibold text-base">
                          Fats
                        </h4>
                        <h5>{product.nutritionalValues.fats} (g)</h5>
                      </div>
                    </td>
                    <td className="border-r-2 solid border-amber-500 w-44 h-36">
                      <div className="grid justify-items-center px-5 ">
                        <img
                          className="w-10 h-10 text-white mb-2"
                          src="/images/saturatedFats.png"
                          alt=""
                        />
                        <h4 className="font-karla font-semibold text-base">
                          Saturated fats
                        </h4>
                        <h5>{product.nutritionalValues.saturatedFats} (g)</h5>
                      </div>
                    </td>
                    <td className="w-44 h-36">
                      <div className="grid justify-items-center px-5 ">
                        <img
                          className="w-10 h-10 text-white mb-2"
                          src="/images/proteins.png"
                          alt=""
                        />
                        <h4 className="font-karla font-semibold text-base">
                          Proteins
                        </h4>
                        <h5>{product.nutritionalValues.proteins} (g)</h5>
                      </div>
                    </td>
                  </tr>
                  <tr>
                    <td className="border-r-2 solid border-amber-500 w-44 h-36">
                      <div className="grid justify-items-center px-5 mt-3">
                        <img
                          className="w-10 h-10 text-white mb-2"
                          src="/images/carbs.png"
                          alt=""
                        />
                        <h4 className="font-karla font-semibold text-base">
                          {" "}
                          <span> Carbohydrates </span>
                        </h4>
                        <h5>{product.nutritionalValues.carbs} (g)</h5>
                      </div>
                    </td>

                    <td className="border-r-2 solid border-amber-500 w-44 h-36">
                      <div className="grid justify-items-center px-5 mt-3">
                        <img
                          className="w-10 h-10 text-white mb-2"
                          src="/images/sugar.png"
                          alt=""
                        />
                        <h4 className="font-karla font-semibold text-base">
                          Sugar
                        </h4>
                        <h5>{product.nutritionalValues.sugar} (g)</h5>
                      </div>
                    </td>
                    <td className="border-r-2 solid border-amber-500 w-44 h-36">
                      <div className="grid justify-items-center px-5 mt-3 ">
                        <img
                          className="w-10 h-10 text-white mb-2"
                          src="/images/fibers.png"
                          alt=""
                        />
                        <h4 className="font-karla font-semibold text-base">
                          Fibers
                        </h4>
                        <h5>{product.nutritionalValues.fibers} (g)</h5>
                      </div>
                    </td>
                    <td className="w-44 h-36">
                      <div className="grid justify-items-center px-5 mt-3">
                        <img
                          className="w-10 h-10 text-white mb-2  "
                          src="/images/salt.png"
                          alt=""
                        />
                        <h4 className="font-karla font-semibold text-base">
                          Salt
                        </h4>
                        <h5>{product.nutritionalValues.salt}(g)</h5>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            {/* Generate a BarChart based on the nutrients of the current product */}
            <div className="mb-10">
              <BarChart width={800} height={250} data={generateBarChartData()}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="ammount on 100 grams" fill="#eb8542" />
              </BarChart>
            </div>
          </div>
          <div className="grid justify-items-center sm:space-x-6 md:flex md:flex-wrap md:mt-5 md:mb-5 md:space-x-10  md:items-baseline">
            <div className="w-96 h-max bg-gradient-to-l justify-items-start rounded-xl text-center font-karla font-semibold text-white text-xl from-amber-400 to-red-400 py-5 mb-5 ">
              <ul>Date: {product.currentPrice.date} </ul>
              <br />
              <ul>Discount amount: {product.currentPrice.discountAmount} </ul>
              <br />
              <ul>
                Discount end date: {product.currentPrice.discountEndDate}{" "}
              </ul>
              <br />
              <ul>Discount price: {product.currentPrice.discountPrice} </ul>
              <br />
              <ul>
                Dsicount start date: {product.currentPrice.discountStartDate}{" "}
              </ul>
              <br />
              <ul>Regular price: {product.currentPrice.regularPrice} </ul>
              <br />
              <ul>Barcode: {product.barcode} </ul>
              <br />
              <ul>Store: {product.store} </ul>
              <br />
            </div>

            <div className="min-w-lg max-w-lg text-center font-karla font-semibold text-white text-xl h-max bg-gradient-to-l rounded-xl from-amber-400 to-red-400 py-5 mb-5 ">
              <ul>
                <h1 className="text-2xl">Description:</h1>
                <br />
                <li>
                  Addiotional informations:{" "}
                  {product.description.additionalInformation}
                </li>
                <br />
                <li>Alcohol: {product.description.alcohol} </li>
                <br />
                <li>Allergens: {product.description.allergens} </li>
                <br />
                <li>
                  Country of origin: {product.description.countryOfOrigin}{" "}
                </li>
                <br />
                <li>Expiry date: {product.description.expiryDate}</li>
                <br />
                <li>Imports: {product.description.imports}</li>
                <br />
                <li>Ingredients: {product.description.ingredients}</li>
                <br />
                <li>Maintenance: {product.description.maintenance}</li>
                <br />
                <li>Producer: {product.description.producer}</li>
              </ul>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ProductDetails;
