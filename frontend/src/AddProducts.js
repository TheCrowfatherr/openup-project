import { useState } from "react";
import { useHistory } from "react-router-dom";
import { API, graphqlOperation } from "aws-amplify";
import { createProduct } from "./graphql/mutations";
import { Dialog } from "@headlessui/react";
const AddProduct = () => {
  const [briefDescription, setbriefDescription] = useState("");
  const [category, setCategory] = useState("");
  const [date, setDate] = useState("");
  const [discountAmount, setDiscountAmount] = useState(0);
  const [discountEndDate, setDiscountEndDate] = useState(0);
  const [discountPrice, setDiscountPrice] = useState(0);
  const [discountStartDate, setDiscountStartDate] = useState(0);
  const [regularPrice, setregularPrice] = useState(0);
  const [additionalInformation, setAdditionalInformation] = useState("");
  const [alcohol, setAlcohol] = useState("");
  const [allergens, setAllergens] = useState("");
  const [countryOfOrigin, setConutryOfOrigin] = useState("");
  const [expiryDate, setExpiryDate] = useState("");
  const [imports, setImports] = useState("");
  const [ingredients, setIngredients] = useState("");
  const [maintenance, setMaintenance] = useState("");
  const [producer, setProducer] = useState("");
  const [name, setName] = useState("");
  const [nutriScore, setNutriScore] = useState("");
  const [carbs, setCarbs] = useState(0);
  const [energy, setEnergy] = useState(0);
  const [fats, setFats] = useState(0);
  const [fibers, setFibers] = useState(0);
  const [proteins, setProteins] = useState(0);
  const [salt, setSalt] = useState(0);
  const [saturatedFats, setSaturatedFats] = useState(0);
  const [sugar, setSugar] = useState(0.0);

  const [isOpen, setIsOpen] = useState(false);
  const [errOpen, setErrOpen] = useState(false);
  const history = useHistory();

  const Notice = () => {
    return (
      <Dialog
        as="div"
        className="overflow-auto fixed top-2 right-4 ring-2 ring-red-500 grid justify-items-center text-white bg-amber-500 rounded-xl px-5 py-5 w-max h-max z-10"
        open={isOpen}
        onClose={() => setIsOpen(false)}
      
      >
        <Dialog.Title className="text-2xl font-karla">
          Successfully submited!
        </Dialog.Title>
        <br />
        <Dialog.Description className="text-lg font-karla">
          New product is added.
        </Dialog.Description>
        <button
          className="px-5 rounded-xl focus:outline-none bg-red-500 hover:ring-2 hover:ring-amber-500"
          onClick={() => {
            setIsOpen(false);
            history.push(`/search`);
          }}
        >
          OK
        </button>
      </Dialog>
    );
  };

  const Error = () => {
    return (
      <Dialog
        as="div"
        className="overflow-auto fixed top-2 right-4 ring-2 ring-red-500 grid justify-items-center text-white bg-amber-500 rounded-xl px-5 py-5 w-max h-max z-10"
        open={errOpen}
        onClose={() => setErrOpen(false)}
        
      >
        <Dialog.Title className="text-2xl font-karla">
          Ohh, something went wrong...
        </Dialog.Title>
        <br />
        <Dialog.Description className="text-lg font-karla">
          Some error occurred, please try again.
        </Dialog.Description>
        <button
          className="px-5 rounded-xl focus:outline-none bg-red-500 hover:ring-2 hover:ring-amber-500"
          onClick={() => {
            setErrOpen(false);
            history.push(`/search`);
          }}
        >
          OK
        </button>
      </Dialog>
    );
  };

  const handleSubmit = (e) => {
    e.preventDefault();

    const product = {
      briefDescription: briefDescription,
      barcode: "",
      category: category,
      currentPrice: {
        date: date,
        discountAmount: discountAmount,
        discountEndDate: discountEndDate,
        discountPrice: discountPrice,
        discountStartDate: discountStartDate,
        regularPrice: regularPrice,
      },
      description: {
        additionalInformation: additionalInformation,
        alcohol: alcohol,
        allergens: allergens,
        countryOfOrigin: countryOfOrigin,
        expiryDate: expiryDate,
        imports: imports,
        ingredients: ingredients,
        maintenance: maintenance,
        producer: producer,
      },
      name: name,
      nutriScore: nutriScore,
      nutritionalValues: {
        carbs: parseFloat(carbs),
        energy: energy,
        fats: parseFloat(fats),
        fibers: parseFloat(fibers),
        proteins: parseFloat(proteins),
        salt: parseFloat(salt),
        saturatedFats: parseFloat(saturatedFats),
        sugar: parseFloat(sugar),
      },
      status: "published",
    };
    API.graphql(graphqlOperation(createProduct, { input: product }))
      .then((res) => {
        setIsOpen(true);
      })
      .catch((err) => {
        alert(JSON.stringify(err, null, 4));
        setErrOpen(true);
      });
  };

  return (
    <div className="rounded-xl text-white font-karla grid justify-items-center">
      {isOpen && <Notice className="z-10"></Notice>}
      {errOpen && <Error className="z-10"></Error>}
      <form
        onSubmit={handleSubmit}
        className="bg-gradient-to-br pr-10 grid justify-items-end h-max rounded-xl w-max mt-10 mb-10 from-amber-400 to-red-400"
        action=""
      >
        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Name
          </label>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Brief description
          </label>
          <input
            value={briefDescription}
            onChange={(e) => setbriefDescription(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Category
          </label>
          <input
            value={category}
            onChange={(e) => setCategory(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Additional informations
          </label>
          <textarea
            value={additionalInformation}
            onChange={(e) => setAdditionalInformation(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Alcohol
          </label>
          <input
            value={alcohol}
            onChange={(e) => setAlcohol(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Allergens
          </label>
          <input
            value={allergens}
            onChange={(e) => setAllergens(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Country of origin
          </label>
          <input
            value={countryOfOrigin}
            onChange={(e) => setConutryOfOrigin(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Expiry date
          </label>
          <input
            value={expiryDate}
            onChange={(e) => setExpiryDate(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Imports
          </label>
          <input
            value={imports}
            onChange={(e) => setImports(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Ingredients
          </label>
          <input
            value={ingredients}
            onChange={(e) => setIngredients(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Maintenance
          </label>
          <input
            value={maintenance}
            onChange={(e) => setMaintenance(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg flex flex-wrap">
          <label className="" htmlFor="name">
            Producer
          </label>
          <input
            value={producer}
            onChange={(e) => setProducer(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Nutri-score
          </label>
          <select
            value={nutriScore}
            defaultValue=""
            onChange={(e) => setNutriScore(e.target.value)}
            className="w-64 text-red-500 rounded-xl ml-5 focus:outline-none px-3 "
            name="nutri-score"
          >
            <option value="">none</option>
            <option value="A">A</option>
            <option value="B">B</option>
            <option value="C">C</option>
            <option value="D">D</option>
            <option value="E">E</option>
          </select>
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label htmlFor="name">Energy (KJ/kcal)</label>
          <input
            value={energy}
            onChange={(e) => setEnergy(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="text"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label htmlFor="name">Fats (g)</label>
          <input
            value={fats}
            onChange={(e) => setFats(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label htmlFor="name">Saturated fats (g)</label>
          <input
            value={saturatedFats}
            onChange={(e) => setSaturatedFats(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label htmlFor="name">Proteins (g)</label>
          <input
            value={proteins}
            onChange={(e) => setProteins(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label htmlFor="name">Carbonhydrates (g)</label>
          <input
            value={carbs}
            onChange={(e) => setCarbs(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label htmlFor="name">Sugar (g)</label>
          <input
            value={sugar}
            onChange={(e) => setSugar(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label htmlFor="name">Fibers (g)</label>
          <input
            value={fibers}
            onChange={(e) => setFibers(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label htmlFor="name">Salt (g)</label>
          <input
            value={salt}
            onChange={(e) => setSalt(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>

        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Date
          </label>
          <input
            value={date}
            onChange={(e) => setDate(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="date"
            placeholder="dd-mm-yyyy"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Regular price
          </label>
          <input
            value={regularPrice}
            onChange={(e) => setregularPrice(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Discount amount
          </label>
          <input
            value={discountAmount}
            onChange={(e) => setDiscountAmount(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Discount price
          </label>
          <input
            value={discountPrice}
            onChange={(e) => setDiscountPrice(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="number"
            min="0"
            step="0.01"
          />
        </div>

        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Discount start date
          </label>
          <input
            value={discountStartDate}
            onChange={(e) => setDiscountStartDate(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="date"
            placeholder="dd-mm-yyyy"
          />
        </div>
        <div className="mt-5 ml-6 text-lg">
          <label className="" htmlFor="name">
            Discount end date
          </label>
          <input
            value={discountEndDate}
            onChange={(e) => setDiscountEndDate(e.target.value)}
            className="rounded-xl w-64 focus:outline-none px-3 text-red-500 ml-5"
            type="date"
            placeholder="dd-mm-yyyy"
          />
        </div>

        <button
          className="mt-5 px-6 rounded-xl focus:outline-none bg-white hover:ring-amber-600 text-amber-600 hover:ring-2 font-karla font-semibold text-lg mb-10"
          to="/search"
        >
          Submit
        </button>
      </form>
    </div>
  );
};

export default AddProduct;
