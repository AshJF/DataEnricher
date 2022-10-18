import requests
import pandas as pd

def run_enrichment(data, lei_data):
    """
    Function to carry out all necessary enrichment steps.

    Args:
        data (DataFrame row)
        lei_data (json)

    Returns:
        DataFrame row
    """

    lei_info = [x for x in lei_data if x["attributes"]["lei"] == data["lei"]][0]
    data["name"] = lei_info["attributes"]["entity"]["legalName"]["name"]
    data["bic"] = ", ".join(lei_info["attributes"]["bic"])
    country = lei_info["attributes"]["entity"]["legalAddress"]["country"]
    
    if country == "GB":
        data["transaction_costs"] =  round((data["notional"] * data["rate"]) - data["notional"], 2)
    elif country == "NL":
        data["transaction_costs"] = round(abs((data["notional"] * (1/data["rate"])) - data["notional"]), 2)
        
    return data

def get_lei_info(lei_list):
    """
    Function to fetch data from the external API.

    Args:
        lei_list (list): Unique lei values from DataFrame

    Returns:
        json
    """
    
    api = "https://api.gleif.org/api/v1/lei-records"
    param_string = ", ".join(lei_list)
    query_params = {"filter[lei]": f"{param_string}"}
    retries = 3

    for i in range(retries):
        try:
            response = requests.get(api, params=query_params, timeout=30)
            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as err:
            print(f"Attempt {i + 1} Error :", err)

    else:
        print("All retry attempts have failed. Exiting the program.")
        raise SystemExit()

def main(file_name):
    """
    Main Function.

    Args:
        file_name (string)
    """
    
    print("Reading CSV data.")
    data = pd.read_csv(file_name)

    print("Reading lei data from external API.")
    response = get_lei_info(data['lei'].unique())

    print("Begin Enrichment.")
    data = data.apply(run_enrichment, args=[response.json()["data"]], axis=1)
    print("Enrichment Completed.")

    data.to_csv("Output.csv", index=False)
    print("Output File Created.")

if __name__ == "__main__":
    main("Data.csv")