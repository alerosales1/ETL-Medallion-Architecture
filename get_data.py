import requests
import pandas as pd


# Function to get data from the ViaCEP API

def get_data(cep):
    endpoint = f'https://viacep.com.br/ws/{cep}/json/'
    try:
        response = requests.get(endpoint, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(
                f"Error: Unable to fetch data for CEP {cep}. Status code: {response.status_code}")
            return None

    except requests.exceptions.ConnectionError as e:
        print(f"Erro de conexão para o cep: {cep}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout ao tentar conectar para o cep: {cep}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer a requisição para o cep: {cep}: {e}")
        return None


user_path = '01-bronze-raw/users.csv'
users_df = pd.read_csv(user_path)

# Extracting the 'cep' column and converting it to a list
cep_lists = users_df['cep'].tolist()
cep_info_list = []

for cep in cep_lists:
    cep_clean = cep.replace('-', '')  # Clean the CEP format
    cep_info = get_data(cep_clean)
    print(cep_info)    # You can process the cep_info as needed
    # If contains error key, skip it
    if 'erro' in cep_info:
        print(f"CEP {cep_clean} not found.")
        continue
    cep_info_list.append(cep_info)

# Saving the collected CEP information to a CSV file
cep_info_df = pd.DataFrame(cep_info_list)
cep_info_df.to_csv('01-bronze-raw/cep_info.csv', index=False)
