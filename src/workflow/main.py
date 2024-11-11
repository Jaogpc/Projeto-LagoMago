# %%
import os
import dotenv
import requests
import json

dotenv.load_dotenv(".env")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

url = f"https://{DATABRICKS_HOST}/api/2.1/jobs/list"
header = {"Authorization" : f"Bearer {DATABRICKS_TOKEN}"}

response = requests.get(url, headers=header)
jobs = response.json()

def list_job_names():
    return [i.replace(".json", "") for i in os.listdir(".") if i.endswith(".json")]


def load_settings(job_name):
    with open(f"{job_name}.json", "r") as open_file:
        settings = json.load(open_file)
    return settings


def resetJob(settings):
    url = f"https://{DATABRICKS_HOST}/api/2.1/jobs/reset"
    header = {"Authorization" : f"Bearer {DATABRICKS_TOKEN}"}

    response = requests.post(url = url, headers = header, json = settings)
    return response


def main():
    for i in list_job_names():
        settings = load_settings(job_name=i)
        response = resetJob(settings=settings)
        if response.status_code == 200:
            print(f"Job '{i}' atualizado com sucesso!")
        else:
            print(f"Não foi possível atualizar o Job '{i}', Error: {response.text}.")


if __name__ == "__main__":
    main()

# %%
