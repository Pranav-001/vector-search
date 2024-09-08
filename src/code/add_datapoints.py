import json
import os

from dotenv import load_dotenv
from google.cloud import aiplatform
from google.oauth2 import service_account
from langchain_google_vertexai import VectorSearchVectorStore, VertexAIEmbeddings

load_dotenv()

# Project and Storage Constants
PROJECT_ID = os.environ["GCS_PROJECT_ID"]
REGION = os.environ["GCS_REGION"]
BUCKET = os.environ["GCS_BUCKET"]
BUCKET_URI = f"gs://{BUCKET}"
CREDENTIALS = os.environ["GCS_CREDENTIAL_FILE"]
INDEX_ID = os.environ["INDEX_ID"]
INDEX_ENDPOINT_ID = os.environ["INDEX_ENDPOINT_ID"]

# The number of dimensions for the textembedding-gecko@003 is 768
# If other embedder is used, the dimensions would probably need to change.
DIMENSIONS = os.environ["DIMENSIONS"]

# Index Constants
DISPLAY_NAME = os.environ["DISPLAY_NAME"]
DEPLOYED_INDEX_ID = os.environ["DEPLOYED_INDEX_ID"]


def flatten_json(data, parent_key="", sep="_"):
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                items.extend(
                    flatten_json({f"{new_key}{sep}{i}": item}, "", sep=sep).items()
                )
        else:
            items.append((new_key, v))
    return dict(items)


def main():
    with open(CREDENTIALS) as f:
        service_account_info = json.load(f)

    my_credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )

    aiplatform.init(
        project=PROJECT_ID,
        location=REGION,
        staging_bucket=BUCKET_URI,
        credentials=my_credentials,
    )
    my_index = aiplatform.MatchingEngineIndex(INDEX_ID)
    my_index_endpoint = aiplatform.MatchingEngineIndexEndpoint(INDEX_ENDPOINT_ID)
    embedding_model = VertexAIEmbeddings(model_name="textembedding-gecko@003")

    # Create a Vector Store
    vector_store = VectorSearchVectorStore.from_components(
        project_id=PROJECT_ID,
        region=REGION,
        gcs_bucket_name=BUCKET,
        index_id=my_index.name,
        endpoint_id=my_index_endpoint.name,
        embedding=embedding_model,
        stream_update=True,
    )

    with open(r"D:\Workspace\vector-search\src\data\data.json", "r") as file:
        data_dict = json.load(file)

    flattened_data = [flatten_json(data) for data in data_dict]
    serialized_data = [
        (" ".join(f"{key}: {value}" for key, value in data.items()))
        for data in flattened_data
    ]

    # Add vectors and mapped text chunks to your vector store
    vector_store.add_texts(texts=serialized_data, is_complete_overwrite=True)


if __name__ == "__main__":
    main()
