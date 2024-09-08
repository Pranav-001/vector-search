import json
import os

from dotenv import load_dotenv
from google.cloud import aiplatform
from google.oauth2 import service_account
from langchain_google_vertexai import VertexAIEmbeddings

load_dotenv()

# Project and Storage Constants
PROJECT_ID = os.environ["GCS_PROJECT_ID"]
REGION = os.environ["GCS_REGION"]
BUCKET = os.environ["GCS_BUCKET"]
BUCKET_URI = f"gs://{BUCKET}"
CREDENTIALS = os.environ["GCS_CREDENTIAL_FILE"]

# The number of dimensions for the textembedding-gecko@003 is 768
# If other embedder is used, the dimensions would probably need to change.
DIMENSIONS = os.environ["DIMENSIONS"]

# Index Constants
DISPLAY_NAME = os.environ["DISPLAY_NAME"]
DEPLOYED_INDEX_ID = os.environ["DEPLOYED_INDEX_ID"]

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
embedding_model = VertexAIEmbeddings(model_name="textembedding-gecko@003")


# NOTE : This operation can take upto 30 seconds
my_index = aiplatform.MatchingEngineIndex.create_tree_ah_index(
    display_name=DISPLAY_NAME,
    dimensions=DIMENSIONS,
    approximate_neighbors_count=150,
    distance_measure_type="DOT_PRODUCT_DISTANCE",
    index_update_method="STREAM_UPDATE",
)

# Create an endpoint
my_index_endpoint = aiplatform.MatchingEngineIndexEndpoint.create(
    display_name=f"{DISPLAY_NAME}-endpoint", public_endpoint_enabled=True
)

# NOTE : This operation can take upto 20 minutes
my_index_endpoint = my_index_endpoint.deploy_index(
    index=my_index, deployed_index_id=DEPLOYED_INDEX_ID
)

my_index_endpoint.deployed_indexes
