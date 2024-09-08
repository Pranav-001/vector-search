import json
import os
import re

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
    queries = [
        "Seeking an experienced Secondary Mathematics Teacher with a B.Ed or M.Sc in Mathematics, 5+ years of teaching experience, ideally in an IB curriculum. Strong skills in problem-solving, adaptability, and leadership required. Must be available for an immediate start.",
        "Looking for a Primary School Teacher specializing in English Literature, holding a B.A or M.A in English, with at least 2 years of classroom experience. The candidate should have excellent communication skills, a passion for literature, and the ability to engage students creatively.",
        "Hiring a Secondary Physics Teacher with a minimum of 4 years of experience teaching in a British or American curriculum, holding a B.Sc or M.Sc in Physics. Must be available by June 2025 and demonstrate skills in classroom management, innovation, and student mentorship.",
        "In need of a Middle School Science Teacher with a degree in Biology or General Science, along with relevant certifications such as a UK Level 5 Diploma. A minimum of 3 years of teaching experience is required, along with strong collaboration, lesson planning, and communication skills.",
        "Looking for a Spanish Language Teacher with native proficiency in Spanish and at least a B.Ed in Modern Languages. Candidates must have 2+ years of experience teaching in international schools and possess excellent organizational, communication, and language teaching skills.",
        "Seeking a Secondary School Art Teacher with a B.A or M.A in Fine Arts, and 3+ years of teaching experience in creative subjects. Strong skills in mentoring, classroom creativity, and the ability to inspire students through hands-on projects are required. Candidates with experience in international curricula are preferred.",
        "Looking for a Secondary Computer Science Teacher with a B.Tech or M.Sc in Computer Science, 3+ years of experience in coding and programming education. Candidates must be fluent in English, demonstrate problem-solving skills, and have experience with project-based learning.",
        "Seeking a Physical Education Teacher for a secondary school with a B.Sc in Physical Education, 5+ years of experience in physical education instruction, and strong skills in teamwork, motivation, and student engagement. Certifications in fitness training or coaching are preferred.",
        "Hiring a Chemistry Teacher with a B.Sc in Chemistry or a related science, with at least 4 years of teaching experience. Experience in an international school setting and the ability to lead lab work and student projects is required. Candidates should possess strong leadership and organizational skills.",
        "Looking for a Geography Teacher with a B.A in Geography, 2+ years of experience teaching in an international curriculum, and strong skills in classroom management, interactive learning, and student engagement. Candidates with additional certifications in social sciences are preferred.",
    ]
    result_dict = []
    for query in queries:
        vector_results = vector_store.similarity_search_with_score(query, k=5)
        result = []
        for vector_result in vector_results:
            user_id = re.search(
                r"user_id:\s([\w-]+)", vector_result[0].page_content
            ).group(1)
            result.append({user_id: vector_result[1]})
        result_dict.append({"query": query, "result": result})

    with open(f"data/results.json", "w") as file:
        file.writelines(json.dumps(result_dict))


if __name__ == "__main__":
    main()
