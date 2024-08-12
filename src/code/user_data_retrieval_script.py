import json
import os
import sys
import warnings
from typing import List

import pandas as pd

warnings.simplefilter(action="ignore", category=DeprecationWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)
import psycopg2

# Add the src directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


from src.configuration import GRAVITY_DATABASE, SOLIS_DATABASE

solis_conn = psycopg2.connect(**SOLIS_DATABASE)
solis_cur = solis_conn.cursor()
gravity_conn = psycopg2.connect(**GRAVITY_DATABASE)
gravity_cur = gravity_conn.cursor()


def get_gravity_value(df, gravity_columns):
    if not df.__len__():
        return df

    for gravity_table_name, field in gravity_columns.items():
        query = f"""
            select
                uuid,
                name
            from
                {gravity_table_name}
            where
                date_deleted is null;
        """
        gravity_cur.execute(query)
        gravity_df = pd.DataFrame(
            gravity_cur.fetchall(),
            columns=["uuid", field.split("_id")[0]],
        )
        df = pd.merge(
            left=df,
            right=gravity_df,
            left_on=field,
            right_on="uuid",
            how="left",
        )
        df.drop(columns=["uuid", field], inplace=True)
    return df


def get_data(query, columns, gravity_columns={}):
    solis_cur.execute(query)
    df = pd.DataFrame(
        solis_cur.fetchall(),
        columns=columns,
    )
    if gravity_columns:
        df = get_gravity_value(df, gravity_columns)
    df.fillna("", inplace=True)
    grouped_data = (
        df.groupby("user_id")
        .apply(
            lambda x: x.drop(columns="user_id").to_dict(orient="records"),
        )
        .to_dict()
    )
    del df
    return grouped_data


def get_users(user_ids=None):
    if user_ids:
        query = f"""
            select
                la.id
            from
                users u
            inner join
                learning_accounts la
            on 
                u.id=la.base_user_id
            where
                u.is_active=true
                and u.deleted_at is null
                and la.deleted_at is null
                and u.uuid in {tuple(user_ids)};
        """
    else:
        query = """
                select
                    id
                from
                    learning_accounts
                where
                    deleted_at is null limit 1000;
            """
    solis_cur.execute(query)
    return [val[0] for val in solis_cur.fetchall()]


def get_preferred_work_locations(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                country_id,
                state_id,
                "sequence"
            from
                preferred_work_locations
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """
    columns = [
        "user_id",
        "country_id",
        "state_id",
        "sequence",
    ]
    gravity_columns = {"countries": "country_id", "states": "state_id"}

    return get_data(query, columns, gravity_columns)


def get_user_awards(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                title,
                issuer,
                TO_CHAR(issued_on, 'YYYY-MM-DD') AS issued_on,
                description,
                certificate,
                certificate_name
            from
                user_awards
            where 
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "user_id",
        "title",
        "issuer",
        "issued_on",
        "description",
        "certificate",
        "certificate_name",
    ]
    return get_data(query, columns)


def get_user_certifications(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                name,
                organisation_name,
                will_expire,
                TO_CHAR(completion_date, 'YYYY-MM-DD') as completion_date,
                TO_CHAR(expiration_date, 'YYYY-MM-DD') as expiration_date,
                case
                    when mode_of_learning = 1 then 'ONLINE'
                    when mode_of_learning = 2 then 'CLASSROOM'
                    when mode_of_learning = 3 then 'BLENDED'
                    when mode_of_learning = 4 then 'DEFAULT'
                end as mode_of_learning,
                suraasa_certification_id,
                case
                    when status = 1 then 'IN_PROGRESS'
                    when status = 2 then 'COMPLETED'
                    when status = 3 then 'SUSPENDED'
                end as status,
                case
                    when user_certification_id is null then false
                    else true
                end as have_evidences
            from
                user_certifications uc
            left join (
                select
                    distinct user_certification_id
                from
                    user_certification_evidences
                where
                    deleted_at is null) uce on
                uc.id = uce.user_certification_id
            where
                uc.deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "user_id",
        "name",
        "organisation_name",
        "will_expire",
        "completion_date",
        "expiration_date",
        "mode_of_learning",
        "suraasa_certification_id",
        "status",
        "have_evidences",
    ]
    return get_data(query, columns)


def get_user_computed_fields(user_ids: List[int]):
    query = f"""
            select
                days_of_experience,
                learning_user_id
            from
                user_computed_fields
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "days_of_experience",
        "user_id",
    ]
    return get_data(query, columns)


def get_user_interests(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                interest
            from
                user_interests
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
            """

    columns = [
        "user_id",
        "interest",
    ]
    return get_data(query, columns)


def get_user_languages(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                language_id,
                case
                    when proficiency = 1 then 'ELEMENTARY'
                    when proficiency = 2 then 'LIMITED_WORKING'
                    when proficiency = 3 then 'PROFESSIONAL_WORKING'
                    when proficiency = 4 then 'FULL_PROFESSIONAL'
                    when proficiency = 5 then 'NATIVE'
                end as proficiency
            from
                user_languages
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "user_id",
        "language_id",
        "proficiency",
    ]
    gravity_columns = {"languages": "language_id"}
    return get_data(query, columns, gravity_columns)


def get_user_profiles(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                TO_CHAR(date_of_birth, 'YYYY-MM-DD') as date_of_birth,
                case
                    when gender = 1 then 'MALE'
                    when gender = 2 then 'FEMALE'
                    when gender = 3 then 'PREFER_NOT_TO_SAY'
                end as gender,
                nationality_id,
                country_id,
                state_id,
                is_verified,
                looking_for_jobs,
                career_aspiration
            from
                user_profiles
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "user_id",
        "date_of_birth",
        "gender",
        "nationality_id",
        "country_id",
        "state_id",
        "is_verified",
        "looking_for_jobs",
        "career_aspiration",
    ]
    gravity_columns = {"countries": "country_id", "states": "state_id"}
    return get_data(query, columns, gravity_columns)


def get_user_projects(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                title,
                currently_working,
                TO_CHAR(start_date, 'YYYY-MM-DD') as start_date,
                TO_CHAR(end_date, 'YYYY-MM-DD') as end_date,
                url,
                description
            from
                user_projects
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
            """

    columns = [
        "user_id",
        "title",
        "currently_working",
        "start_date",
        "end_date",
        "url",
        "description",
    ]
    return get_data(query, columns)


def get_user_publications(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                title,
                publisher,
                TO_CHAR(published_on, 'YYYY-MM-DD') as published_on,
                url,
                description
            from
                user_publications
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
            """

    columns = [
        "user_id",
        "title",
        "publisher",
        "published_on",
        "url",
        "description",
    ]
    return get_data(query, columns)


def get_user_qualifications(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                organisation_name,
                name,
                qualification_field_id,
                TO_CHAR(start_date, 'YYYY-MM-DD') as start_date,
                TO_CHAR(end_date, 'YYYY-MM-DD') as end_date,
                grade,
                case
                    when mode_of_learning = 1 then 'ONLINE'
                    when mode_of_learning = 2 then 'CLASSROOM'
                    when mode_of_learning = 3 then 'BLENDED'
                    when mode_of_learning = 4 then 'DEFAULT'
                end as mode_of_learning,
                suraasa_qualification_id,
                case
                    when status = 1 then 'IN_PROGRESS'
                    when status = 2 then 'COMPLETED'
                    when status = 3 then 'SUSPENDED'
                end as status,
                qualification_level_id,
                case
                    when user_qualification_id is null then false
                    else true
                end as have_evidences
            from
                user_qualifications uq
            left join (
                select
                    distinct user_qualification_id
                from
                    user_qualification_evidences uqe
                where
                    deleted_at is null) uqe on
                uq.id = uqe.user_qualification_id
            where
                uq.deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "user_id",
        "organisation_name",
        "name",
        "qualification_field_id",
        "start_date",
        "end_date",
        "grade",
        "mode_of_learning",
        "suraasa_qualification_id",
        "status",
        "qualification_level_id",
        "have_evidences",
    ]
    gravity_columns = {
        "qualification_fields": "qualification_field_id",
        "qualification_levels": "qualification_level_id",
    }
    return get_data(query, columns, gravity_columns)


def get_user_skills(user_ids: List[int]):
    query = f"""
            select
                learning_account_id,
                skill_name,
                sequence
            from
                user_skills
            where
                deleted_at is null
                and learning_account_id in {tuple(user_ids)};
            """

    columns = [
        "user_id",
        "skill_name",
        "sequence",
    ]
    return get_data(query, columns)


def get_user_subject_experiences(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                subject_id,
                days_of_experience
            from
                user_subject_experiences
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
            """

    columns = [
        "user_id",
        "subject_id",
        "days_of_experience",
    ]
    gravity_columns = {"subjects": "subject_id"}
    return get_data(query, columns, gravity_columns)


def get_user_subject_interests(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                subject_id,
                sequence
            from
                user_subject_interests
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "user_id",
        "subject_id",
        "sequence",
    ]
    gravity_columns = {"subjects": "subject_id"}
    return get_data(query, columns, gravity_columns)


def get_user_test_scores(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                name,
                score,
                TO_CHAR(test_date, 'YYYY-MM-DD') as test_date,
                description,
                case
                    when evidence_document is not null then true
                    when evidence_url is not null then true
                    else false
                end as has_evidence
            from
                user_test_scores
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "user_id",
        "name",
        "score",
        "test_date",
        "description",
        "has_evidence",
    ]
    return get_data(query, columns)


def get_user_work_experiences(user_ids: List[int]):
    query = f"""
            select
                learning_user_id,
                title,
                case
                    when employment_type = 1 then 'FULL_TIME'
                    when employment_type = 2 then 'PART_TIME'
                    when employment_type = 3 then 'FRESHER'
                    when employment_type = 4 then 'INTERN'
                    when employment_type = 5 then 'FREELANCE'
                    when employment_type = 6 then 'SELF_EMPLOYED'
                end as employment_type,
                description,
                organisation_name,
                case
                    when organisation_type = 1 then 'SCHOOL'
                    when organisation_type = 2 then 'COLLEGE_OR_UNIVERSITY'
                    when organisation_type = 3 then 'TUTORING'
                    when organisation_type is null then 'OTHERS'
                end as organisation_type,
                other_organisation_type,
                "country_id",
                "state_id",
                "currently_working",
                TO_CHAR(start_date, 'YYYY-MM-DD') as start_date,
                TO_CHAR(end_date, 'YYYY-MM-DD') as end_date,
                "salary",
                "currency_id",
                "curriculum_id",
                "teaching_level_id",
                "teaching_role_id",
                (SELECT json_agg(we.subject_id) FROM work_experience_subjects we WHERE we.work_experience_id = uwe.id) AS subjects
            from
                user_work_experiences uwe
            where
                deleted_at is null
                and learning_user_id in {tuple(user_ids)};
        """

    columns = [
        "user_id",
        "title",
        "employment_type",
        "description",
        "organisation_name",
        "organisation_type",
        "other_organisation_type",
        "country_id",
        "state_id",
        "currently_working",
        "start_date",
        "end_date",
        "salary",
        "currency_id",
        "curriculum_id",
        "teaching_level_id",
        "teaching_role_id",
        "subjects",
    ]
    gravity_columns = {
        "countries": "country_id",
        "states": "state_id",
        "currencies": "currency_id",
        "curriculum": "curriculum_id",
        "teaching_levels": "teaching_level_id",
        "teaching_roles": "teaching_role_id",
    }
    return get_data(query, columns, gravity_columns)


def chunking(data, size):
    for i in range(0, len(data), size):
        print(i)
        yield data[i : i + size]


def main():
    user_uuid_df = pd.read_csv(r"D:\Workspace\vector-search\src\data\user_uuids.csv")
    user_id_list = get_users(user_uuid_df["user_uuid"].to_list())
    for user_ids in chunking(user_id_list, 1000):
        data = []
        preferred_work_locations = get_preferred_work_locations(user_ids)
        user_awards = get_user_awards(user_ids)
        user_certifications = get_user_certifications(user_ids)
        user_computed_fields = get_user_computed_fields(user_ids)
        user_interests = get_user_interests(user_ids)
        user_languages = get_user_languages(user_ids)
        user_profiles = get_user_profiles(user_ids)
        user_projects = get_user_projects(user_ids)
        user_publications = get_user_publications(user_ids)
        user_qualifications = get_user_qualifications(user_ids)
        user_skills = get_user_skills(user_ids)
        user_subject_experiences = get_user_subject_experiences(user_ids)
        user_subject_interests = get_user_subject_interests(user_ids)
        user_test_scores = get_user_test_scores(user_ids)
        user_work_experiences = get_user_work_experiences(user_ids)
        for user_id in user_ids:
            data.append(
                {
                    "user_id": user_id,
                    "preferred_work_locations": preferred_work_locations.get(
                        user_id, []
                    ),
                    "user_awards": user_awards.get(user_id, []),
                    "user_certifications": user_certifications.get(user_id, []),
                    "user_computed_fields": user_computed_fields.get(user_id, []),
                    "user_interests": user_interests.get(user_id, []),
                    "user_languages": user_languages.get(user_id, []),
                    "user_profiles": user_profiles.get(user_id, []),
                    "user_projects": user_projects.get(user_id, []),
                    "user_publications": user_publications.get(user_id, []),
                    "user_qualifications": user_qualifications.get(user_id, []),
                    "user_skills": user_skills.get(user_id, []),
                    "user_subject_experiences": user_subject_experiences.get(
                        user_id, []
                    ),
                    "user_subject_interests": user_subject_interests.get(user_id, []),
                    "user_test_scores": user_test_scores.get(user_id, []),
                    "user_work_experiences": user_work_experiences.get(user_id, []),
                }
            )
        with open(r"D:\Workspace\vector-search\src\data\data.json", "a") as file:
            file.write(json.dumps(data))


if __name__ == "__main__":
    main()
