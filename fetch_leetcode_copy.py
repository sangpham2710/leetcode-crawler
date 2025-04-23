#!/usr/bin/env python3

import requests
import json
import csv
import sys
import time
from collections import defaultdict

# --- CONFIG ---
# Full cookie string from your working curl command's -b argument
# IMPORTANT: Keep this up-to-date if your session expires
FULL_COOKIE = 'gr_user_id=845f8890-da65-4d47-930d-8ee069a9eaa9; 87b5a3c3f1a55520_gr_last_sent_cs1=sanglythesis; __stripe_mid=f2208bfd-fb64-4949-bfe0-e95de794b3e42a34d1; cf_clearance=uN96HjGbxcPYcxYsezMlw6etWe7EHcBJZftvpA7H8yg-1744469424-1.2.1.1-QoGQJX4sn47epc7yeckg2vQ.gQB8ZzLsi_T5B_tIN6_XIGmA_QLE9hfjFl_QBqiLLyKG2jv5q6Yz.3zRuadMb9TjGxyat7X1IVzFUj4UohyehGha8rEAwaqx7MJd74O_lDNyFIIk2156X1haXBIXDkt5gl1O_RhWdaWhkDjdGr__nnlRdzHss7J4_3_dBy8qX1OJl2D30m0VlH7qLRxfRGFU5CPXbHed2LI91_UHN5sr41MhSCJovmPKss8yeIMYvP2Wb4iaYre4OXKo1BVdlPmTxX6N7JlBEd3rIH2WoUuY4oEH0i9sGqDtVPZs_fF5omqEyAa.BKo4RBuRdZGevJXCdipMqD88Ly2LywN0ji5vN2Tk.0sufSLcW7VDoe07; csrftoken=qCV7HFIWihz6NOKyrxL5gIFLR23zelsxvetapxDdJEnNZ1j56qLRH6LOJDn83kU0; _gid=GA1.2.1476346995.1745157752; INGRESSCOOKIE=8c327cf6cf4f169c9d94b0394e86bc92|8e0876c7c1464cc0ac96bc2edceabd27; ip_check=(false, "101.53.53.147"); _gcl_au=1.1.905053711.1745321970; _ga_DKXQ03QCVK=GS1.1.1745337312.2.0.1745337312.60.0.0; 87b5a3c3f1a55520_gr_session_id=24835e12-89c3-498f-abc3-33aad241a396; 87b5a3c3f1a55520_gr_last_sent_sid_with_cs1=24835e12-89c3-498f-abc3-33aad241a396; 87b5a3c3f1a55520_gr_session_id_sent_vst=24835e12-89c3-498f-abc3-33aad241a396; LEETCODE_SESSION=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJfYXV0aF91c2VyX2lkIjoiMTcxOTYxNTIiLCJfYXV0aF91c2VyX2JhY2tlbmQiOiJkamFuZ28uY29udHJpYi5hdXRoLmJhY2tlbmRzLk1vZGVsQmFja2VuZCIsIl9hdXRoX3VzZXJfaGFzaCI6ImQ4ZWI3OWM0MWUwMDE0MzNjZjA1ZmU3YTkwNjI3ZGFiY2NiMTlhYjg5YTBlM2ExMzlhYWNkMTc2ODE4ZjNlMGMiLCJzZXNzaW9uX3V1aWQiOiJlY2RkM2RiZiIsImlkIjoxNzE5NjE1MiwiZW1haWwiOiJzYW5nbHl0aGVzaXNAZ21haWwuY29tIiwidXNlcm5hbWUiOiJzYW5nbHl0aGVzaXMiLCJ1c2VyX3NsdWciOiJzYW5nbHl0aGVzaXMiLCJhdmF0YXIiOiJodHRwczovL2Fzc2V0cy5sZWV0Y29kZS5jb20vdXNlcnMvZGVmYXVsdF9hdmF0YXIuanBnIiwicmVmcmVzaGVkX2F0IjoxNzQ1MzM3MzEzLCJpcCI6IjU4LjE4Ny4xODQuMTM0IiwiaWRlbnRpdHkiOiJkNmRjYzBhNmRlZjU1ODJmOGQzYTlmN2YyYWRkYjg4YiIsImRldmljZV93aXRoX2lwIjpbIjMyODQ4ODZiOWY1ZGNjYjA2YmJlZjZhMDczOWUyYzU0IiwiNTguMTg3LjE4NC4xMzQiXSwiX3Nlc3Npb25fZXhwaXJ5IjoxMjA5NjAwfQ.-YRmFNt9vav8h2M7HNOKdXr1jdMd_pIlkBRfrBz0ptQ; _ga=GA1.1.322647468.1743747905; 87b5a3c3f1a55520_gr_cs1=sanglythesis; _ga_CDRWKZTDEX=GS1.1.1745337312.29.1.1745337480.52.0.0'

# Use only google for testing, as requested in user edits
COMPANIES = [
    "facebook",
    "google",
    "amazon",
    "uber",
    "google",
    "oracle",
    "apple",
    "linkedin",
    "microsoft",
    "tiktok",
    "goldman-sachs",
    "bloomberg",
    "snapchat",
    "walmart-labs",
    "adobe",
    "atlassian",
    "salesforce",
    "airbnb",
    "citadel",
    "doordash",
    "nvidia",
]
URL = "https://leetcode.com/graphql/"
OUTFILE = "leetcode_company_problems.csv"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"

# Extract CSRF token and UUUserID from cookie for headers (might be redundant but matches examples)
X_CSRF_TOKEN = next(
    (c.split("=")[1] for c in FULL_COOKIE.split(";") if "csrftoken=" in c.strip()), None
)
UUUSERID = next(
    (
        c.split("=")[1]
        for c in FULL_COOKIE.split(";")
        if "LEETCODE_SESSION=" in c.strip()
    ),
    None,
)  # Assuming UUUserID relates to session somehow or find the actual cookie for it
if UUUSERID:
    # Attempt to find the actual uuuserid if present (this logic might need adjustment based on actual cookie name)
    # Using a placeholder value based on the bash script for now if not found directly
    UUUSERID = "3284886b9f5dccb06bbef6a0739e2c54"
# ---------------

# --- Global Headers ---
BASE_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Cookie": FULL_COOKIE,
    "Origin": "https://leetcode.com",
    "User-Agent": USER_AGENT,
    "authorization": ";",  # Send header key with empty value like in example
}
if X_CSRF_TOKEN:
    BASE_HEADERS["x-csrftoken"] = X_CSRF_TOKEN
if UUUSERID:
    BASE_HEADERS["uuuserid"] = UUUSERID
# ----------------------


def make_request(payload, referer):
    """Makes a POST request to the LeetCode GraphQL endpoint."""
    headers = BASE_HEADERS.copy()
    headers["Referer"] = referer
    try:
        response = requests.post(URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        if "errors" in data:
            print(f"🚨 GraphQL error: {json.dumps(data['errors'])}", file=sys.stderr)
            return None
        return data
    except requests.exceptions.RequestException as e:
        print(f"🚨 Request failed: {e}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"🚨 Failed to decode JSON response: {e}", file=sys.stderr)
        print(f"Raw response text: {response.text[:500]}...", file=sys.stderr)
        return None


def main():
    """Main function to fetch and process LeetCode data."""
    # Use defaultdict for easy handling of new entries
    # Key: (company, slug), Value: {'title': str, 'difficulty': str, 'frequency': float, 'tags': str, 'categories': set}
    problem_data = defaultdict(lambda: {"categories": set()})

    for company in COMPANIES:
        print(f"[INFO] Processing company: {company}")
        # --- TEST LIMIT --- #
        # if company != 'google':
        #     print(f"  [DEBUG] Skipping {company} for test.")
        #     continue
        # --- END TEST LIMIT --- #

        # 1. Fetch Categories for the company
        print(f"  [DEBUG] Fetching categories for {company}")
        category_payload = {
            "query": "query favDetail($s:String!){favoriteDetailV2(favoriteSlug:$s){generatedFavoritesInfo{categoriesToSlugs{favoriteSlug}}}}",
            "variables": {"s": company},
        }
        referer_cat = f"https://leetcode.com/"
        cat_response = make_request(category_payload, referer_cat)

        if not cat_response or not cat_response.get("data", {}).get("favoriteDetailV2"):
            print(
                f"  [WARN] Could not fetch or parse categories for {company}. Skipping.",
                file=sys.stderr,
            )
            continue

        categories_info = cat_response["data"]["favoriteDetailV2"].get(
            "generatedFavoritesInfo", {}
        )
        category_slugs = [
            item["favoriteSlug"]
            for item in categories_info.get("categoriesToSlugs", [])
            if item and "favoriteSlug" in item
        ]

        if not category_slugs:
            print(f"  [INFO] No categories found for {company}.")
            continue

        # 2. Fetch Questions for each Category
        for category_slug in category_slugs:
            # --- TEST LIMIT --- #
            # if category_slug != 'google-thirty-days':
            #     print(f"    [DEBUG] Skipping category {category_slug} for test.")
            #     continue
            # --- END TEST LIMIT --- #
            print(f"    [DEBUG] Processing category: {category_slug}")
            limit = 100  # Match the limit used in the payload variables below
            print(f"      [DEBUG] Fetching questions (skip=0, limit={limit})")
            # Updated query to match the second curl command (more fields, filter, version)
            query_string = """
                query favoriteQuestionList(
                    $favoriteSlug: String!,
                    $skip: Int!,
                    $limit: Int!,
                    $filter: FavoriteQuestionFilterInput,
                    $filtersV2: QuestionFilterInput,
                    $sortBy: QuestionSortByInput,
                    $searchKeyword: String,
                    $version: String = "v2"
                ) {
                    favoriteQuestionList(
                        favoriteSlug: $favoriteSlug,
                        filter: $filter,
                        filtersV2: $filtersV2,
                        sortBy: $sortBy,
                        limit: $limit,
                        skip: $skip,
                        searchKeyword: $searchKeyword,
                        version: $version
                    ) {
                        questions {
                            difficulty
                            id
                            paidOnly
                            questionFrontendId
                            status
                            title
                            titleSlug
                            translatedTitle
                            isInMyFavorites
                            frequency
                            acRate
                            topicTags {
                                name
                                nameTranslated
                                slug
                            }
                        }
                        totalLength
                        hasMore
                    }
                }
            """
            questions_payload = {
                "operationName": "favoriteQuestionList",
                "query": query_string,
                "variables": {
                    "favoriteSlug": category_slug,
                    "skip": 0,
                    "limit": limit,  # Use the limit variable defined above
                    "filter": None,  # Added from second curl (assuming null)
                    "filtersV2": {
                        "filterCombineType": "ALL",
                        "statusFilter": {"questionStatuses": [], "operator": "IS"},
                        "difficultyFilter": {"difficulties": [], "operator": "IS"},
                        "languageFilter": {"languageSlugs": [], "operator": "IS"},
                        "topicFilter": {"topicSlugs": [], "operator": "IS"},
                        "acceptanceFilter": {},
                        "frequencyFilter": {},
                        "lastSubmittedFilter": {},
                        "publishedFilter": {},
                        "companyFilter": {"companySlugs": [], "operator": "IS"},
                        "positionFilter": {"positionSlugs": [], "operator": "IS"},
                        "premiumFilter": {"premiumStatus": [], "operator": "IS"},
                    },
                    "searchKeyword": "",
                    "sortBy": {"sortField": "AC_RATE", "sortOrder": "DESCENDING"},
                    "version": "v2",  # Added from second curl
                },
            }
            referer_q = (
                f"https://leetcode.com/company/{company}/?favoriteSlug={category_slug}"
            )

            # --- DEBUG REQUEST --- #
            # print("--- Request Details for Questions ---")
            # print(f"URL: {URL}")
            # # Construct headers for printing (as used by make_request)
            # request_headers = BASE_HEADERS.copy()
            # request_headers["Referer"] = referer_q
            # print(f"Headers: {json.dumps(request_headers, indent=2)}")
            # print(f"Payload: {json.dumps(questions_payload, indent=2)}")
            # print("--- End Request Details ---")
            # print("[INFO] Exiting after printing request details for debugging.")
            # sys.exit(0)
            # --- END DEBUG REQUEST ---

            q_response = make_request(questions_payload, referer_q)

            if (
                not q_response
                or "data" not in q_response
                or "favoriteQuestionList" not in q_response["data"]
            ):
                print(
                    f"      [ERROR] Failed to fetch or parse questions for {category_slug}. Skipping category.",
                    file=sys.stderr,
                )
                time.sleep(1)
                continue

            question_list_data = q_response["data"]["favoriteQuestionList"]
            questions = question_list_data.get("questions", [])
            count = len(questions)
            print(f"      [DEBUG] Received count={count} questions for {category_slug}")

            # Process received questions (only runs if count > 0)
            # processed_first = False # Flag to print only first item
            for item in questions:
                # --- DEBUG PRINT ITEM --- #
                # if not processed_first:
                #     print(f"      [DEBUG ITEM] First item received: {item}")
                #     processed_first = True
                # --- END DEBUG PRINT ITEM --- #

                slug = item.get("titleSlug")
                if not slug:
                    print(
                        f"      [WARN] Skipping item with missing titleSlug: {item}",
                        file=sys.stderr,
                    )
                    continue

                key = (company, slug)
                problem_data[key]["title"] = item.get("title", "")
                problem_data[key]["difficulty"] = item.get("difficulty", "")
                problem_data[key]["frequency"] = item.get(
                    "frequency"
                )  # Keep as number or None
                problem_data[key]["acRate"] = item.get("acRate")
                problem_data[key]["tags"] = ";".join(
                    tag.get("slug", "") for tag in item.get("topicTags", []) if tag
                )
                problem_data[key]["categories"].add(category_slug)

            time.sleep(1)  # Small delay between categories

    # 3. Write data to CSV
    print(f"[INFO] Writing data to {OUTFILE}")
    try:
        with open(OUTFILE, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "company",
                "question_id",
                "title",
                "difficulty",
                "tags",
                "frequency",
                "acRate",
                "categories",
                "url",
            ]
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL
            )

            writer.writeheader()
            for (company, slug), data in problem_data.items():
                # Handle potential None for numeric fields before writing
                ac_rate_val = data.get("acRate")
                freq_val = data.get("frequency")

                # --- DEBUG PRINT --- #
                # print(f"  [DEBUG CSV WRITE] slug: {slug}, acRate value: {ac_rate_val}, type: {type(ac_rate_val)}")
                # --- END DEBUG PRINT --- #

                writer.writerow(
                    {
                        "company": company,
                        "question_id": slug,  # Using slug as question_id like in bash script
                        "title": data.get("title", ""),
                        "difficulty": data.get("difficulty", ""),
                        "tags": data.get("tags", ""),
                        "frequency": freq_val if freq_val is not None else "",
                        "acRate": (
                            ac_rate_val if ac_rate_val is not None else ""
                        ),  # Write float or empty string
                        "categories": ";".join(
                            sorted(list(data["categories"]))
                        ),  # Sort categories for consistent output
                        "url": f"https://leetcode.com/problems/{slug}/",
                    }
                )
        print(f"✅ Done. See {OUTFILE}")
    except IOError as e:
        print(f"🚨 Failed to write CSV file {OUTFILE}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
