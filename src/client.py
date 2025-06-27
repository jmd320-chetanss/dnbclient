import json
import base64
import time
import urllib
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import *

API_KEY = "12345678910abcd"
API_SECRET = "12345678910abcd"

MINIMUM_CONFIDENCE = 5
REGISTRATION_ID = "#############"


class Client:

    def __init__(self, spark: SparkSession, key: str, secret: str):
        self.spark = spark
        self.key = key
        self.secret = secret
        self.api_auth_url = "https://plus.dnb.com/v2/token"
        self.api_url = "https://plus.dnb.com/v1"
        self.auth_token = self.__define_auth_token()
        self.matched_records = None

        try:
            self.session_token = self.__auth_proc()["access_token"]
            print("Succesfully authenticated to the D&B API.")
        except Exception as error:
            print(
                "Authentication Error:\nWe could not authenticate to the D&B API. Please check your credentials."
            )

    schema = StructType(
        [
            StructField("update_date", DateType(), True),
            StructField("accepted", BooleanType(), True),
            StructField("input_id", LongType(), True),
            StructField("input_company_name", StringType(), True),
            StructField("input_streetaddress", StringType(), True),
            StructField("input_city", StringType(), True),
            StructField("input_country", StringType(), True),
            StructField("dnb_duns", StringType(), True),
            StructField("dnb_name", StringType(), True),
            StructField("dnb_nameMatchScore", DoubleType(), True),
            StructField("dnb_status", StringType(), True),
            StructField("dnb_streetaddress", StringType(), True),
            StructField("dnb_city", StringType(), True),
            StructField("dnb_country", StringType(), True),
            StructField("dnb_confidenceCode", LongType(), True),
            StructField("MG_Name", StringType(), True),
            StructField("MG_StreetNumber", StringType(), True),
            StructField("MG_StreetName", StringType(), True),
            StructField("MG_City", StringType(), True),
            StructField("MG_State", StringType(), True),
            StructField("MG_PObox", StringType(), True),
            StructField("MG_Phone", StringType(), True),
            StructField("MG_PostCode", StringType(), True),
            StructField("MG_Density", StringType(), True),
            StructField("MG_Uniqueness", StringType(), True),
            StructField("MG_SIC", StringType(), True),
            StructField("MDP_Name", StringType(), True),
            StructField("MDP_StreetNumber", StringType(), True),
            StructField("MDP_StreetName", StringType(), True),
            StructField("MDP_City", StringType(), True),
            StructField("MDP_State", StringType(), True),
            StructField("MDP_PObox", StringType(), True),
            StructField("MDP_Phone", StringType(), True),
            StructField("MDP_PostCode", StringType(), True),
            StructField("MDP_DUNS", StringType(), True),
            StructField("MDP_SIC", StringType(), True),
            StructField("MDP_Density", StringType(), True),
            StructField("MDP_Uniqueness", StringType(), True),
            StructField("MDP_NationalID", StringType(), True),
            StructField("MDP_URL", StringType(), True),
            StructField("nameMatchScore", DoubleType(), True),
            StructField("matchGradeComponentsCount", IntegerType(), True),
            StructField("confidenceCode", IntegerType(), True),
            StructField("isPrimary", BooleanType(), True),
            StructField("phoneMatch", BooleanType(), True),
            StructField("isExec", BooleanType(), True),
            StructField("urlMatch", BooleanType(), True),
            StructField("exactName", BooleanType(), True),
            StructField("strongName", BooleanType(), True),
            StructField("closeName", BooleanType(), True),
            StructField("regNoMatch", BooleanType(), True),
            StructField("isRegistered", BooleanType(), True),
            StructField("postCodeMatch", BooleanType(), True),
            StructField("addressMatchStrong", BooleanType(), True),
            StructField("addressMatchLoose", BooleanType(), True),
            StructField(
                "customMatchGrade", StringType(), True
            ),  # [Todo]change name to match summary
        ]
    )

    def __define_auth_token(self):
        """
        This function will generate the authentication token for the API This is simple creating an base64 encoded string from the API key and secret
        """
        key_sec = f"{self.key}:{self.secret}"
        encoded_creds = base64.b64encode(str(key_sec).encode("ascii")).decode(
            "ascii"
        )  # Base 64 activation
        return encoded_creds

    def __auth_proc(self):
        """
        This function will create the session token for all api calls calls
        """
        payload = {"grant_type": "client_credentials"}
        headers = {
            "Authorization": f"Basic {self.auth_token}",
        }
        response = requests.request(
            "POST", self.api_auth_url, headers=headers, json=payload
        )
        return response.json()

    def match_dnb(self, record):
        """
        This is the primary API call for each company in the input dataframe. This will package returns a JSON object with the match results
        For complete Match API details, please refer to our documentation website, https://directplus.documentation.dnb.com/openAPI.html?apiID=IDRCleanseMatch
        """

        endpoint = f"{self.api_url}/match/cleanseMatch?"
        params = {
            "candidateMaximumQuantity": 1,
            "name": record["name"],
            "streetAddressLine1": record["streetaddress"],
            "street&addressLocality": record["city"],
            "addressRegion": record["state"],
            "postalCode": record["postal_code"],
            "countryISOAlpha2Code": record["country"],
        }

        payload = {}
        headers = {
            "Authorization": f"Bearer {self.session_token}",
        }
        params = urllib.parse.urlencode(params)
        url2 = endpoint + params

        response = requests.request("GET", url2, headers=headers, data=payload)
        json_resp = response.json()
        return json_resp

    def match_and_cleanse(self, input_df, minimum_confidence=MINIMUM_CONFIDENCE):
        """
        This method updates two object properties with dataframes. The first is a list of all the records that have a match confidence score of defined by the user (default is 7) or higher. The second is a list of all the records that have a match confidence score lower than the user defined score.
        """

        # check input minimum confidence value
        if minimum_confidence > 10:
            raise Exception(
                "The minimum confidence needs to be a number between 1 and 10"
            )

        matched_records_list = []
        data_collect = input_df.collect()
        for row in data_collect:
            results = self.match_dnb(row)
            input_data = results["inquiryDetail"]
            for match in results["matchCandidates"]:
                t = match["organization"]
                # set the accepted value
                if (
                    match["matchQualityInformation"]["confidenceCode"]
                    >= minimum_confidence
                ):
                    accepted = True
                else:
                    accepted = False
                # create the dictionary of results
                matched_cos = {
                    "update_date": date.today(),
                    "input_id": row["source_id"],
                    "input_company_name": row["name"],
                    "input_streetaddress": row["streetaddress"],
                    "input_city": row["city"],
                    "input_country": row["country"],
                    "dnb_duns": t["duns"],
                    "dnb_name": t["primaryName"],
                    "dnb_streetaddress": t["primaryAddress"]["streetAddress"].get(
                        "line1", None
                    ),
                    "dnb_city": t["primaryAddress"]["addressLocality"].get(
                        "name", None
                    ),
                    "dnb_country": t["primaryAddress"]["addressCountry"].get(
                        "isoAlpha2Code", None
                    ),
                    "dnb_status": t["dunsControlStatus"]["operatingStatus"][
                        "description"
                    ],
                    "dnb_confidenceCode": match["matchQualityInformation"][
                        "confidenceCode"
                    ],
                    "accepted": accepted,
                    "dnb_nameMatchScore": match["matchQualityInformation"][
                        "nameMatchScore"
                    ],
                    "dnb_matchgradeComponents": match["matchQualityInformation"][
                        "matchGradeComponents"
                    ],
                    "dnb_matchDataProfileComponents": match["matchQualityInformation"][
                        "matchDataProfileComponents"
                    ],
                }
                matched_records_list.append(matched_cos)

        print(
            f"All records have been processed.\nThe result set is {len(matched_records_list)} records."
        )

        return self.spark.createDataFrame(matched_records_list, schema=self.schema)

    def export_monitoring_registrations(self, reg_id: str):
        """
        This method will add the input dataframe to the monitoring endpoint. The reg_id is the unique identifier for the monitoring endpoint
        """

        endpoint = f"{self.api_url}/monitoring/registrations/{reg_id}/subjects"
        payload = {}
        headers = {
            "Authorization": f"Bearer {self.session_token}",
        }
        response = requests.request("GET", endpoint, headers=headers)
        json_resp = response.json()

        print(json_resp)

    def add_to_monitoring(self, duns: str | list[str], reg_id: str) -> None:
        """
        This method will add the input dataframe to the monitoring endpoint. The reg_id is the unique identifier for the monitoring endpoint
        """

        assert isinstance(duns, str) or (
            isinstance(duns, list) and all(isinstance(d, str) for d in duns)
        ), "All DUNS must be strings."

        duns_list = duns if isinstance(duns, list) else [duns]

        already_registered_count = 0
        successfully_added_count = 0

        for duns in duns_list:
            endpoint = (
                f"{self.api_url}/monitoring/registrations/{reg_id}/duns/{duns}"
            )
            payload = {}
            headers = {
                "Authorization": f"Bearer {self.session_token}",
            }
            url2 = endpoint

            response = requests.request("POST", url2, headers=headers)
            json_resp = response.json()
            # Check for 'error' key and the specific error message
            if "error" in json_resp and json_resp["error"].get("errorCode") == "21012":
                already_registered_count += 1
            elif (
                "information" in json_resp
                and json_resp["information"].get("code") == "21113"
            ):
                successfully_added_count += 1

        message = (
            f"Processed {already_registered_count + successfully_added_count} records.\n"
            f"{already_registered_count} were already registered.\n"
            f"{successfully_added_count} were successfully added to monitoring."
        )

        print(message)
        return

    def delete_from_monitoring(self, duns: str | list[str], reg_id):
        """
        This method will delete the input dataframe to the monitoring endpoint. The reg_id is the unique identifier for the monitoring endpoint
        """

        assert isinstance(duns, str) or (
            isinstance(duns, list) and all(isinstance(d, str) for d in duns)
        ), "All DUNS must be strings."

        duns_list = duns if isinstance(duns, list) else [duns]

        for duns in duns_list:
            endpoint = (
                f"{self.api_url}/monitoring/registrations/{reg_id}/duns/{duns}"
            )
            payload = {}
            headers = {
                "Authorization": f"Bearer {self.session_token}",
            }
            url2 = endpoint

            response = requests.request("DELETE", url2, headers=headers)
            json_resp = response.json()
            print(endpoint)
            print(json_resp)

    def append_data(self, input_df):
        """
        This method will use the input dataframe and call the Data Blocks API endpoint then store the data.
        """

        count = 0
        append_records_list = []

        for row in input_df.collect():
            duns = row["dnb_duns"]
            """
            Each ###BLOCK### needs to be replaced with client-specific Data Blocks, i.e., companyinfo_L2_v1
            """
            endpoint = f"{self.api_url}/data/duns/{duns}?blockIDs=###BLOCK###%2C###BLOCK###%2C###BLOCK###"
            payload = {}
            headers = {
                "Authorization": f"Bearer {self.session_token}",
            }
            url2 = endpoint

            response = requests.request("GET", url2, headers=headers)
            json_resp = response.json()
            count += 1
            append_cos = {
                "update_date": date.today(),
                "D-U-N-S_NUMBER": duns,
                "append_data": json_resp,
            }

            append_records_list.append(append_cos)

        self.appended_records = self.spark.createDataFrame(
            append_records_list, schema=self.schema
        )
        # message = (f"Processed {count} records."

        return print(
            f"All records have been processed.\nThe result set is {len(append_records_list)} records which can be accessed using the appended_records object property"
        )

    def adv_match_and_cleanse(self, input_df, minimum_confidence=MINIMUM_CONFIDENCE):
        """
        Processes each row in the input dataframe by matching it against D&B records
        and storing results with match details and confidence scores.
        """

        if not (1 <= minimum_confidence <= 10):
            raise ValueError("Minimum confidence must be between 1 and 10.")

        matched_records_list = []
        today = date.today()

        for row in input_df.collect():
            results = self.match_dnb(row)
            for match in results.get("matchCandidates", []):
                org = match["organization"]
                addr = org["primaryAddress"]
                addr_locality = addr.get("addressLocality", {})
                addr_country = addr.get("addressCountry", {})
                confidence = match["matchQualityInformation"]["confidenceCode"]
                processed_match = self.processMatch(match, row)

                matched_cos = {
                    "update_date": today,
                    "input_id": row["source_id"],
                    "input_company_name": row["name"],
                    "input_streetaddress": row["streetaddress"],
                    "input_city": row["city"],
                    "input_country": row["country"],
                    "dnb_duns": org.get("duns"),
                    "dnb_name": org.get("primaryName"),
                    "dnb_streetaddress": addr.get("streetAddress", {}).get("line1"),
                    "dnb_city": addr_locality.get("name"),
                    "dnb_country": addr_country.get("isoAlpha2Code"),
                    "dnb_status": org.get("dunsControlStatus", {})
                    .get("operatingStatus", {})
                    .get("description"),
                    "dnb_confidenceCode": confidence,
                    "accepted": confidence >= minimum_confidence,
                    "dnb_nameMatchScore": match["matchQualityInformation"].get(
                        "nameMatchScore"
                    ),
                    # "dnb_matchgradeComponents": match["matchQualityInformation"].get("matchGradeComponents"),
                    # "dnb_matchDataProfileComponents": match["matchQualityInformation"].get("matchDataProfileComponents"),
                }

                matched_records_list.append({**processed_match, **matched_cos})

        self.matched_records = self.spark.createDataFrame(
            matched_records_list, schema=self.schema
        )

        print(
            f"All records have been processed.\nThe result set contains {len(matched_records_list)} records and can be accessed via the matched_records object property."
        )

    def processMatch(self, record, input_record):
        """
        Extracts and returns a dictionary of matchGrade, matchDataProfile components,
        and all derived boolean indicators and the final customMatchGrade.

        This is executed using the Advanced Match process only.
        ---USER ACCEPTS ALL RISKS INVOLVED WITH ACCEPTED DUNS USING THIS POLICY---
        """

        # Basic fields
        match_quality = record["matchQualityInformation"]
        organization = record["organization"]
        primary_address = organization.get("primaryAddress", {})
        iso_code = primary_address.get("addressCountry", {}).get("isoAlpha2Code")

        nameMatchScore = match_quality.get("nameMatchScore", 0)
        matchGradeComponentsCount = match_quality.get("matchGradeComponentsCount", 0)
        confidenceCode = match_quality.get("confidenceCode", 0)

        # Parse matchGrade
        matchGrade = match_quality.get("matchGrade", "").ljust(11, "F")
        mg_keys = [
            "MG_Name",
            "MG_StreetNumber",
            "MG_StreetName",
            "MG_City",
            "MG_State",
            "MG_PObox",
            "MG_Phone",
            "MG_PostCode",
            "MG_Density",
            "MG_Uniqueness",
            "MG_SIC",
        ]
        mg_dict = dict(zip(mg_keys, list(matchGrade)))

        # Parse matchDataProfile
        mdp = match_quality.get("matchDataProfile", "").ljust(28, "F")
        mdp_keys = [
            "MDP_Name",
            "MDP_StreetNumber",
            "MDP_StreetName",
            "MDP_City",
            "MDP_State",
            "MDP_PObox",
            "MDP_Phone",
            "MDP_PostCode",
            "MDP_DUNS",
            "MDP_SIC",
            "MDP_Density",
            "MDP_Uniqueness",
            "MDP_NationalID",
            "MDP_URL",
        ]
        mdp_values = [mdp[i : i + 2] for i in range(0, 28, 2)]
        mdp_dict = dict(zip(mdp_keys, mdp_values))

        # Logic flags
        isPrimary = mdp_dict["MDP_Name"] in ("00", "01")
        phoneMatch = mg_dict["MG_Phone"] == "A"
        isExec = mdp_dict["MDP_Name"] == "03"
        urlMatch = mdp_dict["MDP_URL"] == "00"

        suppliedName = input_record["name"].upper()
        matchName = organization.get("primaryName", "").upper()
        exactName = suppliedName == matchName

        strongName = (nameMatchScore >= 60 and matchGradeComponentsCount > 7) or (
            nameMatchScore >= 70 and matchGradeComponentsCount == 7
        )
        closeName = mg_dict["MG_Name"] in ("A", "B")
        regNoMatch = (mg_dict["MG_Name"] == "Z" and confidenceCode == 10) or mdp_dict[
            "MDP_Name"
        ] == "20"
        isRegistered = bool(organization.get("registrationNumbers"))

        postCodeMatch = input_record["postal_code"] == primary_address.get("postalCode")

        # Address match logic
        valid_country = iso_code in ("GB", "NL")
        valid_street_number = mg_dict["MG_StreetNumber"] in ("A", "B")
        valid_street_name = mg_dict["MG_StreetName"] in ("A", "B")
        valid_city = mg_dict["MG_City"] in ("A", "B")
        valid_state = mg_dict["MG_State"] in ("A", "B")
        valid_postcode = mg_dict["MG_PostCode"] in ("A", "B")

        addressMatchStrong = (
            valid_country
            and (
                (valid_street_number and (postCodeMatch or valid_state))
                or (
                    valid_street_number
                    and valid_city
                    and (postCodeMatch or valid_postcode)
                )
                or (
                    valid_street_number
                    and valid_street_name
                    and (postCodeMatch or valid_postcode)
                )
            )
        ) or (
            mg_dict["MG_StreetNumber"] == "A"
            and mg_dict["MG_StreetName"] == "A"
            and valid_city
        )

        addressMatchLoose = any(
            [
                valid_street_number and valid_street_name,
                valid_street_number and valid_city,
                valid_street_name and valid_city,
                valid_street_number
                and valid_street_name
                and (postCodeMatch or valid_postcode),
                valid_street_number and (postCodeMatch or valid_postcode),
                valid_street_name and (postCodeMatch or valid_postcode),
                valid_city and (postCodeMatch or valid_postcode),
            ]
        )

        # Custom match grade logic
        any_name_match = exactName or strongName or closeName
        any_address_match = addressMatchStrong or addressMatchLoose

        customMatchGrade = "UNC"
        if regNoMatch and any_name_match and any_address_match:
            customMatchGrade = "SIT1"
        elif exactName and any_address_match:
            customMatchGrade = "SIT2"
        elif regNoMatch and any_name_match:
            customMatchGrade = "REG1"
        elif strongName and any_address_match:
            customMatchGrade = "SIT3"
        elif closeName and addressMatchStrong:
            customMatchGrade = "SIT4"
        elif (strongName or exactName) and phoneMatch:
            customMatchGrade = "CMP1"
        elif closeName and phoneMatch:
            customMatchGrade = "CMP2"
        elif exactName and isRegistered:
            customMatchGrade = "CMP3"
        elif exactName:
            customMatchGrade = "CMP4"
        elif strongName and (valid_city or valid_postcode):
            customMatchGrade = "CMP5"
        elif regNoMatch and not any_name_match:
            customMatchGrade = "REG2"
        elif strongName:
            customMatchGrade = "CMP7"
        elif closeName:
            customMatchGrade = "CMP8"
        elif urlMatch:
            customMatchGrade = "URL1"
        elif any_address_match and not any_name_match:
            customMatchGrade = "GEO1"

        # Combine everything into one return dictionary
        result = {
            **mg_dict,
            **mdp_dict,
            "nameMatchScore": nameMatchScore,
            "matchGradeComponentsCount": matchGradeComponentsCount,
            "confidenceCode": confidenceCode,
            "isPrimary": isPrimary,
            "phoneMatch": phoneMatch,
            "isExec": isExec,
            "urlMatch": urlMatch,
            "exactName": exactName,
            "strongName": strongName,
            "closeName": closeName,
            "regNoMatch": regNoMatch,
            "isRegistered": isRegistered,
            "postCodeMatch": postCodeMatch,
            "addressMatchStrong": addressMatchStrong,
            "addressMatchLoose": addressMatchLoose,
            "customMatchGrade": customMatchGrade,
        }

        return result
