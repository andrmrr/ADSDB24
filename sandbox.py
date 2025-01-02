{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": []
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "b9JNOEUQWUOK",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "d7cd04b0-ac9f-4f31-dff7-caf99f2006b9"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Mounted at /content/drive\n"
          ]
        }
      ],
      "source": [
        "from google.colab import drive\n",
        "drive.mount('/content/drive')"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "!pip install --quiet duckdb-engine\n",
        "!pip install --quiet pandas"
      ],
      "metadata": {
        "id": "biMQQXSEa8e8",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "737a22e6-785c-4cc7-cae3-5dfbc3e9657f"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "\u001b[?25l   \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m0.0/49.3 kB\u001b[0m \u001b[31m?\u001b[0m eta \u001b[36m-:--:--\u001b[0m\r\u001b[2K   \u001b[90m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u001b[0m \u001b[32m49.3/49.3 kB\u001b[0m \u001b[31m1.8 MB/s\u001b[0m eta \u001b[36m0:00:00\u001b[0m\n",
            "\u001b[?25h"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "import pandas as pd\n",
        "import numpy as np\n",
        "\n",
        "import duckdb\n",
        "import json\n",
        "import os"
      ],
      "metadata": {
        "id": "xAa3JEiqbGlg"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "base_dir = \"/content/drive/MyDrive/ADSDB24\"\n",
        "temporal_landing = os.path.join(base_dir,  \"landing_zone/temporal\")\n",
        "persistent_landing = os.path.join(base_dir, \"landing_zone/persistent\")\n",
        "dataset_folder = \"datasets_specific\"\n",
        "\n",
        "\"\"\"DuckDB\"\"\"\n",
        "duckdb_folder = \"duckdb_database\"\n",
        "duckdb_formatted = os.path.join(duckdb_folder, \"formatted_zone.db\")\n",
        "duckdb_trusted = os.path.join(duckdb_folder, \"trusted_zone.db\")\n",
        "duckdb_exploitation = os.path.join(duckdb_folder, \"exploitation_zone.db\")\n",
        "duckdb_sandbox = os.path.join(duckdb_folder, \"sandbox_zone.db\")\n",
        "\n",
        "\"\"\"Metadata\"\"\"\n",
        "metadata_folder = \"metadata\"\n",
        "formatted_metadata = \"formatted_metadata.json\"\n",
        "alz_metadata_fname = \"alzheimer_metadata.json\"\n",
        "chr_metadata_fname = \"chronic_disease_indicators_metadata.json\"\n",
        "analytical_sandbox_metadata = \"analytical_sandbox_metadata.json\"\n",
        "\n",
        "data_abbrev = [\"alz\", \"chr\"]"
      ],
      "metadata": {
        "id": "_z3-90tNdw_t"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "List tables from the exploitation zone"
      ],
      "metadata": {
        "id": "ubNb7DWaatqf"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "con_exp = duckdb.connect(os.path.join(base_dir, duckdb_exploitation))\n",
        "tables = con_exp.execute(\"SHOW TABLES\").fetchall()\n",
        "for table in tables:\n",
        "  cnt = con_exp.execute(f\"SELECT COUNT(*) FROM {table[0]}\").fetchall()[0][0]\n",
        "  print(table[0], cnt)\n",
        "\n",
        "con_exp.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "U5R2OhT4wygi",
        "outputId": "96575b21-85b0-4ae3-9b99-01178d636679"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "alzheimer_arthritis_among_older_adults 2400\n",
            "alzheimer_cholesterol_checked_in_past_5_years 2400\n",
            "alzheimer_current_smoking 2400\n",
            "alzheimer_disability_status_including_sensory_or_mobility_limitations 2400\n",
            "alzheimer_frequent_mental_distress 2400\n",
            "alzheimer_high_blood_pressure_ever 2400\n",
            "alzheimer_influenza_vaccine_within_past_year 2400\n",
            "alzheimer_lifetime_diagnosis_of_depression 2400\n",
            "alzheimer_no_leisuretime_physical_activity_within_past_month 2400\n",
            "alzheimer_obesity 2400\n",
            "alzheimer_physically_unhealthy_days_mean_number_of_days 2400\n",
            "alzheimer_selfrated_health_fair_to_poor_health 2400\n",
            "alzheimer_selfrated_health_good_to_excellent_health 2400\n",
            "chronic_disease_indicators_alcohol 6400\n",
            "chronic_disease_indicators_arthritis 16000\n",
            "chronic_disease_indicators_asthma 8000\n",
            "chronic_disease_indicators_cardiovascular_disease 12800\n",
            "chronic_disease_indicators_chronic_kidney_disease 1600\n",
            "chronic_disease_indicators_chronic_obstructive_pulmonary_disease 9600\n",
            "chronic_disease_indicators_diabetes 19200\n",
            "chronic_disease_indicators_immunization 1600\n",
            "chronic_disease_indicators_mental_health 1600\n",
            "chronic_disease_indicators_nutrition_physical_activity_and_weight_status 6400\n",
            "chronic_disease_indicators_overarching_conditions 6400\n",
            "chronic_disease_indicators_tobacco 8000\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "Columns of an alzheimer dataset"
      ],
      "metadata": {
        "id": "H8y_g1qkhqa5"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "con_exp = duckdb.connect(os.path.join(base_dir, duckdb_exploitation))\n",
        "df = con_exp.execute(\"SELECT * FROM chronic_disease_indicators_chronic_obstructive_pulmonary_disease\").fetchdf()\n",
        "print(df.columns)\n",
        "# print(df[[\"Stratification1\", \"StratificationID1\", \"StratificationCategory1\", \"StratificationCategoryID1\",]].head(10))\n",
        "print(df[\"DataValueType\"].unique())\n",
        "con_exp.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "91ocBWz2zLzX",
        "outputId": "ed137058-0661-4a66-9639-9a68a0ac13c1"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Index(['YearStart', 'YearEnd', 'LocationDesc', 'Topic', 'Question',\n",
            "       'DataValueUnit', 'DataValueType', 'DataValue', 'DatavalueFootnote',\n",
            "       'LowConfidenceLimit', 'HighConfidenceLimit', 'StratificationCategory1',\n",
            "       'Stratification1', 'TopicID', 'QuestionID', 'DataValueTypeID',\n",
            "       'StratificationCategoryID1', 'StratificationID1', 'Longitude',\n",
            "       'Latitude'],\n",
            "      dtype='object')\n",
            "['Crude Prevalence' 'Age-adjusted Prevalence']\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "Columns of an chronic dataset"
      ],
      "metadata": {
        "id": "0bt9mgGPhv57"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "con_exp = duckdb.connect(os.path.join(base_dir, duckdb_exploitation))\n",
        "df = con_exp.execute(\"SELECT * FROM alzheimer_disability_status_including_sensory_or_mobility_limitations\").fetchdf()\n",
        "print(df.columns)\n",
        "# print(df.head(10))\n",
        "print(df[\"Data_Value_Type\"].unique())\n",
        "con_exp.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "glOLxh6nxlvb",
        "outputId": "8d484278-28b2-4b8a-dfa0-59e47e4e7ac0"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Index(['YearStart', 'YearEnd', 'LocationDesc', 'Class', 'Topic', 'Question',\n",
            "       'Data_Value_Type', 'Data_Value', 'Data_Value_Footnote',\n",
            "       'Low_Confidence_Limit', 'High_Confidence_Limit', 'Stratification1',\n",
            "       'StratificationCategory2', 'Stratification2', 'ClassID', 'TopicID',\n",
            "       'QuestionID', 'StratificationID1', 'StratificationCategoryID2',\n",
            "       'StratificationID2', 'Longitude', 'Latitude'],\n",
            "      dtype='object')\n",
            "['Percentage']\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "### Target"
      ],
      "metadata": {
        "id": "KoRHHZSGow2K"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "target_question = \"Percentage of older adults who report having a disability \\\n",
        "(includes limitations related to sensory or mobility impairments or a physical, mental, or emotional condition)\"\n",
        "target_table_name = \"alzheimer_disability_status_including_sensory_or_mobility_limitations\"\n",
        "test_table_name = \"chronic_disease_indicators_diabetes\""
      ],
      "metadata": {
        "id": "IyJlMw_aoyRV"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "### Load tables from exploitation to sandbox without unnecessary columns"
      ],
      "metadata": {
        "id": "qvbOE1mB5DhI"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "def exp_to_sandbox(con_exp, con_sb):\n",
        "  tables = con_exp.execute(\"SHOW TABLES\").fetchall()\n",
        "\n",
        "  for table in tables:\n",
        "    # print(table[0])\n",
        "\n",
        "    if table[0].startswith(data_abbrev[0]): #alz\n",
        "      command = f\"\"\"SELECT YearStart, Question, Data_Value AS DataValue,\n",
        "        StratificationCategoryID2 AS StratificationCategory,\n",
        "        Stratification2 AS Stratification, Latitude, Longitude\n",
        "        FROM {table[0]}\n",
        "        WHERE StratificationID1 = '65PLUS'\"\"\"\n",
        "    elif table[0].startswith(data_abbrev[1]): #chr\n",
        "      command = f\"\"\"SELECT YearStart, Question, DataValue,\n",
        "        StratificationCategoryID1 AS StratificationCategory,\n",
        "        Stratification1 AS Stratification, Latitude, Longitude\n",
        "        FROM {table[0]}\n",
        "        WHERE DataValueType NOT LIKE 'Age%'\"\"\"\n",
        "    else:\n",
        "      raise Exception(\"Table must have one of the following prefix: [alzheimer, chronic_disease_indicators]\")\n",
        "\n",
        "    # print(command)\n",
        "    df = con_exp.execute(command).fetchdf()\n",
        "    con_sb.execute(f\"CREATE TABLE {table[0]} AS SELECT * FROM df\")\n",
        "    print(f\"Created table {table[0]} in the sandbox.\")"
      ],
      "metadata": {
        "id": "Jpp7KkUR5GD6"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "### Fix float precision"
      ],
      "metadata": {
        "id": "l1gyYnJGohvW"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "def set_float_precision(df, cols, N=100000):\n",
        "  for col in cols:\n",
        "    df[col] = np.round(df[col]*N).astype(int)\n",
        "    df[col] = df[col] / N\n",
        "\n",
        "  return df"
      ],
      "metadata": {
        "id": "8tAszKU2olub"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "### Reconcile stratificatiors"
      ],
      "metadata": {
        "id": "NR_SILGli16O"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "def reconcile_stratificators(df):\n",
        "  def mapping(s): # Hispanic, Female, Male are the same\n",
        "        if s == 'Asian/Pacific Islander' or s == 'Asian or Pacific Islander' or s == 'Asian, non-Hispanic': # drop ili u other   alz\n",
        "              # return 'Asian_or_Pacific_Islander'\n",
        "              return \"Other\"\n",
        "        elif s == 'Native Am/Alaskan Native' or s == 'American Indian or Alaska Native': # drop   alz\n",
        "              # return 'American_Indian_or_Alaska_Native'\n",
        "              return \"Other\"\n",
        "        elif s == 'White, non-Hispanic':\n",
        "              return 'White'\n",
        "        elif s == 'Black, non-Hispanic':\n",
        "              return 'Black'\n",
        "        elif s == \"Multiracial, non-Hispanic\": # drop ili u other     chr\n",
        "              # return \"Multiracial\"\n",
        "              return \"Other\"\n",
        "        elif  s == \"Other, non-Hispanic\": # drop ili u other   chr\n",
        "              return \"Other\"\n",
        "        elif s == \"NONE\":\n",
        "              return \"Overall\"\n",
        "        else:\n",
        "              return s\n",
        "\n",
        "\n",
        "  df[\"Stratification\"] = df[\"Stratification\"].astype(str).map(mapping)\n",
        "  df = df[df[\"Stratification\"] != \"Other\"] # removing other, because of multiple rows with same year,location,stratification\n",
        "  return df"
      ],
      "metadata": {
        "id": "vCw4ksb3i-ay"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "### Combine tables with the target question"
      ],
      "metadata": {
        "id": "eRqbLe01CV8z"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "def combine_with_target(target_df, question_df, question_cnt):\n",
        "  question_label = f\"Q{question_cnt}\"\n",
        "\n",
        "  # add question to the metadata\n",
        "  questions = dict()\n",
        "  with open(os.path.join(base_dir, metadata_folder, analytical_sandbox_metadata), \"r\") as f:\n",
        "    fmetadata = json.load(f)\n",
        "    questions = fmetadata[\"questions\"]\n",
        "  questions[question_label] = question_df.iloc[0][\"Question\"]\n",
        "  with open(os.path.join(base_dir, metadata_folder, analytical_sandbox_metadata), \"w\") as f:\n",
        "    json.dump({\"questions\": questions}, f)\n",
        "\n",
        "  # add question data value to target_df\n",
        "  question_df = question_df.rename(columns={\"DataValue\" : question_label})\n",
        "  question_df = question_df.drop([\"Question\"], axis=1)\n",
        "  target_df = pd.merge(target_df, question_df,\n",
        "                       on=[\"YearStart\", \"Latitude\", \"Longitude\", \"StratificationCategory\", \"Stratification\"], how=\"inner\")\n",
        "\n",
        "  return target_df\n",
        "\n"
      ],
      "metadata": {
        "id": "3pK7UzerCVk4"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "### Main"
      ],
      "metadata": {
        "id": "j8zph6gRo4pJ"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "con_exp = duckdb.connect(os.path.join(base_dir, duckdb_exploitation))\n",
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))\n",
        "\n",
        "# Remove the combined table if exists\n",
        "final_table_name = \"combined_analytical_table\"\n",
        "con_sb.execute(f\"DROP TABLE IF EXISTS {final_table_name}\")\n",
        "\n",
        "# Load tables to sandbox and remove unnecessary columns\n",
        "exp_to_sandbox(con_exp, con_sb)\n",
        "print()\n",
        "\n",
        "# Target table\n",
        "target_df = con_sb.execute(f\"SELECT * FROM {target_table_name}\").fetchdf()\n",
        "target_df = set_float_precision(target_df, [\"Latitude\", \"Longitude\"])\n",
        "target_df = reconcile_stratificators(target_df)\n",
        "target_df = target_df.rename(columns={\"DataValue\" : \"QTarget\"})\n",
        "with open(os.path.join(base_dir, metadata_folder, analytical_sandbox_metadata), \"w\") as f:\n",
        "  json.dump({\"questions\": {\"QTarget\" : target_df.iloc[0][\"Question\"]}}, f)\n",
        "target_df = target_df.drop([\"Question\"], axis=1)\n",
        "\n",
        "# Combine the tables with the target question\n",
        "tables = con_sb.execute(\"SHOW TABLES\").fetchall()\n",
        "question_cnt = 1\n",
        "for table in tables:\n",
        "  print(f\"Combined {table[0]}.\")\n",
        "  df = con_sb.execute(f\"SELECT * FROM {table[0]}\").fetchdf()\n",
        "  df = set_float_precision(df, [\"Latitude\", \"Longitude\"])\n",
        "  df = reconcile_stratificators(df)\n",
        "  questions_df_dict = dict(tuple(df.groupby(\"Question\")))\n",
        "  for question, question_df in questions_df_dict.items():\n",
        "    target_df = combine_with_target(target_df, question_df, question_cnt)\n",
        "    question_cnt += 1\n",
        "  # remove the table after it has been combined with the target\n",
        "  con_sb.execute(f\"DROP TABLE IF EXISTS {table[0]}\")\n",
        "\n",
        "print(target_df.columns)\n",
        "\n",
        "# save final combined table\n",
        "con_sb.register(\"temp_table\", target_df)\n",
        "con_sb.execute(f\"CREATE TABLE {final_table_name} AS SELECT * FROM temp_table\")\n",
        "\n",
        "con_sb.close()\n",
        "con_exp.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "2DEIO5ru8vcj",
        "outputId": "1254b015-f798-41bd-daf5-6c0d9ca2e00d"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Created table alzheimer_arthritis_among_older_adults in the sandbox.\n",
            "Created table alzheimer_cholesterol_checked_in_past_5_years in the sandbox.\n",
            "Created table alzheimer_current_smoking in the sandbox.\n",
            "Created table alzheimer_disability_status_including_sensory_or_mobility_limitations in the sandbox.\n",
            "Created table alzheimer_frequent_mental_distress in the sandbox.\n",
            "Created table alzheimer_high_blood_pressure_ever in the sandbox.\n",
            "Created table alzheimer_influenza_vaccine_within_past_year in the sandbox.\n",
            "Created table alzheimer_lifetime_diagnosis_of_depression in the sandbox.\n",
            "Created table alzheimer_no_leisuretime_physical_activity_within_past_month in the sandbox.\n",
            "Created table alzheimer_obesity in the sandbox.\n",
            "Created table alzheimer_physically_unhealthy_days_mean_number_of_days in the sandbox.\n",
            "Created table alzheimer_selfrated_health_fair_to_poor_health in the sandbox.\n",
            "Created table alzheimer_selfrated_health_good_to_excellent_health in the sandbox.\n",
            "Created table chronic_disease_indicators_alcohol in the sandbox.\n",
            "Created table chronic_disease_indicators_arthritis in the sandbox.\n",
            "Created table chronic_disease_indicators_asthma in the sandbox.\n",
            "Created table chronic_disease_indicators_cardiovascular_disease in the sandbox.\n",
            "Created table chronic_disease_indicators_chronic_kidney_disease in the sandbox.\n",
            "Created table chronic_disease_indicators_chronic_obstructive_pulmonary_disease in the sandbox.\n",
            "Created table chronic_disease_indicators_diabetes in the sandbox.\n",
            "Created table chronic_disease_indicators_immunization in the sandbox.\n",
            "Created table chronic_disease_indicators_mental_health in the sandbox.\n",
            "Created table chronic_disease_indicators_nutrition_physical_activity_and_weight_status in the sandbox.\n",
            "Created table chronic_disease_indicators_overarching_conditions in the sandbox.\n",
            "Created table chronic_disease_indicators_tobacco in the sandbox.\n",
            "\n",
            "Combined alzheimer_arthritis_among_older_adults.\n",
            "Combined alzheimer_cholesterol_checked_in_past_5_years.\n",
            "Combined alzheimer_current_smoking.\n",
            "Combined alzheimer_disability_status_including_sensory_or_mobility_limitations.\n",
            "Combined alzheimer_frequent_mental_distress.\n",
            "Combined alzheimer_high_blood_pressure_ever.\n",
            "Combined alzheimer_influenza_vaccine_within_past_year.\n",
            "Combined alzheimer_lifetime_diagnosis_of_depression.\n",
            "Combined alzheimer_no_leisuretime_physical_activity_within_past_month.\n",
            "Combined alzheimer_obesity.\n",
            "Combined alzheimer_physically_unhealthy_days_mean_number_of_days.\n",
            "Combined alzheimer_selfrated_health_fair_to_poor_health.\n",
            "Combined alzheimer_selfrated_health_good_to_excellent_health.\n",
            "Combined chronic_disease_indicators_alcohol.\n",
            "Combined chronic_disease_indicators_arthritis.\n",
            "Combined chronic_disease_indicators_asthma.\n",
            "Combined chronic_disease_indicators_cardiovascular_disease.\n",
            "Combined chronic_disease_indicators_chronic_kidney_disease.\n",
            "Combined chronic_disease_indicators_chronic_obstructive_pulmonary_disease.\n",
            "Combined chronic_disease_indicators_diabetes.\n",
            "Combined chronic_disease_indicators_immunization.\n",
            "Combined chronic_disease_indicators_mental_health.\n",
            "Combined chronic_disease_indicators_nutrition_physical_activity_and_weight_status.\n",
            "Combined chronic_disease_indicators_overarching_conditions.\n",
            "Combined chronic_disease_indicators_tobacco.\n",
            "Index(['YearStart', 'QTarget', 'StratificationCategory', 'Stratification',\n",
            "       'Latitude', 'Longitude', 'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8',\n",
            "       'Q9', 'Q10', 'Q11', 'Q12', 'Q13', 'Q14', 'Q15', 'Q16', 'Q17', 'Q18',\n",
            "       'Q19', 'Q20', 'Q21', 'Q22', 'Q23', 'Q24', 'Q25', 'Q26', 'Q27', 'Q28',\n",
            "       'Q29', 'Q30', 'Q31', 'Q32', 'Q33', 'Q34', 'Q35', 'Q36', 'Q37', 'Q38',\n",
            "       'Q39', 'Q40', 'Q41', 'Q42', 'Q43', 'Q44', 'Q45', 'Q46', 'Q47', 'Q48',\n",
            "       'Q49', 'Q50', 'Q51', 'Q52', 'Q53', 'Q54', 'Q55', 'Q56', 'Q57', 'Q58',\n",
            "       'Q59', 'Q60', 'Q61', 'Q62', 'Q63', 'Q64', 'Q65', 'Q66', 'Q67', 'Q68',\n",
            "       'Q69', 'Q70', 'Q71', 'Q72', 'Q73', 'Q74'],\n",
            "      dtype='object')\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "target_df.to_csv(os.path.join(base_dir, \"all_questions.csv\"))"
      ],
      "metadata": {
        "id": "kg40HS0uhJKP"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))\n",
        "tables = con_sb.execute(\"SHOW TABLES\").fetchall()\n",
        "for table in tables:\n",
        "  print(table[0])\n",
        "con_sb.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "Ajk8LKSW-Pqk",
        "outputId": "be126650-a6e9-40ba-c0b8-7296fcce1815"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "combined_analytical_table\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "delete all tables from the sandbox"
      ],
      "metadata": {
        "id": "WEGzvUjl-4wa"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))\n",
        "tables = con_sb.execute(\"SHOW TABLES\").fetchall()\n",
        "for table in tables:\n",
        "  print(table[0])\n",
        "  con_sb.execute(f\"DROP TABLE IF EXISTS {table[0]}\")\n",
        "con_sb.close()"
      ],
      "metadata": {
        "id": "KW4KXzAD-4gx"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Target question\n",
        "Percentage of older adults who report having a disability (includes limitations related to sensory or mobility impairments or a physical, mental, or emotional condition)"
      ],
      "metadata": {
        "id": "ffjFfeygy4uX"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))\n",
        "\n",
        "target_table = con_sb.execute(f\"SELECT * FROM {target_table_name}\").fetchdf()\n",
        "print(target_table.columns)\n",
        "print(target_table[\"StratificationCategory\"].unique())\n",
        "print(target_table[\"Stratification\"].unique())\n",
        "# print(target_table[[\"StratificationCategory\", \"Stratification\"]].head(10))\n",
        "# print(target_table.groupby([\"Latitude\"]).count())\n",
        "# print(target_table.info())\n",
        "\n",
        "print(target_table[target_table[\"Latitude\"] == 21.304850])\n",
        "target_latitude = 21.304850\n",
        "filtered_rows = target_table[np.isclose(target_table[\"Latitude\"], target_latitude)]\n",
        "filtered_rows = filtered_rows[filtered_rows[\"YearStart\"] == 2021]\n",
        "print(filtered_rows.drop([\"Question\"], axis=1))\n",
        "print(filtered_rows.shape[0])\n",
        "\n",
        "\n",
        "con_sb.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "tjrIoqxRNwXC",
        "outputId": "bf6ae510-6733-4025-a5cc-54ab096a3d73"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Index(['YearStart', 'Question', 'DataValue', 'StratificationCategory',\n",
            "       'Stratification', 'Latitude', 'Longitude'],\n",
            "      dtype='object')\n",
            "['RACE' 'GENDER' 'OVERALL']\n",
            "['Hispanic' 'Black, non-Hispanic' 'Male' 'NONE' 'White, non-Hispanic'\n",
            " 'Native Am/Alaskan Native' 'Asian/Pacific Islander' 'Female']\n",
            "Empty DataFrame\n",
            "Columns: [YearStart, Question, DataValue, StratificationCategory, Stratification, Latitude, Longitude]\n",
            "Index: []\n",
            "     YearStart  DataValue StratificationCategory            Stratification  \\\n",
            "139       2021       37.9                 GENDER                      Male   \n",
            "231       2021       -1.0                   RACE       Black, non-Hispanic   \n",
            "252       2021       32.7                   RACE       White, non-Hispanic   \n",
            "294       2021       37.9                OVERALL                      NONE   \n",
            "295       2021       31.8                   RACE                  Hispanic   \n",
            "318       2021       37.9                 GENDER                    Female   \n",
            "342       2021       -1.0                   RACE  Native Am/Alaskan Native   \n",
            "388       2021       38.8                   RACE    Asian/Pacific Islander   \n",
            "\n",
            "     Latitude   Longitude  \n",
            "139  21.30485 -157.857749  \n",
            "231  21.30485 -157.857749  \n",
            "252  21.30485 -157.857749  \n",
            "294  21.30485 -157.857749  \n",
            "295  21.30485 -157.857749  \n",
            "318  21.30485 -157.857749  \n",
            "342  21.30485 -157.857749  \n",
            "388  21.30485 -157.857749  \n",
            "8\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))\n",
        "\n",
        "target_table = con_sb.execute(f\"SELECT * FROM {test_table_name}\").fetchdf()\n",
        "print(target_table.columns)\n",
        "print(target_table[\"StratificationCategory\"].unique())\n",
        "print(target_table[\"Stratification\"].unique())\n",
        "# print(target_table[[\"StratificationCategory\", \"Stratification\"]].head(10))\n",
        "# print(target_table.groupby([\"Latitude\"]).count())\n",
        "# print(target_table.info())\n",
        "# print(target_table[target_table[\"Latitude\"] == 21.304850])\n",
        "target_latitude = 21.304850\n",
        "filtered_rows = target_table[np.isclose(target_table[\"Latitude\"], target_latitude)]\n",
        "filtered_rows = filtered_rows[filtered_rows[\"Question\"] == \"Pneumococcal vaccination among noninstitutionalized adults aged 18-64 years with diagnosed diabetes\"]\n",
        "filtered_rows = filtered_rows[filtered_rows[\"YearStart\"] == 2021]\n",
        "print(filtered_rows.drop([\"Question\"], axis=1))\n",
        "print(filtered_rows[\"Question\"].unique())\n",
        "print(filtered_rows.shape[0])\n",
        "con_sb.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "QIVPU7foPoYc",
        "outputId": "d60baab8-f98c-4a2b-9629-6588dc93baf8"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Index(['YearStart', 'Question', 'DataValue', 'StratificationCategory',\n",
            "       'Stratification', 'Latitude', 'Longitude'],\n",
            "      dtype='object')\n",
            "['GENDER' 'RACE' 'OVERALL']\n",
            "['Female' 'Black, non-Hispanic' 'Other, non-Hispanic' 'Male' 'Hispanic'\n",
            " 'Overall' 'Multiracial, non-Hispanic' 'White, non-Hispanic']\n",
            "      YearStart  DataValue StratificationCategory             Stratification  \\\n",
            "1920       2021       38.2                   RACE  Multiracial, non-Hispanic   \n",
            "1961       2021       -1.0                   RACE        White, non-Hispanic   \n",
            "1964       2021       23.8                 GENDER                       Male   \n",
            "1968       2021       -1.0                   RACE                   Hispanic   \n",
            "1975       2021       31.1                   RACE        Other, non-Hispanic   \n",
            "1999       2021       -1.0                   RACE        Black, non-Hispanic   \n",
            "2003       2021       41.1                 GENDER                     Female   \n",
            "2008       2021       31.7                OVERALL                    Overall   \n",
            "\n",
            "      Latitude   Longitude  \n",
            "1920  21.30485 -157.857749  \n",
            "1961  21.30485 -157.857749  \n",
            "1964  21.30485 -157.857749  \n",
            "1968  21.30485 -157.857749  \n",
            "1975  21.30485 -157.857749  \n",
            "1999  21.30485 -157.857749  \n",
            "2003  21.30485 -157.857749  \n",
            "2008  21.30485 -157.857749  \n",
            "['Pneumococcal vaccination among noninstitutionalized adults aged 18-64 years with diagnosed diabetes']\n",
            "8\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))\n",
        "tables = con_sb.execute(\"SHOW TABLES\").fetchall()\n",
        "for table in tables:\n",
        "  df = con_sb.execute(f\"SELECT * FROM {table[0]}\").fetchdf()\n",
        "  questions = df[\"Question\"].nunique()\n",
        "  print(table[0], df.shape[0], str(questions), str(df.shape[0] // questions))\n",
        "con_sb.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "O7k_CUZ1hUFN",
        "outputId": "21d4e30b-c436-491f-872c-121979478a51"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "alzheimer_arthritis_among_older_adults 800 1 800\n",
            "alzheimer_cholesterol_checked_in_past_5_years 800 1 800\n",
            "alzheimer_current_smoking 800 1 800\n",
            "alzheimer_disability_status_including_sensory_or_mobility_limitations 800 1 800\n",
            "alzheimer_frequent_mental_distress 800 1 800\n",
            "alzheimer_high_blood_pressure_ever 800 1 800\n",
            "alzheimer_influenza_vaccine_within_past_year 800 1 800\n",
            "alzheimer_lifetime_diagnosis_of_depression 800 1 800\n",
            "alzheimer_no_leisuretime_physical_activity_within_past_month 800 1 800\n",
            "alzheimer_obesity 800 1 800\n",
            "alzheimer_physically_unhealthy_days_mean_number_of_days 800 1 800\n",
            "alzheimer_selfrated_health_fair_to_poor_health 800 1 800\n",
            "alzheimer_selfrated_health_good_to_excellent_health 800 1 800\n",
            "chronic_disease_indicators_alcohol 3200 4 800\n",
            "chronic_disease_indicators_arthritis 8000 10 800\n",
            "chronic_disease_indicators_asthma 4000 5 800\n",
            "chronic_disease_indicators_cardiovascular_disease 6400 8 800\n",
            "chronic_disease_indicators_chronic_kidney_disease 800 1 800\n",
            "chronic_disease_indicators_chronic_obstructive_pulmonary_disease 4800 6 800\n",
            "chronic_disease_indicators_diabetes 9600 12 800\n",
            "chronic_disease_indicators_immunization 800 1 800\n",
            "chronic_disease_indicators_mental_health 800 1 800\n",
            "chronic_disease_indicators_nutrition_physical_activity_and_weight_status 3200 4 800\n",
            "chronic_disease_indicators_overarching_conditions 3200 4 800\n",
            "chronic_disease_indicators_tobacco 4000 5 800\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))\n",
        "problem_question = \"Binge drinking frequency among adults aged >= 18 years who binge drink\"\n",
        "alcohol_df = con_sb.execute(\"SELECT * FROM chronic_disease_indicators_alcohol\").fetchdf()\n",
        "# alcohol_df = con_sb.execute(\"SELECT * FROM alzheimer_selfrated_health_good_to_excellent_health\").fetchdf()\n",
        "# problem_question = \"Percentage of older adults who self-reported that their health is \\\"good\\\", \\\"very good\\\", or \\\"excellent\\\"\"\n",
        "\n",
        "quest_df = alcohol_df[alcohol_df[\"Question\"] == problem_question]\n",
        "print(quest_df.shape[0])\n",
        "print(quest_df[\"Stratification\"].unique())\n",
        "counts = quest_df.groupby(\"Longitude\").size()\n",
        "# print(counts)\n",
        "\n",
        "# filtered_quest_df = quest_df[quest_df[\"YearStart\"] == 2021]\n",
        "# filtered_quest_df = filtered_quest_df[filtered_quest_df[\"Stratification\"] == \"Other\"]\n",
        "# target_latitude = 39.360700\n",
        "# filtered_quest_df = filtered_quest_df[np.isclose(filtered_quest_df[\"Latitude\"], target_latitude)]\n",
        "\n",
        "# print(filtered_quest_df.shape[0])\n",
        "# print(filtered_quest_df.head(10))\n",
        "\n",
        "# 46.355649 lat\n",
        "# -94.794201 long\n",
        "con_sb.close()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "wdQLv9l_G31b",
        "outputId": "185c6956-5d15-4bd4-c637-0de422730c49"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "800\n",
            "['Black, non-Hispanic' 'Male' 'Other, non-Hispanic' 'White, non-Hispanic'\n",
            " 'Multiracial, non-Hispanic' 'Female' 'Hispanic' 'Overall']\n",
            "Longitude\n",
            "-157.857749    16\n",
            "-147.722059    16\n",
            "-121.000000    16\n",
            "-120.470011    16\n",
            "-120.155031    16\n",
            "-117.071841    16\n",
            "-114.363730    16\n",
            "-111.763811    16\n",
            "-111.587131    16\n",
            "-109.424421    16\n",
            "-108.109830    16\n",
            "-106.240581    16\n",
            "-106.133611    16\n",
            "-100.373531    16\n",
            "-100.118421    16\n",
            "-99.426770     16\n",
            "-99.365721     16\n",
            "-98.200781     16\n",
            "-97.521070     16\n",
            "-94.794201     16\n",
            "-93.816491     16\n",
            "-92.566300     16\n",
            "-92.445680     16\n",
            "-92.274491     16\n",
            "-89.816371     16\n",
            "-89.538031     16\n",
            "-88.997710     16\n",
            "-86.631861     16\n",
            "-86.149960     16\n",
            "-85.774491     16\n",
            "-84.774971     16\n",
            "-84.714390     16\n",
            "-83.627580     16\n",
            "-82.404260     16\n",
            "-81.045371     16\n",
            "-80.712640     16\n",
            "-79.159250     16\n",
            "-78.457890     16\n",
            "-77.860700     16\n",
            "-77.036871     16\n",
            "-76.609260     16\n",
            "-75.577741     16\n",
            "-75.543970     16\n",
            "-74.273691     16\n",
            "-72.649841     16\n",
            "-72.517641     16\n",
            "-72.082691     16\n",
            "-71.522470     16\n",
            "-71.500361     16\n",
            "-68.985031     16\n",
            "dtype: int64\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "# OLD"
      ],
      "metadata": {
        "id": "YVLEx3HN5NWm"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "# New database for the analytical sandbox\n",
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))"
      ],
      "metadata": {
        "id": "6SGZUXCGw9DE"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "# Database from trusted zone\n",
        "con_tru = duckdb.connect(os.path.join(base_dir, duckdb_trusted))\n",
        "\n",
        "# Load tables from exploitation zone\n",
        "tables = con_tru.execute(\"SHOW TABLES\").fetchall()\n",
        "for table in tables:\n",
        "  print(table)\n",
        "\n",
        "# New database for the analytical sandbox\n",
        "con_sb = duckdb.connect(os.path.join(base_dir, duckdb_sandbox))"
      ],
      "metadata": {
        "id": "UmnjMgTWasLd",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "7382ecf0-e630-48bc-f573-ef870688f29d"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "('alzheimer',)\n",
            "('chronic_disease_indicators',)\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "Create tables for each category of disease\n"
      ],
      "metadata": {
        "id": "fm9Lo1tdnsnH"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "def map_categories(con_tru, con_sb):\n",
        "\n",
        "  # Get chronic disease table from the database\n",
        "  chr_sandbox = con_tru.execute(\"SELECT * FROM chronic_disease_indicators\").fetchdf()\n",
        "\n",
        "  unique_topics = chr_sandbox['Topic'].unique()\n",
        "\n",
        "  # Do not analyze Other Adults, Disability, Reproductive Health, Oral Health, Overarching Conditions\n",
        "  # since they are very broad or there are too few instances\n",
        "  topic_categories = {\n",
        "    \"Asthma\": \"Respiratory Diseases\",\n",
        "    \"Chronic Obstructive Pulmonary Disease\": \"Respiratory Diseases\",\n",
        "    \"Cardiovascular Disease\": \"Chronic Diseases\",\n",
        "    \"Alcohol\": \"Substance Use and Addictions\",\n",
        "    \"Chronic Kidney Disease\": \"Chronic Diseases\",\n",
        "    \"Diabetes\": \"Chronic Diseases\",\n",
        "    \"Nutrition, Physical Activity, and Weight Status\": \"Lifestyle and Wellness\",\n",
        "    \"Tobacco\": \"Substance Use and Addictions\",\n",
        "    \"Arthritis\": \"Chronic Diseases\",\n",
        "    \"Mental Health\": \"Mental and Behavioral Health\",\n",
        "    \"Immunization\": \"Preventative Health\"\n",
        "  }\n",
        "\n",
        "  # Map into new variable 'Category'\n",
        "  chr_sandbox['Category'] = chr_sandbox['Topic'].map(topic_categories)\n",
        "\n",
        "  # Drop rows with null categories if desired\n",
        "  chr_sandbox = chr_sandbox[chr_sandbox['Category'].notnull()]\n",
        "\n",
        "  categories = chr_sandbox['Category'].unique()\n",
        "  # Divide data into tables filtered by 'Category'\n",
        "  for category in categories:\n",
        "    category_data = chr_sandbox[chr_sandbox['Category'] == category]\n",
        "\n",
        "    # Create a table name by formatting the category string\n",
        "    table_name = category.lower().replace(\" \", \"_\") + \"_table\"\n",
        "\n",
        "    # Put table in the sandbox database\n",
        "    con_sb.execute(f\"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM category_data\")\n",
        "\n",
        "    print(f\"Table '{table_name}' created with {len(category_data)} rows.\")\n"
      ],
      "metadata": {
        "id": "reWp65AwnsKU"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "Main for analytical sandbox"
      ],
      "metadata": {
        "id": "E6j1129jMKVq"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "map_categories(con_tru, con_sb)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "e7B_hhOqMMw3",
        "outputId": "a13b9f6e-1dae-432b-a69d-e4453b520835"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Table 'respiratory_diseases_table' created with 628 rows.\n",
            "Table 'chronic_diseases_table' created with 1332 rows.\n",
            "Table 'substance_use_and_addictions_table' created with 390 rows.\n",
            "Table 'lifestyle_and_wellness_table' created with 303 rows.\n",
            "Table 'mental_and_behavioral_health_table' created with 44 rows.\n",
            "Table 'preventative_health_table' created with 32 rows.\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "Delete all tables"
      ],
      "metadata": {
        "id": "4Q_H58n1NrRg"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "# # Fetch all table names from the database\n",
        "# tables = con_sb.execute(\"SHOW TABLES\").fetchall()\n",
        "\n",
        "# # Loop through and drop each table\n",
        "# for table in tables:\n",
        "#     table_name = table[0]  # Extract table name from the tuple\n",
        "#     con_sb.execute(f\"DROP TABLE {table_name}\")\n",
        "#     print(f\"Deleted table: {table_name}\")\n",
        "\n",
        "# # Verify the database is empty\n",
        "# remaining_tables = con_sb.execute(\"SHOW TABLES\").fetchall()\n",
        "# if not remaining_tables:\n",
        "#     print(\"All tables have been successfully deleted.\")\n",
        "# else:\n",
        "#     print(f\"Some tables remain: {remaining_tables}\")"
      ],
      "metadata": {
        "id": "jvdZ8bLoMBWh",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "0db0c438-466e-43fa-f297-43c50343da50"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "All tables have been successfully deleted.\n"
          ]
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "Check tables in database"
      ],
      "metadata": {
        "id": "PMZNH_1-cN63"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "# List all tables in the database\n",
        "tables = con_sb.execute(\"SHOW TABLES\").fetchall()\n",
        "\n",
        "# Print the list of tables\n",
        "if not tables:\n",
        "    print(\"No tables found in the database.\")\n",
        "else:\n",
        "    print(\"Tables in the database:\")\n",
        "    for table in tables:\n",
        "        print(table[0])  # Table name is the first element in each tuple"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "8JY53eIX6mrC",
        "outputId": "ec7ac456-f776-4676-b360-026a397df7d2"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Tables in the database:\n",
            "chronic_diseases_table\n",
            "lifestyle_and_wellness_table\n",
            "mental_and_behavioral_health_table\n",
            "preventative_health_table\n",
            "respiratory_diseases_table\n",
            "substance_use_and_addictions_table\n"
          ]
        }
      ]
    }
  ]
}