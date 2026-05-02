import csv
import hashlib
import hmac
import io
import json
import os
import random
import re
import secrets
import smtplib
import textwrap
import urllib.parse
import uuid
import zipfile
import zlib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import joblib
from matplotlib.backends.backend_pdf import PdfPages
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover - optional dependency
    gspread = None
    Credentials = None
try:
    import pdfplumber
except Exception:
    pdfplumber = None


# ============================================================
# 1. APP IDENTITY, FILE PATHS, AND SHARED SCHEMA DEFINITIONS
# ============================================================
APP_TITLE = "EduPulse: The Basic Education Transition Analyzer"
DATA_FILE = "akatsi_district_data.csv"
USERS_FILE = "users.csv"
CIRCUITS_FILE = "circuits.csv"
APP_CONFIG_FILE = "app_config.json"
NOTIFICATIONS_FILE = "notifications.csv"
SCENARIOS_FILE = "saved_scenarios.csv"
CONTACTS_FILE = "contact_directory.csv"
MANUAL_PREDICTIONS_FILE = "manual_predictions.csv"
MODEL_FILE = "bece_models.joblib"
CALIBRATION_FILE = "school_calibration.json"
OFFICIAL_MATCH_REVIEW_FILE = "official_match_reviews.csv"
BRAND_IMAGE = r"C:\Users\SAVIOUR\Downloads\app_logo.png"
FINAL_SUFFIX = "_Final_BECE"
PREDICTED_SUFFIX = "_Predicted_BECE"
MIGRATION_SCHOOL_TYPE_PLACEHOLDER = "Needs Review"
PLACEMENT_ORDER = ["Category A", "Category B", "Category C", "Category D/SP"]
USERS_COLUMNS = ["username", "password", "role", "school", "district", "security_key", "email", "security_question", "security_answer"]
EXPECTED_CIRCUIT_COLUMNS = ["School_Name", "Circuit", "School_Type"]
NOTIFICATION_COLUMNS = [
    "notification_id",
    "created_at",
    "district",
    "target_role",
    "event_type",
    "created_by",
    "school",
    "circuit",
    "student_id",
    "student_name",
    "message",
    "status",
]
BLOOMCORE_FOOTER_TEXT = "Powered by BloomCore Technologies"
HEADTEACHER_TEMPLATE_ROWS = 250
SMTP_CONFIG_FIELDS = [
    "smtp_host",
    "smtp_port",
    "smtp_username",
    "smtp_password",
    "smtp_sender_email",
    "smtp_use_tls",
]
SCENARIO_COLUMNS = [
    "scenario_id",
    "created_at",
    "district",
    "school",
    "circuit",
    "internal_tracking_id",
    "student_id",
    "official_index_number",
    "student_name",
    "scenario_name",
    "intervention_note",
    "current_attendance",
    "target_attendance",
    "current_assignment",
    "target_assignment",
    "current_mock",
    "target_mock",
    "current_aggregate",
    "predicted_aggregate",
    "current_placement",
    "predicted_placement",
    "current_best_six_raw_total",
    "predicted_best_six_raw_total",
    "current_raw_average",
    "predicted_raw_average",
    "best_two_electives",
    "prediction_payload_json",
]
CONTACT_COLUMNS = [
    "district",
    "school",
    "contact_name",
    "role",
    "email",
    "phone",
    "whatsapp_number",
    "preferred_channel",
    "updated_at",
]
MANUAL_PREDICTION_COLUMNS = [
    "prediction_id",
    "created_at",
    "district",
    "school",
    "circuit",
    "school_type",
    "student_id",
    "student_name",
    "aggregate",
    "best_six_raw_total",
    "placement",
    "placement_category",
    "created_by",
]
OFFICIAL_MATCH_REVIEW_COLUMNS = [
    "review_id",
    "created_at",
    "district",
    "school",
    "circuit",
    "source_label",
    "official_index_number",
    "student_name",
    "date_of_birth",
    "reason",
    "status",
    "payload_json",
]
DIRECTOR_REGISTRATION_KEY_ENV = "EDUPULSE_OWNER_SECRET"
GOOGLE_SHEETS_ID_ENV = "EDUPULSE_GOOGLE_SHEETS_ID"
GOOGLE_SERVICE_ACCOUNT_JSON_ENV = "EDUPULSE_GOOGLE_SERVICE_ACCOUNT_JSON"
VALID_SCHOOL_TYPES = ["Public", "Private"]
RUNTIME_SCHOOL_TYPES = VALID_SCHOOL_TYPES + [MIGRATION_SCHOOL_TYPE_PLACEHOLDER, "Not Specified"]
WAEC_ACCEPTED_FIELDS = [
    "INDEX NUMBER",
    "NAME",
    "GENDER",
    "DOB",
    "RESULTS",
]
WAEC_EXPLICIT_VERTICAL_LINES = [0, 250, 420, 475, 555, 842]
SUBJECT_DISPLAY_NAMES = {
    "Mathematics": "Mathematics",
    "English_Language": "English Language",
    "Integrated_Science": "Science",
    "Social_Studies": "Social Studies",
    "ICT": "Computing",
    "RME": "R.M.E.",
    "BDT": "Career Technology",
    "Creative_Arts": "C. A. & Design",
    "French": "French",
    "Arabic": "Arabic",
    "Ewe": "Ewe",
}
MODEL_KEY_OVERRIDES = {
    "ICT": "ICT",
    "BDT": "BDT",
    "French": "",
}
WAEC_RESULT_SUBJECT_MAP = {
    "ENGLISH LANGUAGE": "English_Language_Final_BECE",
    "ENGLISH LANG": "English_Language_Final_BECE",
    "ENGLISH LANG.": "English_Language_Final_BECE",
    "ENGLISH": "English_Language_Final_BECE",
    "MATHEMATICS": "Mathematics_Final_BECE",
    "MATHS": "Mathematics_Final_BECE",
    "MATH": "Mathematics_Final_BECE",
    "SCIENCE": "Integrated_Science_Final_BECE",
    "INTEGRATED SCIENCE": "Integrated_Science_Final_BECE",
    "SOCIAL STUDIES": "Social_Studies_Final_BECE",
    "SOCIAL STUD": "Social_Studies_Final_BECE",
    "SOCIAL STUD.": "Social_Studies_Final_BECE",
    "SOCIAL": "Social_Studies_Final_BECE",
    "RME": "RME_Final_BECE",
    "R.M.E": "RME_Final_BECE",
    "R.M.E.": "RME_Final_BECE",
    "REL. & MORAL EDUC.": "RME_Final_BECE",
    "RELIGIOUS AND MORAL EDUCATION": "RME_Final_BECE",
    "EWE": "Ewe_Final_BECE",
    "EPE": "Ewe_Final_BECE",
    "CAREER TECHNOLOGY": "BDT_Final_BECE",
    "CAREER TECH": "BDT_Final_BECE",
    "CAREER TECH.": "BDT_Final_BECE",
    "BDT": "BDT_Final_BECE",
    "BASIC DESIGN AND TECHNOLOGY": "BDT_Final_BECE",
    "CREATIVE ARTS DESIGN": "Creative_Arts_Final_BECE",
    "CREATIVE ARTS & DES.": "Creative_Arts_Final_BECE",
    "CREATIVE ARTS & DESIGN": "Creative_Arts_Final_BECE",
    "C. A. & DESIGN": "Creative_Arts_Final_BECE",
    "CA DESIGN": "Creative_Arts_Final_BECE",
    "C A DESIGN": "Creative_Arts_Final_BECE",
    "C.A. DESIGN": "Creative_Arts_Final_BECE",
    "FRENCH": "French_Final_BECE",
    "ARABIC": "Arabic_Final_BECE",
    "COMPUTING": "ICT_Final_BECE",
    "ICT": "ICT_Final_BECE",
    "INFORMATION AND COMMUNICATION TECHNOLOGY": "ICT_Final_BECE",
}
SUBJECT_IMPORT_ALIASES = {
    "Student_ID": ["student id", "student_id", "index number", "index_number", "candidate number", "candidate_no", "candidate id"],
    "Official_Index_Number": ["official index number", "official_index_number", "waec index number", "waec_index_number"],
    "Student_Name": ["student name", "student_name", "candidate name", "candidate_name", "full name", "name"],
    "Gender": ["gender", "sex"],
    "Date_of_Birth": ["date of birth", "dob", "birth date", "date_of_birth"],
    "School_Name": ["school name", "school_name", "school"],
    "Circuit": ["circuit"],
    "School_Type": ["school type", "school_type", "ownership", "public or private"],
    "Attendance_Percent": ["attendance percent", "attendance_percent", "attendance", "attendance percentage"],
    "Official_Results_Raw": ["results", "result listing", "official results"],
    "Mathematics_Final_BECE": ["mathematics", "math", "maths", "mathematics final bece", "mathematics score", "math score"],
    "English_Language_Final_BECE": ["english language", "english", "english language final bece", "english score"],
    "Integrated_Science_Final_BECE": ["integrated science", "science", "science score", "integrated science final bece"],
    "Social_Studies_Final_BECE": ["social studies", "social", "social studies score", "social studies final bece"],
    "ICT_Final_BECE": ["ict", "information and communication technology", "ict score"],
    "RME_Final_BECE": ["rme", "religious and moral education", "rme score"],
    "BDT_Final_BECE": ["bdt", "basic design and technology", "bdt score"],
    "Creative_Arts_Final_BECE": ["creative arts design", "c.a. design", "c. a. & design", "creative arts"],
    "French_Final_BECE": ["french", "french score"],
    "Arabic_Final_BECE": ["arabic", "arabic score"],
    "Ewe_Final_BECE": ["ewe", "ewe score"],
}
SUBJECT_PREFIXES = [
    "Mathematics",
    "English_Language",
    "Integrated_Science",
    "Social_Studies",
    "ICT",
    "RME",
    "BDT",
    "Creative_Arts",
    "French",
    "Arabic",
    "Ewe",
]
FINAL_SUBJECT_COLUMNS = [f"{prefix}{FINAL_SUFFIX}" for prefix in SUBJECT_PREFIXES]
PREDICTED_SUBJECT_COLUMNS = [f"{prefix}{PREDICTED_SUFFIX}" for prefix in SUBJECT_PREFIXES]
EXPECTED_DATA_COLUMNS = [
    "Internal_Tracking_ID",
    "Student_ID",
    "Official_Index_Number",
    "Student_Name",
    "Gender",
    "Date_of_Birth",
    "School_Name",
    "Circuit",
    "School_Type",
    "Attendance_Percent",
    "Mathematics_Assignments",
    "Mathematics_Term1_Exam",
    "Mathematics_Term2_Exam",
    "Mathematics_Mock1",
    "Mathematics_Mock2",
    "Mathematics_Predicted_BECE",
    "Mathematics_Final_BECE",
    "English_Language_Assignments",
    "English_Language_Term1_Exam",
    "English_Language_Term2_Exam",
    "English_Language_Mock1",
    "English_Language_Mock2",
    "English_Language_Predicted_BECE",
    "English_Language_Final_BECE",
    "Integrated_Science_Assignments",
    "Integrated_Science_Term1_Exam",
    "Integrated_Science_Term2_Exam",
    "Integrated_Science_Mock1",
    "Integrated_Science_Mock2",
    "Integrated_Science_Predicted_BECE",
    "Integrated_Science_Final_BECE",
    "Social_Studies_Assignments",
    "Social_Studies_Term1_Exam",
    "Social_Studies_Term2_Exam",
    "Social_Studies_Mock1",
    "Social_Studies_Mock2",
    "Social_Studies_Predicted_BECE",
    "Social_Studies_Final_BECE",
    "ICT_Assignments",
    "ICT_Term1_Exam",
    "ICT_Term2_Exam",
    "ICT_Mock1",
    "ICT_Mock2",
    "ICT_Predicted_BECE",
    "ICT_Final_BECE",
    "RME_Assignments",
    "RME_Term1_Exam",
    "RME_Term2_Exam",
    "RME_Mock1",
    "RME_Mock2",
    "RME_Predicted_BECE",
    "RME_Final_BECE",
    "BDT_Assignments",
    "BDT_Term1_Exam",
    "BDT_Term2_Exam",
    "BDT_Mock1",
    "BDT_Mock2",
    "BDT_Predicted_BECE",
    "BDT_Final_BECE",
    "Creative_Arts_Assignments",
    "Creative_Arts_Term1_Exam",
    "Creative_Arts_Term2_Exam",
    "Creative_Arts_Mock1",
    "Creative_Arts_Mock2",
    "Creative_Arts_Predicted_BECE",
    "Creative_Arts_Final_BECE",
    "French_Assignments",
    "French_Term1_Exam",
    "French_Term2_Exam",
    "French_Mock1",
    "French_Mock2",
    "French_Predicted_BECE",
    "French_Final_BECE",
    "Arabic_Assignments",
    "Arabic_Term1_Exam",
    "Arabic_Term2_Exam",
    "Arabic_Mock1",
    "Arabic_Mock2",
    "Arabic_Predicted_BECE",
    "Arabic_Final_BECE",
    "Ewe_Assignments",
    "Ewe_Term1_Exam",
    "Ewe_Term2_Exam",
    "Ewe_Mock1",
    "Ewe_Mock2",
    "Ewe_Predicted_BECE",
    "Ewe_Final_BECE",
    "Official_Results_Raw",
    "Math_Improvement",
    "Math_Consistency",
    "Action_Zone",
]
PREDICTION_TEMPLATE_COLUMNS = [
    column
    for column in EXPECTED_DATA_COLUMNS
    if (
        not column.endswith(FINAL_SUFFIX)
        and not column.endswith(PREDICTED_SUFFIX)
        and column not in {"Internal_Tracking_ID", "Official_Index_Number", "Official_Results_Raw", "Math_Improvement", "Math_Consistency", "Action_Zone"}
    )
]
HEADTEACHER_UPLOAD_TEMPLATE_COLUMNS = [
    column
    for column in PREDICTION_TEMPLATE_COLUMNS
    if column not in {"School_Name", "Circuit", "School_Type"}
]
CORE_FINAL_SUBJECTS = [
    "Mathematics_Final_BECE",
    "English_Language_Final_BECE",
    "Integrated_Science_Final_BECE",
    "Social_Studies_Final_BECE",
]
ASSESSMENT_SUFFIXES = ["Assignments", "Term1_Exam", "Term2_Exam", "Mock1", "Mock2"]
MASTER_CREDENTIALS = {
    "director_akatsi": {
        "password": "admin123",
        "role": "Director",
        "school": "ALL",
        "district": "Akatsi South Municipal",
        "security_key": "AKATSISOUTH-0000",
    }
}

# Add this to your file paths at the top of analyser_app.py
DIRECTOR_KEYS_FILE = "director_keys.json"

def init_director_keys():
    """
    Creates the vault if it doesn't exist. 
    I've added one master key to get you started!
    """
    if not os.path.exists(DIRECTOR_KEYS_FILE):
        initial_keys = {
            "BLOOMCORE-MASTER-2026": {"status": "unused"}
        }
        with open(DIRECTOR_KEYS_FILE, "w") as f:
            json.dump(initial_keys, f)

def validate_and_burn_director_key(entered_key):
    """
    Checks if a key is valid and unused. If yes, it marks it as 'used' (burns it)
    so no other district can ever use it again.
    """
    # 1. Open the vault and check the keys
    if not os.path.exists(DIRECTOR_KEYS_FILE):
        return False
        
    with open(DIRECTOR_KEYS_FILE, "r") as f:
        keys_db = json.load(f)
        
    # 2. Check if the key exists AND hasn't been used yet
    if entered_key in keys_db and keys_db[entered_key]["status"] == "unused":
        
        # 3. The key is good! Now we "burn" it by changing the status to used.
        keys_db[entered_key]["status"] = "used"
        
        # 4. Save the locked vault back to the file
        with open(DIRECTOR_KEYS_FILE, "w") as f:
            json.dump(keys_db, f)
            
        return True # Registration can proceed!
    else:
        # Key is fake, mistyped, or already burned.
        return False

# ============================================================
# 2. STREAMLIT PAGE CONFIGURATION, STYLING, AND SESSION STATE
# ============================================================
st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")

st.markdown(
    """
    <style>
    /* ---------------------------------------------------------
       SIDEBAR & METRIC STYLES 
    --------------------------------------------------------- */
    [data-testid="stSidebarUserContent"] > div:first-child {
        min-height: 100vh;
        display: flex;
        flex-direction: column;
    }
    [data-testid="stSidebarUserContent"] > div:first-child > div:last-child {
        margin-top: auto;
    }
    [data-testid="stMetricValue"] {
        font-size: 32px;
        color: #1E3A8A;
        font-weight: bold;
    }
    .metric-card {
        background-color: #F8FAFC;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #1E3A8A;
        box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.05);
        margin-bottom: 10px;
    }
    .sidebar-brand-card {
        background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
        border: 1px solid #bfdbfe;
        border-radius: 14px;
        padding: 0.9rem 1rem;
        margin: 0.2rem 0 0.8rem 0;
    }
    .sidebar-kicker {
        color: #1d4ed8;
        font-size: 0.78rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .sidebar-title {
        color: #0f172a;
        font-size: 1.15rem;
        font-weight: 800;
        margin-top: 0.15rem;
    }
    .sidebar-subtitle {
        color: #475569;
        font-size: 0.92rem;
        margin-top: 0.2rem;
    }
    .scroll-top-link {
        margin-top: 0.8rem;
        text-align: right;
    }
    .scroll-top-link a {
        display: inline-block;
        padding: 0.42rem 0.8rem;
        border-radius: 999px;
        background: #e2e8f0;
        color: #0f172a;
        font-size: 0.88rem;
        font-weight: 600;
        text-decoration: none;
    }
    .scroll-top-link a:hover {
        background: #cbd5e1;
    }
    .sidebar-footer-text {
        text-align: center;
        color: #475569;
        font-size: 0.9rem;
        font-weight: 600;
        padding-top: 0.25rem;
    }
    .mobile-sync-card {
        background: linear-gradient(135deg, #eff6ff 0%, #f8fafc 100%);
        border: 1px solid #cbd5e1;
        border-radius: 14px;
        padding: 0.9rem 1rem;
        margin-bottom: 1rem;
    }
    .mobile-sync-title {
        color: #0f172a;
        font-size: 1rem;
        font-weight: 800;
        margin-bottom: 0.35rem;
    }
    .mobile-sync-copy {
        color: #334155;
        font-size: 0.92rem;
        margin-bottom: 0;
    }

    /* Media Queries for Mobile Responsiveness */
    @media (max-width: 768px) {
        .metric-card {
            padding: 12px;
        }
        .scroll-top-link {
            text-align: center;
        }
        .stButton > button,
        .stDownloadButton > button {
            width: 100%;
            min-height: 3rem;
            font-size: 1rem;
        }
        .mobile-sync-card {
            padding: 1rem;
        }
    }
    
    /* ---------------------------------------------------------
       🎓 ELITE EDUPULSE ONBOARDING EXPERIENCE
    --------------------------------------------------------- */
    /* Hero Section */
    .hero-container {
        background: linear-gradient(135deg, #0f172a 0%, #1e3a8a 100%);
        padding: 2.5rem 2rem;
        border-radius: 18px;
        color: white !important;
        margin-bottom: 1.5rem;
    }
    .hero-container h1, .hero-container p {
        color: white !important;
    }

    /* Card Grid */
    .card-grid {
        display: flex;
        gap: 1.5rem;
        margin-top: 1.2rem;
    }

    /* Individual Cards */
    .info-card {
        flex: 1;
        background: white !important;
        padding: 1.5rem;
        border-radius: 16px;
        box-shadow: 0 10px 25px rgba(0,0,0,0.1);
        border: 1px solid #e2e8f0;
        border-top: 5px solid #1d4ed8; /* Visual accent */
    }

    /* Text inside Cards - Explicit Colors */
    .info-card h3 {
        color: #0f172a !important; /* Dark Blue/Black */
        font-size: 1.2rem;
        font-weight: 700;
        margin-bottom: 0.8rem;
    }
    .info-card ul {
        color: #334155 !important; /* Slate Gray */
        padding-left: 1.2rem;
    }
    .info-card li {
        margin-bottom: 0.5rem;
        font-size: 0.95rem;
        color: #334155 !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)


def init_session_state():
    """
    Initializes required Streamlit session state variables.
    Checks if a key exists; if not, sets it to its default value.
    """
    default_states = {
        "logged_in": False,
        "current_user": "",
        "user_role": "",
        "user_school": "",
        "user_district": "",
        "auth_nav": "Login",
        "pending_setup_role": "",
        "auth_flash_message": "",
        "auth_flash_severity": "success",
        "portal_flash_message": "",
        "portal_flash_severity": "success",
        "latest_registered_school": "",
        "latest_registered_district": "",
        "latest_director_key": ""
    }

    for key, default_value in default_states.items():
        if key not in st.session_state:
            st.session_state[key] = default_value



# ============================================================
# 3. SECURITY, ACCOUNT MANAGEMENT, AND APP CONFIGURATION
# ============================================================
def hash_password(password):
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return f"pbkdf2_sha256${salt.hex()}${digest.hex()}"


def verify_password(plain_password, stored_password):
    if isinstance(stored_password, str) and stored_password.startswith("pbkdf2_sha256$"):
        try:
            _, salt_hex, digest_hex = stored_password.split("$", 2)
            computed = hashlib.pbkdf2_hmac(
                "sha256",
                plain_password.encode("utf-8"),
                bytes.fromhex(salt_hex),
                200_000,
            ).hex()
            return hmac.compare_digest(computed, digest_hex)
        except ValueError:
            return False
    return stored_password == plain_password


def load_app_config():
    default_config = {
        "district_name": "",
        "director_username": "",
        "headteacher_security_key": "",
        "director_registration_key": "",
        "director_registration_key_created_at": "",
        "smtp_host": "",
        "smtp_port": "587",
        "smtp_username": "",
        "smtp_password": "",
        "smtp_sender_email": "",
        "smtp_use_tls": "true",
    }

    saved_config = read_app_config_record(default_config)

    if not isinstance(saved_config, dict):
        return default_config

    merged_config = default_config.copy()
    merged_config.update({key: str(value).strip() for key, value in saved_config.items() if key in default_config})
    return merged_config


def save_app_config(config):
    safe_config = {
        "district_name": str(config.get("district_name", "")).strip(),
        "director_username": str(config.get("director_username", "")).strip(),
        "headteacher_security_key": str(config.get("headteacher_security_key", "")).strip(),
        "director_registration_key": str(config.get("director_registration_key", "")).strip(),
        "director_registration_key_created_at": str(config.get("director_registration_key_created_at", "")).strip(),
        "smtp_host": str(config.get("smtp_host", "")).strip(),
        "smtp_port": str(config.get("smtp_port", "587")).strip() or "587",
        "smtp_username": str(config.get("smtp_username", "")).strip(),
        "smtp_password": str(config.get("smtp_password", "")).strip(),
        "smtp_sender_email": str(config.get("smtp_sender_email", "")).strip(),
        "smtp_use_tls": str(config.get("smtp_use_tls", "true")).strip().lower() or "true",
    }
    write_app_config_record(safe_config)


def normalize_school_type(value):
    token = re.sub(r"[^a-z]", "", str(value).strip().lower())
    if token in {"public", "gov", "government"}:
        return "Public"
    if token in {"private", "priv"}:
        return "Private"
    if token in {"needsreview", "notspecified", "unclassified", "unknown"}:
        return MIGRATION_SCHOOL_TYPE_PLACEHOLDER
    return ""


def normalize_school_type_series(series):
    return series.fillna("").astype(str).str.strip().apply(normalize_school_type)


def summarize_invalid_school_type_values(series):
    raw_values = series.fillna("").astype(str).str.strip()
    canonical = raw_values.apply(normalize_school_type)
    invalid_values = sorted(raw_values[(raw_values != "") & (canonical == "")].unique().tolist())
    return invalid_values


def normalize_student_name(value):
    cleaned = re.sub(r"[^A-Z0-9 ]+", " ", str(value).upper())
    tokens = [token for token in re.sub(r"\s+", " ", cleaned).strip().split(" ") if token]
    return " ".join(sorted(tokens))


def normalize_gender_token(value):
    token = str(value).strip().lower()
    if token in {"m", "male", "boy", "boys"}:
        return "M"
    if token in {"f", "female", "girl", "girls"}:
        return "F"
    return ""


def normalize_date_of_birth(value):
    raw_value = str(value).strip()
    if not raw_value or raw_value.lower() in {"nan", "none"}:
        return ""

    normalized = raw_value.replace(".", "/").replace("-", "/")
    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", normalized)
    if match:
        day, month, year = [int(part) for part in match.groups()]
        if year < 100:
            current_two_digit_year = datetime.utcnow().year % 100
            year += 2000 if year <= current_two_digit_year else 1900
        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return ""

    parsed = pd.to_datetime(pd.Series([raw_value]), dayfirst=True, errors="coerce").iloc[0]
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def normalize_date_of_birth_series(series):
    return series.fillna("").astype(str).apply(normalize_date_of_birth)


def generate_internal_tracking_id():
    return f"track-{uuid.uuid4().hex[:16]}"


def build_internal_match_key(school_name, student_name, date_of_birth):
    school_token = normalize_student_name(school_name)
    student_token = normalize_student_name(student_name)
    dob_token = normalize_date_of_birth(date_of_birth)
    if not school_token or not student_token or not dob_token:
        return ""
    return f"{school_token}::{student_token}::{dob_token}"


def get_backend_worksheet_name(table_key):
    name = Path(str(table_key)).stem.upper()
    if len(name) <= 99:
        return name
    return name[:99]


_LAST_GOOGLE_SHEETS_ERROR = ""


def _streamlit_secret_get(key, default=None):
    try:
        if hasattr(st, "secrets") and st.secrets is not None and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return default


def resolve_google_sheet_id():
    sheet_id = os.environ.get(GOOGLE_SHEETS_ID_ENV, "").strip()
    if sheet_id:
        return sheet_id
    value = _streamlit_secret_get(GOOGLE_SHEETS_ID_ENV)
    if value is not None:
        return str(value).strip()
    return ""


def resolve_google_service_account_json_string():
    # I will first check if the environment variable is set directly
    raw = os.environ.get(GOOGLE_SERVICE_ACCOUNT_JSON_ENV, "").strip()
    if raw:
        return raw

    # Next, I'll check if the JSON was saved directly as a string in Streamlit Secrets
    value = _streamlit_secret_get(GOOGLE_SERVICE_ACCOUNT_JSON_ENV)
    if value is not None and str(value).strip():
        return str(value).strip()

    # Finally, I will check if it was formatted under the [gcp_service_account] section
    gcp_block = _streamlit_secret_get("gcp_service_account")

    # Instead of using isinstance() which fails on Streamlit's custom Secrets object,
    # I will simply check if the block exists, and convert it to a standard dictionary!
    if gcp_block is not None:
        # I am converting it to dict() so json.dumps can easily serialize it to text
        return json.dumps(dict(gcp_block))

    # If nothing is found, I will return an empty string
    return ""


def get_google_sheet_last_error():
    return _LAST_GOOGLE_SHEETS_ERROR


def get_google_sheet():
    global _LAST_GOOGLE_SHEETS_ERROR
    _LAST_GOOGLE_SHEETS_ERROR = ""
    sheet_id = resolve_google_sheet_id()
    service_account_json = resolve_google_service_account_json_string()
    if not sheet_id:
        _LAST_GOOGLE_SHEETS_ERROR = "Missing Google Sheet ID. Set EDUPULSE_GOOGLE_SHEETS_ID in secrets or environment."
        return None
    if not service_account_json:
        _LAST_GOOGLE_SHEETS_ERROR = (
            "Missing service account JSON. Use either EDUPULSE_GOOGLE_SERVICE_ACCOUNT_JSON "
            "or a [gcp_service_account] table in Streamlit secrets (type, project_id, private_key, …)."
        )
        return None
    if gspread is None or Credentials is None:
        _LAST_GOOGLE_SHEETS_ERROR = "gspread/google-auth not installed. Add gspread and google-auth to requirements."
        return None
    try:
        credentials_info = json.loads(service_account_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        return client.open_by_key(sheet_id)
    except Exception as exc:
        _LAST_GOOGLE_SHEETS_ERROR = f"{type(exc).__name__}: {exc}"
        return None


def read_table_df(table_key, columns, dtype=str):
    sheet = get_google_sheet()
    worksheet_name = get_backend_worksheet_name(table_key)
    if sheet is not None:
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except Exception:
            return pd.DataFrame(columns=columns)
        try:
            records = worksheet.get_all_records(default_blank="")
            df = pd.DataFrame(records)
        except Exception:
            return pd.DataFrame(columns=columns)
        if df.empty:
            return pd.DataFrame(columns=columns)
        for column in columns:
            if column not in df.columns:
                df[column] = ""
        safe_df = df[columns].copy().fillna("")
        for column in columns:
            safe_df[column] = safe_df[column].astype(str).str.strip()
        return safe_df

    if not os.path.isfile(table_key):
        return pd.DataFrame(columns=columns)
    try:
        # Handle BOM (Byte Order Mark) issues common in Excel-to-CSV exports
        local_df = pd.read_csv(table_key, dtype=dtype, encoding='utf-8-sig').fillna("")
        # Clean column names to remove any hidden characters
        local_df.columns = [c.encode('ascii', 'ignore').decode('ascii').strip() for c in local_df.columns]
    except Exception:
        return pd.DataFrame(columns=columns)
    for column in columns:
        if column not in local_df.columns:
            local_df[column] = ""
    safe_local_df = local_df[columns].copy()
    for column in columns:
        safe_local_df[column] = safe_local_df[column].astype(str).str.strip()
    return safe_local_df


def write_table_df(table_key, df, columns):
    safe_df = df.copy()
    for column in columns:
        if column not in safe_df.columns:
            safe_df[column] = ""
    safe_df = safe_df[columns].fillna("")

    sheet = get_google_sheet()
    worksheet_name = get_backend_worksheet_name(table_key)
    if sheet is not None:
        try:
            try:
                worksheet = sheet.worksheet(worksheet_name)
            except Exception:
                worksheet = sheet.add_worksheet(title=worksheet_name, rows=max(1000, len(safe_df) + 5), cols=max(26, len(columns) + 2))
            worksheet.clear()
            # Explicitly convert each cell to string to handle mixed types
            payload = [columns]
            for row in safe_df.values.tolist():
                payload.append([str(cell) if cell is not None else "" for cell in row])
            worksheet.update("A1", payload)
            return
        except Exception:
            pass

    safe_df.to_csv(table_key, index=False)


def get_final_subject_column(subject_col_or_prefix):
    prefix = str(subject_col_or_prefix).replace(FINAL_SUFFIX, "").replace(PREDICTED_SUFFIX, "")
    return f"{prefix}{FINAL_SUFFIX}"


def get_predicted_subject_column(subject_col_or_prefix):
    prefix = str(subject_col_or_prefix).replace(FINAL_SUFFIX, "").replace(PREDICTED_SUFFIX, "")
    return f"{prefix}{PREDICTED_SUFFIX}"


def build_empty_student_row():
    return {column: pd.NA for column in EXPECTED_DATA_COLUMNS}


def migrate_legacy_circuit_df(circuit_df):
    if circuit_df is None:
        return pd.DataFrame(columns=EXPECTED_CIRCUIT_COLUMNS), False

    working_df = circuit_df.copy()
    changed = False
    for column in EXPECTED_CIRCUIT_COLUMNS:
        if column not in working_df.columns:
            working_df[column] = ""
            changed = True

    working_df = clean_uploaded_dataframe(working_df)
    working_df["School_Name"] = working_df["School_Name"].fillna("").astype(str).str.strip()
    working_df["Circuit"] = working_df["Circuit"].fillna("").astype(str).str.strip()
    raw_school_types = working_df["School_Type"].copy()
    working_df["School_Type"] = normalize_school_type_series(working_df["School_Type"])
    missing_school_type_mask = working_df["School_Type"].eq("")
    if missing_school_type_mask.any():
        changed = True
        working_df.loc[missing_school_type_mask, "School_Type"] = MIGRATION_SCHOOL_TYPE_PLACEHOLDER
    if not raw_school_types.fillna("").astype(str).equals(working_df["School_Type"].fillna("").astype(str)):
        changed = True

    working_df = working_df[EXPECTED_CIRCUIT_COLUMNS].copy()
    working_df = working_df[(working_df["School_Name"] != "") & (working_df["Circuit"] != "")]
    working_df = working_df.drop_duplicates(subset=["School_Name"], keep="last").reset_index(drop=True)
    return working_df, changed


def migrate_legacy_student_df(student_df, school_profile_lookup=None):
    if student_df is None:
        return pd.DataFrame(columns=EXPECTED_DATA_COLUMNS), False

    working_df = student_df.copy()
    changed = False
    for column in EXPECTED_DATA_COLUMNS:
        if column not in working_df.columns:
            working_df[column] = pd.NA
            changed = True

    working_df = clean_uploaded_dataframe(working_df)
    for column in EXPECTED_DATA_COLUMNS:
        if column not in working_df.columns:
            working_df[column] = pd.NA
            changed = True
    working_df = working_df[EXPECTED_DATA_COLUMNS].copy()

    text_columns = [
        "Internal_Tracking_ID",
        "Student_ID",
        "Official_Index_Number",
        "Student_Name",
        "Gender",
        "Date_of_Birth",
        "School_Name",
        "Circuit",
        "School_Type",
        "Official_Results_Raw",
        "Action_Zone",
    ]
    for column in text_columns:
        working_df[column] = working_df[column].fillna("").astype(str).str.strip()
        working_df.loc[working_df[column].isin(["nan", "None"]), column] = ""

    working_df["Date_of_Birth"] = normalize_date_of_birth_series(working_df["Date_of_Birth"])

    if school_profile_lookup is None:
        school_profile_lookup = {}
    if school_profile_lookup and "School_Name" in working_df.columns:
        mapped_circuits = working_df["School_Name"].map(lambda school: school_profile_lookup.get(school, {}).get("Circuit", ""))
        mapped_school_types = working_df["School_Name"].map(lambda school: school_profile_lookup.get(school, {}).get("School_Type", ""))
        if working_df["Circuit"].fillna("").astype(str).str.strip().eq("").any():
            changed = True
        if working_df["School_Type"].fillna("").astype(str).str.strip().eq("").any():
            changed = True
        working_df["Circuit"] = working_df["Circuit"].mask(working_df["Circuit"].eq(""), mapped_circuits)
        working_df["School_Type"] = working_df["School_Type"].mask(working_df["School_Type"].eq(""), mapped_school_types)

    raw_school_types = working_df["School_Type"].copy()
    working_df["School_Type"] = normalize_school_type_series(working_df["School_Type"])
    missing_school_type_mask = working_df["School_Type"].eq("")
    if missing_school_type_mask.any():
        working_df.loc[missing_school_type_mask, "School_Type"] = MIGRATION_SCHOOL_TYPE_PLACEHOLDER
        changed = True
    if not raw_school_types.fillna("").astype(str).equals(working_df["School_Type"].fillna("").astype(str)):
        changed = True

    internal_id_mask = working_df["Internal_Tracking_ID"].fillna("").astype(str).str.strip().eq("")
    if internal_id_mask.any():
        changed = True
        working_df.loc[internal_id_mask, "Internal_Tracking_ID"] = [generate_internal_tracking_id() for _ in range(int(internal_id_mask.sum()))]

    official_id_mask = working_df["Official_Index_Number"].fillna("").astype(str).str.strip().eq("")
    candidate_official_id_mask = working_df["Student_ID"].fillna("").astype(str).str.fullmatch(r"\d{10}")
    fill_official_mask = official_id_mask & candidate_official_id_mask.fillna(False)
    if fill_official_mask.any():
        changed = True
        working_df.loc[fill_official_mask, "Official_Index_Number"] = working_df.loc[fill_official_mask, "Student_ID"]

    for predicted_col, final_col, prefix in zip(PREDICTED_SUBJECT_COLUMNS, FINAL_SUBJECT_COLUMNS, SUBJECT_PREFIXES):
        predicted_values = pd.to_numeric(working_df[predicted_col], errors="coerce")
        final_values = pd.to_numeric(working_df[final_col], errors="coerce")
        assessment_columns = [f"{prefix}_{suffix}" for suffix in ASSESSMENT_SUFFIXES if f"{prefix}_{suffix}" in working_df.columns]
        has_assessment = working_df[assessment_columns].apply(pd.to_numeric, errors="coerce").notna().any(axis=1) if assessment_columns else pd.Series(False, index=working_df.index)
        has_official_context = working_df["Official_Index_Number"].fillna("").astype(str).str.strip().ne("") | working_df["Official_Results_Raw"].fillna("").astype(str).str.strip().ne("")
        legacy_prediction_mask = predicted_values.isna() & final_values.notna() & has_assessment & ~has_official_context
        if legacy_prediction_mask.any():
            working_df.loc[legacy_prediction_mask, predicted_col] = working_df.loc[legacy_prediction_mask, final_col]
            working_df.loc[legacy_prediction_mask, final_col] = pd.NA
            changed = True

    return working_df, changed


def run_storage_migrations():
    sheet = get_google_sheet()
    if sheet is not None:
        current_circuits_df = read_table_df(CIRCUITS_FILE, EXPECTED_CIRCUIT_COLUMNS)
        migrated_circuits_df, circuits_changed = migrate_legacy_circuit_df(current_circuits_df)
        if circuits_changed:
            write_table_df(CIRCUITS_FILE, migrated_circuits_df, EXPECTED_CIRCUIT_COLUMNS)

        school_profile_lookup = migrated_circuits_df.set_index("School_Name")[["Circuit", "School_Type"]].to_dict("index") if not migrated_circuits_df.empty else {}
        current_student_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
        migrated_student_df, student_changed = migrate_legacy_student_df(current_student_df, school_profile_lookup=school_profile_lookup)
        if student_changed:
            write_table_df(DATA_FILE, migrated_student_df, EXPECTED_DATA_COLUMNS)
        return

    if os.path.isfile(CIRCUITS_FILE):
        try:
            local_circuits_df = pd.read_csv(CIRCUITS_FILE, dtype=str).fillna("")
        except Exception:
            local_circuits_df = pd.DataFrame(columns=EXPECTED_CIRCUIT_COLUMNS)
        migrated_circuits_df, circuits_changed = migrate_legacy_circuit_df(local_circuits_df)
        if circuits_changed:
            migrated_circuits_df.to_csv(CIRCUITS_FILE, index=False)
    else:
        migrated_circuits_df = pd.DataFrame(columns=EXPECTED_CIRCUIT_COLUMNS)

    school_profile_lookup = migrated_circuits_df.set_index("School_Name")[["Circuit", "School_Type"]].to_dict("index") if not migrated_circuits_df.empty else {}
    if os.path.isfile(DATA_FILE):
        try:
            local_student_df = pd.read_csv(DATA_FILE, dtype=str).fillna("")
        except Exception:
            local_student_df = pd.DataFrame(columns=EXPECTED_DATA_COLUMNS)
        migrated_student_df, student_changed = migrate_legacy_student_df(local_student_df, school_profile_lookup=school_profile_lookup)
        if student_changed:
            migrated_student_df.to_csv(DATA_FILE, index=False)


def read_app_config_record(default_config):
    config_columns = list(default_config.keys())
    config_df = read_table_df(APP_CONFIG_FILE, config_columns)
    if not config_df.empty:
        return config_df.tail(1).iloc[0].to_dict()
    if os.path.isfile(APP_CONFIG_FILE):
        try:
            with open(APP_CONFIG_FILE, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            return default_config
    return default_config


def write_app_config_record(config):
    config_columns = list(config.keys())
    write_table_df(APP_CONFIG_FILE, pd.DataFrame([config], columns=config_columns), config_columns)
    try:
        with open(APP_CONFIG_FILE, "w", encoding="utf-8") as handle:
            json.dump(config, handle, indent=2)
    except Exception:
        pass


def get_scope_label():
    config = load_app_config()
    return config["district_name"] or "District/Municipal"


def slugify_name(value):
    cleaned = re.sub(r"[^A-Za-z0-9]+", "", str(value).upper())
    return cleaned[:12] or "DIRECTOR"


def generate_security_key(district_name):
    return f"{slugify_name(district_name)}-{random.randint(1000, 9999)}"


def get_platform_owner_secret():
    env_val = os.environ.get(DIRECTOR_REGISTRATION_KEY_ENV, "").strip()
    if env_val:
        return env_val
    secret_val = _streamlit_secret_get(DIRECTOR_REGISTRATION_KEY_ENV)
    if secret_val is not None and str(secret_val).strip():
        return str(secret_val).strip()
    return "BloomCore-Owner-Set-Me"


def generate_director_registration_key():
    return f"DIRECTOR-{random.randint(100000, 999999)}"


def set_director_registration_key(new_key):
    config = load_app_config()
    config["director_registration_key"] = str(new_key).strip()
    config["director_registration_key_created_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    save_app_config(config)


def consume_director_registration_key():
    config = load_app_config()
    config["director_registration_key"] = ""
    config["director_registration_key_created_at"] = ""
    save_app_config(config)


def validate_password_strength(password):
    """
    Check password strength requirements.
    Returns (is_valid, requirements_list) where requirements_list is a list of tuples (requirement, is_met).
    """
    requirements = [
        ("At least 8 characters", len(password) >= 8),
        ("At least 1 uppercase letter (A-Z)", any(c.isupper() for c in password)),
        ("At least 1 lowercase letter (a-z)", any(c.islower() for c in password)),
        ("At least 1 digit (0-9)", any(c.isdigit() for c in password)),
        ("At least 1 special symbol (!@#$%^&* etc.)", any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password)),
    ]
    is_valid = all(met for _, met in requirements)
    return is_valid, requirements


def render_password_strength_indicator(password):
    """Render a visual password strength indicator in Streamlit."""
    if not password:
        return
    is_valid, requirements = validate_password_strength(password)
    score = sum(1 for _, met in requirements if met)
    total = len(requirements)
    if score <= 2:
        color, emoji, label = "#ff4b4b", "🔴", "Weak"
    elif score <= 4:
        color, emoji, label = "#ffa421", "🟡", "Moderate"
    else:
        color, emoji, label = "#2ecc71", "🟢", "Strong"
    st.markdown(
        f"<div style='margin: -10px 0 10px 0;'>"
        f"<span style='color: {color}; font-weight: 600;'>{emoji} Password Strength: {label} ({score}/{total})</span>"
        f"</div>",
        unsafe_allow_html=True,
    )
    with st.expander("Password Requirements", expanded=not is_valid):
        for req, met in requirements:
            icon = "✅" if met else "❌"
            color = "#2ecc71" if met else "#999"
            st.markdown(f"<span style='color: {color};'>{icon} {req}</span>", unsafe_allow_html=True)


def render_password_match_indicator(password, confirm_password):
    """Render a visual indicator showing if passwords match."""
    if not confirm_password:
        return
    if password == confirm_password:
        st.markdown(
            "<div style='margin: -10px 0 10px 0;'><span style='color: #2ecc71; font-weight: 600;'>✅ Passwords match</span></div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div style='margin: -10px 0 10px 0;'><span style='color: #ff4b4b; font-weight: 600;'>❌ Passwords do not match</span></div>",
            unsafe_allow_html=True,
        )


def director_registration_key_is_valid(input_key):
    config = load_app_config()
    return bool(str(input_key).strip() and str(input_key).strip() == str(config.get("director_registration_key", "")).strip())


def activate_director_context(username, district_name, security_key):
    config = load_app_config()
    config.update(
        {
            "district_name": district_name,
            "director_username": username,
            "headteacher_security_key": security_key,
        }
    )
    save_app_config(config)


def load_users_df():
    users_df = read_table_df(USERS_FILE, USERS_COLUMNS)
    if users_df.empty:
        return pd.DataFrame(columns=USERS_COLUMNS)

    for column in USERS_COLUMNS:
        if column not in users_df.columns:
            users_df[column] = ""

    users_df = users_df[USERS_COLUMNS].copy()
    for column in USERS_COLUMNS:
        users_df[column] = users_df[column].astype(str).str.strip()
    users_df = users_df[users_df["username"] != ""]
    return users_df


def save_users_df(users_df):
    normalized_df = users_df.copy()
    for column in USERS_COLUMNS:
        if column not in normalized_df.columns:
            normalized_df[column] = ""
    normalized_df = normalized_df[USERS_COLUMNS].fillna("")
    write_table_df(USERS_FILE, normalized_df, USERS_COLUMNS)


def load_users():
    users_df = load_users_df()
    return users_df.set_index("username").to_dict("index")


# ============================================================
# 4. NOTIFICATIONS, SAVED SCENARIOS, AND CONTACT DIRECTORY
# ============================================================
def load_notifications_df():
    notifications_df = read_table_df(NOTIFICATIONS_FILE, NOTIFICATION_COLUMNS)
    if notifications_df.empty:
        return pd.DataFrame(columns=NOTIFICATION_COLUMNS)

    for column in NOTIFICATION_COLUMNS:
        if column not in notifications_df.columns:
            notifications_df[column] = ""

    notifications_df = notifications_df[NOTIFICATION_COLUMNS].copy()
    for column in NOTIFICATION_COLUMNS:
        notifications_df[column] = notifications_df[column].astype(str).str.strip()
    return notifications_df


def save_notifications_df(notifications_df):
    safe_df = notifications_df.copy()
    for column in NOTIFICATION_COLUMNS:
        if column not in safe_df.columns:
            safe_df[column] = ""
    safe_df = safe_df[NOTIFICATION_COLUMNS].fillna("")
    write_table_df(NOTIFICATIONS_FILE, safe_df, NOTIFICATION_COLUMNS)


def load_scenarios_df():
    scenarios_df = read_table_df(SCENARIOS_FILE, SCENARIO_COLUMNS)
    if scenarios_df.empty:
        return pd.DataFrame(columns=SCENARIO_COLUMNS)

    for column in SCENARIO_COLUMNS:
        if column not in scenarios_df.columns:
            scenarios_df[column] = ""
    scenarios_df = scenarios_df[SCENARIO_COLUMNS].copy()
    for column in SCENARIO_COLUMNS:
        scenarios_df[column] = scenarios_df[column].astype(str).str.strip()
    return scenarios_df


def save_scenarios_df(scenarios_df):
    safe_df = scenarios_df.copy()
    for column in SCENARIO_COLUMNS:
        if column not in safe_df.columns:
            safe_df[column] = ""
    safe_df = safe_df[SCENARIO_COLUMNS].fillna("")
    write_table_df(SCENARIOS_FILE, safe_df, SCENARIO_COLUMNS)


def load_contacts_df():
    contacts_df = read_table_df(CONTACTS_FILE, CONTACT_COLUMNS)
    if contacts_df.empty:
        return pd.DataFrame(columns=CONTACT_COLUMNS)

    for column in CONTACT_COLUMNS:
        if column not in contacts_df.columns:
            contacts_df[column] = ""
    contacts_df = contacts_df[CONTACT_COLUMNS].copy()
    for column in CONTACT_COLUMNS:
        contacts_df[column] = contacts_df[column].astype(str).str.strip()
    return contacts_df


def save_contacts_df(contacts_df):
    safe_df = contacts_df.copy()
    for column in CONTACT_COLUMNS:
        if column not in safe_df.columns:
            safe_df[column] = ""
    safe_df = safe_df[CONTACT_COLUMNS].fillna("")
    write_table_df(CONTACTS_FILE, safe_df, CONTACT_COLUMNS)


def load_manual_predictions_df():
    manual_df = read_table_df(MANUAL_PREDICTIONS_FILE, MANUAL_PREDICTION_COLUMNS)
    if manual_df.empty:
        return pd.DataFrame(columns=MANUAL_PREDICTION_COLUMNS)

    for column in MANUAL_PREDICTION_COLUMNS:
        if column not in manual_df.columns:
            manual_df[column] = ""
    return manual_df[MANUAL_PREDICTION_COLUMNS].copy()


def save_manual_predictions_df(manual_df):
    safe_df = manual_df.copy()
    for column in MANUAL_PREDICTION_COLUMNS:
        if column not in safe_df.columns:
            safe_df[column] = ""
    safe_df = safe_df[MANUAL_PREDICTION_COLUMNS].fillna("")
    write_table_df(MANUAL_PREDICTIONS_FILE, safe_df, MANUAL_PREDICTION_COLUMNS)


def save_manual_prediction(prediction_row):
    manual_df = load_manual_predictions_df()
    record_df = pd.DataFrame([prediction_row], columns=MANUAL_PREDICTION_COLUMNS)
    manual_df = pd.concat([manual_df, record_df], ignore_index=True)
    save_manual_predictions_df(manual_df)


def build_manual_prediction_summary_df(district=""):
    manual_df = load_manual_predictions_df()
    if manual_df.empty:
        return manual_df

    if district:
        manual_df = manual_df[
            manual_df["district"].fillna("").astype(str).str.strip().eq(str(district).strip())
        ].copy()

    if manual_df.empty:
        return manual_df

    manual_df["created_at"] = manual_df["created_at"].fillna("").astype(str).str.strip()
    manual_df["dedupe_key"] = np.where(
        manual_df["student_id"].fillna("").astype(str).str.strip() != "",
        manual_df["school"].fillna("").astype(str).str.strip() + "::" + manual_df["student_id"].fillna("").astype(str).str.strip(),
        manual_df["school"].fillna("").astype(str).str.strip() + "::" + manual_df["student_name"].fillna("").astype(str).str.strip(),
    )
    manual_df = manual_df.sort_values("created_at", ascending=False).drop_duplicates(subset=["dedupe_key"], keep="first")
    for numeric_column in ["aggregate", "best_six_raw_total"]:
        manual_df[numeric_column] = pd.to_numeric(manual_df[numeric_column], errors="coerce")
    return manual_df.drop(columns=["dedupe_key"], errors="ignore").reset_index(drop=True)


def load_match_review_df():
    review_df = read_table_df(OFFICIAL_MATCH_REVIEW_FILE, OFFICIAL_MATCH_REVIEW_COLUMNS)
    if review_df.empty:
        return pd.DataFrame(columns=OFFICIAL_MATCH_REVIEW_COLUMNS)

    for column in OFFICIAL_MATCH_REVIEW_COLUMNS:
        if column not in review_df.columns:
            review_df[column] = ""
    return review_df[OFFICIAL_MATCH_REVIEW_COLUMNS].copy()


def save_match_review_df(review_df):
    safe_df = review_df.copy()
    for column in OFFICIAL_MATCH_REVIEW_COLUMNS:
        if column not in safe_df.columns:
            safe_df[column] = ""
    safe_df = safe_df[OFFICIAL_MATCH_REVIEW_COLUMNS].fillna("")
    write_table_df(OFFICIAL_MATCH_REVIEW_FILE, safe_df, OFFICIAL_MATCH_REVIEW_COLUMNS)


def append_match_review(review_row):
    review_df = load_match_review_df()
    review_df = pd.concat([review_df, pd.DataFrame([review_row], columns=OFFICIAL_MATCH_REVIEW_COLUMNS)], ignore_index=True)
    save_match_review_df(review_df)


def upsert_contact(contact_row):
    contacts_df = load_contacts_df()
    district = str(contact_row.get("district", "")).strip()
    school = str(contact_row.get("school", "")).strip()
    mask = (
        contacts_df["district"].fillna("").astype(str).str.strip().eq(district)
        & contacts_df["school"].fillna("").astype(str).str.strip().eq(school)
    )
    updated_row = pd.DataFrame([contact_row], columns=CONTACT_COLUMNS)
    contacts_df = contacts_df.loc[~mask].copy()
    contacts_df = pd.concat([contacts_df, updated_row], ignore_index=True)
    save_contacts_df(contacts_df)


def smtp_config_is_ready(config):
    return bool(str(config.get("smtp_host", "")).strip() and str(config.get("smtp_sender_email", "")).strip())


def create_notification(
    event_type,
    message,
    target_role="Director",
    school="",
    circuit="",
    student_id="",
    student_name="",
    created_by="",
    district="",
):
    notifications_df = load_notifications_df()
    notification_row = pd.DataFrame(
        [
            {
                "notification_id": f"notif-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}-{random.randint(1000, 9999)}",
                "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "district": district or get_scope_label(),
                "target_role": target_role,
                "event_type": event_type,
                "created_by": created_by,
                "school": school,
                "circuit": circuit,
                "student_id": student_id,
                "student_name": student_name,
                "message": message,
                "status": "Unread",
            }
        ]
    )
    notifications_df = pd.concat([notifications_df, notification_row], ignore_index=True)
    save_notifications_df(notifications_df)


def get_notifications(target_role="Director", district="", unread_only=False):
    notifications_df = load_notifications_df()
    if notifications_df.empty:
        return notifications_df

    filtered_df = notifications_df[
        notifications_df["target_role"].fillna("").astype(str).str.strip() == str(target_role).strip()
    ].copy()

    if district:
        filtered_df = filtered_df[
            filtered_df["district"].fillna("").astype(str).str.strip() == str(district).strip()
        ].copy()

    if unread_only:
        filtered_df = filtered_df[
            filtered_df["status"].fillna("").astype(str).str.strip().eq("Unread")
        ].copy()

    return filtered_df.sort_values("created_at", ascending=False).reset_index(drop=True)


def mark_notifications_as_read(target_role="Director", district=""):
    notifications_df = load_notifications_df()
    if notifications_df.empty:
        return

    mask = notifications_df["target_role"].fillna("").astype(str).str.strip().eq(str(target_role).strip())
    if district:
        mask = mask & notifications_df["district"].fillna("").astype(str).str.strip().eq(str(district).strip())
    notifications_df.loc[mask, "status"] = "Read"
    save_notifications_df(notifications_df)


def initialize_empty_student_dataset():
    write_table_df(DATA_FILE, pd.DataFrame(columns=EXPECTED_DATA_COLUMNS), EXPECTED_DATA_COLUMNS)


def initialize_empty_circuit_dataset():
    write_table_df(CIRCUITS_FILE, pd.DataFrame(columns=EXPECTED_CIRCUIT_COLUMNS), EXPECTED_CIRCUIT_COLUMNS)


def register_user(username, password, role, school, district="", security_key="", email="", security_question="", security_answer=""):
    username = username.strip()
    school = school.strip()
    district = district.strip()
    security_key = security_key.strip()
    email = email.strip()
    security_question = security_question.strip()
    security_answer = security_answer.strip()

    if not username:
        raise ValueError("Username is required.")
    if not password:
        raise ValueError("Password is required.")
    if len(password) < 6:
        raise ValueError("Password must be at least 6 characters.")
    if role == "Director":
        school = "ALL"
        if not district:
            raise ValueError("District/Municipal name is required for Director registration.")
        if not security_key:
            security_key = generate_security_key(district)
    elif not school:
        raise ValueError("Please choose a school.")

    users_df = load_users_df()
    existing_users = {**MASTER_CREDENTIALS, **load_users()}
    if username in existing_users:
        raise ValueError("That username already exists.")

    # Hash security answer if provided
    hashed_security_answer = hash_security_answer(security_answer) if security_answer else ""

    users_df = pd.concat(
        [
            users_df,
            pd.DataFrame(
                [
                    {
                        "username": username,
                        "password": hash_password(password),
                        "role": role,
                        "school": school,
                        "district": district,
                        "security_key": security_key,
                        "email": email,
                        "security_question": security_question,
                        "security_answer": hashed_security_answer,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    save_users_df(users_df)
    return {"username": username, "district": district, "security_key": security_key, "school": school}


# ============================================================
# 4b. PASSWORD RESET AND RECOVERY FUNCTIONS
# ============================================================
SECURITY_QUESTION_OPTIONS = [
    "What was the name of your first school?",
    "What is your mother's maiden name?",
    "What was the name of your first pet?",
    "What city were you born in?",
    "What is your favorite book?",
]


def hash_security_answer(answer):
    """Hash security answer for storage (case-insensitive)."""
    normalized = str(answer).strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def verify_security_answer(stored_hash, provided_answer):
    """Verify a security answer against stored hash."""
    return stored_hash == hash_security_answer(provided_answer)


def generate_reset_token():
    """Generate a secure random reset token."""
    return secrets.token_urlsafe(32)


def get_user_by_username(username):
    """Get user record by username, returns None if not found."""
    all_users = {**MASTER_CREDENTIALS, **load_users()}
    return all_users.get(username)


def update_user_password(username, new_password):
    """Update a user's password. Returns True on success, False on failure."""
    users_df = load_users_df()
    if users_df.empty or username not in users_df["username"].values:
        return False
    hashed_password = hash_password(new_password)
    users_df.loc[users_df["username"] == username, "password"] = hashed_password
    save_users_df(users_df)
    return True


def verify_owner_secret(provided_secret):
    """Verify the platform owner secret for admin password resets."""
    return str(provided_secret).strip() == get_platform_owner_secret()


def send_password_reset_email(user_email, reset_token, username):
    """Send password reset email if SMTP is configured. Returns (success, message)."""
    config = load_app_config()
    smtp_host = config.get("smtp_host", "").strip()
    smtp_port = int(config.get("smtp_port", "587"))
    smtp_username = config.get("smtp_username", "").strip()
    smtp_password = config.get("smtp_password", "").strip()
    smtp_sender = config.get("smtp_sender_email", "").strip()
    use_tls = str(config.get("smtp_use_tls", "true")).lower() == "true"

    if not all([smtp_host, smtp_username, smtp_password, smtp_sender]):
        return False, "Email service not configured. Please use security question or contact your Director."

    try:
        msg = MIMEMultipart()
        msg["From"] = smtp_sender
        msg["To"] = user_email
        msg["Subject"] = "EduPulse Password Reset Request"

        body = f"""Hello {username},

You requested a password reset for your EduPulse account.

Your reset code is: {reset_token}

Enter this code in the password reset page to set a new password.
This code expires in 30 minutes.

If you did not request this reset, please contact your Director immediately.

---
EduPulse Education Management System
"""
        msg.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP(smtp_host, smtp_port)
        if use_tls:
            server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_sender, user_email, msg.as_string())
        server.quit()
        return True, "Reset code sent to your email."
    except Exception as exc:
        return False, f"Failed to send email: {exc}"


# ============================================================
# 5. CIRCUIT MAPPING, EXCEL TEMPLATES, AND DATASET VALIDATION
# ============================================================
def load_circuit_lookup():
    mapping_df = load_circuit_mapping_df()
    if mapping_df.empty:
        return {}
    # Create case-insensitive lookup with lowercase keys
    base_lookup = pd.Series(mapping_df["Circuit"].values, index=mapping_df["School_Name"]).to_dict()
    case_insensitive_lookup = {}
    for school_name, circuit in base_lookup.items():
        case_insensitive_lookup[school_name] = circuit
        case_insensitive_lookup[school_name.lower()] = circuit
        case_insensitive_lookup[school_name.upper()] = circuit
    return case_insensitive_lookup


def load_school_profile_lookup():
    mapping_df = load_circuit_mapping_df()
    if mapping_df.empty:
        return {}
    mapping_df = mapping_df.drop_duplicates(subset=["School_Name"], keep="last")
    # Create case-insensitive lookup by normalizing school names to lowercase
    lookup = mapping_df.set_index("School_Name")[["Circuit", "School_Type"]].to_dict("index")
    # Add lowercase keys for case-insensitive matching
    case_insensitive_lookup = {}
    for school_name, profile in lookup.items():
        case_insensitive_lookup[school_name] = profile
        case_insensitive_lookup[school_name.lower()] = profile
        case_insensitive_lookup[school_name.upper()] = profile
    return case_insensitive_lookup


def load_circuit_mapping_df():
    mapping_df = read_table_df(CIRCUITS_FILE, EXPECTED_CIRCUIT_COLUMNS)
    if mapping_df.empty:
        return pd.DataFrame(columns=EXPECTED_CIRCUIT_COLUMNS)

    mapping_df.columns = [str(column).replace("﻿", "").strip() for column in mapping_df.columns]
    if "School_Type" not in mapping_df.columns:
        mapping_df["School_Type"] = ""

    columns_valid, _ = validate_circuit_columns(mapping_df.columns.tolist())
    if not columns_valid:
        return pd.DataFrame(columns=EXPECTED_CIRCUIT_COLUMNS)

    mapping_df = clean_uploaded_dataframe(mapping_df)
    mapping_df["School_Name"] = mapping_df["School_Name"].astype(str).str.strip()
    mapping_df["Circuit"] = mapping_df["Circuit"].astype(str).str.strip()
    mapping_df["School_Type"] = normalize_school_type_series(mapping_df["School_Type"])
    mapping_df.loc[mapping_df["School_Type"] == "", "School_Type"] = MIGRATION_SCHOOL_TYPE_PLACEHOLDER
    mapping_df = mapping_df[(mapping_df["School_Name"] != "") & (mapping_df["Circuit"] != "")]
    return mapping_df[EXPECTED_CIRCUIT_COLUMNS].drop_duplicates(subset=["School_Name"], keep="last").reset_index(drop=True)


def load_all_circuits():
    lookup = load_circuit_lookup()
    return sorted({str(circuit).strip() for circuit in lookup.values() if str(circuit).strip()})


def combine_known_circuits(*sources):
    circuits = set()
    for source in sources:
        if source is None:
            continue
        if isinstance(source, pd.DataFrame):
            if "Circuit" not in source.columns:
                continue
            values = source["Circuit"].tolist()
        elif isinstance(source, pd.Series):
            values = source.tolist()
        else:
            values = list(source)

        for value in values:
            circuit = str(value).strip()
            if circuit and circuit.lower() not in {"nan", "none"}:
                circuits.add(circuit)
    return sorted(circuits)


def load_school_options():
    circuits_df = load_circuit_mapping_df()
    if circuits_df.empty:
        return []
    return sorted(circuits_df["School_Name"].dropna().astype(str).str.strip().unique().tolist())


def get_circuit_file_status():
    circuits_df = load_circuit_mapping_df()
    if circuits_df.empty:
        return {
            "ready": False,
            "severity": "info",
            "message": "No active circuits file has been uploaded yet. The Director must upload the official circuits dataset before school onboarding can continue.",
        }
    return {
        "ready": True,
        "severity": "success",
        "message": f"Active circuits file ready: {len(circuits_df)} school-to-circuit mappings detected.",
        "rows": len(circuits_df),
    }


def validate_live_dataset_columns(columns):
    normalized_columns = [str(column).replace("﻿", "").strip() for column in columns]
    required_columns = ["Student_ID", "Student_Name", "School_Name"]
    missing_required = [column for column in required_columns if column not in normalized_columns]
    if missing_required:
        return False, "The active dataset is missing required columns: " + ", ".join(missing_required)
    return True, "Live dataset columns are readable."


def validate_prediction_template_columns(columns):
    normalized_columns = [str(column).replace("﻿", "").strip() for column in columns]
    if normalized_columns == PREDICTION_TEMPLATE_COLUMNS:
        return True, "Column structure matches the required prediction template."
    if sorted(normalized_columns) == sorted(PREDICTION_TEMPLATE_COLUMNS) and len(normalized_columns) == len(PREDICTION_TEMPLATE_COLUMNS):
        return True, "Column names match the required prediction template."
    if normalized_columns == HEADTEACHER_UPLOAD_TEMPLATE_COLUMNS:
        return True, "Column structure matches the headteacher upload template."
    if sorted(normalized_columns) == sorted(HEADTEACHER_UPLOAD_TEMPLATE_COLUMNS) and len(normalized_columns) == len(HEADTEACHER_UPLOAD_TEMPLATE_COLUMNS):
        return True, "Column names match the headteacher upload template."

    _required = HEADTEACHER_UPLOAD_TEMPLATE_COLUMNS
    missing_columns = [column for column in _required if column not in normalized_columns]
    extra_columns = [column for column in normalized_columns if column not in PREDICTION_TEMPLATE_COLUMNS]
    message_parts = []
    if missing_columns:
        message_parts.append("Missing columns: " + ", ".join(missing_columns))
    if extra_columns:
        message_parts.append("Unexpected columns: " + ", ".join(extra_columns))
    if not missing_columns and not extra_columns:
        message_parts.append(
            "The header names are correct but the column order was changed. Please use the template without editing the header row."
        )
    return False, "Dataset template mismatch. " + " ".join(message_parts)


def validate_circuit_columns(columns):
    normalized_columns = [str(column).replace("﻿", "").strip() for column in columns]
    legacy_columns = ["School_Name", "Circuit"]
    if normalized_columns == EXPECTED_CIRCUIT_COLUMNS:
        return True, "Circuit template columns match the required template."
    if normalized_columns == legacy_columns:
        return False, "School_Type is required for new uploads. Legacy files will be migrated to a visible placeholder, but new uploads should use only Public or Private."

    missing_columns = [column for column in legacy_columns if column not in normalized_columns]
    extra_columns = [column for column in normalized_columns if column not in EXPECTED_CIRCUIT_COLUMNS]
    message_parts = []
    if missing_columns:
        message_parts.append("Missing columns: " + ", ".join(missing_columns))
    if extra_columns:
        message_parts.append("Unexpected columns: " + ", ".join(extra_columns))
    if "School_Type" not in normalized_columns:
        message_parts.append("School_Type is missing. Use only Public or Private.")
    if not message_parts:
        message_parts.append(
            "The header names are correct but the column order was changed. Please use the template without editing the header row."
        )
    return False, "Circuit template mismatch. " + " ".join(message_parts)


def build_excel_template(columns, filename="Template", sheet_name="EduPulse_Data", num_rows=100, school_type_default=False):
    df = pd.DataFrame("", index=range(num_rows), columns=columns)
    if school_type_default and "School_Type" in df.columns:
        df["School_Type"] = "Public"

    # Primary: xlsxwriter (styled headers, locked rows, branding footer)
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            header_format = workbook.add_format({
                "bold": True,
                "text_wrap": True,
                "valign": "vcenter",
                "fg_color": "#1E3A8A",
                "font_color": "#FFFFFF",
                "border": 1,
            })
            unlocked_format = workbook.add_format({"locked": False})
            warning_format = workbook.add_format({"italic": True, "font_color": "#CC0000", "bold": True})
            worksheet.protect()
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, 20, unlocked_format)
            worksheet.freeze_panes(1, 0)
            worksheet.write(num_rows + 1, 0, f"\u00a9 2026 {BLOOMCORE_FOOTER_TEXT}", warning_format)
            worksheet.write(num_rows + 2, 0, "WARNING: DO NOT RENAME OR MOVE COLUMN HEADERS.", warning_format)
        return output.getvalue()
    except Exception:
        pass

    # Fallback 1: openpyxl (unstyled but valid xlsx)
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        return output.getvalue()
    except Exception:
        pass

    # Fallback 2: plain CSV bytes if neither Excel engine is available
    return build_template_csv_bytes(columns)


def build_template_csv_bytes(columns):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["# WARNING: DO NOT EDIT OR DELETE THESE COLUMN HEADERS - ONLY FILL IN DATA ROWS BELOW"])
    writer.writerow(["#"])
    writer.writerow(columns)
    for _ in range(3):
        row = [""] * len(columns)
        if "School_Type" in columns:
            row[columns.index("School_Type")] = "Public"
        writer.writerow(row)
    footer_row = [""] * len(columns)
    footer_row[0] = BLOOMCORE_FOOTER_TEXT
    writer.writerow(footer_row)
    return output.getvalue().encode("utf-8")


def build_school_student_id_prefix(school):
    school_slug = slugify_name(school)
    school_hash = hashlib.sha1(str(school).strip().upper().encode("utf-8")).hexdigest()[:4].upper()
    return f"{school_slug[:8]}-{school_hash}"


def get_next_school_student_id_number(school, prefix):
    student_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
    if student_df.empty:
        return 1

    student_df.columns = [str(column).replace("\ufeff", "").strip() for column in student_df.columns]
    columns_valid, _ = validate_live_dataset_columns(student_df.columns.tolist())
    if not columns_valid or "Student_ID" not in student_df.columns or "School_Name" not in student_df.columns:
        return 1

    student_df = clean_uploaded_dataframe(student_df)
    school_ids = student_df.loc[
        student_df["School_Name"].fillna("").astype(str).str.strip().eq(str(school).strip()),
        "Student_ID",
    ].fillna("").astype(str).str.strip()

    pattern = re.compile(rf"^{re.escape(prefix)}-(\d+)$")
    matching_numbers = []
    for student_id in school_ids:
        match = pattern.match(student_id)
        if match:
            matching_numbers.append(int(match.group(1)))

    return (max(matching_numbers) + 1) if matching_numbers else 1


def resolve_attendance_default_value(uploaded_df=None, existing_school_df=None, existing_all_df=None):
    candidate_series = []
    if uploaded_df is not None and "Attendance_Percent" in uploaded_df.columns:
        candidate_series.append(pd.to_numeric(uploaded_df["Attendance_Percent"], errors="coerce"))
    if existing_school_df is not None and "Attendance_Percent" in existing_school_df.columns:
        candidate_series.append(pd.to_numeric(existing_school_df["Attendance_Percent"], errors="coerce"))
    if existing_all_df is not None and "Attendance_Percent" in existing_all_df.columns:
        candidate_series.append(pd.to_numeric(existing_all_df["Attendance_Percent"], errors="coerce"))

    for series in candidate_series:
        if series.notna().any():
            return round(float(series.dropna().mean()), 1)
    return 75.0


def autofill_missing_attendance(student_df, existing_school_df=None, existing_all_df=None):
    if student_df.empty or "Attendance_Percent" not in student_df.columns:
        return student_df.copy(), 0, None

    filled_df = student_df.copy()
    attendance_values = pd.to_numeric(filled_df["Attendance_Percent"], errors="coerce")
    missing_mask = attendance_values.isna()
    missing_count = int(missing_mask.sum())
    if missing_count == 0:
        return filled_df, 0, None

    default_value = resolve_attendance_default_value(
        uploaded_df=filled_df,
        existing_school_df=existing_school_df,
        existing_all_df=existing_all_df,
    )
    filled_df.loc[missing_mask, "Attendance_Percent"] = default_value
    return filled_df, missing_count, default_value


def assign_missing_school_student_ids(student_df, school, existing_school_df=None):
    if student_df.empty or "Student_ID" not in student_df.columns:
        return student_df.copy(), 0

    assigned_df = student_df.copy()
    prefix = build_school_student_id_prefix(school)
    pattern = re.compile(rf"^{re.escape(prefix)}-(\d+)$")
    used_numbers = set()

    for source_df in [existing_school_df, assigned_df]:
        if source_df is None or source_df.empty or "Student_ID" not in source_df.columns:
            continue
        source_ids = source_df["Student_ID"].fillna("").astype(str).str.strip()
        for student_id in source_ids:
            match = pattern.match(student_id)
            if match:
                used_numbers.add(int(match.group(1)))

    next_number = (max(used_numbers) + 1) if used_numbers else 1
    missing_id_mask = assigned_df["Student_ID"].fillna("").astype(str).str.strip().eq("")
    missing_count = int(missing_id_mask.sum())

    if missing_count == 0:
        return assigned_df, 0

    generated_ids = []
    for _ in range(missing_count):
        while next_number in used_numbers:
            next_number += 1
        generated_ids.append(f"{prefix}-{next_number:04d}")
        used_numbers.add(next_number)
        next_number += 1

    assigned_df.loc[missing_id_mask, "Student_ID"] = generated_ids
    return assigned_df, missing_count


def assign_missing_internal_tracking_ids(student_df):
    if student_df.empty or "Internal_Tracking_ID" not in student_df.columns:
        return student_df.copy(), 0

    assigned_df = student_df.copy()
    missing_mask = assigned_df["Internal_Tracking_ID"].fillna("").astype(str).str.strip().eq("")
    missing_count = int(missing_mask.sum())
    if missing_count == 0:
        return assigned_df, 0

    assigned_df.loc[missing_mask, "Internal_Tracking_ID"] = [generate_internal_tracking_id() for _ in range(missing_count)]
    return assigned_df, missing_count


def get_edupulse_headers(for_template=True):
    """
    Context-aware header generator.
    for_template=True  -> blank download (no Final_BECE or analytics columns)
    for_template=False -> full analysis columns (includes Final_BECE and Action_Zone)
    """
    if for_template:
        return HEADTEACHER_UPLOAD_TEMPLATE_COLUMNS
    return EXPECTED_DATA_COLUMNS


def build_headteacher_student_template_bytes(school, circuit="", school_type="", num_students=None):
    num_rows = int(num_students) if num_students and int(num_students) > 0 else HEADTEACHER_TEMPLATE_ROWS
    columns = get_edupulse_headers(for_template=True)

    # Primary: professional xlsxwriter template with school-name heading
    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df = pd.DataFrame("", index=range(num_rows), columns=columns)
            df.to_excel(writer, index=False, sheet_name="Student_Data", startrow=2)
            workbook = writer.book
            worksheet = writer.sheets["Student_Data"]

            header_fmt = workbook.add_format({
                "bold": True,
                "font_size": 18,
                "align": "center",
                "valign": "vcenter",
                "font_color": "#FFFFFF",
                "bg_color": "#102A43",
            })
            col_header_fmt = workbook.add_format({
                "bold": True,
                "bg_color": "#1E3A8A",
                "font_color": "#FFFFFF",
                "border": 1,
                "text_wrap": True,
                "valign": "vcenter",
            })
            unlocked_fmt = workbook.add_format({"locked": False})
            footer_fmt = workbook.add_format({
                "font_size": 10,
                "font_color": "#9AA5B1",
                "italic": True,
            })

            heading_text = f"{str(school).upper()} - STUDENT ACADEMIC RECORD TEMPLATE"
            worksheet.merge_range(0, 0, 1, len(columns) - 1, heading_text, header_fmt)

            for col_num, col_name in enumerate(columns):
                worksheet.write(2, col_num, col_name, col_header_fmt)
                worksheet.set_column(col_num, col_num, 25, unlocked_fmt)

            # Explicitly unlock every data entry row so headteachers can type in them
            for data_row in range(3, num_rows + 3):
                worksheet.set_row(data_row, None, unlocked_fmt)

            worksheet.freeze_panes(3, 0)
            worksheet.protect()
            worksheet.write(999, 0, f"\u00a9 2026 {BLOOMCORE_FOOTER_TEXT}", footer_fmt)
            worksheet.write(1000, 0, "WARNING: DO NOT RENAME COLUMN HEADERS IN ROW 3.", footer_fmt)

        return output.getvalue()
    except Exception:
        pass

    # Fallback: generic styled template without merged heading
    return build_excel_template(
        columns=columns,
        filename=f"{school}_EduPulse_Template",
        sheet_name="Student_Data",
        num_rows=num_rows,
    )


def clean_uploaded_dataframe(df):
    cleaned_df = df.copy()
    cleaned_df.columns = [str(column).replace("\ufeff", "").strip() for column in cleaned_df.columns]
    cleaned_df = cleaned_df.dropna(how="all")
    if cleaned_df.empty:
        return cleaned_df

    first_column = cleaned_df.columns[0]
    first_col_str = cleaned_df[first_column].fillna("").astype(str).str.strip()
    # Drop exact footer text, copyright lines (©), and WARNING metadata rows
    metadata_mask = (
        first_col_str.eq(BLOOMCORE_FOOTER_TEXT)
        | first_col_str.str.startswith("\u00a9")
        | first_col_str.str.upper().str.startswith("WARNING")
    )
    cleaned_df = cleaned_df.loc[~metadata_mask].copy()
    cleaned_df = cleaned_df.dropna(how="all")
    return cleaned_df


def get_data_file_status():
    data_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
    if data_df.empty:
        return {
            "ready": False,
            "severity": "info",
            "message": "No active student dataset has been synced yet. Headteachers will upload their own school Excel files after the Director loads the circuits dataset.",
        }
    columns_valid, column_message = validate_live_dataset_columns(data_df.columns.tolist())
    if not columns_valid:
        return {
            "ready": False,
            "severity": "error",
            "message": column_message,
        }

    return {
        "ready": True,
        "severity": "success",
        "message": f"Active dataset ready: {len(data_df)} student rows detected.",
        "rows": len(data_df),
    }


def get_school_sync_status(school):
    school = str(school).strip()
    if not school:
        return {
            "ready": False,
            "severity": "warning",
            "rows": 0,
            "message": "No school was attached to this Headteacher account.",
        }

    student_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
    if student_df.empty:
        return {
            "ready": False,
            "severity": "info",
            "rows": 0,
            "message": f"No student dataset has been synced yet for {school}. Upload the school dataset first.",
        }

    student_df.columns = [str(column).replace("\ufeff", "").strip() for column in student_df.columns]
    columns_valid, column_message = validate_live_dataset_columns(student_df.columns.tolist())
    if not columns_valid:
        return {
            "ready": False,
            "severity": "error",
            "rows": 0,
            "message": column_message,
        }

    student_df = clean_uploaded_dataframe(student_df)
    if "School_Name" not in student_df.columns:
        return {
            "ready": False,
            "severity": "error",
            "rows": 0,
            "message": "The active student dataset does not contain a School_Name column.",
        }

    school_mask = student_df["School_Name"].fillna("").astype(str).str.strip().eq(school)
    school_rows = int(school_mask.sum())
    if school_rows == 0:
        return {
            "ready": False,
            "severity": "info",
            "rows": 0,
            "message": f"{school} has not synced any student rows yet. Upload the school dataset before first login.",
        }

    return {
        "ready": True,
        "severity": "success",
        "rows": school_rows,
        "message": f"{school} is synced with {school_rows} student rows.",
    }


def render_status_message(status):
    severity = status.get("severity", "info")
    message = status.get("message", "")
    if severity == "success":
        st.success(message)
    elif severity == "warning":
        st.warning(message)
    elif severity == "error":
        st.error(message)
    else:
        st.info(message)


def show_auth_flash():
    message = st.session_state.get("auth_flash_message", "")
    if not message:
        return

    severity = st.session_state.get("auth_flash_severity", "success")
    if severity == "error":
        st.error(message)
    elif severity == "warning":
        st.warning(message)
    else:
        st.success(message)

    st.session_state["auth_flash_message"] = ""
    st.session_state["auth_flash_severity"] = "success"


def show_portal_flash():
    message = st.session_state.get("portal_flash_message", "")
    if not message:
        return

    severity = st.session_state.get("portal_flash_severity", "success")
    if severity == "error":
        st.error(message)
    elif severity == "warning":
        st.warning(message)
    else:
        st.success(message)

    st.session_state["portal_flash_message"] = ""
    st.session_state["portal_flash_severity"] = "success"


# ============================================================
# 6. DATA LOADING, SCORING RULES, AND GENERAL CALCULATIONS
# ============================================================
@st.cache_data
def load_data(show_errors=True):
    status = get_data_file_status()
    if not status["ready"]:
        if show_errors:
            render_status_message(status)
        return pd.DataFrame(), []

    try:
        df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
    except Exception as exc:
        st.error(f"Critical data error: {exc}")
        return pd.DataFrame(), []

    school_profile_lookup = load_school_profile_lookup()
    df, _ = migrate_legacy_student_df(clean_uploaded_dataframe(df), school_profile_lookup=school_profile_lookup)

    for column in ["Internal_Tracking_ID", "Student_ID", "Official_Index_Number", "Student_Name", "Gender", "Date_of_Birth", "School_Name", "Circuit", "School_Type", "Action_Zone"]:
        if column in df.columns:
            df[column] = df[column].fillna("").astype(str).str.strip()
            df.loc[df[column].isin(["nan", "None"]), column] = ""

    if "School_Name" in df.columns:
        mapped_circuits = df["School_Name"].map(lambda school: school_profile_lookup.get(school, {}).get("Circuit", ""))
        mapped_school_types = df["School_Name"].map(lambda school: school_profile_lookup.get(school, {}).get("School_Type", ""))
        df["Circuit"] = df["Circuit"].mask(df["Circuit"].eq(""), mapped_circuits).fillna(df["School_Name"])
        df["School_Type"] = df["School_Type"].mask(df["School_Type"].eq(""), mapped_school_types).fillna(MIGRATION_SCHOOL_TYPE_PLACEHOLDER)
        df["Circuit"] = df["Circuit"].fillna("").astype(str).str.strip()
        df["School_Type"] = normalize_school_type_series(df["School_Type"])
        df.loc[df["School_Type"] == "", "School_Type"] = MIGRATION_SCHOOL_TYPE_PLACEHOLDER

    if "Action_Zone" in df.columns:
        df["Action_Zone"] = df["Action_Zone"].apply(normalize_action_zone)

    if "Date_of_Birth" in df.columns:
        df["Date_of_Birth"] = normalize_date_of_birth_series(df["Date_of_Birth"])

    subject_cols = [column for column in FINAL_SUBJECT_COLUMNS if column in df.columns]

    if "Student_Name" in df.columns and "Student_ID" in df.columns:
        df["Search_Label"] = (
            df["Student_Name"].fillna("").astype(str).str.strip()
            + " ("
            + df["Student_ID"].fillna("").astype(str).str.strip()
            + ")"
        )

    return df, subject_cols


def get_bece_grade(score):
    if pd.isna(score):
        return 9

    score = float(score)
    if score >= 80:
        return 1
    if score >= 70:
        return 2
    if score >= 60:
        return 3
    if score >= 55:
        return 4
    if score >= 50:
        return 5
    if score >= 45:
        return 6
    if score >= 40:
        return 7
    if score >= 35:
        return 8
    return 9


def grade_to_score(grade):
    grade = int(grade)
    return max(0, min(100, 100 - (grade * 10) + 5))


def classify_cssps_tie_break(best_six_raw_total):
    best_six_raw_total = safe_float(best_six_raw_total, 0.0)
    if best_six_raw_total >= 480:
        return "Very Strong Tie-Break"
    if best_six_raw_total >= 450:
        return "Strong Tie-Break"
    if best_six_raw_total >= 420:
        return "Competitive Tie-Break"
    return "Tie-Break Risk"


def predict_placement(aggregate, best_six_raw_total=None):
    """
    Enhanced CSSPS Prediction Logic.
    Based on GES placement standards for Akatsi South Municipal schools.
    Includes Best Six Raw Score tie-breaking for Director-Grade precision.
    """
    raw = safe_float(best_six_raw_total, 0.0)
    
    if aggregate <= 10:
        # High-performance Category A threshold
        if raw >= 480:
            return "Category A (Priority)"
        return "Category A"
    elif aggregate <= 18:
        return "Category B"
    elif aggregate <= 30:
        return "Category C"
    elif aggregate <= 36:
        return "Category D/Catchment"
    else:
        return "Self-Placement (SP)"


def normalize_gender(value):
    text = str(value).strip().lower()
    if text in {"f", "female", "girl", "girls"}:
        return "Girls"
    if text in {"m", "male", "boy", "boys"}:
        return "Boys"
    if text in {"", "nan", "none"}:
        return "Unspecified"
    return str(value).strip().title()


def format_subject_name(subject_col):
    prefix = get_subject_prefix(subject_col)
    return SUBJECT_DISPLAY_NAMES.get(prefix, prefix.replace("_", " "))


def is_core_subject(subject_col):
    return get_subject_prefix(subject_col) in {get_subject_prefix(column) for column in CORE_FINAL_SUBJECTS}


def safe_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def average_row_values(row, columns, default):
    if not columns:
        return default

    values = pd.to_numeric(pd.Series([row.get(column) for column in columns]), errors="coerce")
    if values.notna().any():
        return float(values.mean())
    return default


def normalize_action_zone(value):
    text = str(value).strip().upper()
    if "FLYER" in text:
        return "FLYER"
    if "DIAMOND" in text:
        return "DIAMOND"
    if "STEADY" in text:
        return "STEADY"
    if "CRITICAL" in text:
        return "CRITICAL"
    return str(value).strip()


def action_zone_from_average(score):
    if score >= 70:
        return "FLYER"
    if score >= 60:
        return "DIAMOND"
    if score >= 50:
        return "STEADY"
    return "CRITICAL"


def assign_student_action_zone(row):
    """
    Categorizes students based on the 'First Thinking' principle for intervention.
    Automated Action Zone identification for PLC meetings and interventions.
    """
    # Calculate Math & Science average for technical focus
    math_mock2 = safe_float(row.get('Mathematics_Mock2'), 0)
    science_mock2 = safe_float(row.get('Integrated_Science_Mock2'), 0)
    stem_avg = (math_mock2 + science_mock2) / 2
    
    # 1. Critical Support (Below 40% in core subjects)
    if stem_avg < 40:
        return "CRITICAL: Intensive Support"
    
    # 2. Performance Consistency Check (The 'Flyer' Logic)
    improvement = safe_float(row.get('Math_Improvement', 0), 0)
    if improvement > 10:
        return "RISING STAR: Maintain Momentum"
    
    # 3. High Flyers (Above 80% and consistent)
    consistency = safe_float(row.get('Math_Consistency', 0), 0)
    if stem_avg >= 80 and consistency > 70:
        return "ELITE: Category A Track"
        
    return "STABLE: Standard Monitoring"


# ============================================================
# 7. ML FORECASTING ENGINE AND BECE OUTCOME COMPUTATION
# ============================================================
def get_file_signature(path):
    if not os.path.isfile(path):
        return ""
    stat = os.stat(path)
    return f"{int(stat.st_mtime_ns)}-{stat.st_size}"


def normalize_header_text(value):
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def get_subject_prefix(subject_col):
    return str(subject_col).replace(FINAL_SUFFIX, "").replace(PREDICTED_SUFFIX, "")


def get_subject_model_key(subject_col):
    prefix = get_subject_prefix(subject_col)
    return MODEL_KEY_OVERRIDES.get(prefix, prefix)


def get_subject_source_columns(subject_col):
    prefix = get_subject_prefix(subject_col)
    return {
        "assignments": f"{prefix}_Assignments",
        "term1": f"{prefix}_Term1_Exam",
        "term2": f"{prefix}_Term2_Exam",
        "mock1": f"{prefix}_Mock1",
        "mock2": f"{prefix}_Mock2",
        "predicted": get_predicted_subject_column(prefix),
        "final": get_final_subject_column(prefix),
    }


def safe_mean(values, default=0.0):
    numeric = pd.to_numeric(pd.Series(list(values)), errors="coerce").dropna()
    if numeric.empty:
        return default
    return float(numeric.mean())


def compute_continuous_assessment_score(row, subject_col):
    columns = get_subject_source_columns(subject_col)
    return safe_mean(
        [
            row.get(columns["assignments"]),
            row.get(columns["term1"]),
            row.get(columns["term2"]),
        ],
        default=safe_float(row.get(columns["mock2"]), safe_float(row.get(subject_col), 50.0)),
    )


def build_subject_feature_snapshot(row, subject_col, attendance_value=None, assignment_value=None, mock_value=None):
    columns = get_subject_source_columns(subject_col)
    current_final = safe_float(
        row.get(columns["predicted"]),
        safe_float(row.get(columns["final"]), 50.0),
    )
    attendance = safe_float(row.get("Attendance_Percent"), 75.0)
    assignments = safe_float(row.get(columns["assignments"]), current_final)
    term1 = safe_float(row.get(columns["term1"]), assignments)
    term2 = safe_float(row.get(columns["term2"]), assignments)
    mock1 = safe_float(row.get(columns["mock1"]), term2)
    mock2 = safe_float(row.get(columns["mock2"]), current_final)

    if attendance_value is not None:
        attendance = float(attendance_value)
    if assignment_value is not None:
        assignments = float(assignment_value)
    if mock_value is not None:
        mock2 = float(mock_value)

    original_improvement = mock2 - mock1
    if mock_value is not None:
        mock1 = max(0.0, min(100.0, float(mock_value) - original_improvement))
        term2 = safe_float(row.get(columns["term2"]), float(mock_value))

    continuous_assessment = safe_mean([assignments, term1, term2], default=assignments)
    improvement = mock2 - mock1
    consistency = assignments - mock2
    weighted_proxy = (continuous_assessment * 0.30) + (mock2 * 0.70)

    feature_snapshot = {
        "Attendance_Percent": max(0.0, min(100.0, attendance)),
        columns["assignments"]: max(0.0, min(100.0, assignments)),
        columns["term1"]: max(0.0, min(100.0, term1)),
        columns["term2"]: max(0.0, min(100.0, term2)),
        columns["mock1"]: max(0.0, min(100.0, mock1)),
        columns["mock2"]: max(0.0, min(100.0, mock2)),
        f"{get_subject_prefix(subject_col)}_Continuous_Assessment": max(0.0, min(100.0, continuous_assessment)),
        f"{get_subject_prefix(subject_col)}_CA_30_Component": max(0.0, min(100.0, continuous_assessment * 0.30)),
        f"{get_subject_prefix(subject_col)}_Exam_70_Proxy": max(0.0, min(100.0, mock2 * 0.70)),
        f"{get_subject_prefix(subject_col)}_Weighted_30_70_Proxy": max(0.0, min(100.0, weighted_proxy)),
        f"{get_subject_prefix(subject_col)}_Improvement": improvement,
        f"{get_subject_prefix(subject_col)}_Consistency": consistency,
    }
    return feature_snapshot


@st.cache_resource(show_spinner=False)
def load_pretrained_ml_models(model_signature):
    # The predictor now loads externally trained subject models from
    # bece_models.joblib instead of training new models inside the app.
    if not model_signature:
        return {}

    try:
        raw_bundle = joblib.load(MODEL_FILE)
    except Exception:
        return {}

    if not isinstance(raw_bundle, dict):
        return {}

    bundle = {}
    for subject_key, estimator in raw_bundle.items():
        feature_columns = list(getattr(estimator, "feature_names_in_", []))
        if not feature_columns:
            continue
        bundle[str(subject_key).strip()] = {
            "model": estimator,
            "feature_columns": feature_columns,
            "train_rows": np.nan,
            "cv_r2": np.nan,
            "cv_mae": np.nan,
            "source": MODEL_FILE,
        }
    return bundle


@st.cache_resource(show_spinner=False)
def load_calibration(calibration_signature):
    if not calibration_signature:
        return {}

    try:
        with open(CALIBRATION_FILE, "r", encoding="utf-8") as handle:
            calibration_map = json.load(handle)
    except Exception:
        return {}

    return calibration_map if isinstance(calibration_map, dict) else {}


def get_live_ml_bundle():
    return load_pretrained_ml_models(get_file_signature(MODEL_FILE))


def get_live_calibration_map():
    return load_calibration(get_file_signature(CALIBRATION_FILE))


def get_school_subject_bias(calibration_map, school_name, subject_col):
    school_key = str(school_name).strip()
    subject_key = get_subject_model_key(subject_col)
    school_map = calibration_map.get(school_key, {})
    if not isinstance(school_map, dict):
        return 0.0
    return safe_float(school_map.get(subject_key, 0.0), 0.0)


def predict_subject_score_ml(row, subject_col, model_bundle, attendance_value=None, assignment_value=None, mock_value=None):
    # Build the exact feature vector expected by the pretrained model and
    # fall back to the weighted 30/70 proxy only if a subject model is missing.
    feature_snapshot = build_subject_feature_snapshot(
        row,
        subject_col,
        attendance_value=attendance_value,
        assignment_value=assignment_value,
        mock_value=mock_value,
    )
    calibration_map = get_live_calibration_map()
    school_bias = get_school_subject_bias(calibration_map, row.get("School_Name", ""), subject_col)
    model_details = model_bundle.get(get_subject_model_key(subject_col))
    baseline_score = safe_float(
        row.get(columns["predicted"]) if (columns := get_subject_source_columns(subject_col)) else None,
        safe_float(row.get(columns["final"]), feature_snapshot[f"{get_subject_prefix(subject_col)}_Weighted_30_70_Proxy"]),
    )

    if not model_details:
        predicted_score = feature_snapshot[f"{get_subject_prefix(subject_col)}_Weighted_30_70_Proxy"]
        model_status = "Weighted 30/70 fallback"
    else:
        feature_frame = pd.DataFrame([feature_snapshot])[model_details["feature_columns"]]
        predicted_score = float(model_details["model"].predict(feature_frame)[0])
        model_status = "Calibrated ML model"

    predicted_score = max(0.0, min(100.0, predicted_score + school_bias))
    return {
        "subject_col": subject_col,
        "subject_name": format_subject_name(subject_col),
        "current_score": round(baseline_score, 1),
        "predicted_score": round(predicted_score, 1),
        "predicted_grade": get_bece_grade(predicted_score),
        "continuous_assessment": round(feature_snapshot[f"{get_subject_prefix(subject_col)}_Continuous_Assessment"], 1),
        "ca_30_component": round(feature_snapshot[f"{get_subject_prefix(subject_col)}_CA_30_Component"], 1),
        "exam_70_proxy": round(feature_snapshot[f"{get_subject_prefix(subject_col)}_Exam_70_Proxy"], 1),
        "weighted_30_70_proxy": round(feature_snapshot[f"{get_subject_prefix(subject_col)}_Weighted_30_70_Proxy"], 1),
        "model_status": model_status,
        "school_bias": round(school_bias, 2),
    }


def resolve_subject_score(row, subject_col, data_mode="best_available"):
    subject_sources = get_subject_source_columns(subject_col)
    predicted_score = pd.to_numeric(pd.Series([row.get(subject_sources["predicted"])]), errors="coerce").iloc[0]
    official_score = pd.to_numeric(pd.Series([row.get(subject_sources["final"])]), errors="coerce").iloc[0]

    if data_mode == "official":
        return official_score
    if data_mode == "forecast":
        return predicted_score
    return official_score if pd.notna(official_score) else predicted_score


def compute_student_outcome_details(row, subject_cols, score_override=None, data_mode="best_available"):
    score_override = score_override or {}
    subject_details = []
    core_details = []
    elective_details = []

    for subject in subject_cols:
        subject_sources = get_subject_source_columns(subject)
        override_value = score_override.get(subject, score_override.get(subject_sources["predicted"]))
        raw_score = override_value if override_value is not None else resolve_subject_score(row, subject, data_mode=data_mode)
        numeric_score = pd.to_numeric(pd.Series([raw_score]), errors="coerce").iloc[0]
        if pd.isna(numeric_score):
            continue

        grade = get_bece_grade(numeric_score)
        detail = {
            "subject_col": subject,
            "subject_name": format_subject_name(subject),
            "score": float(numeric_score),
            "grade": grade,
        }
        subject_details.append(detail)
        if is_core_subject(subject):
            core_details.append(detail)
        else:
            elective_details.append(detail)

    best_two_electives = sorted(
        elective_details,
        key=lambda item: (item["grade"], -item["score"], item["subject_name"]),
    )[:2]
    aggregate = sum(item["grade"] for item in core_details) + sum(item["grade"] for item in best_two_electives)
    best_six_raw_total = sum(item["score"] for item in core_details) + sum(item["score"] for item in best_two_electives)
    best_six_subjects = core_details + best_two_electives
    raw_average = (best_six_raw_total / len(best_six_subjects)) if best_six_subjects else 0.0

    return {
        "aggregate": float(aggregate),
        "core_details": core_details,
        "elective_details": elective_details,
        "best_two_electives": best_two_electives,
        "best_two_subject_names": ", ".join(item["subject_name"] for item in best_two_electives) or "None selected",
        "best_six_raw_total": round(float(best_six_raw_total), 1),
        "raw_average": round(float(raw_average), 1),
        "subject_details": subject_details,
        "placement": predict_placement(aggregate),
        "placement_outlook": predict_placement(aggregate, best_six_raw_total=best_six_raw_total),
        "tie_break_band": classify_cssps_tie_break(best_six_raw_total) if aggregate <= 10 else "",
    }


def compute_student_aggregate(row, subject_cols, data_mode="best_available"):
    outcome_details = compute_student_outcome_details(row, subject_cols, data_mode=data_mode)
    core_grades = [item["grade"] for item in outcome_details["core_details"]]
    elective_grades = [item["grade"] for item in outcome_details["elective_details"]]
    return outcome_details["aggregate"], core_grades, elective_grades


def build_aggregate_dataframe(df, subject_cols, data_mode="best_available"):
    if df.empty or not subject_cols:
        return pd.DataFrame()

    rows = []
    for _, student_row in df.iterrows():
        outcome_details = compute_student_outcome_details(student_row, subject_cols, data_mode=data_mode)
        row = {
            "Internal_Tracking_ID": student_row.get("Internal_Tracking_ID", ""),
            "Student_ID": student_row.get("Student_ID", ""),
            "Official_Index_Number": student_row.get("Official_Index_Number", ""),
            "Student_Name": student_row.get("Student_Name", ""),
            "School_Name": student_row.get("School_Name", ""),
            "Circuit": student_row.get("Circuit", student_row.get("School_Name", "")),
            "Aggregate": outcome_details["aggregate"],
            "Passed": 1 if outcome_details["aggregate"] <= 30 else 0,
            "Placement_Category": outcome_details["placement"],
            "Best_Two_Electives": outcome_details["best_two_subject_names"],
            "Best_Six_Raw_Total": outcome_details["best_six_raw_total"],
            "Best_Six_Raw_Average": outcome_details["raw_average"],
            "Data_Mode": data_mode,
        }
        if data_mode == "best_available":
            row["Data_Mode"] = "Official" if any(
                pd.notna(pd.to_numeric(pd.Series([student_row.get(get_final_subject_column(subject))]), errors="coerce").iloc[0])
                for subject in subject_cols
            ) else "Forecast"
        if "Gender" in df.columns:
            row["Gender"] = normalize_gender(student_row.get("Gender", ""))
        if "School_Type" in df.columns:
            row["School_Type"] = str(student_row.get("School_Type", "Not Specified")).strip() or "Not Specified"
        rows.append(row)

    return pd.DataFrame(rows)


def render_scroll_to_top():
    st.markdown(
        '<div class="scroll-top-link"><a href="#page-top">↑ Scroll to top</a></div>',
        unsafe_allow_html=True,
    )


# ============================================================
# 8. SIDEBAR, AUTHENTICATION, AND LOGIN/REGISTRATION FLOWS
# ============================================================
def render_sidebar(df, subject_cols, role, school):
    scope_label = get_scope_label()
    if role == "Director":
        scope_df = df.copy()
    elif "School_Name" in df.columns:
        scope_df = df[df["School_Name"] == school].copy()
    else:
        scope_df = pd.DataFrame()
    scope_agg_df = build_aggregate_dataframe(scope_df, subject_cols)
    scope_student_count = len(scope_df)
    scope_subject_count = len(subject_cols)
    scope_avg_agg = f"{scope_agg_df['Aggregate'].mean():.1f}" if not scope_agg_df.empty else "--"
    scope_pass_rate = f"{scope_agg_df['Passed'].mean() * 100:.1f}%" if not scope_agg_df.empty else "--"
    scope_school_count = scope_df["School_Name"].nunique() if "School_Name" in scope_df.columns else 0
    scope_circuit_count = scope_df["Circuit"].nunique() if "Circuit" in scope_df.columns else 0
    scope_circuits = (
        sorted(scope_df["Circuit"].dropna().astype(str).str.strip().unique().tolist())
        if "Circuit" in scope_df.columns
        else []
    )

    with st.sidebar:
        if os.path.isfile(BRAND_IMAGE):
            st.image(BRAND_IMAGE, use_container_width=True)
        st.markdown(
            f"""
            <div class="sidebar-brand-card">
                <div class="sidebar-kicker">The Basic Education Transition Analyzer</div>
                <div class="sidebar-title">EduPulse</div>
                <div class="sidebar-subtitle">{scope_label} insights and forecasting</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.write(f"**Logged in as:** {st.session_state['current_user']}")
        st.caption(f"{role} access for {scope_label}" if role == "Director" else f"{role} access for {school}")

        st.markdown("#### Quick Snapshot")
        top_metrics = st.columns(2)
        with top_metrics[0]:
            st.metric("Students", scope_student_count)
        with top_metrics[1]:
            st.metric("Subjects", scope_subject_count)

        lower_metrics = st.columns(2)
        with lower_metrics[0]:
            st.metric("Avg Agg", scope_avg_agg)
        with lower_metrics[1]:
            st.metric("Pass Rate", scope_pass_rate)

        with st.expander("📌 Coverage Summary"):
            if role == "Director":
                st.write(f"Schools loaded: {scope_school_count}")
                st.write(f"Active circuits in data: {scope_circuit_count}")
                st.write(f"Mapped circuits total: {len(load_all_circuits())}")
            else:
                st.write(f"School: {school}")
                st.write(f"Circuit: {', '.join(scope_circuits) if scope_circuits else 'Not mapped'}")
                st.write(f"Students loaded: {scope_student_count}")
            st.write(f"BECE subjects tracked: {scope_subject_count}")

        with st.expander("🎯 Placement Guide"):
            st.write("Category A: Aggregate 6-10")
            st.write("Category B: Aggregate 11-20")
            st.write("Category C: Aggregate 21-35")
            st.write("Category D/SP: Aggregate 36+")

        if role == "Director":
            unread_notifications = get_notifications("Director", district=scope_label, unread_only=True)
            all_notifications = get_notifications("Director", district=scope_label, unread_only=False)
            with st.expander(f"🔔 Director Alerts ({len(unread_notifications)})", expanded=len(unread_notifications) > 0):
                if all_notifications.empty:
                    st.caption("No director alerts yet.")
                else:
                    for _, notification in all_notifications.head(8).iterrows():
                        st.write(notification["message"])
                        st.caption(notification["created_at"])
                    if len(all_notifications) > 8:
                        st.caption(f"Showing latest 8 of {len(all_notifications)} alerts.")
                    if not unread_notifications.empty and st.button(
                        "Mark Alerts as Read",
                        key="mark_director_alerts_read",
                        use_container_width=True,
                    ):
                        mark_notifications_as_read("Director", district=scope_label)
                        st.rerun()

        st.markdown("---")
        with st.expander("📖 Action Zone Dictionary"):
            st.write("**🚀 FLYER**: Excellent! Scoring 70%+ across subjects.")
            st.write("**💎 DIAMOND**: Great potential (60-69%). Needs a small push.")
            st.write("**📈 STEADY**: Average (50-59%). Needs to avoid falling.")
            st.write("**⚠️ CRITICAL**: Below 50%. Needs urgent intervention.")

        if st.button("Refresh Dashboard", key="refresh_dashboard", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        footer = st.container()
        with footer:
            st.markdown("---")
            if st.button("Logout", key="sidebar_logout", use_container_width=True):
                for key in ["logged_in", "current_user", "user_role", "user_school", "user_district"]:
                    st.session_state[key] = False if key == "logged_in" else ""
                st.session_state["auth_nav"] = "Login"
                st.session_state["pending_setup_role"] = ""
                st.rerun()
            st.markdown(
                '<div class="sidebar-footer-text">Powered by BloomCore Technologies</div>',
                unsafe_allow_html=True,
            )


def login_ui():
    active_config = load_app_config()
    active_scope_label = active_config["district_name"] or "District/Municipal"
    if not st.session_state.logged_in:
        st.markdown("""
        <div class="hero-container">
            <h1 style="color: white !important; margin-top: 0;">🎓 Welcome to EduPulse</h1>
            <p style="color: rgba(255,255,255,0.9) !important; font-size: 1.1rem;">
                Follow your path below to unlock the full analytics system.
            </p>
        </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    # 🏛️ DIRECTOR CARD
    with col1:
        st.markdown("""
        <div class="info-card">
            <h3>🏛️ Director Setup</h3>
            <ol>
                <li>Register as Director</li>
                <li>Receive Security Code</li>
                <li>Download Circuit Template</li>
                <li>Upload Municipality/District Circuit Data</li>
            </ol>
            <p style="font-size:13px; color:#475569 !important;">
                Enables district-wide analytics & control
            </p>
        </div>
        """, unsafe_allow_html=True)

    # 🏫 HEADTEACHER CARD
    with col2:
        st.markdown("""
        <div class="info-card" style="border-top-color: #059669;">
            <h3>🏫 Headteacher Setup</h3>
            <ol>
                <li>Register with Director Code</li>
                <li>Download School Template</li>
                <li>Upload Student Data</li>
                <li>Access Dashboard</li>
            </ol>
            <p style="font-size:13px; color:#475569 !important;">
                Unlock school-level insights
            </p>
        </div>
        """, unsafe_allow_html=True)
        
    st.markdown("<br>", unsafe_allow_html=True)

    st.warning("⚠️ You must upload data before accessing dashboards")

    st.write("---")
    st.title(f"🔐 {APP_TITLE}")
    menu = ["Login", "Register Director", "Register Headteacher", "Forgot Password"]
    if st.session_state.get("auth_nav") not in menu:
        st.session_state["auth_nav"] = "Login"
    choice = st.sidebar.selectbox("Navigation", menu, index=menu.index(st.session_state["auth_nav"]))
    st.session_state["auth_nav"] = choice

    if choice == "Login":
        show_auth_flash()
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login", type="primary"):
            all_users = {**MASTER_CREDENTIALS, **load_users()}
            user_record = all_users.get(username)

            if not (user_record and verify_password(password, user_record["password"])):
                st.error("Invalid credentials.")
            elif user_record["role"] == "Headteacher":
                school_sync_status = get_school_sync_status(user_record["school"])
                if not school_sync_status["ready"]:
                    st.session_state["pending_setup_role"] = "Headteacher"
                    st.session_state["latest_registered_school"] = user_record["school"]
                    st.session_state["latest_registered_district"] = user_record.get("district", "") or active_scope_label
                    render_status_message(school_sync_status)
                else:
                    st.session_state.update(
                        {
                            "logged_in": True,
                            "current_user": username,
                            "user_role": user_record["role"],
                            "user_school": user_record["school"],
                            "user_district": user_record.get("district", ""),
                        }
                    )
                    st.session_state["pending_setup_role"] = ""
                    st.rerun()
            else:
                st.session_state.update(
                    {
                        "logged_in": True,
                        "current_user": username,
                        "user_role": user_record["role"],
                        "user_school": user_record["school"],
                        "user_district": user_record.get("district", ""),
                    }
                )
                st.session_state["pending_setup_role"] = ""
                if user_record["role"] == "Director":
                    activate_director_context(
                        username,
                        user_record.get("district", "") or active_scope_label,
                        user_record.get("security_key", "") or active_config.get("headteacher_security_key", ""),
                    )
                st.rerun()

        if st.session_state.get("pending_setup_role") == "Director":
            st.markdown("---")
            render_circuit_setup(
                title="🗺️ Pre-Login Circuits Setup",
                description=(
                    f"Before logging in to {active_scope_label}, you can download the official circuits template, fill it, and upload it here."
                ),
                key_prefix="prelogin_director_circuit_setup",
                redirect_to_login=True,
            )
        elif st.session_state.get("pending_setup_role") == "Headteacher" and st.session_state.get("latest_registered_school"):
            st.markdown("---")
            render_headteacher_bulk_upload(
                st.session_state["latest_registered_school"],
                key_prefix="prelogin_headteacher_student_upload",
                redirect_to_login=True,
            )

    elif choice == "Forgot Password":
        st.title("🔑 Password Recovery")
        st.write("Reset your password using one of the following methods:")

        recovery_method = st.radio(
            "Select Recovery Method",
            ["Security Question", "Email Reset Code", "Owner Override (Admin)"],
            help="Choose how you want to verify your identity and reset your password"
        )

        if recovery_method == "Security Question":
            # Step 1: Username lookup form
            if not st.session_state.get("reset_username_verified"):
                with st.form("security_question_lookup"):
                    username = st.text_input("Username")
                    if st.form_submit_button("Lookup Account"):
                        user = get_user_by_username(username) if username else None
                        if not user:
                            st.error("Username not found.")
                        elif not user.get("security_question"):
                            st.warning("This account does not have a security question set up. Please use Email Reset or contact your Director.")
                        else:
                            st.session_state["reset_username"] = username
                            st.session_state["reset_security_question"] = user["security_question"]
                            st.session_state["reset_security_answer_hash"] = user.get("security_answer", "")
                            st.session_state["reset_username_verified"] = True
                            st.rerun()
            else:
                # Step 2: Password reset form with security question
                st.info(f"Security Question: {st.session_state.get('reset_security_question', '')}")
                with st.form("security_question_reset"):
                    security_answer = st.text_input("Your Answer", type="password")
                    new_password = st.text_input("New Password", type="password", key="sq_new_password")
                    confirm_password = st.text_input("Confirm New Password", type="password", key="sq_confirm_password")

                    if new_password:
                        render_password_strength_indicator(new_password)
                    if confirm_password:
                        render_password_match_indicator(new_password, confirm_password)

                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.form_submit_button("Reset Password"):
                            username = st.session_state.get("reset_username", "")
                            stored_answer_hash = st.session_state.get("reset_security_answer_hash", "")

                            if not verify_security_answer(stored_answer_hash, security_answer):
                                st.error("Incorrect security answer.")
                            else:
                                is_strong, _ = validate_password_strength(new_password)
                                if not is_strong:
                                    st.error("New password does not meet security requirements.")
                                elif new_password != confirm_password:
                                    st.error("Passwords do not match.")
                                elif update_user_password(username, new_password):
                                    st.success("Password reset successfully! Please log in with your new password.")
                                    # Clear session state
                                    st.session_state["reset_username"] = None
                                    st.session_state["reset_security_question"] = None
                                    st.session_state["reset_security_answer_hash"] = None
                                    st.session_state["reset_username_verified"] = False
                                    st.session_state["auth_nav"] = "Login"
                                    st.rerun()
                                else:
                                    st.error("Failed to update password. Please try again.")
                    with col2:
                        if st.form_submit_button("Cancel"):
                            st.session_state["reset_username"] = None
                            st.session_state["reset_security_question"] = None
                            st.session_state["reset_security_answer_hash"] = None
                            st.session_state["reset_username_verified"] = False
                            st.rerun()

        elif recovery_method == "Email Reset Code":
            with st.form("email_reset"):
                username = st.text_input("Username")
                email = st.text_input("Registered Email Address")

                if st.form_submit_button("Send Reset Code"):
                    user = get_user_by_username(username) if username else None
                    if not user:
                        st.error("Username not found.")
                    elif user.get("email", "").lower() != email.lower().strip():
                        st.error("Email does not match the registered email for this account.")
                    else:
                        reset_token = generate_reset_token()
                        success, message = send_password_reset_email(user["email"], reset_token, username)
                        if success:
                            st.session_state["password_reset_token"] = reset_token
                            st.session_state["password_reset_user"] = username
                            st.success(f"Reset code sent to {user['email']}. Check your inbox and enter the code below.")
                        else:
                            st.error(message)

            # Token verification section
            if st.session_state.get("password_reset_token"):
                st.markdown("---")
                with st.form("verify_reset_token"):
                    entered_token = st.text_input("Enter Reset Code from Email")
                    new_password = st.text_input("New Password", type="password", key="email_new_password")
                    confirm_password = st.text_input("Confirm New Password", type="password", key="email_confirm_password")

                    if new_password:
                        render_password_strength_indicator(new_password)
                    if confirm_password:
                        render_password_match_indicator(new_password, confirm_password)

                    if st.form_submit_button("Reset Password"):
                        if entered_token != st.session_state.get("password_reset_token"):
                            st.error("Invalid reset code.")
                        else:
                            is_strong, _ = validate_password_strength(new_password)
                            if not is_strong:
                                st.error("New password does not meet security requirements.")
                            elif new_password != confirm_password:
                                st.error("Passwords do not match.")
                            else:
                                username = st.session_state.get("password_reset_user")
                                if update_user_password(username, new_password):
                                    st.success("Password reset successfully! Please log in with your new password.")
                                    st.session_state["password_reset_token"] = None
                                    st.session_state["password_reset_user"] = None
                                    st.session_state["auth_nav"] = "Login"
                                    st.rerun()
                                else:
                                    st.error("Failed to update password.")

        elif recovery_method == "Owner Override (Admin)":
            with st.form("owner_override_reset"):
                owner_secret = st.text_input("Owner Secret Key", type="password")
                username = st.text_input("Username to Reset")
                new_password = st.text_input("New Password", type="password", key="owner_new_password")
                confirm_password = st.text_input("Confirm New Password", type="password", key="owner_confirm_password")

                if new_password:
                    render_password_strength_indicator(new_password)
                if confirm_password:
                    render_password_match_indicator(new_password, confirm_password)

                if st.form_submit_button("Reset Password"):
                    if not verify_owner_secret(owner_secret):
                        st.error("Invalid owner secret key.")
                    else:
                        user = get_user_by_username(username)
                        if not user:
                            st.error("Username not found.")
                        else:
                            is_strong, _ = validate_password_strength(new_password)
                            if not is_strong:
                                st.error("New password does not meet security requirements.")
                            elif new_password != confirm_password:
                                st.error("Passwords do not match.")
                            elif update_user_password(username, new_password):
                                st.success(f"Password for {username} has been reset successfully!")
                                st.session_state["auth_nav"] = "Login"
                                st.rerun()
                            else:
                                st.error("Failed to update password.")

    elif choice == "Register Director":
        st.title("📋 Director Registration")
        st.write(
            "Create the Director account for a new district or municipality. Each Director registration requires a one-time BloomCore access code from the platform owner. That code is burned immediately after verification — it cannot be reused."
        )

        if active_config["district_name"]:
            st.info(
                f"Current active deployment: {active_config['district_name']} | Headteacher key: {active_config['headteacher_security_key']}"
            )

        # ── PHASE 1: Key Verification Gate ────────────────────────────────────
        if not st.session_state.get("dir_reg_key_verified", False):
            st.info("Enter your unique one-time Authorization Code to unlock the registration form.")
            auth_code = st.text_input(
                "Authorization Code",
                type="password",
                help="This code is for one-time use only. It will be burned immediately after verification.",
                key="dir_reg_auth_code_input",
            )
            if st.button("🔓 Verify & Unlock", key="dir_reg_verify_btn"):
                entered = str(auth_code).strip()
                # Accept both the JSON-vault keys and the legacy app_config key
                if validate_and_burn_director_key(entered) or director_registration_key_is_valid(entered):
                    st.session_state["dir_reg_key_verified"] = True
                    st.session_state["dir_reg_burned_key"] = entered
                    st.success("✅ Identity verified! The registration form is now open.")
                    st.rerun()
                else:
                    st.error("❌ Invalid or already-used code. Please contact BloomCore Support.")

        # ── PHASE 2: Registration Form (only after key is verified) ───────────
        else:
            st.success("✅ Authorization code accepted. Complete your Director profile below.")
            st.divider()
            st.markdown("### Complete Your Profile")

            with st.form("director_registration_form"):
                district_name = st.text_input("District/Municipal Name")
                username = st.text_input("Director Username")
                email = st.text_input("Email Address (for password recovery)")
                password = st.text_input("Password", type="password", key="dir_reg_password")
                confirm_password = st.text_input("Confirm Password", type="password", key="dir_reg_confirm_password")

                st.markdown("---")
                st.write("🔐 **Security Question (for password recovery)**")
                security_question = st.selectbox("Select a security question", SECURITY_QUESTION_OPTIONS)
                security_answer = st.text_input("Your Answer (remember this for password recovery)", type="password")

                password_val = st.session_state.get("dir_reg_password", "")
                confirm_val = st.session_state.get("dir_reg_confirm_password", "")
                if password_val:
                    render_password_strength_indicator(password_val)
                if confirm_val:
                    render_password_match_indicator(password_val, confirm_val)

                submitted = st.form_submit_button("🏛️ Finalize Director Registration")
                if submitted:
                    is_strong, _ = validate_password_strength(password_val)
                    if not is_strong:
                        st.error("Password does not meet security requirements. Please check the requirements above.")
                    elif password_val != confirm_val:
                        st.error("Passwords do not match.")
                    elif not security_answer.strip():
                        st.error("Security answer is required for password recovery.")
                    else:
                        security_key = generate_security_key(district_name)
                        try:
                            result = register_user(
                                username,
                                password_val,
                                "Director",
                                "ALL",
                                district=district_name,
                                security_key=security_key,
                                email=email,
                                security_question=security_question,
                                security_answer=security_answer,
                            )
                            # Burn the legacy app_config key too if it was used
                            consume_director_registration_key()
                            activate_director_context(result["username"], result["district"], result["security_key"])
                            initialize_empty_circuit_dataset()
                            initialize_empty_student_dataset()
                            st.session_state["dir_reg_key_verified"] = False
                            st.session_state["dir_reg_burned_key"] = ""
                            st.session_state["latest_director_key"] = result["security_key"]
                            st.session_state["latest_registered_district"] = result["district"]
                            st.session_state["auth_nav"] = "Login"
                            st.session_state["pending_setup_role"] = "Director"
                            st.session_state["auth_flash_message"] = (
                                f"Director account created for {result['district']}. Headteacher security key: {result['security_key']}"
                            )
                            st.session_state["auth_flash_severity"] = "success"
                            st.rerun()
                        except ValueError as exc:
                            st.error(str(exc))

    else:
        st.title("📝 Headteacher Registration")
        if not active_config["headteacher_security_key"]:
            st.warning("A Director must register first so the app can generate a valid Headteacher security key.")
            return

        st.info(f"Active District/Municipality: {active_scope_label}")
        reg_code = st.text_input(
            "District/Municipal Security Key",
            type="password",
            help="Contact District/Municipal Office for code",
        )

        if reg_code == active_config["headteacher_security_key"]:
            school_options = load_school_options()
            if not school_options:
                st.warning("No schools are available yet. The Director must upload the circuits dataset first so Headteachers can choose their school.")
                return

            with st.form("registration_form"):
                username = st.text_input("New Username")
                email = st.text_input("Email Address (for password recovery)")
                password = st.text_input("New Password", type="password", key="ht_reg_password")
                confirm_password = st.text_input("Confirm Password", type="password", key="ht_reg_confirm_password")
                school = st.selectbox("Select Your School", school_options)

                st.markdown("---")
                st.write("🔐 **Security Question (for password recovery)**")
                security_question = st.selectbox("Select a security question", SECURITY_QUESTION_OPTIONS, key="ht_security_q")
                security_answer = st.text_input("Your Answer (remember this for password recovery)", type="password", key="ht_security_a")

                # Real-time password indicators
                password = st.session_state.get("ht_reg_password", "")
                confirm_password = st.session_state.get("ht_reg_confirm_password", "")
                if password:
                    render_password_strength_indicator(password)
                if confirm_password:
                    render_password_match_indicator(password, confirm_password)

                submitted = st.form_submit_button("Create Account")
                if submitted:
                    is_strong, _ = validate_password_strength(password)
                    if not is_strong:
                        st.error("Password does not meet security requirements. Please check the requirements above and try again.")
                    elif password != confirm_password:
                        st.error("Passwords do not match.")
                    elif not security_answer.strip():
                        st.error("Security answer is required for password recovery.")
                    else:
                        try:
                            result = register_user(
                                username,
                                password,
                                "Headteacher",
                                school,
                                district=active_scope_label,
                                email=email,
                                security_question=security_question,
                                security_answer=security_answer,
                            )
                            st.session_state["latest_registered_school"] = result["school"]
                            st.session_state["latest_registered_district"] = active_scope_label
                            st.session_state["auth_nav"] = "Login"
                            st.session_state["pending_setup_role"] = "Headteacher"
                            st.session_state["auth_flash_message"] = (
                                f"Headteacher account created for {result['school']}. You can upload student data before logging in."
                            )
                            st.session_state["auth_flash_severity"] = "success"
                            st.rerun()
                        except ValueError as exc:
                            st.error(str(exc))

        elif reg_code != "":
            st.error("Incorrect District/Municipal Security Key.")


def render_metric_card(label, value, delta=None, delta_color="normal", help_text=None):
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric(label, value, delta=delta, delta_color=delta_color, help=help_text)
    st.markdown("</div>", unsafe_allow_html=True)


# ============================================================
# 9. DATA IMPORT, SCHOOL SYNC, AND SCENARIO STORAGE HELPERS
# ============================================================
def read_uploaded_csv(uploaded_file, dtype=None):
    uploaded_bytes = uploaded_file.getvalue()
    name = getattr(uploaded_file, "name", "") or ""
    if name.lower().endswith(".xlsx") or name.lower().endswith(".xls"):
        # Excel upload — skip the first two rows if they contain a merged heading
        try:
            uploaded_df = pd.read_excel(io.BytesIO(uploaded_bytes), dtype=dtype, header=0)
            # If the first column value looks like a heading (non-column-name string), skip it
            first_val = str(uploaded_df.columns[0]).strip().upper()
            if first_val not in [str(c).strip().upper() for c in EXPECTED_CIRCUIT_COLUMNS + HEADTEACHER_UPLOAD_TEMPLATE_COLUMNS]:
                uploaded_df = pd.read_excel(io.BytesIO(uploaded_bytes), dtype=dtype, header=2)
        except Exception:
            uploaded_df = pd.read_excel(io.BytesIO(uploaded_bytes), dtype=dtype)
    else:
        uploaded_df = pd.read_csv(io.BytesIO(uploaded_bytes), dtype=dtype, comment='#')
    uploaded_df = clean_uploaded_dataframe(uploaded_df)
    return uploaded_bytes, uploaded_df


def prepare_student_upload_df(uploaded_df):
    cleaned_df = clean_uploaded_dataframe(uploaded_df)
    for column in EXPECTED_DATA_COLUMNS:
        if column not in cleaned_df.columns:
            cleaned_df[column] = pd.NA

    cleaned_df = cleaned_df[EXPECTED_DATA_COLUMNS].copy()
    for column in ["Internal_Tracking_ID", "Student_ID", "Official_Index_Number", "Student_Name", "Gender", "Date_of_Birth", "School_Name", "Circuit", "School_Type", "Action_Zone", "Official_Results_Raw"]:
        cleaned_df[column] = cleaned_df[column].fillna("").astype(str).str.strip()
        cleaned_df.loc[cleaned_df[column].isin(["nan", "None"]), column] = ""
    cleaned_df["Date_of_Birth"] = normalize_date_of_birth_series(cleaned_df["Date_of_Birth"])

    school_profile_lookup = load_school_profile_lookup()
    if school_profile_lookup:
        mapped_circuit = cleaned_df["School_Name"].map(lambda school: school_profile_lookup.get(school, {}).get("Circuit", ""))
        mapped_school_type = cleaned_df["School_Name"].map(lambda school: school_profile_lookup.get(school, {}).get("School_Type", ""))
        cleaned_df["Circuit"] = cleaned_df["Circuit"].mask(cleaned_df["Circuit"].eq(""), pd.NA)
        cleaned_df["School_Type"] = cleaned_df["School_Type"].mask(cleaned_df["School_Type"].eq(""), pd.NA)
        cleaned_df["Circuit"] = cleaned_df["Circuit"].fillna(mapped_circuit).fillna("")
        cleaned_df["School_Type"] = cleaned_df["School_Type"].fillna(mapped_school_type).fillna("")
    original_school_type_series = cleaned_df["School_Type"].copy()
    cleaned_df["School_Type"] = normalize_school_type_series(cleaned_df["School_Type"])
    invalid_school_types = summarize_invalid_school_type_values(original_school_type_series)
    if invalid_school_types:
        raise ValueError(
            "School_Type must be Public or Private. Invalid values detected: "
            + ", ".join(invalid_school_types[:10])
            + ("..." if len(invalid_school_types) > 10 else "")
        )
    missing_school_type_mask = cleaned_df["School_Type"].fillna("").astype(str).str.strip().eq("")
    if missing_school_type_mask.any():
        cleaned_df.loc[missing_school_type_mask, "School_Type"] = MIGRATION_SCHOOL_TYPE_PLACEHOLDER

    meaningful_columns = [
        column
        for column in EXPECTED_DATA_COLUMNS
        if column not in {"Internal_Tracking_ID", "Student_ID", "Official_Index_Number", "School_Name", "Circuit", "School_Type"}
    ]
    meaningful_mask = pd.Series(False, index=cleaned_df.index)
    for column in meaningful_columns:
        if cleaned_df[column].dtype == "object":
            meaningful_mask = meaningful_mask | cleaned_df[column].fillna("").astype(str).str.strip().ne("")
        else:
            meaningful_mask = meaningful_mask | cleaned_df[column].notna()
    cleaned_df = cleaned_df[meaningful_mask].copy()
    if cleaned_df.empty:
        return cleaned_df

    return populate_provisional_final_scores(cleaned_df)


def write_dataframe_to_csv(df, path, columns):
    safe_df = df.copy()
    for column in columns:
        if column not in safe_df.columns:
            safe_df[column] = pd.NA
    safe_df = safe_df[columns]
    write_table_df(path, safe_df, columns)


def populate_provisional_final_scores(student_df):
    if student_df.empty:
        return student_df.copy()

    enriched_df = student_df.copy()
    model_bundle = get_live_ml_bundle()
    final_subject_cols = [column for column in FINAL_SUBJECT_COLUMNS if column in EXPECTED_DATA_COLUMNS]

    for index, row in enriched_df.iterrows():
        predicted_scores = []
        for subject_col in final_subject_cols:
            subject_sources = get_subject_source_columns(subject_col)
            predicted_column = subject_sources["predicted"]
            existing_prediction = pd.to_numeric(pd.Series([row.get(predicted_column)]), errors="coerce").iloc[0]
            official_final = pd.to_numeric(pd.Series([row.get(subject_sources["final"])]), errors="coerce").iloc[0]
            source_values = pd.to_numeric(
                pd.Series([row.get(subject_sources[suffix]) for suffix in ["assignments", "term1", "term2", "mock1", "mock2"]]),
                errors="coerce",
            )
            has_source_data = source_values.notna().any()
            if pd.isna(existing_prediction) and has_source_data:
                predicted_score = predict_subject_score_ml(row, subject_col, model_bundle)["predicted_score"]
                enriched_df.at[index, predicted_column] = round(predicted_score, 1)
                existing_prediction = predicted_score
            if pd.isna(existing_prediction) and pd.notna(official_final):
                existing_prediction = official_final
            if pd.notna(existing_prediction):
                predicted_scores.append(float(existing_prediction))

        math_mock1 = safe_float(row.get("Mathematics_Mock1"), np.nan)
        math_mock2 = safe_float(row.get("Mathematics_Mock2"), np.nan)
        math_assignment = safe_float(row.get("Mathematics_Assignments"), np.nan)
        if not pd.isna(math_mock1) and not pd.isna(math_mock2):
            enriched_df.at[index, "Math_Improvement"] = round(float(math_mock2 - math_mock1), 1)
        if not pd.isna(math_assignment) and not pd.isna(math_mock2):
            enriched_df.at[index, "Math_Consistency"] = round(float(math_assignment - math_mock2), 1)
        if predicted_scores:
            enriched_df.at[index, "Action_Zone"] = action_zone_from_average(float(np.mean(predicted_scores)))

    return enriched_df


def coerce_score_series(series):
    cleaned_series = series.fillna("").astype(str).str.strip()
    normalized_series = cleaned_series.str.replace("%", "", regex=False)
    numeric_series = pd.to_numeric(normalized_series, errors="coerce")
    grade_mask = numeric_series.between(1, 9, inclusive="both")
    numeric_series.loc[grade_mask] = numeric_series.loc[grade_mask].round().astype(int).apply(grade_to_score)
    return numeric_series


def decode_pdf_stream_bytes(object_bytes):
    stream_match = re.search(rb"stream\r?\n(.*?)\r?\nendstream", object_bytes, re.S)
    if not stream_match:
        return b""
    stream_bytes = stream_match.group(1)
    if b"/FlateDecode" in object_bytes:
        try:
            return zlib.decompress(stream_bytes)
        except Exception:
            return b""
    return stream_bytes


def build_waec_line_groups(page):
    words = page.extract_words() or []
    line_buckets = {}
    for word in words:
        y_key = round(float(word.get("top", 0.0)) / 3) * 3
        line_buckets.setdefault(y_key, []).append(word)

    grouped_lines = []
    for y_key in sorted(line_buckets):
        ordered_words = sorted(line_buckets[y_key], key=lambda item: item.get("x0", 0.0))
        grouped_lines.append(
            {
                "top": y_key,
                "words": ordered_words,
                "text": " ".join(word.get("text", "") for word in ordered_words).strip(),
            }
        )
    return grouped_lines


def extract_waec_school_name(pdf):
    if not pdf.pages:
        return ""

    first_page_text = pdf.pages[0].extract_text() or ""
    for line in [line.strip() for line in first_page_text.splitlines() if line.strip()]:
        upper_line = line.upper()
        if "JHS" in upper_line and not any(token in upper_line for token in ["RESULT", "WAEC", "SCHOOL #", "DATE", "HTTPS", "EXAMINATIONS COUNCIL"]):
            return line.strip()
    return ""


def is_waec_noise_line(line_text):
    upper_text = str(line_text).upper().strip()
    if not upper_text:
        return True
    noise_tokens = [
        "WAEC RESULTS LISTING",
        "THE WEST AFRICAN EXAMINATIONS COUNCIL",
        "THE BASIC SCHOOL CERTIFICATE RESULTS LISTING",
        "INDEX NUMBER NAME GENDER DOB RESULTS",
        "HTTPS://RESULTSLISTING.WAECGH.ORG/SEARCH",
        "COMMITTED TO EXCELLENCE",
        "SCHOOL #",
        "DATE :",
        "DISCLAIMER:",
        "AT THE TIME OF RELEASE",
        "THE FINAL RESULTS",
        "RESULTS SHOWN IN THIS LISTING",
    ]
    if any(token in upper_text for token in noise_tokens):
        return True
    if upper_text.startswith("TOTAL NUMBER OF CANDIDATES"):
        return True
    if re.fullmatch(r"\d+/\d+", upper_text):
        return True
    return False


def extract_waec_line_fields(line_words):
    field_parts = {"index": [], "name": [], "gender": [], "dob": [], "results": []}
    for word in line_words:
        text = str(word.get("text", "")).strip()
        if not text:
            continue
        x0 = float(word.get("x0", 0.0))
        if x0 < WAEC_EXPLICIT_VERTICAL_LINES[1]:
            field_parts["index"].append(text)
        elif x0 < WAEC_EXPLICIT_VERTICAL_LINES[2]:
            field_parts["name"].append(text)
        elif x0 < WAEC_EXPLICIT_VERTICAL_LINES[3]:
            field_parts["gender"].append(text)
        elif x0 < WAEC_EXPLICIT_VERTICAL_LINES[4]:
            field_parts["dob"].append(text)
        else:
            field_parts["results"].append(text)

    return {key: " ".join(values).strip() for key, values in field_parts.items()}


_NAME_NOISE_WORDS = {"the", "of", "at", "in", "is", "are", "for", "and", "to", "this", "that", "with", "shown", "time", "final", "release", "listing", "results", "will", "be", "certificate"}

def clean_waec_name_fragment(name_fragment):
    name_text = str(name_fragment).strip()
    name_text = re.sub(r"[^A-Za-z0-9 ]+", " ", name_text)
    name_text = re.sub(r"\s+", " ", name_text).strip()
    if not name_text:
        return ""
    if name_text.isdigit():
        return ""
    words = name_text.lower().split()
    noise_count = sum(1 for w in words if w in _NAME_NOISE_WORDS)
    if noise_count >= 2 or (len(words) > 4 and noise_count >= 1):
        return ""
    return name_text


def clean_waec_results_fragment(results_fragment):
    results_text = str(results_fragment).strip()
    results_text = re.sub(r"\s+", " ", results_text).strip(" ,")
    if not results_text:
        return ""
    if any(token in results_text.upper() for token in ["HTTPS://", "DISCLAIMER", "TOTAL NUMBER OF CANDIDATES"]):
        return ""
    return results_text


def extract_waec_pdf_rows(pdf_bytes, fallback_school_name=""):
    if pdfplumber is None:
        raise ValueError("pdfplumber is not installed. Please install it to process PDF files.")

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        school_name = extract_waec_school_name(pdf) or Path(fallback_school_name).stem.replace("_", " ").strip()

        # Stitch ALL pages into one block to handle page-break splits
        full_text = ""
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                full_text += page_text + "\n"

        if not full_text.strip():
            raise ValueError("No readable text found in the uploaded WAEC PDF.")

        # ── STRATEGY 1: Pipe-delimited layout (WAEC Official Listing format) ──────
        # Handles: "0711019001 | AMADAH SETH | Male | 12/03/2009 | ENGLISH - 5 ..."
        pipe_pattern = re.compile(
            r"(\d{10})\s*\|\s*([^|]+?)\s*\|\s*(Male|Female)\s*\|\s*(\d{2}/\d{2}/\d{4})",
            re.IGNORECASE,
        )
        
        if pipe_pattern.search(full_text):
            students = []
            current_student = None
            for line in full_text.split("\n"):
                match = pipe_pattern.search(line)
                if match:
                    if current_student:
                        students.append(current_student)
                    results_part = line.split("|")[-1].strip() if line.count("|") >= 4 else ""
                    current_student = {
                        "Student_ID": match.group(1).strip(),
                        "Student_Name": match.group(2).strip(),
                        "Gender": normalize_gender_token(match.group(3).strip()),
                        "Date_of_Birth": normalize_date_of_birth(match.group(4).strip()),
                        "Official_Results_Raw": results_part,
                    }
                elif current_student:
                    if "Total Number of Candidates" in line:
                        students.append(current_student)
                        current_student = None
                    elif "|" in line:
                        # Wrapped result text on continuation lines
                        parts = line.split("|")
                        if len(parts) > 1:
                            current_student["Official_Results_Raw"] += " " + parts[-1].strip()
            if current_student:
                students.append(current_student)
            if students:
                return school_name, students

        # ── STRATEGY 2: Full-text regex stitch (no pipe delimiters) ──────────────
        # Remove noise that could break the regex
        noise_patterns = [
            r"Total Number of Candidates.*",
            r"https?://\S+.*",
            r"\d{1,2}/\d{1,2}/\d{2,4},?\s*\d{1,2}:\d{2}\s*(?:AM|PM).*",
            r"WAEC Results Listing",
            r"INDEX\s+NUMBER\s+NAME\s+GENDER\s+DOB\s+RESULTS",
            r"Page\s+\d+\s+of\s+\d+",
        ]
        clean_text = full_text
        for pattern in noise_patterns:
            clean_text = re.sub(pattern, "", clean_text, flags=re.IGNORECASE)

        student_pattern = re.compile(
            r"(\b\d{10}\b)(.*?)(?=\b\d{10}\b|Total Number|$)",
            re.IGNORECASE | re.DOTALL,
        )
        parsed_students = []
        for match in student_pattern.finditer(clean_text):
            index_number = match.group(1).strip()
            raw_block = re.sub(r"\s+", " ", match.group(2).replace("\n", " ")).strip()

            # Only accept WAEC-format index numbers (start with 07)
            if not re.match(r"^07\d{8}$", index_number):
                continue

            meta_match = re.search(
                r"([A-Z][A-Z\s\.\-]+?)\s+(Male|Female)\s+(\d{2}/\d{2}/\d{4})",
                raw_block,
                re.IGNORECASE,
            )
            if not meta_match:
                continue

            full_name = re.sub(r"\s+", " ", meta_match.group(1)).strip()
            gender = meta_match.group(2).strip()
            dob = meta_match.group(3).strip()
            results_part = raw_block[meta_match.end():].strip(" ,|")
            if "RESULTS" in results_part.upper():
                results_part = re.split(r"RESULTS\s*", results_part, flags=re.IGNORECASE)[-1]
            results_part = re.sub(r"\s+", " ", results_part).strip()

            parsed_students.append({
                "Student_ID": index_number,
                "Student_Name": full_name,
                "Gender": normalize_gender_token(gender),
                "Date_of_Birth": normalize_date_of_birth(dob),
                "Official_Results_Raw": results_part,
            })

        if parsed_students:
            return school_name, parsed_students

        # ── STRATEGY 3: Line-by-line fallback ────────────────────────────────────
        return _extract_waec_pdf_line_by_line(pdf, school_name)


def _extract_waec_pdf_line_by_line(pdf, school_name):
    """Line-by-line extraction when table extraction fails."""
    extracted_data = []
    index_pattern = re.compile(r'^\d{10}$')
    current_student = None
    
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
        
        lines = text.split('\n')
        
        for line in lines:
            clean_line = line.strip()
            
            # Check if this line is an Index Number (10 digits)
            if index_pattern.match(clean_line):
                if current_student:
                    extracted_data.append(current_student)
                
                current_student = {
                    "Student_ID": clean_line,
                    "Student_Name": "",
                    "Gender": "",
                    "Date_of_Birth": "",
                    "Official_Results_Raw": "",
                }
            
            elif current_student:
                # Skip header noise
                skip_words = ["INDEX NUMBER", "NAME", "GENDER", "DOB", "RESULTS", 
                              "WAEC", "TOTAL", "CANDIDATES", "HTTPS://", "4/25/25", "PM", "AM"]
                if any(word in clean_line.upper() for word in skip_words):
                    continue
                
                if clean_line.upper() in ["MALE", "FEMALE"]:
                    current_student["Gender"] = clean_line
                    continue
                
                dob_match = re.match(r'(\d{2}/\d{2}/\d{4})$', clean_line)
                if dob_match:
                    current_student["Date_of_Birth"] = dob_match.group(1)
                    continue
                
                if any(char.isalpha() for char in clean_line):
                    if re.search(r'[A-Z]+\s*-\s*\d', clean_line):
                        current_student["Official_Results_Raw"] += " " + clean_line
                    else:
                        current_student["Student_Name"] += " " + clean_line
                else:
                    current_student["Official_Results_Raw"] += " " + clean_line
    
    if current_student:
        extracted_data.append(current_student)
    
    # Clean up
    for student in extracted_data:
        student["Student_Name"] = re.sub(r'\s+', ' ', student["Student_Name"]).strip()
        student["Official_Results_Raw"] = re.sub(r'\s+', ' ', student["Official_Results_Raw"]).strip()
        student["Gender"] = normalize_gender_token(student["Gender"])
        student["Date_of_Birth"] = normalize_date_of_birth(student["Date_of_Birth"])
    
    if extracted_data:
        return school_name, extracted_data
    
    # Final fallback to regex-based
    return _extract_waec_pdf_text_based(pdf, school_name)


def _extract_waec_pdf_text_based(pdf, school_name):
    """Fallback text-based extraction when table extraction fails."""
    full_text = ""
    for page in pdf.pages:
        page_text = page.extract_text() or ""
        full_text += "\n" + page_text + "\n"

    if not full_text.strip():
        raise ValueError("No readable text found in the uploaded WAEC PDF.")

    # Clean up noise patterns
    noise_patterns = [
        r"Total Number of Candidates:.*",
        r"https?://resultslisting\.waecgh\.org.*",
        r"\d{1,2}/\d{1,2}/\d{2,4},?\s*\d{1,2}:\d{2}\s*(AM|PM).*",
        r"WAEC Results Listing",
        r"INDEX NUMBER\s+NAME\s+GENDER\s+DOB\s+RESULTS",
        r"Page \d+ of \d+",
    ]
    
    clean_text = full_text
    for pattern in noise_patterns:
        clean_text = re.sub(pattern, "", clean_text, flags=re.IGNORECASE)
    
    # Use regex with DOTALL to capture multi-line student blocks
    student_pattern = r'(\b07\d{8}\b)(.*?)(?=\b07\d{8}\b|$)'
    matches = re.findall(student_pattern, clean_text, re.DOTALL)
    
    extracted_rows = []
    
    for index_no, raw_block in matches:
        index_number = index_no.strip()
        raw_data = raw_block.strip()
        
        if not index_number or len(index_number) != 10:
            continue
        
        # Normalize whitespace
        raw_data_normalized = re.sub(r'\n+', ' ', raw_data)
        raw_data_normalized = re.sub(r'\s+', ' ', raw_data_normalized)
        
        # Extract name, gender, DOB
        meta_match = re.search(
            r"([A-Z][A-Z\s\.]+?)\s+(Male|Female)\s+(\d{2}/\d{2}/\d{4})",
            raw_data_normalized,
            re.IGNORECASE
        )
        
        if meta_match:
            full_name = meta_match.group(1).strip()
            gender = meta_match.group(2).strip()
            dob = meta_match.group(3).strip()
            
            # Extract results
            results_part = raw_data_normalized
            if "RESULTS" in raw_data_normalized.upper():
                results_split = re.split(r'RESULTS\s*', raw_data_normalized, flags=re.IGNORECASE)
                if len(results_split) > 1:
                    results_part = results_split[-1]
            else:
                match_end = meta_match.end()
                results_part = raw_data_normalized[match_end:]
            
            results_part = re.sub(r"\s+", " ", results_part).strip(" ,")
            
            extracted_rows.append({
                "Student_ID": index_number,
                "Student_Name": full_name,
                "Gender": normalize_gender_token(gender),
                "Date_of_Birth": normalize_date_of_birth(dob),
                "Official_Results_Raw": results_part,
            })
    
    return school_name, extracted_rows


def _extract_waec_pdf_rows_fallback_v2(clean_text, school_name):
    """Fallback extraction when primary regex pattern fails - uses line-by-line approach."""
    lines = clean_text.split("\n")
    extracted_rows = []
    current_record = None
    pending_name_parts = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Check for index number - this starts a new record
        index_match = re.search(r'\b(07\d{8})\b', line)
        
        if index_match:
            # Save previous record with assembled name
            if current_record and current_record.get("Student_ID"):
                if pending_name_parts:
                    current_record["Student_Name"] = " ".join(pending_name_parts).strip()
                extracted_rows.append(current_record)
                pending_name_parts = []
            
            # Start new record
            current_record = {
                "Student_ID": index_match.group(1),
                "Student_Name": "",
                "Gender": "",
                "Date_of_Birth": "",
                "Official_Results_Raw": "",
            }
            
            # Try to extract fields from this line after the index
            rest = line[index_match.end():].strip()
            
            # Look for Name + Gender + DOB pattern in the rest
            # Name comes first (all caps), then Gender, then DOB
            meta_match = re.search(r"([A-Z][A-Z\s]+?)\s+(Male|Female)\s+(\d{2}/\d{2}/\d{4})", rest, re.IGNORECASE)
            if meta_match:
                current_record["Student_Name"] = meta_match.group(1).strip()
                current_record["Gender"] = meta_match.group(2)
                current_record["Date_of_Birth"] = meta_match.group(3)
                # Results come after DOB
                results_part = rest[meta_match.end():].strip()
                current_record["Official_Results_Raw"] = results_part
            else:
                # Name might be split across lines - start collecting
                name_part = re.match(r"([A-Z][A-Z\s]*)", rest)
                if name_part:
                    pending_name_parts = [name_part.group(1).strip()]
                # Look for gender/dob on this line
                gender_match = re.search(r'(Male|Female)', rest, re.IGNORECASE)
                if gender_match:
                    current_record["Gender"] = gender_match.group(1)
                dob_match = re.search(r'(\d{2}/\d{2}/\d{4})', rest)
                if dob_match:
                    current_record["Date_of_Birth"] = dob_match.group(1)
                
        elif current_record:
            # Check if this line has gender/dob (completes the name)
            gender_match = re.search(r'(Male|Female)', line, re.IGNORECASE)
            if gender_match and not current_record["Gender"]:
                # Assemble name from pending parts
                if pending_name_parts:
                    current_record["Student_Name"] = " ".join(pending_name_parts).strip()
                    pending_name_parts = []
                current_record["Gender"] = gender_match.group(1)
                dob_match = re.search(r'(\d{2}/\d{2}/\d{4})', line)
                if dob_match:
                    current_record["Date_of_Birth"] = dob_match.group(1)
                # Results might follow
                results_part = line.split(dob_match.group(1))[-1] if dob_match else line[gender_match.end():]
                current_record["Official_Results_Raw"] = results_part.strip()
            elif gender_match and current_record["Gender"]:
                # This is results data for the current record
                current_record["Official_Results_Raw"] += " " + line
            elif not current_record["Gender"] and pending_name_parts is not None:
                # Still collecting name parts
                name_part = re.match(r"([A-Z][A-Z\s]*)\s*$", line)
                if name_part:
                    pending_name_parts.append(name_part.group(1).strip())
            else:
                # Append to results
                current_record["Official_Results_Raw"] += " " + line
    
    # Don't forget last record
    if current_record and current_record.get("Student_ID"):
        if pending_name_parts and not current_record["Student_Name"]:
            current_record["Student_Name"] = " ".join(pending_name_parts).strip()
        extracted_rows.append(current_record)
    
    return school_name, extracted_rows


SUBJECT_ALIASES = {
    "C.A. & DESIGN": "CREATIVE ARTS DESIGN",
    "C. A. & DESIGN": "CREATIVE ARTS DESIGN",
    "CA & DESIGN": "CREATIVE ARTS DESIGN",
    "C A & DESIGN": "CREATIVE ARTS DESIGN",
    "CA DESIGN": "CREATIVE ARTS DESIGN",
    "C.A. DESIGN": "CREATIVE ARTS DESIGN",
    "C. A. DESIGN": "CREATIVE ARTS DESIGN",
    "CREATIVE ARTS & DESIGN": "CREATIVE ARTS DESIGN",
    "CREATIVE ARTS & DES.": "CREATIVE ARTS DESIGN",
    "CREATIVE ARTS AND DESIGN": "CREATIVE ARTS DESIGN",
    "FRENCH": "FRENCH",
    "COMPUTING": "COMPUTING",
    "ICT": "COMPUTING",
    "INFORMATION AND COMMUNICATION TECHNOLOGY": "COMPUTING",
    "ENGLISH LANG.": "ENGLISH LANGUAGE",
    "ENGLISH LANG": "ENGLISH LANGUAGE",
    "SOCIAL STUD.": "SOCIAL STUDIES",
    "SOCIAL STUD": "SOCIAL STUDIES",
    "R.M.E.": "RME",
    "R. M. E.": "RME",
    "R.M.E": "RME",
    "CAREER TECH.": "CAREER TECHNOLOGY",
    "CAREER TECH": "CAREER TECHNOLOGY",
    "BASIC DESIGN AND TECHNOLOGY": "CAREER TECHNOLOGY",
    "BDT": "CAREER TECHNOLOGY",
    "EPE": "EWE",
}


def normalize_subject_name(raw_name):
    """Resolves subject name aliases to their canonical form.
    Works for input from PDFs, Excel/CSV uploads, and Google Sheets.
    """
    clean_name = str(raw_name).strip().upper()
    clean_name = re.sub(r"\s+", " ", clean_name)
    return SUBJECT_ALIASES.get(clean_name, clean_name)


def normalize_waec_subject_label(subject_label):
    label = str(subject_label).upper().strip()
    label = re.sub(r"\s+", " ", label)
    # Resolve through alias map first (handles all known variants including C.A.&Design)
    resolved = normalize_subject_name(label)
    if resolved != label:
        return resolved
    # Secondary normalization for patterns not in the alias map
    label = label.replace(":", " ")
    label = label.replace("&", " ")
    label = re.sub(r"[^A-Z ]+", " ", label)
    return re.sub(r"\s+", " ", label).strip()


def map_waec_result_text_to_scores(results_text):
    normalized_text = str(results_text).upper().replace(":", " ")
    normalized_text = normalized_text.replace("\t", " ")
    normalized_text = normalized_text.replace("\r", " ")
    normalized_text = normalized_text.replace("\n", " ")
    normalized_text = normalized_text.replace("\x0f", " ")
    normalized_text = normalized_text.replace("\x06", " ")
    normalized_text = normalized_text.replace("\x14", " ")
    normalized_text = re.sub(r"\s+", " ", normalized_text).strip()

    subject_scores = {}
    for subject_label, grade_text in re.findall(r"([A-Z. ]+?)\s*-\s*([1-9])", normalized_text):
        normalized_label = normalize_waec_subject_label(subject_label)
        target_column = WAEC_RESULT_SUBJECT_MAP.get(normalized_label)
        if target_column:
            subject_scores[target_column] = grade_to_score(int(grade_text))
    return subject_scores


def prepare_official_pdf_import(uploaded_bytes, uploaded_name, expected_school="", forced_school=""):
    parsed_school_name, extracted_rows = extract_waec_pdf_rows(uploaded_bytes, fallback_school_name=uploaded_name)
    resolved_school = str(forced_school).strip() or str(parsed_school_name).strip()
    if expected_school and parsed_school_name and str(expected_school).strip().casefold() != str(parsed_school_name).strip().casefold():
        raise ValueError(f"The uploaded WAEC PDF belongs to {parsed_school_name}, not {expected_school}.")
    if not resolved_school:
        raise ValueError("EduPulse could not determine the school name from the uploaded WAEC PDF.")

    school_profile_lookup = load_school_profile_lookup()
    # Case-insensitive school lookup: try exact match, then lowercase, then uppercase
    school_profile = school_profile_lookup.get(resolved_school, {})
    if not school_profile:
        school_profile = school_profile_lookup.get(resolved_school.lower(), {})
    if not school_profile:
        school_profile = school_profile_lookup.get(resolved_school.upper(), {})
    resolved_circuit = school_profile.get("Circuit", "")
    resolved_school_type = school_profile.get("School_Type", "Not Specified")
    if not resolved_circuit:
        raise ValueError(f"{resolved_school} is not yet mapped to a circuit. Update the circuits file before importing official results.")

    official_rows = []
    for extracted_row in extracted_rows:
        official_row = {column: pd.NA for column in EXPECTED_DATA_COLUMNS}
        official_index_number = str(extracted_row.get("Student_ID", "")).strip()
        official_row["Student_ID"] = official_index_number
        official_row["Official_Index_Number"] = official_index_number
        official_row["Student_Name"] = str(extracted_row.get("Student_Name", "")).strip()
        official_row["Gender"] = normalize_gender_token(extracted_row.get("Gender", ""))
        official_row["Date_of_Birth"] = normalize_date_of_birth(extracted_row.get("Date_of_Birth", ""))
        official_row["School_Name"] = resolved_school
        official_row["Circuit"] = resolved_circuit
        official_row["School_Type"] = resolved_school_type
        official_row["Official_Results_Raw"] = str(extracted_row.get("Official_Results_Raw", "")).strip()
        official_row.update(map_waec_result_text_to_scores(official_row["Official_Results_Raw"]))
        official_rows.append(official_row)

    official_df = pd.DataFrame(official_rows, columns=EXPECTED_DATA_COLUMNS)
    completed_df = prepare_student_upload_df(official_df)
    if completed_df.empty:
        raise ValueError("The official WAEC PDF was read, but no completed student rows were detected.")

    matched_fields = WAEC_ACCEPTED_FIELDS.copy()
    return completed_df, matched_fields, resolved_school, resolved_circuit, resolved_school_type


def build_official_alias_lookup():
    alias_lookup = {}
    for target_column, aliases in SUBJECT_IMPORT_ALIASES.items():
        alias_lookup[normalize_header_text(target_column)] = target_column
        for alias in aliases:
            alias_lookup[normalize_header_text(alias)] = target_column
    return alias_lookup


def map_official_import_columns(uploaded_df):
    alias_lookup = build_official_alias_lookup()
    mapped_columns = {}
    for source_column in uploaded_df.columns:
        normalized = normalize_header_text(source_column)
        target_column = alias_lookup.get(normalized)
        if target_column and target_column not in mapped_columns:
            mapped_columns[target_column] = source_column
    return mapped_columns


def prepare_official_results_import(uploaded_df, school, school_circuit):
    mapped_columns = map_official_import_columns(uploaded_df)
    required_columns = ["Student_Name"]
    missing_required = [column for column in required_columns if column not in mapped_columns]
    if missing_required:
        raise ValueError(
            "The official import file is missing the required field(s): "
            + ", ".join(missing_required)
            + ". Include a student name column and try again."
        )

    school_profile_lookup = load_school_profile_lookup()
    school_profile = school_profile_lookup.get(school, {}) or school_profile_lookup.get(school.lower(), {}) or school_profile_lookup.get(school.upper(), {})
    school_type = school_profile.get("School_Type", "Not Specified")
    official_df = pd.DataFrame(index=uploaded_df.index, columns=EXPECTED_DATA_COLUMNS)
    for column in EXPECTED_DATA_COLUMNS:
        official_df[column] = pd.NA

    for target_column, source_column in mapped_columns.items():
        official_df[target_column] = uploaded_df[source_column]

    for column in ["Student_ID", "Official_Index_Number", "Student_Name", "Gender", "Date_of_Birth", "Official_Results_Raw"]:
        if column in official_df.columns:
            official_df[column] = official_df[column].fillna("").astype(str).str.strip()
    if "Official_Index_Number" in official_df.columns:
        missing_official_index_mask = official_df["Official_Index_Number"].eq("") & official_df["Student_ID"].str.fullmatch(r"\d{10}", na=False)
        official_df.loc[missing_official_index_mask, "Official_Index_Number"] = official_df.loc[missing_official_index_mask, "Student_ID"]
    official_df["Gender"] = official_df["Gender"].apply(normalize_gender_token)
    official_df["Date_of_Birth"] = normalize_date_of_birth_series(official_df["Date_of_Birth"])
    official_df["School_Name"] = school
    official_df["Circuit"] = school_circuit
    official_df["School_Type"] = school_type

    subject_cols = [column for column in EXPECTED_DATA_COLUMNS if column.endswith(FINAL_SUFFIX)]
    for subject_col in subject_cols:
        if subject_col in mapped_columns:
            official_df[subject_col] = coerce_score_series(uploaded_df[mapped_columns[subject_col]])

    if "Official_Results_Raw" in mapped_columns:
        for row_index, raw_results in official_df["Official_Results_Raw"].items():
            parsed_scores = map_waec_result_text_to_scores(raw_results)
            for target_column, target_value in parsed_scores.items():
                if pd.isna(official_df.at[row_index, target_column]):
                    official_df.at[row_index, target_column] = target_value

    completed_df = prepare_student_upload_df(official_df)
    if completed_df.empty:
        raise ValueError("The official file was read, but no completed student rows were found after cleanup.")

    matched_fields = sorted(mapped_columns.keys())
    return completed_df, matched_fields


def build_match_review_row(official_row, school, circuit, source_label, reason):
    return {
        "review_id": f"review-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}-{random.randint(1000, 9999)}",
        "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "district": st.session_state.get("user_district", "") or get_scope_label(),
        "school": school,
        "circuit": circuit,
        "source_label": source_label,
        "official_index_number": str(official_row.get("Official_Index_Number") or official_row.get("Student_ID", "")).strip(),
        "student_name": str(official_row.get("Student_Name", "")).strip(),
        "date_of_birth": normalize_date_of_birth(official_row.get("Date_of_Birth", "")),
        "reason": reason,
        "status": "Pending Review",
        "payload_json": json.dumps({column: str(official_row.get(column, "")) for column in EXPECTED_DATA_COLUMNS}),
    }


def merge_official_results_for_school(existing_school_df, official_df, school, school_circuit, source_label="official WAEC result upload"):
    existing_working_df = existing_school_df.copy()
    if existing_working_df.empty:
        existing_working_df = pd.DataFrame(columns=EXPECTED_DATA_COLUMNS)
    for column in EXPECTED_DATA_COLUMNS:
        if column not in existing_working_df.columns:
            existing_working_df[column] = pd.NA
    existing_working_df = existing_working_df[EXPECTED_DATA_COLUMNS].copy()
    existing_working_df["_existing_row_id"] = np.arange(len(existing_working_df))
    existing_working_df["_used"] = False
    existing_working_df["_match_key"] = existing_working_df.apply(
        lambda row: build_internal_match_key(row.get("School_Name", school), row.get("Student_Name", ""), row.get("Date_of_Birth", "")),
        axis=1,
    )
    existing_working_df["_official_index_key"] = existing_working_df["Official_Index_Number"].fillna("").astype(str).str.strip()
    existing_working_df["_student_id_key"] = existing_working_df["Student_ID"].fillna("").astype(str).str.strip()

    merged_rows = []
    review_rows = []
    matched_count = 0
    official_only_count = 0

    for _, official_row in official_df.iterrows():
        official_index = str(official_row.get("Official_Index_Number") or official_row.get("Student_ID", "")).strip()
        official_name = str(official_row.get("Student_Name", "")).strip()
        official_dob = normalize_date_of_birth(official_row.get("Date_of_Birth", ""))
        official_match_key = build_internal_match_key(school, official_name, official_dob)

        matched_candidates = pd.DataFrame()
        if official_index:
            matched_candidates = existing_working_df[
                ~existing_working_df["_used"]
                & (
                    existing_working_df["_official_index_key"].eq(official_index)
                    | existing_working_df["_student_id_key"].eq(official_index)
                )
            ].copy()

        if matched_candidates.empty and official_match_key:
            matched_candidates = existing_working_df[
                ~existing_working_df["_used"]
                & existing_working_df["_match_key"].eq(official_match_key)
            ].copy()

        if len(matched_candidates) > 1:
            review_rows.append(build_match_review_row(official_row, school, school_circuit, source_label, "Ambiguous strict match: multiple existing students share the same school/name/DOB signature."))
            continue

        if len(matched_candidates) == 1:
            matched_count += 1
            matched_index = matched_candidates.index[0]
            existing_working_df.at[matched_index, "_used"] = True
            merged_row = existing_working_df.loc[matched_index, EXPECTED_DATA_COLUMNS].copy()

            for column in ["Student_Name", "Gender", "Date_of_Birth", "Official_Results_Raw"]:
                official_value = str(official_row.get(column, "")).strip()
                if official_value:
                    merged_row[column] = official_value
            if official_index:
                merged_row["Student_ID"] = official_index
                merged_row["Official_Index_Number"] = official_index

            merged_row["School_Name"] = school
            merged_row["Circuit"] = school_circuit
            merged_row["School_Type"] = official_row.get("School_Type", merged_row.get("School_Type", MIGRATION_SCHOOL_TYPE_PLACEHOLDER))
            if not str(merged_row.get("Internal_Tracking_ID", "")).strip():
                merged_row["Internal_Tracking_ID"] = generate_internal_tracking_id()

            for final_col in FINAL_SUBJECT_COLUMNS:
                official_value = pd.to_numeric(pd.Series([official_row.get(final_col)]), errors="coerce").iloc[0]
                if pd.notna(official_value):
                    merged_row[final_col] = official_value

            merged_rows.append(merged_row.to_dict())
            continue

        if not official_match_key:
            review_rows.append(build_match_review_row(official_row, school, school_circuit, source_label, "Missing normalized name or DOB prevented strict matching."))
            continue

        official_only_count += 1
        new_row = {column: official_row.get(column, pd.NA) for column in EXPECTED_DATA_COLUMNS}
        new_row["Internal_Tracking_ID"] = generate_internal_tracking_id()
        new_row["Student_ID"] = official_index or str(official_row.get("Student_ID", "")).strip()
        new_row["Official_Index_Number"] = official_index
        new_row["School_Name"] = school
        new_row["Circuit"] = school_circuit
        new_row["School_Type"] = official_row.get("School_Type", MIGRATION_SCHOOL_TYPE_PLACEHOLDER)
        merged_rows.append(new_row)

    remaining_existing_rows = existing_working_df.loc[~existing_working_df["_used"], EXPECTED_DATA_COLUMNS].copy()
    merged_df = pd.concat([remaining_existing_rows, pd.DataFrame(merged_rows, columns=EXPECTED_DATA_COLUMNS)], ignore_index=True)
    return merged_df, review_rows, matched_count, official_only_count


def sync_student_upload(prepared_df, school, school_circuit, redirect_to_login=False, source_label="student dataset"):

    # Each sync replaces only the current school's rows, keeping the
    # rest of the municipality dataset intact for other schools.
    data_status = get_data_file_status()
    if data_status["ready"]:
        existing_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
        existing_df = prepare_student_upload_df(existing_df)
    else:
        existing_df = pd.DataFrame(columns=EXPECTED_DATA_COLUMNS)

    previous_school_rows = int(
        existing_df["School_Name"].fillna("").astype(str).str.strip().eq(school).sum()
    ) if "School_Name" in existing_df.columns else 0

    uploaded_ids = prepared_df["Student_ID"].fillna("").astype(str).str.strip()
    duplicate_uploaded_ids = sorted(uploaded_ids[uploaded_ids.duplicated()].unique().tolist())
    if duplicate_uploaded_ids:
        raise ValueError(
            "The uploaded file contains duplicate Student_ID values. Please correct them before syncing: "
            + ", ".join(duplicate_uploaded_ids[:10])
            + ("..." if len(duplicate_uploaded_ids) > 10 else "")
        )

    existing_df = existing_df[
        existing_df["School_Name"].fillna("").astype(str).str.strip() != school
    ].copy()
    existing_ids = set(existing_df["Student_ID"].fillna("").astype(str).str.strip().tolist())
    conflicting_ids = sorted([student_id for student_id in uploaded_ids.tolist() if student_id in existing_ids])
    if conflicting_ids:
        raise ValueError(
            "Some Student_ID values in this upload already belong to another school in the active dataset. "
            "Download a fresh template so the IDs continue from the correct range. Conflicting IDs: "
            + ", ".join(conflicting_ids[:10])
            + ("..." if len(conflicting_ids) > 10 else "")
        )

    combined_df = pd.concat([existing_df, prepared_df], ignore_index=True)
    if "Student_ID" in combined_df.columns:
        combined_df["Student_ID"] = combined_df["Student_ID"].fillna("").astype(str).str.strip()
        combined_df = combined_df.drop_duplicates(subset=["Student_ID"], keep="last")

    write_dataframe_to_csv(combined_df, DATA_FILE, EXPECTED_DATA_COLUMNS)
    uploader_name = st.session_state.get("current_user", "Headteacher")
    create_notification(
        event_type="student_bulk_sync",
        message=(
            f"Bulk {source_label} synced by Headteacher {uploader_name} for {school}: "
            f"{len(prepared_df)} student row(s) synced for {school} in {school_circuit}. "
            f"Previous school rows: {previous_school_rows}; current school rows: {len(prepared_df)}."
        ),
        target_role="Director",
        school=school,
        circuit=school_circuit,
        created_by=st.session_state.get("current_user", ""),
        district=st.session_state.get("user_district", "") or get_scope_label(),
    )
    st.cache_data.clear()
    if redirect_to_login:
        st.session_state["auth_nav"] = "Login"
        st.session_state["auth_flash_message"] = f"Student data for {school} synced successfully. You can now log in."
        st.session_state["auth_flash_severity"] = "success"
        st.session_state["pending_setup_role"] = ""
    else:
        st.session_state["portal_flash_message"] = (
            f"✅ {source_label.title()} synced successfully for {school}. The Director dashboard now has the latest school data."
        )
        st.session_state["portal_flash_severity"] = "success"


def sync_multi_school_upload(prepared_df, source_label="official WAEC PDF import"):
    if prepared_df.empty:
        raise ValueError("No student rows were prepared for sync.")

    data_status = get_data_file_status()
    if data_status["ready"]:
        existing_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
        existing_df = prepare_student_upload_df(existing_df)
    else:
        existing_df = pd.DataFrame(columns=EXPECTED_DATA_COLUMNS)

    uploaded_schools = sorted(prepared_df["School_Name"].fillna("").astype(str).str.strip().unique().tolist())
    previous_counts = (
        existing_df.groupby("School_Name").size().to_dict()
        if not existing_df.empty and "School_Name" in existing_df.columns
        else {}
    )
    existing_df = existing_df[
        ~existing_df["School_Name"].fillna("").astype(str).str.strip().isin(uploaded_schools)
    ].copy() if "School_Name" in existing_df.columns else existing_df.copy()

    uploaded_ids = prepared_df["Student_ID"].fillna("").astype(str).str.strip()
    duplicate_uploaded_ids = sorted(uploaded_ids[uploaded_ids.duplicated()].unique().tolist())
    if duplicate_uploaded_ids:
        raise ValueError(
            "The uploaded files contain duplicate Student_ID values. Please correct them before syncing: "
            + ", ".join(duplicate_uploaded_ids[:10])
            + ("..." if len(duplicate_uploaded_ids) > 10 else "")
        )

    existing_ids = set(existing_df["Student_ID"].fillna("").astype(str).str.strip().tolist()) if "Student_ID" in existing_df.columns else set()
    conflicting_ids = sorted([student_id for student_id in uploaded_ids.tolist() if student_id in existing_ids])
    if conflicting_ids:
        raise ValueError(
            "Some Student_ID values in this upload already belong to another school in the active dataset. Conflicting IDs: "
            + ", ".join(conflicting_ids[:10])
            + ("..." if len(conflicting_ids) > 10 else "")
        )

    combined_df = pd.concat([existing_df, prepared_df], ignore_index=True)
    if "Student_ID" in combined_df.columns:
        combined_df["Student_ID"] = combined_df["Student_ID"].fillna("").astype(str).str.strip()
        combined_df = combined_df.drop_duplicates(subset=["Student_ID"], keep="last")

    write_dataframe_to_csv(combined_df, DATA_FILE, EXPECTED_DATA_COLUMNS)
    st.cache_data.clear()
    summary_bits = []
    for school_name in uploaded_schools:
        current_count = int(prepared_df["School_Name"].fillna("").astype(str).str.strip().eq(school_name).sum())
        summary_bits.append(f"{school_name}: {previous_counts.get(school_name, 0)} -> {current_count}")
    st.session_state["portal_flash_message"] = (
        f"Official results synced successfully for {len(uploaded_schools)} school(s). " + "; ".join(summary_bits)
    )
    st.session_state["portal_flash_severity"] = "success"


def build_school_sync_status_df(df):
    mapping_df = load_circuit_mapping_df()
    if mapping_df.empty:
        status_df = pd.DataFrame(columns=["School_Name", "Circuit", "School_Type"])
    else:
        status_df = mapping_df[["School_Name", "Circuit", "School_Type"]].copy()

    if df.empty or "School_Name" not in df.columns:
        student_counts = pd.DataFrame(columns=["School_Name", "Students Uploaded"])
        student_school_types = pd.DataFrame(columns=["School_Name", "School_Type"])
    else:
        student_counts = (
            df[df["School_Name"].fillna("").astype(str).str.strip() != ""]
            .groupby("School_Name", as_index=False)
            .size()
            .rename(columns={"size": "Students Uploaded"})
        )
        if "School_Type" in df.columns:
            student_school_types = (
                df[["School_Name", "School_Type"]]
                .copy()
                .assign(
                    School_Name=lambda frame: frame["School_Name"].fillna("").astype(str).str.strip(),
                    School_Type=lambda frame: frame["School_Type"].fillna("").astype(str).str.strip(),
                )
            )
            student_school_types = student_school_types[student_school_types["School_Name"] != ""]
            student_school_types = student_school_types.drop_duplicates(subset=["School_Name"], keep="last")
        else:
            student_school_types = pd.DataFrame(columns=["School_Name", "School_Type"])

    if status_df.empty and not student_counts.empty:
        status_df = student_counts.copy()
        status_df["Circuit"] = ""
        status_df["School_Type"] = "Not Specified"
    else:
        status_df = status_df.merge(student_counts, on="School_Name", how="outer")

    if not student_school_types.empty:
        status_df = status_df.merge(student_school_types, on="School_Name", how="left", suffixes=("", "_from_data"))
        if "School_Type_from_data" in status_df.columns:
            status_df["School_Type"] = status_df["School_Type"].fillna("")
            status_df.loc[status_df["School_Type"].astype(str).str.strip() == "", "School_Type"] = status_df["School_Type_from_data"]
            status_df = status_df.drop(columns=["School_Type_from_data"])

    if "Students Uploaded" not in status_df.columns:
        upload_columns = [column for column in status_df.columns if str(column).startswith("Students Uploaded")]
        if upload_columns:
            status_df["Students Uploaded"] = (
                status_df[upload_columns]
                .apply(pd.to_numeric, errors="coerce")
                .fillna(0)
                .max(axis=1)
            )
            status_df = status_df.drop(columns=upload_columns)
        else:
            status_df["Students Uploaded"] = 0

    status_df["School_Name"] = status_df["School_Name"].fillna("").astype(str).str.strip()
    status_df["Circuit"] = status_df["Circuit"].fillna("").astype(str).str.strip()
    status_df["School_Type"] = status_df["School_Type"].fillna("Not Specified").astype(str).str.strip()
    status_df.loc[status_df["School_Type"] == "", "School_Type"] = "Not Specified"
    status_df["Students Uploaded"] = pd.to_numeric(status_df["Students Uploaded"], errors="coerce").fillna(0).astype(int)
    status_df = status_df[status_df["School_Name"] != ""].copy()
    status_df["Status"] = status_df["Students Uploaded"].apply(
        lambda count: "Synced" if count > 0 else "Awaiting Upload"
    )
    return status_df.sort_values(["Status", "Circuit", "School_Name"], ascending=[False, True, True]).reset_index(drop=True)


def save_prediction_scenario(student_row, scenario_name, intervention_note, scenario_inputs, current_outcome, predicted_outcome, predicted_rows):
    scenarios_df = load_scenarios_df()
    scenario_record = {
        "scenario_id": f"scenario-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}-{random.randint(1000, 9999)}",
        "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "district": st.session_state.get("user_district", "") or get_scope_label(),
        "school": str(student_row.get("School_Name", "")).strip(),
        "circuit": str(student_row.get("Circuit", "")).strip(),
        "student_id": str(student_row.get("Student_ID", "")).strip(),
        "student_name": str(student_row.get("Student_Name", "")).strip(),
        "scenario_name": str(scenario_name).strip() or "Untitled scenario",
        "intervention_note": str(intervention_note).strip(),
        "current_attendance": f"{scenario_inputs['current_attendance']:.1f}",
        "target_attendance": f"{scenario_inputs['target_attendance']:.1f}",
        "current_assignment": f"{scenario_inputs['current_assignment']:.1f}",
        "target_assignment": f"{scenario_inputs['target_assignment']:.1f}",
        "current_mock": f"{scenario_inputs['current_mock']:.1f}",
        "target_mock": f"{scenario_inputs['target_mock']:.1f}",
        "current_aggregate": f"{current_outcome['aggregate']:.1f}",
        "predicted_aggregate": f"{predicted_outcome['aggregate']:.1f}",
        "current_placement": current_outcome["placement_outlook"],
        "predicted_placement": predicted_outcome["placement_outlook"],
        "current_best_six_raw_total": f"{current_outcome['best_six_raw_total']:.1f}",
        "predicted_best_six_raw_total": f"{predicted_outcome['best_six_raw_total']:.1f}",
        "current_raw_average": f"{current_outcome['raw_average']:.1f}",
        "predicted_raw_average": f"{predicted_outcome['raw_average']:.1f}",
        "best_two_electives": predicted_outcome["best_two_subject_names"],
        "prediction_payload_json": json.dumps(predicted_rows),
    }
    scenarios_df = pd.concat([scenarios_df, pd.DataFrame([scenario_record])], ignore_index=True)
    save_scenarios_df(scenarios_df)


def build_student_scenario_history(student_id):
    scenarios_df = load_scenarios_df()
    if scenarios_df.empty:
        return scenarios_df

    history_df = scenarios_df[
        scenarios_df["student_id"].fillna("").astype(str).str.strip().eq(str(student_id).strip())
    ].copy()
    if history_df.empty:
        return history_df

    for column in [
        "current_aggregate",
        "predicted_aggregate",
        "current_best_six_raw_total",
        "predicted_best_six_raw_total",
    ]:
        history_df[column] = pd.to_numeric(history_df[column], errors="coerce")

    return history_df.sort_values("created_at", ascending=False).reset_index(drop=True)


def build_scenario_calibration_df(live_df, subject_cols):
    scenarios_df = load_scenarios_df()
    if scenarios_df.empty or live_df.empty or "Student_ID" not in live_df.columns:
        return pd.DataFrame()

    lookup_df = live_df.drop_duplicates(subset=["Student_ID"], keep="last").copy()
    lookup_df["Student_ID"] = lookup_df["Student_ID"].fillna("").astype(str).str.strip()
    calibration_rows = []

    for _, scenario_row in scenarios_df.iterrows():
        student_id = str(scenario_row.get("student_id", "")).strip()
        live_match = lookup_df[lookup_df["Student_ID"] == student_id]
        if live_match.empty:
            continue

        live_row = live_match.iloc[0]
        actual_outcome = compute_student_outcome_details(live_row, subject_cols)
        try:
            predicted_rows = json.loads(scenario_row.get("prediction_payload_json", "[]"))
        except json.JSONDecodeError:
            predicted_rows = []

        predicted_lookup = {
            item.get("subject_col"): pd.to_numeric(pd.Series([item.get("Predicted Score", item.get("predicted_score"))]), errors="coerce").iloc[0]
            for item in predicted_rows
        }
        absolute_errors = []
        for subject_col in subject_cols:
            predicted_score = predicted_lookup.get(subject_col)
            actual_score = pd.to_numeric(pd.Series([live_row.get(subject_col)]), errors="coerce").iloc[0]
            if pd.isna(predicted_score) or pd.isna(actual_score):
                continue
            absolute_errors.append(abs(float(predicted_score) - float(actual_score)))

        calibration_rows.append(
            {
                "Scenario": scenario_row.get("scenario_name", ""),
                "Student_ID": student_id,
                "Student_Name": scenario_row.get("student_name", ""),
                "School_Name": scenario_row.get("school", ""),
                "Saved At": scenario_row.get("created_at", ""),
                "Predicted Aggregate": pd.to_numeric(pd.Series([scenario_row.get("predicted_aggregate")]), errors="coerce").iloc[0],
                "Actual Aggregate": actual_outcome["aggregate"],
                "Predicted Placement": scenario_row.get("predicted_placement", ""),
                "Actual Placement": actual_outcome["placement_outlook"],
                "MAE (Subjects)": round(float(np.mean(absolute_errors)), 2) if absolute_errors else np.nan,
                "Best 2 Electives": scenario_row.get("best_two_electives", ""),
            }
        )

    if not calibration_rows:
        return pd.DataFrame()
    return pd.DataFrame(calibration_rows).sort_values("Saved At", ascending=False).reset_index(drop=True)


def generate_intervention_recommendations(predicted_rows, current_attendance, target_attendance, target_assignment, target_mock):
    recommendations = []
    weak_subjects = [row["Subject"] for row in predicted_rows if row["Predicted Score"] < 45][:3]
    if target_attendance - current_attendance >= 5:
        recommendations.append("Sustain the attendance plan with weekly follow-up and early morning check-ins.")
    if target_assignment >= 60:
        recommendations.append("Protect assignment completion with a daily homework review slot and marking feedback.")
    if target_mock >= 60:
        recommendations.append("Increase mock readiness with targeted revision drills and timed practice papers.")
    if weak_subjects:
        recommendations.append("Prioritize extra support in: " + ", ".join(weak_subjects) + ".")
    if not recommendations:
        recommendations.append("Keep current routines stable and review one weak subject every week.")
    return recommendations


def build_student_counselling_sheet_pdf(student_row, scenario_name, intervention_note, current_outcome, predicted_outcome, predicted_rows, recommendations):
    buffer = io.BytesIO()
    student_name = str(student_row.get("Student_Name", "")).strip()
    school_name = str(student_row.get("School_Name", "")).strip()
    circuit_name = str(student_row.get("Circuit", "")).strip()

    with PdfPages(buffer) as pdf:
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.axis("off")
        body_lines = [
            APP_TITLE,
            "",
            f"Placement Counselling Sheet: {student_name}",
            f"Student ID: {student_row.get('Student_ID', '')}",
            f"School: {school_name}",
            f"Circuit: {circuit_name}",
            f"Scenario: {scenario_name or 'Saved scenario'}",
            "",
            f"Current Aggregate: {current_outcome['aggregate']:.1f}",
            f"Predicted Aggregate: {predicted_outcome['aggregate']:.1f}",
            f"Current Placement: {current_outcome['placement_outlook']}",
            f"Predicted Placement: {predicted_outcome['placement_outlook']}",
            f"Best Two Electives: {predicted_outcome['best_two_subject_names']}",
            f"Best Six Raw Total: {predicted_outcome['best_six_raw_total']:.1f}",
            "",
            "Recommended interventions:",
        ]
        body_lines.extend([f"• {recommendation}" for recommendation in recommendations])
        if intervention_note.strip():
            body_lines.extend(["", "Intervention note:", intervention_note.strip()])
        body_lines.extend(["", BLOOMCORE_FOOTER_TEXT])
        ax.text(0.03, 0.97, "\n".join(body_lines), va="top", ha="left", fontsize=11, family="monospace")
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        subject_table = pd.DataFrame(predicted_rows)[
            ["Subject", "Current Score", "Predicted Score", "Predicted Grade", "Risk Flag"]
        ]
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.axis("off")
        ax.set_title("Subject Forecast Summary", loc="left", fontsize=14, fontweight="bold")
        table = ax.table(
            cellText=subject_table.values,
            colLabels=subject_table.columns,
            cellLoc="left",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    buffer.seek(0)
    return buffer.getvalue()


# ============================================================
# 10. DATA SETUP, UPLOAD UX, REPORTS, AND COMMUNICATION TOOLS
# ============================================================
def generate_professional_excel(district_name, columns):
    df = pd.DataFrame("", index=range(50), columns=columns)
    df["School_Type"] = ""

    try:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Circuits", startrow=2)
            workbook = writer.book
            worksheet = writer.sheets["Circuits"]

            title_format = workbook.add_format({
                "bold": True,
                "font_size": 16,
                "align": "center",
                "valign": "vcenter",
                "font_color": "#1E3A8A",
                "bg_color": "#F3F4F6",
            })
            col_header_format = workbook.add_format({
                "bold": True,
                "bg_color": "#1E3A8A",
                "font_color": "#FFFFFF",
                "border": 1,
            })
            unlocked_format = workbook.add_format({"locked": False})
            warning_format = workbook.add_format({"italic": True, "font_color": "#CC0000", "bold": True})

            heading_text = f"{str(district_name).upper()} - EDU-PULSE CIRCUIT TEMPLATE"
            worksheet.merge_range(0, 0, 1, len(columns) - 1, heading_text, title_format)

            for col_num, col_name in enumerate(columns):
                worksheet.write(2, col_num, col_name, col_header_format)
                col_width = max(len(str(col_name)) + 5, 20)
                worksheet.set_column(col_num, col_num, col_width, unlocked_format)

            worksheet.freeze_panes(3, 0)
            worksheet.protect()
            footer_row = 50 + 4
            worksheet.write(footer_row, 0, f"\u00a9 2026 {BLOOMCORE_FOOTER_TEXT}", warning_format)
            worksheet.write(footer_row + 1, 0, "WARNING: DO NOT RENAME OR MOVE COLUMN HEADERS.", warning_format)

        return output.getvalue()
    except Exception:
        pass

    return build_excel_template(columns, filename="Director_Circuit_Map", sheet_name="Circuits", num_rows=50, school_type_default=False)


def render_circuit_setup(title, description, key_prefix, redirect_to_login=False):
    st.markdown(f"### {title}")
    st.write(description)

    _district_name = load_app_config().get("district_name", "") or "District/Municipal"
    left_col, right_col = st.columns([1, 1])
    with left_col:
        st.download_button(
            "Download Circuits Template (Excel)",
            generate_professional_excel(_district_name, EXPECTED_CIRCUIT_COLUMNS),
            file_name="edupulse_circuits_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_download",
            use_container_width=True,
        )
        st.caption("Use the fixed `School_Name`, `Circuit`, and `School_Type` headers exactly as downloaded. School_Type must be Public or Private.")

    with right_col:
        uploaded_file = st.file_uploader(
            "Upload completed circuits file (Excel or CSV)",
            type=["xlsx", "csv"],
            key=f"{key_prefix}_upload",
        )

    with st.expander("Preview the required circuits columns"):
        st.dataframe(pd.DataFrame({"Required Columns": EXPECTED_CIRCUIT_COLUMNS}), use_container_width=True)

    if uploaded_file is None:
        render_scroll_to_top()
        return

    try:
        _, uploaded_df = read_uploaded_csv(uploaded_file, dtype=str)
    except Exception as exc:
        st.error(f"The uploaded circuits file could not be read: {exc}")
        render_scroll_to_top()
        return

    columns_valid, column_message = validate_circuit_columns(uploaded_df.columns.tolist())
    if not columns_valid:
        st.error(column_message)
        render_scroll_to_top()
        return

    if uploaded_df.empty:
        st.warning("The uploaded circuits file has the correct headers, but it does not contain any school rows yet.")
        render_scroll_to_top()
        return

    if "School_Type" not in uploaded_df.columns:
        uploaded_df["School_Type"] = ""
    cleaned_df = uploaded_df[EXPECTED_CIRCUIT_COLUMNS].copy()
    cleaned_df["School_Name"] = cleaned_df["School_Name"].fillna("").astype(str).str.strip()
    cleaned_df["Circuit"] = cleaned_df["Circuit"].fillna("").astype(str).str.strip()
    original_school_type_series = cleaned_df["School_Type"].copy()
    cleaned_df["School_Type"] = normalize_school_type_series(cleaned_df["School_Type"])

    # Identify rows where School_Type is still blank or invalid after normalization
    invalid_mask = ~cleaned_df["School_Type"].isin(["Public", "Private"])
    invalid_rows = cleaned_df[invalid_mask & (cleaned_df["School_Name"] != "")]
    if not invalid_rows.empty:
        st.warning(f"⚠️ Found {len(invalid_rows)} row(s) with missing or invalid School_Type.")
        st.dataframe(
            invalid_rows[["School_Name", "Circuit", "School_Type"]].reset_index(drop=True),
            use_container_width=True,
        )
        st.error("Please ensure every school is marked as 'Public' or 'Private' and re-upload.")
        render_scroll_to_top()
        return
    cleaned_df = cleaned_df[(cleaned_df["School_Name"] != "") & (cleaned_df["Circuit"] != "")]

    if cleaned_df.empty:
        st.warning("Every uploaded circuits row was blank after cleanup. Please add school and circuit values first.")
        render_scroll_to_top()
        return

    st.success(f"✅ Circuits template validation passed. Detected rows: {len(cleaned_df)} | Circuits: {cleaned_df['Circuit'].nunique()}")
    st.markdown("### 📋 Preview Uploaded Data")
    st.info("Please review all schools below. Scroll the table to verify every school is present before confirming.")

    search_query = st.text_input("Search for a school to verify (e.g., 'Awasive')", key=f"{key_prefix}_search")
    if search_query.strip():
        display_df = cleaned_df[cleaned_df["School_Name"].str.contains(search_query.strip(), case=False, na=False)]
    else:
        display_df = cleaned_df
    st.dataframe(display_df.reset_index(drop=True), use_container_width=True, height=500)

    if st.button("Confirm & Replace Active Circuits Dataset", type="primary", key=f"{key_prefix}_replace"):
        write_dataframe_to_csv(cleaned_df, CIRCUITS_FILE, EXPECTED_CIRCUIT_COLUMNS)
        st.cache_data.clear()
        if redirect_to_login:
            st.session_state["auth_nav"] = "Login"
            st.session_state["auth_flash_message"] = "The active circuits dataset was replaced successfully. You can now log in."
            st.session_state["auth_flash_severity"] = "success"
            st.session_state["pending_setup_role"] = ""
        else:
            st.success("The active circuits dataset was replaced successfully.")
        st.rerun()

    render_scroll_to_top()


def render_headteacher_bulk_upload(school, key_prefix, redirect_to_login=False):
    st.markdown("### 📚 Headteacher Student Data Workspace")
    school = str(school).strip()
    school_profile_lookup = load_school_profile_lookup()
    school_profile = school_profile_lookup.get(school, {}) or school_profile_lookup.get(school.lower(), {}) or school_profile_lookup.get(school.upper(), {})
    school_circuit = school_profile.get("Circuit", "")
    school_type = school_profile.get("School_Type", "Not Specified")
    st.write(
        f"Use the prediction template for continuous-assessment forecasting, or import the official WAEC BECE result PDF for {school} when the final results are released."
    )
    st.caption(
        "Attendance_Percent belongs to the prediction template only. Official WAEC results do not include attendance, so EduPulse will keep using attendance only inside forecasting workflows."
    )
    st.markdown(
        """
        <div class="mobile-sync-card">
            <div class="mobile-sync-title">📱 Mobile Quick Sync</div>
            <p class="mobile-sync-copy">
                On phones, use the prediction template when you want a guided entry sheet, or upload the standard WAEC PDF once the school receives the official BECE release.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("How to work out Attendance_Percent for the prediction template"):
        st.write("Formula: `(Days Present / Total School Days) x 100`")
        st.write("Example: if a student attended 142 out of 150 school days, attendance is 94.7%.")
        st.write("If you do not have the attendance figure ready, leave the field blank and EduPulse will fill it during sync.")

    template_tab, official_tab = st.tabs(["📊 Prediction Template Sync", "📄 Official WAEC Result Import"])

    with template_tab:
        st.markdown("#### Step 1 — Download your school template")
        count_col, dl_col = st.columns([1, 2])
        with count_col:
            num_students = st.number_input(
                "Number of JHS 3 students",
                min_value=1,
                max_value=500,
                value=st.session_state.get(f"{key_prefix}_student_count", 20),
                step=1,
                help="Enter your exact JHS 3 class size. The template will have exactly that many rows — one per student.",
                key=f"{key_prefix}_student_count",
            )
        with dl_col:
            st.write("")
            st.write("")
            try:
                excel_bytes = build_headteacher_student_template_bytes(school, school_circuit, school_type, num_students=int(num_students))
                st.download_button(
                    f"📥 Download {school} Template ({int(num_students)} rows)",
                    excel_bytes,
                    file_name=f"edupulse_{school.lower().replace(' ', '_')}_template.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"{key_prefix}_download_csv",
                    use_container_width=True,
                )
            except Exception as _exc:
                st.warning(f"Could not generate template: {_exc}")
        st.caption(
            f"The template has {int(num_students)} blank rows ready to fill. School, circuit, and school type are added automatically on upload — you only need to fill in Student_Name, Gender, Date_of_Birth, Attendance_Percent, and subject assessment scores. Student IDs are also assigned automatically when you sync."
        )

        st.markdown("#### Step 2 — Upload your completed template")
        left_col, right_col = st.columns([1, 1])
        with left_col:
            uploaded_file = st.file_uploader(
                "Upload completed prediction template (Excel or CSV)",
                type=["xlsx", "csv"],
                key=f"{key_prefix}_upload",
            )
        with right_col:
            pass

        with st.expander("Preview the required prediction-template columns"):
            st.dataframe(pd.DataFrame({"Required Columns": HEADTEACHER_UPLOAD_TEMPLATE_COLUMNS}), use_container_width=True, height=320)

        if uploaded_file is not None:
            try:
                _, uploaded_df = read_uploaded_csv(uploaded_file, dtype={"Student_ID": str})
                columns_valid, column_message = validate_prediction_template_columns(uploaded_df.columns.tolist())
                if not columns_valid:
                    st.error(column_message)
                elif uploaded_df.empty:
                    st.warning("The uploaded prediction template has the correct headers, but it does not contain any student rows yet.")
                elif not school_circuit:
                    st.error("This school is not yet mapped to a circuit. Ask the Director to update the circuits dataset before syncing school data.")
                else:
                    prepared_df = prepare_student_upload_df(uploaded_df)
                    if prepared_df.empty:
                        st.warning("The uploaded prediction template does not contain any completed student rows yet.")
                    else:
                        non_blank_school_values = prepared_df["School_Name"][prepared_df["School_Name"] != ""].unique().tolist()
                        if non_blank_school_values and any(value != school for value in non_blank_school_values):
                            st.error("This upload contains rows for a different school. Headteacher uploads must contain only the registered school.")
                        else:
                            prepared_df["School_Name"] = school
                            prepared_df["Circuit"] = school_circuit
                            prepared_df["School_Type"] = school_type

                            # Automatically filter out all 'ghost rows' (empty student names)
                            # We only keep rows where a Student_Name was actually typed in
                            prepared_df = prepared_df[prepared_df["Student_Name"].fillna("").astype(str).str.strip() != ""]

                            # Check if there is any actual data left after filtering
                            if prepared_df.empty:
                                st.error("No valid student names were found in the uploaded file. Please ensure you typed the names correctly.")
                            else:
                                data_status = get_data_file_status()
                                if data_status["ready"]:
                                    existing_school_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
                                    existing_school_df = prepare_student_upload_df(existing_school_df)
                                    existing_school_df = existing_school_df[
                                        existing_school_df["School_Name"].fillna("").astype(str).str.strip() == school
                                    ].copy()
                                    existing_all_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
                                    existing_all_df = prepare_student_upload_df(existing_all_df)
                                else:
                                    existing_school_df = pd.DataFrame(columns=EXPECTED_DATA_COLUMNS)
                                    existing_all_df = pd.DataFrame(columns=EXPECTED_DATA_COLUMNS)

                                prepared_df, auto_assigned_count = assign_missing_school_student_ids(prepared_df, school, existing_school_df)
                                prepared_df, auto_filled_attendance_count, attendance_fill_value = autofill_missing_attendance(
                                    prepared_df,
                                    existing_school_df=existing_school_df,
                                    existing_all_df=existing_all_df,
                                )
                                prepared_df = populate_provisional_final_scores(prepared_df)

                                st.success(f"✅ Prediction template validation passed. Detected **{len(prepared_df)}** student rows for **{school}**.")

                                info_parts = [f"Circuit: {school_circuit}", f"School Type: {school_type}"]
                                if auto_assigned_count > 0:
                                    info_parts.append(f"Auto-assigned Student IDs: {auto_assigned_count}")
                                if auto_filled_attendance_count > 0 and attendance_fill_value is not None:
                                    info_parts.append(f"Auto-filled Attendance: {auto_filled_attendance_count} rows at {attendance_fill_value:.1f}%")
                                st.info(" | ".join(info_parts))

                                st.markdown("### 👁️ Review Uploaded Data")
                                st.write("Check each student row carefully before submitting. Use the search box to confirm a specific student is present.")

                                student_search = st.text_input(
                                    "🔍 Search by student name",
                                    placeholder="e.g. Kofi Mensah",
                                    key=f"{key_prefix}_student_search",
                                )
                                if student_search.strip():
                                    display_df = prepared_df[
                                        prepared_df["Student_Name"].fillna("").astype(str).str.contains(student_search.strip(), case=False, na=False)
                                    ]
                                    if display_df.empty:
                                        st.warning(f"No student matching '{student_search}' found in this upload.")
                                    else:
                                        st.success(f"Found {len(display_df)} matching row(s).")
                                else:
                                    display_df = prepared_df

                                st.dataframe(display_df.reset_index(drop=True), use_container_width=True, height=500)

                                with st.expander("📊 Score preview (first 10 rows)"):
                                    score_cols = ["Student_Name"] + [c for c in prepared_df.columns if any(s in c for s in ["Assignments", "Term1", "Term2", "Mock"])]
                                    st.dataframe(prepared_df[score_cols].head(10), use_container_width=True)

                                st.divider()
                                if st.button("✅ Confirm & Sync to Director Dashboard", type="primary", key=f"{key_prefix}_sync"):
                                    try:
                                        sync_student_upload(
                                            prepared_df,
                                            school,
                                            school_circuit,
                                            redirect_to_login=redirect_to_login,
                                            source_label="prediction template",
                                        )
                                        st.rerun()
                                    except ValueError as exc:
                                        st.error(str(exc))
            except Exception as exc:
                st.error(f"The uploaded prediction template could not be processed: {exc}")

    with official_tab:
        st.write("Upload the standard WAEC BECE result file for this school. EduPulse reads the WAEC fields directly from the PDF: Index Number, Name, Gender, DOB, and Results.")
        with st.expander("Accepted official result fields"):
            official_field_rows = pd.DataFrame(
                [
                    {"WAEC Field": "INDEX NUMBER", "Used For": "Student_ID"},
                    {"WAEC Field": "NAME", "Used For": "Student_Name"},
                    {"WAEC Field": "GENDER", "Used For": "Gender"},
                    {"WAEC Field": "DOB", "Used For": "Date_of_Birth"},
                    {"WAEC Field": "RESULTS", "Used For": "Official_Results_Raw plus subject score extraction"},
                ]
            )
            st.dataframe(official_field_rows, use_container_width=True)

        official_file = st.file_uploader(
            "Upload official WAEC result file",
            type=["pdf", "csv"],
            key=f"{key_prefix}_official_upload",
        )

        if official_file is not None:
            if not school_circuit:
                st.error("This school is not yet mapped to a circuit. Ask the Director to update the circuits CSV before importing official results.")
            else:
                try:
                    if official_file.name.lower().endswith(".pdf"):
                        prepared_official_df, matched_fields, parsed_school, parsed_circuit, parsed_school_type = prepare_official_pdf_import(
                            official_file.getvalue(),
                            official_file.name,
                            expected_school=school,
                            forced_school=school,
                        )
                    else:
                        _, official_df = read_uploaded_csv(official_file, dtype={"Student_ID": str})
                        prepared_official_df, matched_fields = prepare_official_results_import(official_df, school, school_circuit)
                        parsed_school = school
                        parsed_circuit = school_circuit
                        parsed_school_type = school_type

                    data_status = get_data_file_status()
                    if data_status["ready"]:
                        existing_school_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
                        existing_school_df = prepare_student_upload_df(existing_school_df)
                        existing_school_df = existing_school_df[
                            existing_school_df["School_Name"].fillna("").astype(str).str.strip() == school
                        ].copy()
                        existing_all_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
                        existing_all_df = prepare_student_upload_df(existing_all_df)
                    else:
                        existing_school_df = pd.DataFrame(columns=EXPECTED_DATA_COLUMNS)
                        existing_all_df = pd.DataFrame(columns=EXPECTED_DATA_COLUMNS)

                    prepared_official_df["School_Name"] = school
                    prepared_official_df["Circuit"] = school_circuit
                    prepared_official_df["School_Type"] = school_type
                    prepared_official_df, auto_assigned_count = assign_missing_school_student_ids(
                        prepared_official_df,
                        school,
                        existing_school_df,
                    )
                    prepared_official_df, auto_filled_attendance_count, attendance_fill_value = autofill_missing_attendance(
                        prepared_official_df,
                        existing_school_df=existing_school_df,
                        existing_all_df=existing_all_df,
                    )

                    st.success("Official WAEC result import mapped successfully.")
                    st.write(
                        f"Parsed school: {parsed_school} | Circuit: {parsed_circuit} | School Type: {parsed_school_type} | Matched fields: {', '.join(matched_fields)}"
                        + (f" | Auto-assigned Student_IDs: {auto_assigned_count}" if auto_assigned_count else "")
                        + (
                            f" | Auto-filled Attendance rows: {auto_filled_attendance_count} at {attendance_fill_value:.1f}%"
                            if auto_filled_attendance_count and attendance_fill_value is not None
                            else ""
                        )
                    )
                    preview_cols = [column for column in ["Student_ID", "Student_Name", "Gender", "Date_of_Birth", "School_Name", "Circuit", "School_Type"] if column in prepared_official_df.columns]
                    st.dataframe(prepared_official_df[preview_cols].head(10), use_container_width=True)

                    if st.button("Sync Official WAEC Results to Director Dashboard", type="primary", key=f"{key_prefix}_official_sync"):
                        try:
                            sync_student_upload(
                                prepared_official_df,
                                school,
                                school_circuit,
                                redirect_to_login=redirect_to_login,
                                source_label="official WAEC result upload",
                            )
                            st.rerun()
                        except ValueError as exc:
                            st.error(str(exc))
                except Exception as exc:
                    st.error(f"The official WAEC file could not be processed: {exc}")

    render_scroll_to_top()


def render_director_official_results_intake(key_prefix):
    st.markdown("### 📑 Official WAEC Results Intake")
    st.write("Directors can replace school rows from the standard WAEC BECE result PDFs either one school at a time or in a multi-school bulk upload.")
    with st.expander("Accepted official result fields"):
        st.dataframe(
            pd.DataFrame(
                [
                    {"WAEC Field": "INDEX NUMBER", "Used For": "Student_ID"},
                    {"WAEC Field": "NAME", "Used For": "Student_Name"},
                    {"WAEC Field": "GENDER", "Used For": "Gender"},
                    {"WAEC Field": "DOB", "Used For": "Date_of_Birth"},
                    {"WAEC Field": "RESULTS", "Used For": "Subject extraction and archival raw text"},
                ]
            ),
            use_container_width=True,
        )

    single_tab, bulk_tab = st.tabs(["Single School Upload", "Bulk School Upload"])

    with single_tab:
        single_file = st.file_uploader(
            "Upload one official WAEC result PDF",
            type=["pdf"],
            key=f"{key_prefix}_single_pdf",
        )
        if single_file is not None:
            try:
                prepared_df, matched_fields, parsed_school, parsed_circuit, parsed_school_type = prepare_official_pdf_import(
                    single_file.getvalue(),
                    single_file.name,
                )
                preview_cols = [column for column in ["Student_ID", "Student_Name", "Gender", "Date_of_Birth", "School_Name", "Circuit", "School_Type"] if column in prepared_df.columns]
                st.success("Official WAEC PDF parsed successfully.")
                st.write(f"School: {parsed_school} | Circuit: {parsed_circuit} | School Type: {parsed_school_type} | Fields: {', '.join(matched_fields)}")
                st.dataframe(prepared_df[preview_cols].head(12), use_container_width=True)
                if st.button("Sync This School's Official Results", type="primary", key=f"{key_prefix}_single_sync"):
                    sync_student_upload(prepared_df, parsed_school, parsed_circuit, source_label="official WAEC PDF")
                    st.rerun()
            except Exception as exc:
                st.error(f"The uploaded WAEC PDF could not be processed: {exc}")

    with bulk_tab:
        bulk_files = st.file_uploader(
            "Upload multiple official WAEC result PDFs",
            type=["pdf"],
            accept_multiple_files=True,
            key=f"{key_prefix}_bulk_pdf",
        )
        if bulk_files:
            prepared_frames = []
            summary_rows = []
            duplicate_school_names = []
            seen_schools = set()
            for uploaded_file in bulk_files:
                try:
                    prepared_df, matched_fields, parsed_school, parsed_circuit, parsed_school_type = prepare_official_pdf_import(
                        uploaded_file.getvalue(),
                        uploaded_file.name,
                    )
                    school_key = parsed_school.strip().casefold()
                    if school_key in seen_schools:
                        duplicate_school_names.append(parsed_school)
                        continue
                    seen_schools.add(school_key)
                    prepared_frames.append(prepared_df)
                    summary_rows.append(
                        {
                            "School_Name": parsed_school,
                            "Circuit": parsed_circuit,
                            "School_Type": parsed_school_type,
                            "Students Parsed": len(prepared_df),
                            "Matched Fields": ", ".join(matched_fields),
                        }
                    )
                except Exception as exc:
                    summary_rows.append(
                        {
                            "School_Name": uploaded_file.name,
                            "Circuit": "",
                            "School_Type": "",
                            "Students Parsed": 0,
                            "Matched Fields": f"Error: {exc}",
                        }
                    )

            summary_df = pd.DataFrame(summary_rows)
            if not summary_df.empty:
                st.dataframe(summary_df, use_container_width=True)
            if duplicate_school_names:
                st.warning("Duplicate school PDFs were skipped in this batch: " + ", ".join(sorted(set(duplicate_school_names))))
            valid_frames = [frame for frame in prepared_frames if not frame.empty]
            if valid_frames:
                combined_df = pd.concat(valid_frames, ignore_index=True)
                if st.button("Sync Bulk Official Results", type="primary", key=f"{key_prefix}_bulk_sync"):
                    sync_multi_school_upload(combined_df, source_label="bulk official WAEC PDF import")
                    st.rerun()
            else:
                st.info("Upload at least one readable WAEC PDF to prepare a bulk sync.")



def render_director_data_setup(data_status, standalone=False, key_prefix="director_data_setup"):

    scope_label = get_scope_label()
    active_config = load_app_config()
    circuit_status = get_circuit_file_status()
    current_df, _ = load_data(show_errors=False) if data_status["ready"] else (pd.DataFrame(), [])
    school_sync_df = build_school_sync_status_df(current_df)
    if standalone:
        st.title("🗂️ Director Data Setup")
        st.write(
            f"Before EduPulse can open the analytics workspace for {scope_label}, the Director should upload the official circuits CSV. Headteachers will then download school-specific prediction templates, and either Heads or the Director can later sync the official WAEC result PDFs for final analysis."
        )
    else:
        st.markdown("### 🗂️ Data Setup & Replacement")
        st.write(
            f"Directors manage the circuits map and may also bulk-load official WAEC BECE result PDFs. Headteachers handle prediction-template uploads and their own school-level official-result uploads."
        )

    render_status_message(circuit_status)
    if data_status["ready"]:
        render_status_message(data_status)
    else:
        st.info("Student uploads will begin appearing here as Headteachers sync their school CSV files.")
    if active_config["headteacher_security_key"]:
        st.info(
            f"Headteacher registration key for {active_config['district_name'] or scope_label}: {active_config['headteacher_security_key']}"
        )

    render_circuit_setup(
        title="🗺️ Circuits CSV Setup",
        description="Download the official circuits template, fill each school with its circuit, and upload it to replace the sample circuits dataset.",
        key_prefix=f"{key_prefix}_circuits",
    )

    st.markdown("---")
    st.markdown("### 🏫 Headteacher Upload Coverage")
    st.write("Each school appears below with its current sync status. Schools that have not uploaded yet remain at zero until their Headteacher syncs the student CSV.")
    if school_sync_df.empty:
        st.warning("No schools are available yet. Upload the circuits CSV first to begin school onboarding.")
    else:
        coverage_cols = st.columns(3)
        with coverage_cols[0]:
            st.metric("Mapped Schools", len(school_sync_df))
        with coverage_cols[1]:
            st.metric("Schools Synced", int((school_sync_df["Students Uploaded"] > 0).sum()))
        with coverage_cols[2]:
            st.metric("Awaiting Upload", int((school_sync_df["Students Uploaded"] == 0).sum()))
        st.dataframe(school_sync_df, use_container_width=True)

    render_scroll_to_top()


def render_data_waiting_screen(data_status):
    scope_label = get_scope_label()
    st.title("🕒 Waiting for School Data Sync")
    st.write(
        f"The analytics workspace for {scope_label} cannot open yet for this Headteacher account. Upload the school CSV first so the dashboard can open with your school's student records."
    )
    render_status_message(data_status)
    render_scroll_to_top()


def render_circuit_waiting_screen(status):
    scope_label = get_scope_label()
    st.title("🗺️ Waiting for Circuits Setup")
    st.write(
        f"The circuits map for {scope_label} is not ready yet. Ask the Director to upload the official circuits CSV before Headteacher onboarding can continue."
    )
    render_status_message(status)
    render_scroll_to_top()


def ordinal_rank(position):
    if 10 <= position % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(position % 10, "th")
    return f"{position}{suffix}"


def build_scope_report_tables(scope_df, subject_cols, scope_label, school_sync_df=None):
    agg_df = build_aggregate_dataframe(scope_df, subject_cols)
    tables = {}

    summary_rows = [
        {"Metric": "Scope", "Value": scope_label},
        {"Metric": "Students", "Value": int(len(scope_df))},
        {"Metric": "Schools", "Value": int(scope_df["School_Name"].nunique()) if "School_Name" in scope_df.columns else 0},
        {"Metric": "Circuits", "Value": int(scope_df["Circuit"].nunique()) if "Circuit" in scope_df.columns else 0},
        {"Metric": "Average Aggregate", "Value": round(float(agg_df["Aggregate"].mean()), 2) if not agg_df.empty else np.nan},
        {"Metric": "Pass Rate %", "Value": round(float(agg_df["Passed"].mean() * 100), 2) if not agg_df.empty else np.nan},
    ]
    tables["summary_metrics"] = pd.DataFrame(summary_rows)

    if not agg_df.empty:
        tables["student_placement_summary"] = agg_df.sort_values(["Aggregate", "Student_Name"], ascending=[True, True]).reset_index(drop=True)
        tables["school_placement_summary"] = (
            agg_df.groupby(["School_Name", "Placement_Category"]).size().unstack(fill_value=0).reset_index()
        )
        tables["circuit_placement_summary"] = (
            agg_df.groupby(["Circuit", "Placement_Category"]).size().unstack(fill_value=0).reset_index()
        )
    else:
        tables["student_placement_summary"] = pd.DataFrame()
        tables["school_placement_summary"] = pd.DataFrame()
        tables["circuit_placement_summary"] = pd.DataFrame()

    if scope_df.empty:
        tables["subject_summary"] = pd.DataFrame(columns=["Subject", "Average Score"])
    else:
        tables["subject_summary"] = pd.DataFrame(
            {
                "Subject": [format_subject_name(subject) for subject in subject_cols],
                "Average Score": [round(float(pd.to_numeric(scope_df[subject], errors="coerce").mean()), 2) for subject in subject_cols],
            }
        ).sort_values("Average Score", ascending=True)

    if not scope_df.empty and subject_cols and "School_Name" in scope_df.columns:
        # Convert subject columns to numeric before groupby
        numeric_df = scope_df.copy()
        for subject in subject_cols:
            numeric_df[subject] = pd.to_numeric(numeric_df[subject], errors="coerce")
        school_rankings = (
            numeric_df.groupby("School_Name")[subject_cols]
            .mean()
            .mean(axis=1)
            .reset_index(name="Actual BECE Average")
            .sort_values(["Actual BECE Average", "School_Name"], ascending=[False, True])
            .reset_index(drop=True)
        )
        school_rankings["Position"] = school_rankings.index + 1
        tables["school_rankings"] = school_rankings[
            ["Position", "School_Name", "Actual BECE Average"]
        ].copy()
        tables["school_rankings"]["Actual BECE Average"] = tables["school_rankings"]["Actual BECE Average"].round(2)
    else:
        tables["school_rankings"] = pd.DataFrame(columns=["Position", "School_Name", "Actual BECE Average"])

    if not agg_df.empty and "Gender" in agg_df.columns:
        gender_summary = (
            agg_df[agg_df["Gender"] != "Unspecified"]
            .groupby("Gender", as_index=False)
            .agg(
                Average_Aggregate=("Aggregate", "mean"),
                Average_Best_Six_Raw_Total=("Best_Six_Raw_Total", "mean"),
            )
        )
        gender_summary["Average_Aggregate"] = gender_summary["Average_Aggregate"].round(2)
        gender_summary["Average_Best_Six_Raw_Total"] = gender_summary["Average_Best_Six_Raw_Total"].round(2)
        if not scope_df.empty and subject_cols:
            gender_raw = (
                pd.concat(
                    [
                        scope_df[["Gender"]].rename(columns={"Gender": "Gender"}),
                        scope_df[subject_cols]
                        .apply(pd.to_numeric, errors="coerce")
                        .mean(axis=1)
                        .rename("Average_Subject_Score"),
                    ],
                    axis=1,
                )
                .assign(Gender=lambda df_: df_["Gender"].apply(normalize_gender))
            )
            gender_raw = gender_raw[gender_raw["Gender"] != "Unspecified"]
            gender_subject_summary = gender_raw.groupby("Gender", as_index=False)["Average_Subject_Score"].mean()
            gender_summary = gender_summary.merge(gender_subject_summary, on="Gender", how="left")
            gender_summary["Average_Subject_Score"] = gender_summary["Average_Subject_Score"].round(2)
        tables["gender_summary"] = gender_summary
    else:
        tables["gender_summary"] = pd.DataFrame(columns=["Gender", "Average_Aggregate", "Average_Best_Six_Raw_Total", "Average_Subject_Score"])

    tables["audit_export"] = scope_df.drop(columns=["Search_Label"], errors="ignore").copy()

    if school_sync_df is not None:
        tables["school_sync_coverage"] = school_sync_df.copy()

    return tables


def build_briefing_zip_bytes(scope_df, subject_cols, scope_label, school_sync_df=None):
    tables = build_scope_report_tables(scope_df, subject_cols, scope_label, school_sync_df=school_sync_df)
    buffer = io.BytesIO()
    slug = slugify_name(scope_label).lower() or "edupulse"

    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zip_handle:
        for table_name, table_df in tables.items():
            csv_bytes = table_df.to_csv(index=False).encode("utf-8")
            zip_handle.writestr(f"{slug}_{table_name}.csv", csv_bytes)

    buffer.seek(0)
    return buffer.getvalue()


def build_briefing_pdf_bytes(scope_df, subject_cols, scope_label, school_sync_df=None):
    tables = build_scope_report_tables(scope_df, subject_cols, scope_label, school_sync_df=school_sync_df)
    agg_df = build_aggregate_dataframe(scope_df, subject_cols)
    buffer = io.BytesIO()
    downloaded_at = datetime.now().strftime("%d %B %Y, %H:%M")
    report_year = datetime.now().year
    school_rankings = tables.get("school_rankings", pd.DataFrame())
    gender_summary = tables.get("gender_summary", pd.DataFrame())
    calibration_map = get_live_calibration_map()

    def add_page_footer(ax):
        ax.text(0.02, 0.03, f"Downloaded: {downloaded_at}", fontsize=8.5, color="#475569", transform=ax.transAxes)
        ax.text(0.98, 0.03, BLOOMCORE_FOOTER_TEXT, fontsize=8.5, color="#0f172a", ha="right", transform=ax.transAxes)

    def add_header(ax, title, insight):
        ax.text(0.03, 0.94, title, fontsize=16, fontweight="bold", color="#0f172a", transform=ax.transAxes)
        ax.text(0.03, 0.89, insight, fontsize=10.5, color="#334155", style="italic", transform=ax.transAxes)

    with PdfPages(buffer) as pdf:
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.axis("off")
        ax.text(0.5, 0.84, "GHANA EDUCATION SERVICE", ha="center", fontsize=20, fontweight="bold", color="#0f172a", transform=ax.transAxes)
        ax.text(0.5, 0.77, f"{scope_label} District Directorate", ha="center", fontsize=14, color="#1d4ed8", transform=ax.transAxes)
        ax.text(
            0.5,
            0.62,
            f"STRATEGIC BECE PERFORMANCE BRIEFING: {report_year} TRANSITION ANALYSIS",
            ha="center",
            fontsize=22,
            fontweight="bold",
            color="#0f172a",
            wrap=True,
            transform=ax.transAxes,
        )
        ax.text(
            0.5,
            0.48,
            "Data-Driven Insights for Academic Excellence and Resource Allocation",
            ha="center",
            fontsize=13,
            color="#475569",
            style="italic",
            transform=ax.transAxes,
        )
        ax.text(
            0.5,
            0.26,
            "Prepared from live district data, calibrated school-level forecasting, and BECE transition intelligence.",
            ha="center",
            fontsize=11,
            color="#334155",
            transform=ax.transAxes,
        )
        add_page_footer(ax)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.axis("off")
        add_header(
            ax,
            "DISTRICT PERFORMANCE AT A GLANCE: KEY INDICATORS",
            "Key takeaway: the district pulse combines student volume, aggregate quality, pass-rate movement, and raw-score competitiveness.",
        )
        summary_table = tables["summary_metrics"].copy()
        top_school = school_rankings.iloc[0]["School_Name"] if not school_rankings.empty else "Awaiting data"
        top_school_average = school_rankings.iloc[0]["Actual BECE Average"] if not school_rankings.empty else np.nan
        extra_metrics = pd.DataFrame(
            [
                {"Metric": "Top Ranked School", "Value": top_school},
                {"Metric": "Top School Actual BECE Average", "Value": round(float(top_school_average), 2) if pd.notna(top_school_average) else np.nan},
                {"Metric": "Category A Tie-Break Rule", "Value": "Best 6 raw score used for edge-case competitiveness"},
            ]
        )
        summary_table = pd.concat([summary_table, extra_metrics], ignore_index=True)
        summary_render = ax.table(
            cellText=summary_table.values,
            colLabels=summary_table.columns,
            cellLoc="left",
            bbox=[0.03, 0.18, 0.94, 0.6],
        )
        summary_render.auto_set_font_size(False)
        summary_render.set_fontsize(10)
        summary_render.scale(1, 1.5)
        add_page_footer(ax)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.axis("off")
        add_header(
            ax,
            "SCHOOL RANKING: ACTUAL BECE AVERAGE PERFORMANCE",
            "Key takeaway: ranking by actual BECE average reveals where strong instruction is already happening and where peer-learning support should begin.",
        )
        ranking_table = school_rankings.copy()
        if not ranking_table.empty:
            ranking_table["Rank"] = ranking_table["Position"].apply(lambda value: ordinal_rank(int(value)))
            ranking_table = ranking_table[["Rank", "School_Name", "Actual BECE Average"]]
            render_table = ax.table(
                cellText=ranking_table.values,
                colLabels=["Position", "School", "Actual BECE Average"],
                cellLoc="left",
                bbox=[0.03, 0.12, 0.94, 0.72],
            )
            render_table.auto_set_font_size(False)
            render_table.set_fontsize(9)
            render_table.scale(1, 1.4)
        else:
            ax.text(0.03, 0.7, "No school ranking data is available yet.", fontsize=12, color="#475569", transform=ax.transAxes)
        add_page_footer(ax)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        subject_summary = tables["subject_summary"].dropna()
        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        if subject_summary.empty:
            ax.axis("off")
            add_header(
                ax,
                "CURRICULUM STRENGTHS & CRITICAL LEARNING GAPS",
                "Key takeaway: subject-level averages highlight where curriculum support, teacher coaching, and materials should be concentrated first.",
            )
            ax.text(0.03, 0.7, "No subject-performance data is available yet.", fontsize=12, color="#475569", transform=ax.transAxes)
        else:
            top_subject = subject_summary.sort_values("Average Score", ascending=False).iloc[0]
            weak_subject = subject_summary.sort_values("Average Score", ascending=True).iloc[0]
            ax.barh(subject_summary["Subject"], subject_summary["Average Score"], color="#2563eb")
            ax.set_title("CURRICULUM STRENGTHS & CRITICAL LEARNING GAPS", loc="left", fontsize=16, fontweight="bold")
            ax.text(
                0.0,
                1.02,
                f"Key takeaway: {top_subject['Subject']} is the strongest district subject, while {weak_subject['Subject']} needs the fastest learning recovery response.",
                fontsize=10.5,
                color="#334155",
                style="italic",
                transform=ax.transAxes,
            )
            ax.set_xlabel("Average Score")
            ax.grid(axis="x", linestyle="--", alpha=0.3)
        add_page_footer(ax)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(11.69, 8.27))
        placement_summary = agg_df["Placement_Category"].value_counts().reindex(PLACEMENT_ORDER, fill_value=0) if not agg_df.empty else pd.Series(dtype=float)
        ax_left.bar(placement_summary.index, placement_summary.values, color=["#1d4ed8", "#16a34a", "#f59e0b", "#dc2626"])
        ax_left.set_title("SENIOR HIGH SCHOOL (SHS) TRANSITION & PLACEMENT OUTLOOK", loc="left", fontsize=14, fontweight="bold")
        ax_left.text(
            0.0,
            1.02,
            "Key takeaway: placement outlook should be read together with raw-score tie-break strength for Category A edge cases.",
            fontsize=10,
            color="#334155",
            style="italic",
            transform=ax_left.transAxes,
        )
        ax_left.set_ylabel("Student Count")

        if gender_summary.empty:
            ax_right.axis("off")
            ax_right.text(0.05, 0.85, "Girls/Boys comparison is not available yet.", fontsize=12, color="#475569", transform=ax_right.transAxes)
        else:
            ax_right.bar(gender_summary["Gender"], gender_summary["Average_Subject_Score"].fillna(0), color=["#e11d48", "#2563eb"])
            ax_right.set_title("GIRLS VS BOYS: AVERAGE ACTUAL PERFORMANCE", loc="left", fontsize=14, fontweight="bold")
            ax_right.text(
                0.0,
                1.02,
                "Key takeaway: gender parity monitoring helps the Directorate target support where performance gaps are emerging.",
                fontsize=10,
                color="#334155",
                style="italic",
                transform=ax_right.transAxes,
            )
            ax_right.set_ylabel("Average Subject Score")
        fig.text(0.02, 0.03, f"Downloaded: {downloaded_at}", fontsize=8.5, color="#475569")
        fig.text(0.98, 0.03, BLOOMCORE_FOOTER_TEXT, fontsize=8.5, color="#0f172a", ha="right")
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.axis("off")
        add_header(
            ax,
            "TARGETED INTERVENTION PRIORITY LIST: STUDENTS REQUIRING URGENT SUPPORT",
            "Key takeaway: this list converts risk into action by identifying the students who need the fastest instructional and pastoral response.",
        )
        if not agg_df.empty:
            risk_df = agg_df.sort_values(["Aggregate", "Best_Six_Raw_Total"], ascending=[False, True]).head(15).copy()
            risk_df = risk_df[["Student_Name", "School_Name", "Aggregate", "Best_Six_Raw_Total", "Placement_Category"]]
            risk_df.columns = ["Student", "School", "Aggregate", "Best 6 Raw Total", "Placement"]
            risk_table = ax.table(
                cellText=risk_df.values,
                colLabels=risk_df.columns,
                cellLoc="left",
                bbox=[0.03, 0.12, 0.94, 0.72],
            )
            risk_table.auto_set_font_size(False)
            risk_table.set_fontsize(9)
            risk_table.scale(1, 1.4)
        else:
            ax.text(0.03, 0.7, "No intervention list is available yet.", fontsize=12, color="#475569", transform=ax.transAxes)
        add_page_footer(ax)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(11.69, 8.27))
        ax.axis("off")
        add_header(
            ax,
            "METHODOLOGY: PREDICTIVE ANALYTICS BY BLOOMCORE TECHNOLOGIES",
            "Key takeaway: the forecasting engine blends pretrained subject models, 30/70 BECE weighting logic, raw-score tie-break context, and school-bias calibration.",
        )
        methodology_lines = [
            "1. Subject-specific machine-learning models are loaded from bece_models.joblib.",
            "2. Continuous Assessment (30%) and external-exam proxy logic (70%) remain visible in the forecast tables.",
            "3. Category A predictions include a raw-score tie-break strength signal for competitive edge cases.",
            f"4. Historical school calibration map loaded: {'Yes' if calibration_map else 'No'}",
            "5. School calibration adjusts predictions where mock marking is historically stricter or easier than final BECE outcomes.",
            "6. Best-two electives are selected dynamically using grade first and raw score as the tie-breaker.",
        ]
        ax.text(
            0.03,
            0.82,
            "\n\n".join(textwrap.fill(line, width=90) for line in methodology_lines),
            va="top",
            ha="left",
            fontsize=11,
            color="#0f172a",
            transform=ax.transAxes,
        )
        add_page_footer(ax)
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    buffer.seek(0)
    return buffer.getvalue()


def send_email_alert(config, recipient_email, subject, body):
    port = int(str(config.get("smtp_port", "587")).strip() or "587")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.get("smtp_sender_email", "")
    message["To"] = recipient_email
    message.set_content(body)

    with smtplib.SMTP(config.get("smtp_host", ""), port, timeout=20) as smtp_server:
        use_tls = str(config.get("smtp_use_tls", "true")).strip().lower() != "false"
        if use_tls:
            smtp_server.starttls()
        username = str(config.get("smtp_username", "")).strip()
        password = str(config.get("smtp_password", "")).strip()
        if username:
            smtp_server.login(username, password)
        smtp_server.send_message(message)


def build_mailto_link(recipient_email, subject, body):
    query = urllib.parse.urlencode({"subject": subject, "body": body})
    return f"mailto:{recipient_email}?{query}"


def build_whatsapp_link(number, body):
    clean_number = re.sub(r"[^0-9]", "", str(number))
    return f"https://wa.me/{clean_number}?text={urllib.parse.quote(body)}"


def build_sms_link(number, body):
    return f"sms:{re.sub(r'[^0-9+]', '', str(number))}?body={urllib.parse.quote(body)}"


def render_smtp_settings(key_prefix):
    active_config = load_app_config()
    with st.expander("Email Delivery Settings"):
        with st.form(f"{key_prefix}_smtp_form"):
            smtp_host = st.text_input("SMTP Host", value=active_config.get("smtp_host", ""))
            smtp_port = st.text_input("SMTP Port", value=active_config.get("smtp_port", "587"))
            smtp_username = st.text_input("SMTP Username", value=active_config.get("smtp_username", ""))
            smtp_password = st.text_input("SMTP Password", value=active_config.get("smtp_password", ""), type="password")
            smtp_sender_email = st.text_input("Sender Email", value=active_config.get("smtp_sender_email", ""))
            smtp_use_tls = st.checkbox(
                "Use TLS",
                value=str(active_config.get("smtp_use_tls", "true")).strip().lower() != "false",
            )
            if st.form_submit_button("Save Email Settings"):
                active_config.update(
                    {
                        "smtp_host": smtp_host,
                        "smtp_port": smtp_port,
                        "smtp_username": smtp_username,
                        "smtp_password": smtp_password,
                        "smtp_sender_email": smtp_sender_email,
                        "smtp_use_tls": "true" if smtp_use_tls else "false",
                    }
                )
                save_app_config(active_config)
                st.success("Email delivery settings saved.")


def render_headteacher_contact_form(school, key_prefix):
    contacts_df = load_contacts_df()
    district = st.session_state.get("user_district", "") or get_scope_label()
    current_contact = contacts_df[
        contacts_df["district"].fillna("").astype(str).str.strip().eq(district)
        & contacts_df["school"].fillna("").astype(str).str.strip().eq(school)
    ].tail(1)
    current_values = current_contact.iloc[0].to_dict() if not current_contact.empty else {}

    st.markdown("### 📞 School Contact Profile")
    st.write("Save the email, phone, or WhatsApp contact that the Director should use for alerts and follow-up.")
    with st.form(f"{key_prefix}_contact_form"):
        contact_name = st.text_input("Contact Name", value=current_values.get("contact_name", ""))
        role_name = st.text_input("Role", value=current_values.get("role", "Headteacher"))
        email = st.text_input("Email", value=current_values.get("email", ""))
        phone = st.text_input("Phone Number", value=current_values.get("phone", ""))
        whatsapp_number = st.text_input("WhatsApp Number", value=current_values.get("whatsapp_number", ""))
        preferred_channel = st.selectbox(
            "Preferred Alert Channel",
            ["Email", "WhatsApp", "SMS"],
            index=["Email", "WhatsApp", "SMS"].index(current_values.get("preferred_channel", "Email"))
            if current_values.get("preferred_channel", "Email") in ["Email", "WhatsApp", "SMS"]
            else 0,
        )
        if st.form_submit_button("Save Contact Profile"):
            upsert_contact(
                {
                    "district": district,
                    "school": school,
                    "contact_name": contact_name.strip(),
                    "role": role_name.strip() or "Headteacher",
                    "email": email.strip(),
                    "phone": phone.strip(),
                    "whatsapp_number": whatsapp_number.strip(),
                    "preferred_channel": preferred_channel,
                    "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                }
            )
            st.success("School contact profile saved for Director alerts.")


def build_communication_queue(df, subject_cols):
    queue_rows = []
    scope_label = get_scope_label()
    contacts_df = load_contacts_df()
    school_sync_df = build_school_sync_status_df(df)
    agg_df = build_aggregate_dataframe(df, subject_cols)

    for _, school_row in school_sync_df.iterrows():
        if int(school_row.get("Students Uploaded", 0)) > 0:
            continue
        school_name = school_row.get("School_Name", "")
        contact_match = contacts_df[
            contacts_df["district"].fillna("").astype(str).str.strip().eq(scope_label)
            & contacts_df["school"].fillna("").astype(str).str.strip().eq(str(school_name).strip())
        ].tail(1)
        contact_record = contact_match.iloc[0].to_dict() if not contact_match.empty else {}
        message = (
            f"EduPulse reminder: {school_name} has not synced its student dataset yet for {scope_label}. "
            "Please upload the school CSV so the district dashboard stays complete."
        )
        queue_rows.append(
            {
                "Alert Type": "Missing Upload",
                "School_Name": school_name,
                "Circuit": school_row.get("Circuit", ""),
                "Recipient": contact_record.get("contact_name", ""),
                "Email": contact_record.get("email", ""),
                "Phone": contact_record.get("phone", ""),
                "WhatsApp": contact_record.get("whatsapp_number", ""),
                "Preferred Channel": contact_record.get("preferred_channel", "Email"),
                "Subject": f"EduPulse Upload Reminder: {school_name}",
                "Message": message,
            }
        )

    if not agg_df.empty:
        risk_df = agg_df[agg_df["Placement_Category"] == "Category D/SP"].copy().head(20)
        for _, risk_row in risk_df.iterrows():
            school_name = risk_row.get("School_Name", "")
            contact_match = contacts_df[
                contacts_df["district"].fillna("").astype(str).str.strip().eq(scope_label)
                & contacts_df["school"].fillna("").astype(str).str.strip().eq(str(school_name).strip())
            ].tail(1)
            contact_record = contact_match.iloc[0].to_dict() if not contact_match.empty else {}
            message = (
                f"EduPulse alert: {risk_row.get('Student_Name', '')} from {school_name} is currently projected in "
                f"{risk_row.get('Placement_Category', 'Category D/SP')} with aggregate {risk_row.get('Aggregate', '')}. "
                "Please review attendance, assignments, and mock preparation."
            )
            queue_rows.append(
                {
                    "Alert Type": "At-Risk Student",
                    "School_Name": school_name,
                    "Circuit": risk_row.get("Circuit", ""),
                    "Recipient": contact_record.get("contact_name", ""),
                    "Email": contact_record.get("email", ""),
                    "Phone": contact_record.get("phone", ""),
                    "WhatsApp": contact_record.get("whatsapp_number", ""),
                    "Preferred Channel": contact_record.get("preferred_channel", "Email"),
                    "Subject": f"EduPulse Student Risk Alert: {risk_row.get('Student_Name', '')}",
                    "Message": message,
                }
            )

    return pd.DataFrame(queue_rows)


def render_communication_center(df, subject_cols, key_prefix):
    st.markdown("### 📣 Communication Center")
    st.write("Send WhatsApp notices only: to one school, a selected group of schools, or all schools in the active district.")
    contacts_df = load_contacts_df()
    scope_label = get_scope_label()

    district_contacts_df = contacts_df[
        contacts_df["district"].fillna("").astype(str).str.strip().eq(scope_label)
    ].copy() if not contacts_df.empty else pd.DataFrame()
    school_options = sorted(
        district_contacts_df["school"].dropna().astype(str).str.strip().loc[lambda values: values != ""].unique().tolist()
    ) if not district_contacts_df.empty else []

    with st.expander("Create WhatsApp Director Notice"):
        if not school_options:
            st.info("No school contacts found for this district yet. Headteachers should save their contact profile first.")
        target_mode = st.radio(
            "Recipient Scope",
            ["Single School", "Selected Schools", "All Schools"],
            horizontal=True,
            key=f"{key_prefix}_target_mode",
        )
        selected_single_school = ""
        selected_school_group = []
        if target_mode == "Single School":
            selected_single_school = (
                st.selectbox("Choose School", school_options, key=f"{key_prefix}_single_school")
                if school_options
                else ""
            )
        elif target_mode == "Selected Schools":
            selected_school_group = st.multiselect(
                "Choose Schools",
                school_options,
                key=f"{key_prefix}_school_group",
            )

        notice_subject = st.text_input("Notice Subject", key=f"{key_prefix}_custom_subject")
        notice_body = st.text_area("Notice Message", key=f"{key_prefix}_custom_body", height=120)
        composed_message = (
            f"{notice_subject.strip()}\n\n{notice_body.strip()}"
            if notice_subject.strip()
            else notice_body.strip()
        )

        if target_mode == "Single School":
            target_schools = [selected_single_school] if selected_single_school else []
        elif target_mode == "Selected Schools":
            target_schools = selected_school_group
        else:
            target_schools = school_options

        target_contact_df = district_contacts_df[
            district_contacts_df["school"].fillna("").astype(str).str.strip().isin(target_schools)
        ].copy() if target_schools else pd.DataFrame()
        # Guard against empty DataFrame or missing whatsapp_number column
        if target_contact_df.empty or "whatsapp_number" not in target_contact_df.columns:
            target_contact_df = pd.DataFrame(columns=["school", "contact_name", "whatsapp_number_clean"])
        else:
            target_contact_df["whatsapp_number_clean"] = target_contact_df["whatsapp_number"].fillna("").astype(str).str.strip()
        target_contact_df = target_contact_df[target_contact_df["whatsapp_number_clean"] != ""]
        target_contact_df = target_contact_df.drop_duplicates(subset=["school"], keep="last")
        resolved_rows = target_contact_df[["school", "contact_name", "whatsapp_number_clean"]].rename(
            columns={"school": "School", "contact_name": "Contact", "whatsapp_number_clean": "WhatsApp"}
        )
        if not resolved_rows.empty:
            st.dataframe(resolved_rows, use_container_width=True)

        if composed_message and not target_contact_df.empty:
            if len(target_contact_df) == 1:
                whatsapp_number = target_contact_df.iloc[0]["whatsapp_number_clean"]
                st.markdown(f"[Open WhatsApp Draft]({build_whatsapp_link(whatsapp_number, composed_message)})")
            else:
                st.caption("WhatsApp opens one recipient per click. Use the buttons below to open each draft.")
                for _, row in target_contact_df.iterrows():
                    school_name = str(row.get("school", "")).strip()
                    whatsapp_number = str(row.get("whatsapp_number_clean", "")).strip()
                    if whatsapp_number:
                        st.markdown(
                            f"- [{school_name or 'Unnamed School'} WhatsApp Draft]({build_whatsapp_link(whatsapp_number, composed_message)})"
                        )
        elif composed_message and target_contact_df.empty:
            st.warning("No WhatsApp numbers are available for the selected target schools.")

    queue_df = build_communication_queue(df, subject_cols)
    if queue_df.empty:
        st.info("No alert queue items are available yet.")
        return

    st.dataframe(queue_df, use_container_width=True, height=320)
    st.download_button(
        "Download Alert Queue CSV",
        queue_df.to_csv(index=False).encode("utf-8"),
        file_name="edupulse_alert_queue.csv",
        mime="text/csv",
        key=f"{key_prefix}_download_alerts",
    )

    selected_alert = st.selectbox(
        "Preview an alert",
        options=queue_df.index.tolist(),
        format_func=lambda idx: f"{queue_df.loc[idx, 'Alert Type']} | {queue_df.loc[idx, 'School_Name']}",
        key=f"{key_prefix}_select_alert",
    )
    alert_row = queue_df.loc[selected_alert]
    st.markdown("#### Alert Preview")
    st.write(f"**Subject:** {alert_row['Subject']}")
    st.text_area("Message", value=alert_row["Message"], height=140, key=f"{key_prefix}_message_preview")

    action_cols = st.columns(3)
    with action_cols[0]:
        st.caption("Email channel disabled for this deployment.")
    with action_cols[1]:
        if alert_row["WhatsApp"]:
            st.markdown(f"[Open WhatsApp Draft]({build_whatsapp_link(alert_row['WhatsApp'], alert_row['Message'])})")
    with action_cols[2]:
        st.caption("SMS channel disabled for this deployment.")


def render_reports_center(scope_df, subject_cols, role, school="", key_prefix="reports"):
    scope_label = school if role == "Headteacher" and school else get_scope_label()
    school_sync_df = build_school_sync_status_df(scope_df) if role == "Headteacher" else build_school_sync_status_df(load_data(show_errors=False)[0])
    st.markdown("### 📦 Briefing Packs & Calibration")
    st.write("Download meeting-ready CSV/PDF packs and monitor saved scenario calibration.")

    report_scope_df = scope_df.copy()
    report_title = scope_label
    if role == "Director" and not scope_df.empty:
        scope_mode = st.radio(
            "Report Scope",
            ["District/Municipal", "Circuit", "School"],
            horizontal=True,
            key=f"{key_prefix}_scope_mode",
        )
        if scope_mode == "Circuit" and "Circuit" in scope_df.columns:
            circuit_options = sorted(scope_df["Circuit"].dropna().astype(str).str.strip().unique().tolist())
            selected_circuit = st.selectbox("Choose Circuit", circuit_options, key=f"{key_prefix}_scope_circuit")
            report_scope_df = scope_df[scope_df["Circuit"].fillna("").astype(str).str.strip() == selected_circuit].copy()
            report_title = f"{selected_circuit} Circuit"
        elif scope_mode == "School" and "School_Name" in scope_df.columns:
            school_options = sorted(scope_df["School_Name"].dropna().astype(str).str.strip().unique().tolist())
            selected_school = st.selectbox("Choose School", school_options, key=f"{key_prefix}_scope_school")
            report_scope_df = scope_df[scope_df["School_Name"].fillna("").astype(str).str.strip() == selected_school].copy()
            report_title = selected_school

    download_cols = st.columns(2)
    with download_cols[0]:
        st.download_button(
            "Download Briefing Pack (ZIP/CSV)",
            build_briefing_zip_bytes(report_scope_df, subject_cols, report_title, school_sync_df=school_sync_df),
            file_name=f"{slugify_name(report_title).lower()}_briefing_pack.zip",
            mime="application/zip",
            key=f"{key_prefix}_zip",
            use_container_width=True,
        )
    with download_cols[1]:
        st.download_button(
            "Download Briefing Pack (PDF)",
            build_briefing_pdf_bytes(report_scope_df, subject_cols, report_title, school_sync_df=school_sync_df),
            file_name=f"{slugify_name(report_title).lower()}_briefing_pack.pdf",
            mime="application/pdf",
            key=f"{key_prefix}_pdf",
            use_container_width=True,
        )

    calibration_df = build_scenario_calibration_df(scope_df, subject_cols)
    st.markdown("#### Predicted vs Actual Calibration")
    if calibration_df.empty:
        st.info("Calibration history will appear after scenarios are saved and the students remain available in the live dataset.")
    else:
        st.dataframe(calibration_df, use_container_width=True)
        st.download_button(
            "Download Calibration CSV",
            calibration_df.to_csv(index=False).encode("utf-8"),
            file_name="edupulse_calibration.csv",
            mime="text/csv",
            key=f"{key_prefix}_calibration",
        )


# ============================================================
# 11. DIRECTOR AND SCHOOL DASHBOARDS, TABLES, AND AUDITS
# ============================================================
def render_director_dashboard(df, subject_cols):
    scope_label = get_scope_label()
    agg_df = build_aggregate_dataframe(df, subject_cols)
    unread_notifications = get_notifications("Director", district=scope_label, unread_only=True)
    if agg_df.empty:
        agg_df = pd.DataFrame(
            columns=[
                "Student_ID",
                "Student_Name",
                "School_Name",
                "Circuit",
                "Aggregate",
                "Passed",
                "Placement_Category",
                "Gender",
            ]
        )
    mapped_circuits = load_all_circuits()
    school_sync_df = build_school_sync_status_df(df)
    all_schools = school_sync_df["School_Name"].tolist()
    all_circuits = combine_known_circuits(mapped_circuits, agg_df)

    if not unread_notifications.empty:
        st.warning(f"You have {len(unread_notifications)} new Headteacher update notification(s). Check the Director Alerts panel in the sidebar.")

    st.markdown("### 🏫 School Upload Coverage")
    st.write("Headteachers upload their own school CSV files. Schools without a synced upload stay visible here with zero students and an awaiting status.")
    if school_sync_df.empty:
        st.warning("No school-to-circuit mappings are available yet. Upload the circuits CSV to begin onboarding schools.")
    else:
        coverage_cols = st.columns(3)
        with coverage_cols[0]:
            render_metric_card("Mapped Schools", len(school_sync_df))
        with coverage_cols[1]:
            render_metric_card("Schools Synced", int((school_sync_df["Students Uploaded"] > 0).sum()))
        with coverage_cols[2]:
            render_metric_card("Awaiting Upload", int((school_sync_df["Students Uploaded"] == 0).sum()))
        st.dataframe(school_sync_df, use_container_width=True)

    st.markdown(f"### 🏆 {scope_label} Excellence Rankings")
    st.write("Ranking schools by average Best 6 Aggregate. Lower is better. Schools without uploads stay at zero with an awaiting-upload status.")

    grouped_school_stats = (
        agg_df.groupby("School_Name", as_index=False)
        .agg({"Aggregate": "mean", "Passed": "mean"})
        .sort_values("Aggregate", ascending=True)
    )
    if school_sync_df.empty:
        school_stats = grouped_school_stats.copy()
        if "Circuit" not in school_stats.columns:
            school_stats["Circuit"] = ""
        school_stats["Students Uploaded"] = 0
        school_stats["Upload Status"] = "Synced"
    else:
        school_stats = school_sync_df.merge(grouped_school_stats, on="School_Name", how="left")
    school_stats["Upload Status"] = school_stats["Students Uploaded"].apply(
        lambda count: "Synced" if int(count) > 0 else "Awaiting Upload"
    )
    school_stats["Pass Rate %"] = school_stats["Passed"].fillna(0) * 100
    school_stats["Aggregate_Display"] = school_stats["Aggregate"].fillna(0)
    school_stats["Aggregate_Label"] = school_stats["Aggregate"].apply(
        lambda value: "No data" if pd.isna(value) else f"{value:.1f}"
    )
    school_stats = school_stats.sort_values(
        by=["Upload Status", "Aggregate", "School_Name"],
        ascending=[False, True, True],
        na_position="last",
    )

    fig_agg = px.bar(
        school_stats,
        x="Aggregate_Display",
        y="School_Name",
        orientation="h",
        title="Average School Aggregate (Lower = Stronger)",
        color="Upload Status",
        color_discrete_map={"Synced": "#2563eb", "Awaiting Upload": "#94a3b8"},
        text="Aggregate_Label",
    )
    st.plotly_chart(fig_agg, use_container_width=True)

    st.markdown(f"### 📈 {scope_label} Pass Rate (%)")
    fig_pass = px.bar(
        school_stats.sort_values(["Upload Status", "Pass Rate %", "School_Name"], ascending=[False, False, True]),
        x="Pass Rate %",
        y="School_Name",
        orientation="h",
        color="Upload Status",
        color_discrete_map={"Synced": "#16a34a", "Awaiting Upload": "#94a3b8"},
        title="Percentage of Students with Aggregate <= 30",
        text_auto=".1f",
    )
    st.plotly_chart(fig_pass, use_container_width=True)

    st.markdown("---")
    metric_columns = st.columns(4)
    active_school_stats = school_stats[school_stats["Upload Status"] == "Synced"].copy()
    if active_school_stats.empty:
        st.info("No school has synced student data yet. Schools will move out of Awaiting Upload as each Headteacher submits the school CSV.")
    with metric_columns[0]:
        render_metric_card("👥 Total Students", len(df))
    with metric_columns[1]:
        avg_aggregate = f"{active_school_stats['Aggregate'].mean():.1f}" if not active_school_stats.empty else "--"
        render_metric_card(f"📉 {scope_label} Avg Aggregate", avg_aggregate)
    with metric_columns[2]:
        pass_rate = f"{active_school_stats['Passed'].mean() * 100:.1f}%" if not active_school_stats.empty else "--"
        render_metric_card(f"✅ {scope_label} Pass Rate", pass_rate)
    with metric_columns[3]:
        lead_school = (
            active_school_stats.sort_values("Aggregate", ascending=True).iloc[0]["School_Name"]
            if not active_school_stats.empty
            else "Awaiting first upload"
        )
        render_metric_card("🏆 Lead School", lead_school)

    st.markdown("---")
    st.markdown("### ⚖️ Gender Performance Parity")
    if not agg_df.empty and "Gender" in agg_df.columns:
        gender_stats = (
            agg_df[agg_df["Gender"] != "Unspecified"]
            .groupby("Gender", as_index=False)["Aggregate"]
            .mean()
            .sort_values("Aggregate", ascending=True)
        )

        if not gender_stats.empty:
            g_col1, g_col2 = st.columns([1, 2])

            with g_col1:
                if len(gender_stats) >= 2:
                    leader = gender_stats.iloc[0]
                    trailer = gender_stats.iloc[1]
                    gap = float(trailer["Aggregate"] - leader["Aggregate"])
                    render_metric_card(
                        "Gender Gap (Agg)",
                        f"{gap:.2f}",
                        delta=f"{leader['Gender']} leading",
                    )
                else:
                    render_metric_card("Gender Group", gender_stats.iloc[0]["Gender"])

            with g_col2:
                fig_gender = px.bar(
                    gender_stats,
                    x="Gender",
                    y="Aggregate",
                    color="Gender",
                    color_discrete_map={"Boys": "#3498db", "Girls": "#e74c3c"},
                    title="Average Aggregate by Gender",
                    text_auto=".2f",
                )
                st.plotly_chart(fig_gender, use_container_width=True)

            if len(gender_stats) >= 2:
                leader = gender_stats.iloc[0]
                trailer = gender_stats.iloc[1]
                gap = float(trailer["Aggregate"] - leader["Aggregate"])
                if gap < 1:
                    st.success(
                        f"Parity is strong. The current gap is only {gap:.1f} aggregate points."
                    )
                else:
                    st.warning(
                        f"{trailer['Gender']} are trailing {leader['Gender']} by {gap:.1f} aggregate points."
                    )
            else:
                st.info(f"Only one gender group is represented in the current {scope_label} data.")
        else:
            st.warning("Gender values are present but could not be grouped for analysis.")
    elif agg_df.empty:
        st.info("Gender analysis will appear after schools begin syncing student data.")
    else:
        st.warning("Gender data is missing from the database. Please update the source file.")

    st.markdown("---")
    st.markdown(f"### 🎓 {scope_label} CSSPS Placement Forecast")
    placement_summary = (
        agg_df["Placement_Category"]
        .value_counts()
        .reindex(PLACEMENT_ORDER, fill_value=0)
        .reset_index()
    )
    placement_summary.columns = ["Category", "Student Count"]

    fig_place = px.pie(
        placement_summary,
        values="Student Count",
        names="Category",
        hole=0.4,
        title=f"Projected Placement Distribution ({scope_label}-Wide)",
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    st.plotly_chart(fig_place, use_container_width=True)

    st.markdown("---")
    st.markdown("### 🗺️ Circuit Pressure Map")
    circuit_stats = (
        agg_df.groupby("Circuit", as_index=False)["Aggregate"]
        .mean()
    )
    if all_circuits:
        circuit_stats = pd.DataFrame({"Circuit": all_circuits}).merge(circuit_stats, on="Circuit", how="left")

    circuit_stats["Status"] = circuit_stats["Aggregate"].apply(
        lambda value: "No Current Data" if pd.isna(value) else "Has Data"
    )
    circuit_stats["Aggregate_Display"] = circuit_stats["Aggregate"].fillna(0)
    circuit_stats["Aggregate_Label"] = circuit_stats["Aggregate"].apply(
        lambda value: "No data" if pd.isna(value) else f"{value:.1f}"
    )
    circuit_stats = circuit_stats.sort_values(
        by=["Status", "Aggregate", "Circuit"],
        ascending=[True, True, True],
        na_position="last",
    )

    fig_heat = px.bar(
        circuit_stats,
        x="Aggregate_Display",
        y="Circuit",
        orientation="h",
        color="Status",
        text="Aggregate_Label",
        title=f"Circuit Pressure Map ({len(all_circuits)} Circuits)",
        color_discrete_map={"Has Data": "#dc2626", "No Current Data": "#94a3b8"},
    )
    st.plotly_chart(fig_heat, use_container_width=True)
    missing_circuits = circuit_stats.loc[circuit_stats["Status"] == "No Current Data", "Circuit"].tolist()
    if missing_circuits:
        st.caption(
            "The following circuits are in the mapping but have no student records yet in the current dataset: "
            + ", ".join(missing_circuits)
            + "."
        )

    available_circuit_stats = circuit_stats[circuit_stats["Status"] == "Has Data"].copy()
    if not available_circuit_stats.empty:
        best_circuit = available_circuit_stats.sort_values("Aggregate", ascending=True).iloc[0]["Circuit"]
        worst_circuit = available_circuit_stats.sort_values("Aggregate", ascending=False).iloc[0]["Circuit"]
        st.info(
            f"{scope_label} strategy: {best_circuit} is currently the strongest circuit. "
            f"A peer-mentoring exchange between {best_circuit} and {worst_circuit} could help spread best practices."
        )

    st.markdown("---")
    st.markdown(f"### 📊 {scope_label}-Wide Placement Summaries")

    school_pivot = (
        agg_df.groupby(["School_Name", "Placement_Category"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=PLACEMENT_ORDER, fill_value=0)
    )
    if all_schools:
        school_pivot = school_pivot.reindex(all_schools, fill_value=0)
    school_pivot["Total"] = school_pivot.sum(axis=1)
    school_pivot.loc["Total Students"] = school_pivot.sum(axis=0)
    st.write("#### 🏫 Performance by School")
    try:
        st.dataframe(
            school_pivot.style.background_gradient(cmap="YlGn"),
            use_container_width=True,
        )
    except Exception:
        st.dataframe(school_pivot, use_container_width=True)

    circuit_pivot = (
        agg_df.groupby(["Circuit", "Placement_Category"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=PLACEMENT_ORDER, fill_value=0)
    )
    if all_circuits:
        circuit_pivot = circuit_pivot.reindex(all_circuits, fill_value=0)
    circuit_pivot["Total"] = circuit_pivot.sum(axis=1)
    circuit_pivot.loc["Total Students"] = circuit_pivot.sum(axis=0)
    st.markdown("---")
    st.write("#### 🗺️ Performance by Circuit")
    try:
        st.dataframe(
            circuit_pivot.style.background_gradient(cmap="BuPu"),
            use_container_width=True,
        )
    except Exception:
        st.dataframe(circuit_pivot, use_container_width=True)
    st.caption(
        f"Placement summary check: {int(agg_df.shape[0])} students loaded and {int(circuit_pivot.loc['Total Students', 'Total'])} students counted across circuit totals."
    )

    manual_prediction_df = build_manual_prediction_summary_df(scope_label)
    manual_prediction_df = manual_prediction_df[
        manual_prediction_df["placement_category"].fillna("").astype(str).isin(["Category A", "Category B", "Category C"])
    ].copy() if not manual_prediction_df.empty else pd.DataFrame()
    if not manual_prediction_df.empty:
        st.markdown("---")
        st.markdown("### Manual Category Forecast Watchlist")
        st.write("These are the latest headteacher-saved manual placement predictions for students expected to reach Category A, B, or C schools.")
        manual_display_df = manual_prediction_df[
            ["student_name", "student_id", "school", "circuit", "school_type", "aggregate", "best_six_raw_total", "placement", "created_by", "created_at"]
        ].rename(
            columns={
                "student_name": "Student_Name",
                "student_id": "Student_ID",
                "school": "School_Name",
                "circuit": "Circuit",
                "school_type": "School_Type",
                "aggregate": "Aggregate",
                "best_six_raw_total": "Best_Six_Raw_Total",
                "placement": "Placement_Outlook",
                "created_by": "Predicted_By",
                "created_at": "Saved_At",
            }
        )
        st.dataframe(
            manual_display_df.sort_values(["Aggregate", "Best_Six_Raw_Total"], ascending=[True, False]),
            use_container_width=True,
        )

        school_prediction_rank = (
            manual_prediction_df.groupby(["school", "placement_category"]).size().unstack(fill_value=0)
            .reindex(columns=["Category A", "Category B", "Category C"], fill_value=0)
            .reset_index()
            .rename(columns={"school": "School_Name"})
        )
        school_prediction_rank = school_prediction_rank.sort_values(["Category A", "Category B", "Category C", "School_Name"], ascending=[False, False, False, True]).reset_index(drop=True)
        school_prediction_rank.index = school_prediction_rank.index + 1
        school_prediction_rank.insert(0, "Rank", school_prediction_rank.index)
        st.write("#### School Ranking by Manual Category Predictions")
        st.dataframe(school_prediction_rank, use_container_width=True)

        circuit_prediction_rank = (
            manual_prediction_df.groupby(["circuit", "placement_category"]).size().unstack(fill_value=0)
            .reindex(columns=["Category A", "Category B", "Category C"], fill_value=0)
            .reset_index()
            .rename(columns={"circuit": "Circuit"})
        )
        circuit_prediction_rank = circuit_prediction_rank.sort_values(["Category A", "Category B", "Category C", "Circuit"], ascending=[False, False, False, True]).reset_index(drop=True)
        circuit_prediction_rank.index = circuit_prediction_rank.index + 1
        circuit_prediction_rank.insert(0, "Rank", circuit_prediction_rank.index)
        st.write("#### Circuit Ranking by Manual Category Predictions")
        st.dataframe(circuit_prediction_rank, use_container_width=True)

    st.markdown("---")
    render_prediction_vs_actual(df, subject_cols, scope_label=scope_label)
    render_scroll_to_top()


def render_prediction_vs_actual(df, subject_cols, scope_label=""):
    """Show side-by-side predicted vs actual BECE results for students who have both."""
    st.markdown("### 📊 Predicted vs Actual BECE Results")

    if df.empty:
        st.info("No student data available for comparison.")
        return

    predicted_cols = [c for c in PREDICTED_SUBJECT_COLUMNS if c in df.columns]
    final_cols = [c for c in FINAL_SUBJECT_COLUMNS if c in df.columns]

    if not final_cols:
        st.info("Official BECE results have not been synced yet. Upload the WAEC PDF to enable this comparison.")
        return

    # Only show students with at least one official result
    has_official = df[final_cols].apply(pd.to_numeric, errors="coerce").notna().any(axis=1)
    matched_df = df[has_official].copy()

    if matched_df.empty:
        st.info("No students have official WAEC results synced yet. This table will populate after the WAEC PDF is uploaded.")
        return

    subject_display = {
        "Mathematics": "Maths",
        "English_Language": "English",
        "Integrated_Science": "Science",
        "Social_Studies": "Social",
        "ICT": "ICT",
        "RME": "RME",
        "BDT": "BDT",
        "French": "CA Design",
        "Ewe": "Ewe",
    }

    # Build comparison rows
    comparison_rows = []
    for _, row in matched_df.iterrows():
        record = {
            "Student_Name": str(row.get("Student_Name", "")).strip(),
            "Gender": str(row.get("Gender", "")).strip(),
            "School_Name": str(row.get("School_Name", "")).strip(),
        }
        pred_scores = []
        actual_scores = []
        for prefix in SUBJECT_PREFIXES:
            pred_col = f"{prefix}{PREDICTED_SUFFIX}"
            final_col = f"{prefix}{FINAL_SUFFIX}"
            short = subject_display.get(prefix, prefix)
            pred_val = pd.to_numeric(pd.Series([row.get(pred_col)]), errors="coerce").iloc[0]
            actual_val = pd.to_numeric(pd.Series([row.get(final_col)]), errors="coerce").iloc[0]
            if pd.notna(actual_val):
                record[f"{short} (Pred)"] = round(float(pred_val), 1) if pd.notna(pred_val) else "—"
                record[f"{short} (Actual)"] = round(float(actual_val), 1)
                if pd.notna(pred_val):
                    record[f"{short} (Δ)"] = round(float(actual_val - pred_val), 1)
                pred_scores.append(pred_val if pd.notna(pred_val) else actual_val)
                actual_scores.append(actual_val)
        if actual_scores:
            record["Actual Avg"] = round(float(np.mean(actual_scores)), 1)
            record["Predicted Avg"] = round(float(np.mean([s for s in pred_scores if pd.notna(s)])), 1) if any(pd.notna(s) for s in pred_scores) else "—"
        comparison_rows.append(record)

    if not comparison_rows:
        st.info("No matched records found for comparison.")
        return

    comparison_df = pd.DataFrame(comparison_rows)
    st.caption(f"**{len(comparison_df)}** student(s) with official WAEC results synced{f' — {scope_label}' if scope_label else ''}.")
    st.dataframe(comparison_df, use_container_width=True)

    csv_bytes = comparison_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download Comparison CSV",
        csv_bytes,
        file_name=f"predicted_vs_actual{'_' + scope_label.replace(' ', '_') if scope_label else ''}.csv",
        mime="text/csv",
        key=f"dl_pred_vs_actual_{scope_label}",
    )


def render_audit_table(display_df, key_prefix, filename):
    st.markdown("### 📋 Student Audit")

    audit_df = display_df.drop(columns=["Search_Label"], errors="ignore").copy()
    preferred_order = ["Student_Name", "Gender", "School_Name", "Circuit", "School_Type"]
    ordered_columns = [column for column in preferred_order if column in audit_df.columns]
    ordered_columns.extend(column for column in audit_df.columns if column not in ordered_columns)
    audit_df = audit_df[ordered_columns]

    st.caption("Filters below use strict matching by field.")
    filter_columns = st.columns(5)

    student_name_filter = ""
    student_id_filter = ""
    school_filter = "All"
    circuit_filter = "All"
    gender_filter = "All"

    with filter_columns[0]:
        if "Student_Name" in audit_df.columns:
            student_name_filter = st.text_input(
                "Student Name",
                key=f"{key_prefix}_student_name_filter",
                help="Exact student-name match.",
            ).strip()

    with filter_columns[1]:
        if "Student_ID" in audit_df.columns:
            student_id_filter = st.text_input(
                "Student ID",
                key=f"{key_prefix}_student_id_filter",
                help="Exact student-ID match.",
            ).strip()

    with filter_columns[2]:
        if "School_Name" in audit_df.columns:
            school_options = ["All"] + sorted(
                audit_df["School_Name"].fillna("").astype(str).str.strip().loc[lambda values: values != ""].unique().tolist()
            )
            school_filter = st.selectbox("School", school_options, key=f"{key_prefix}_school_filter")

    with filter_columns[3]:
        if "Circuit" in audit_df.columns:
            circuit_options = ["All"] + sorted(
                audit_df["Circuit"].fillna("").astype(str).str.strip().loc[lambda values: values != ""].unique().tolist()
            )
            circuit_filter = st.selectbox("Circuit", circuit_options, key=f"{key_prefix}_circuit_filter")

    with filter_columns[4]:
        if "Gender" in audit_df.columns:
            gender_options = ["All"] + sorted(
                audit_df["Gender"].fillna("").astype(str).str.strip().loc[lambda values: values != ""].unique().tolist()
            )
            gender_filter = st.selectbox("Gender", gender_options, key=f"{key_prefix}_gender_filter")

    if student_name_filter and "Student_Name" in audit_df.columns:
        audit_df = audit_df[
            audit_df["Student_Name"].fillna("").astype(str).str.strip().str.casefold() == student_name_filter.casefold()
        ]

    if student_id_filter and "Student_ID" in audit_df.columns:
        audit_df = audit_df[
            audit_df["Student_ID"].fillna("").astype(str).str.strip().str.casefold() == student_id_filter.casefold()
        ]

    if school_filter != "All" and "School_Name" in audit_df.columns:
        audit_df = audit_df[
            audit_df["School_Name"].fillna("").astype(str).str.strip() == school_filter
        ]

    if circuit_filter != "All" and "Circuit" in audit_df.columns:
        audit_df = audit_df[
            audit_df["Circuit"].fillna("").astype(str).str.strip() == circuit_filter
        ]

    if gender_filter != "All" and "Gender" in audit_df.columns:
        audit_df = audit_df[
            audit_df["Gender"].fillna("").astype(str).str.strip() == gender_filter
        ]

    st.caption(f"Matching rows: {len(audit_df)}")
    st.dataframe(audit_df, use_container_width=True)
    st.download_button(
        "Download current view as CSV",
        audit_df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        key=f"{key_prefix}_download",
    )
    render_scroll_to_top()


def render_school_dashboard(school_df, subject_cols):
    if school_df.empty:
        st.warning("No data is available for this school.")
        render_scroll_to_top()
        return
    if not subject_cols:
        st.warning("No BECE subject columns were found for school analysis.")
        render_scroll_to_top()
        return

    school_name = school_df["School_Name"].iloc[0]
    agg_df = build_aggregate_dataframe(school_df, subject_cols)

    st.markdown(f"### 📊 School Performance: {school_name}")
    st.write("A quick view of current BECE readiness, placement outlook, and action-zone pressure.")

    metric_columns = st.columns(4)
    with metric_columns[0]:
        render_metric_card("👥 Total Students", len(school_df))
    with metric_columns[1]:
        render_metric_card("📉 Avg Aggregate", f"{agg_df['Aggregate'].mean():.1f}")
    with metric_columns[2]:
        render_metric_card("✅ Pass Rate", f"{agg_df['Passed'].mean() * 100:.1f}%")
    with metric_columns[3]:
        best_student = agg_df.sort_values("Aggregate", ascending=True).iloc[0]["Student_Name"]
        render_metric_card("🌟 Top Student", best_student)

    left_col, right_col = st.columns(2)
    with left_col:
        subject_summary = pd.DataFrame(
            {
                "Subject": [format_subject_name(subject) for subject in subject_cols],
                "Average Score": [
                    safe_float(pd.to_numeric(school_df[subject], errors="coerce").mean(), 0.0)
                    for subject in subject_cols
                ],
            }
        ).sort_values("Average Score", ascending=True)

        fig_subjects = px.bar(
            subject_summary,
            x="Average Score",
            y="Subject",
            orientation="h",
            color="Average Score",
            color_continuous_scale="Blues",
            title="Average BECE Score by Subject",
            text_auto=".1f",
        )
        st.plotly_chart(fig_subjects, use_container_width=True)

    with right_col:
        if "Action_Zone" in school_df.columns and school_df["Action_Zone"].astype(str).str.strip().ne("").any():
            zone_summary = (
                school_df["Action_Zone"]
                .fillna("Unclassified")
                .astype(str)
                .apply(normalize_action_zone)
                .value_counts()
                .reset_index()
            )
            zone_summary.columns = ["Action Zone", "Student Count"]
            fig_zones = px.pie(
                zone_summary,
                values="Student Count",
                names="Action Zone",
                title="Action Zone Distribution",
                color_discrete_sequence=px.colors.qualitative.Safe,
            )
            fig_zones.update_traces(showlegend=False)
            st.plotly_chart(fig_zones, use_container_width=True)
        else:
            placement_summary = (
                agg_df["Placement_Category"]
                .value_counts()
                .reindex(PLACEMENT_ORDER, fill_value=0)
                .reset_index()
            )
            placement_summary.columns = ["Category", "Student Count"]
            fig_school_place = px.pie(
                placement_summary,
                values="Student Count",
                names="Category",
                title="School Placement Forecast",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(fig_school_place, use_container_width=True)

    summary_table = (
        agg_df[["Student_Name", "Student_ID", "Aggregate", "Placement_Category"]]
        .sort_values("Aggregate", ascending=True)
        .reset_index(drop=True)
    )
    st.markdown("#### 🏅 Best 6 Aggregate Ranking")
    st.dataframe(summary_table, use_container_width=True)

    st.markdown("---")
    render_prediction_vs_actual(school_df, subject_cols, scope_label=school_name)
    render_scroll_to_top()


# ============================================================
# 12. HEADTEACHER ENTRY FORMS AND WHAT-IF PREDICTION WORKSPACE
# ============================================================
def manual_entry_form(df, subject_cols, school):
    st.markdown("### ➕ Add Single Student")
    st.info("Use this update form to add one new student after the initial school CSV upload. The record syncs immediately to the Director dashboard under the same school and mapped circuit.")
    st.caption("Enter continuous assessment scores (0-100) for each subject. EduPulse will forecast BECE grades from these assessments.")

    if not subject_cols:
        st.warning("No BECE subject columns were found, so results cannot be entered yet.")
        render_scroll_to_top()
        return

    csv_columns = [column for column in df.columns if column != "Search_Label"]
    suggested_student_id = f"{build_school_student_id_prefix(school)}-{get_next_school_student_id_number(school, build_school_student_id_prefix(school)):04d}"
    school_profile_lookup = load_school_profile_lookup()
    school_profile = school_profile_lookup.get(school, {}) or school_profile_lookup.get(school.lower(), {}) or school_profile_lookup.get(school.upper(), {})
    suggested_attendance = resolve_attendance_default_value(
        existing_school_df=df[df["School_Name"].fillna("").astype(str).str.strip() == school].copy() if "School_Name" in df.columns else None,
        existing_all_df=df,
    )

    with st.form("bece_submit", clear_on_submit=True):
        student_name = st.text_input("Student Full Name")
        student_id = st.text_input("Student ID", value=suggested_student_id, help="A school-specific ID is suggested automatically. You can edit it if needed.")
        gender = st.selectbox("Gender", ["Not specified", "F", "M"])
        date_of_birth = st.text_input("Date of Birth (optional)", placeholder="Example: 21/10/2008")
        attendance_percent = st.number_input(
            "Attendance Percent (optional)",
            min_value=0.0,
            max_value=100.0,
            value=float(suggested_attendance),
            step=0.1,
            help="Leave blank to use school average, or enter the student's attendance percentage."
        )

        st.markdown("---")
        st.subheader("📝 Continuous Assessment Records")
        st.info("Enter raw scores (0-100) for each assessment period. These are the same fields as in your CSV template.")

        # Build subject prefixes from subject_cols (which are Final_BECE columns)
        subject_prefixes = []
        for subject in subject_cols:
            prefix = subject.replace(FINAL_SUFFIX, "")
            if prefix and prefix not in subject_prefixes:
                subject_prefixes.append(prefix)

        # Create display names for tabs
        display_names = {p: p.replace("_", " ").title() for p in subject_prefixes}
        display_names.update({
            "Mathematics": "Mathematics",
            "English_Language": "English Language",
            "Integrated_Science": "Integrated Science",
            "Social_Studies": "Social Studies",
            "ICT": "ICT",
            "RME": "R.M.E.",
            "BDT": "BDT (Pre-Tech)",
            "French": "French",
            "Ewe": "Ewe",
        })

        # Create tabs for each subject
        subject_tabs = st.tabs([display_names.get(p, p.replace("_", " ").title()) for p in subject_prefixes])

        # Dict to store all assessment inputs
        assessment_data = {}

        for i, prefix in enumerate(subject_prefixes):
            with subject_tabs[i]:
                st.markdown(f"**{display_names.get(prefix, prefix.replace('_', ' ').title())} Assessment Scores**")

                # Create 5 columns for the assessment fields
                col1, col2, col3, col4, col5 = st.columns(5)

                with col1:
                    val_assign = st.number_input(
                        "Assignments", 0, 100, 0,
                        key=f"{prefix}_ass_{school.replace(' ', '_')}"
                    )
                    assessment_data[f"{prefix}_Assignments"] = val_assign

                with col2:
                    val_t1 = st.number_input(
                        "Term 1 Exam", 0, 100, 0,
                        key=f"{prefix}_t1_{school.replace(' ', '_')}"
                    )
                    assessment_data[f"{prefix}_Term1_Exam"] = val_t1

                with col3:
                    val_t2 = st.number_input(
                        "Term 2 Exam", 0, 100, 0,
                        key=f"{prefix}_t2_{school.replace(' ', '_')}"
                    )
                    assessment_data[f"{prefix}_Term2_Exam"] = val_t2

                with col4:
                    val_m1 = st.number_input(
                        "Mock 1", 0, 100, 0,
                        key=f"{prefix}_m1_{school.replace(' ', '_')}"
                    )
                    assessment_data[f"{prefix}_Mock1"] = val_m1

                with col5:
                    val_m2 = st.number_input(
                        "Mock 2", 0, 100, 0,
                        key=f"{prefix}_m2_{school.replace(' ', '_')}"
                    )
                    assessment_data[f"{prefix}_Mock2"] = val_m2

        st.markdown("---")

        if st.form_submit_button("Upload to District/Municipal Records", type="primary"):
            final_student_id = student_id.strip() or suggested_student_id
            if not student_name.strip() or not final_student_id.strip():
                st.error("Student name and student ID are required.")
                return

            existing_ids = df["Student_ID"].astype(str).str.strip() if "Student_ID" in df.columns else pd.Series(dtype=str)
            if final_student_id.strip() in set(existing_ids.tolist()):
                st.error("That Student ID already exists in the district/municipal records.")
                return

            record = {column: pd.NA for column in csv_columns}
            record["Student_ID"] = final_student_id.strip()
            record["Student_Name"] = student_name.strip()
            record["School_Name"] = school
            record["Date_of_Birth"] = date_of_birth.strip()
            circuit_lookup = load_circuit_lookup()
            record["Circuit"] = school_profile.get("Circuit", "") or circuit_lookup.get(school, "") or circuit_lookup.get(school.lower(), "") or circuit_lookup.get(school.upper(), "") or school
            record["School_Type"] = school_profile.get("School_Type", "Not Specified")
            record["Attendance_Percent"] = round(float(attendance_percent), 1)

            if "Gender" in record:
                record["Gender"] = "" if gender == "Not specified" else gender

            # Store all continuous assessment scores
            for column_name, score in assessment_data.items():
                if column_name in record:
                    record[column_name] = score

            # Calculate provisional final scores from assessments for any visible final columns
            final_scores = []
            for subject in subject_cols:
                prefix = subject.replace(FINAL_SUFFIX, "")
                assessment_cols = [f"{prefix}_{s}" for s in ASSESSMENT_SUFFIXES if f"{prefix}_{s}" in record]
                if assessment_cols:
                    scores = [record.get(c, 0) for c in assessment_cols if pd.notna(record.get(c, 0))]
                    if scores:
                        avg_score = sum(scores) / len(scores)
                        final_scores.append(avg_score)
                        if subject in record:
                            record[subject] = avg_score

            if "Math_Improvement" in record:
                record["Math_Improvement"] = 0
            if "Math_Consistency" in record:
                record["Math_Consistency"] = 0
            if "Action_Zone" in record and final_scores:
                record["Action_Zone"] = action_zone_from_average(sum(final_scores) / len(final_scores))

            new_row_df = pd.DataFrame([record]).reindex(columns=csv_columns)
            combined_rows_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
            combined_rows_df = pd.concat([combined_rows_df, new_row_df.reindex(columns=EXPECTED_DATA_COLUMNS)], ignore_index=True)
            write_table_df(DATA_FILE, combined_rows_df, EXPECTED_DATA_COLUMNS)
            create_notification(
                event_type="student_added",
                message=(
                    f"New student added by {st.session_state.get('current_user', 'Headteacher')}: "
                    f"{student_name.strip()} ({final_student_id.strip()}) for {school} in {record.get('Circuit', school)}."
                ),
                target_role="Director",
                school=school,
                circuit=str(record.get("Circuit", school)).strip(),
                student_id=final_student_id.strip(),
                student_name=student_name.strip(),
                created_by=st.session_state.get("current_user", ""),
                district=st.session_state.get("user_district", "") or get_scope_label(),
            )
            st.cache_data.clear()
            st.session_state["portal_flash_message"] = (
                f"✅ Student added successfully: {student_name.strip()} ({final_student_id.strip()}) has been synced to {school}."
            )
            st.session_state["portal_flash_severity"] = "success"
            st.rerun()
    render_scroll_to_top()


def render_manual_grade_predictor(school):
    with st.expander("✏️ Manual BECE Grade Entry (Placement Predictor)"):
        st.write("Enter the four core grades and the two best elective grades to forecast CSSPS placement.")
        st.caption("When you save a manual prediction here, the Director dashboard will receive it and rank schools and circuits by their latest Category A, B, and C predictions.")

        student_cols = st.columns([1.2, 1])
        with student_cols[0]:
            student_name = st.text_input("Student Name", key="manual_prediction_student_name")
        with student_cols[1]:
            student_id = st.text_input("Student ID (optional)", key="manual_prediction_student_id")

        core_cols = st.columns(4)
        with core_cols[0]:
            math_grade = st.number_input("Math", 1, 9, 5, key="manual_math")
        with core_cols[1]:
            english_grade = st.number_input("English", 1, 9, 5, key="manual_english")
        with core_cols[2]:
            science_grade = st.number_input("Science", 1, 9, 5, key="manual_science")
        with core_cols[3]:
            social_grade = st.number_input("Social", 1, 9, 5, key="manual_social")

        elective_cols = st.columns(2)
        with elective_cols[0]:
            elective_one = st.number_input("Best Elective Grade", 1, 9, 5, key="manual_elective_1")
        with elective_cols[1]:
            elective_two = st.number_input("2nd Best Elective Grade", 1, 9, 5, key="manual_elective_2")

        aggregate = math_grade + english_grade + science_grade + social_grade + elective_one + elective_two
        best_six_raw_total = sum(grade_to_score(grade) for grade in [math_grade, english_grade, science_grade, social_grade, elective_one, elective_two])
        placement = predict_placement(aggregate, best_six_raw_total=best_six_raw_total)
        placement_category = predict_placement(aggregate)

        metric_cols = st.columns(3)
        with metric_cols[0]:
            st.metric("Total Aggregate", aggregate, help="Lower is better.")
        with metric_cols[1]:
            st.metric("Best 6 Raw Total", f"{best_six_raw_total:.0f}")
        with metric_cols[2]:
            st.info(f"**Predicted Placement:** {placement}")

        if st.button("Save Manual Prediction to Director Dashboard", key="save_manual_prediction", type="primary"):
            if not student_name.strip():
                st.error("Enter the student name before saving this prediction.")
            else:
                school_profile_lookup = load_school_profile_lookup()
                school_profile = school_profile_lookup.get(school, {}) or school_profile_lookup.get(school.lower(), {}) or school_profile_lookup.get(school.upper(), {})
                save_manual_prediction(
                    {
                        "prediction_id": f"manual-pred-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}-{random.randint(1000, 9999)}",
                        "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "district": st.session_state.get("user_district", "") or get_scope_label(),
                        "school": school,
                        "circuit": school_profile.get("Circuit", ""),
                        "school_type": school_profile.get("School_Type", "Not Specified"),
                        "student_id": student_id.strip(),
                        "student_name": student_name.strip(),
                        "aggregate": aggregate,
                        "best_six_raw_total": round(float(best_six_raw_total), 1),
                        "placement": placement,
                        "placement_category": placement_category,
                        "created_by": st.session_state.get("current_user", ""),
                    }
                )
                create_notification(
                    event_type="manual_prediction_saved",
                    message=(
                        f"Manual placement prediction saved by Headteacher {st.session_state.get('current_user', 'Headteacher')} for {student_name.strip()} "
                        f"at {school}: {placement}."
                    ),
                    target_role="Director",
                    school=school,
                    circuit=school_profile.get("Circuit", ""),
                    student_id=student_id.strip(),
                    student_name=student_name.strip(),
                    created_by=st.session_state.get("current_user", ""),
                    district=st.session_state.get("user_district", "") or get_scope_label(),
                )
                st.success("Manual prediction saved. The Director dashboard will now include it in the school and circuit forecast summaries.")

        st.caption("This manual predictor stores the latest placement forecast for the named student so the Director can compare predicted transition strength across schools and circuits.")
    render_scroll_to_top()


def render_student_predictor(display_df, subject_cols, key_prefix):
    # This workspace compares current student performance with a
    # scenario-driven ML projection and lets staff save the scenario.
    st.markdown("### 🔍 Student-Specific Deep Dive")

    if display_df.empty:
        st.warning("No student data is available for prediction.")
        render_scroll_to_top()
        return
    if not subject_cols:
        st.warning("No BECE subject columns were found for prediction.")
        render_scroll_to_top()
        return

    working_df = display_df.copy()
    if "Search_Label" not in working_df.columns and {"Student_Name", "Student_ID"}.issubset(working_df.columns):
        working_df["Search_Label"] = (
            working_df["Student_Name"].astype(str).str.strip()
            + " ("
            + working_df["Student_ID"].astype(str).str.strip()
            + ")"
        )

    if "Search_Label" not in working_df.columns:
        st.warning("Student search labels could not be created from the current data.")
        render_scroll_to_top()
        return

    options = ["Select a student..."] + sorted(working_df["Search_Label"].dropna().unique().tolist())
    student_choice = st.selectbox("Search for a student", options=options, key=f"{key_prefix}_student")

    if student_choice == "Select a student...":
        render_scroll_to_top()
        return

    student_match = working_df[working_df["Search_Label"] == student_choice]
    if student_match.empty:
        st.warning("The selected student could not be found.")
        render_scroll_to_top()
        return

    row = student_match.iloc[0]
    student_name = row["Student_Name"]
    current_outcome = compute_student_outcome_details(row, subject_cols)
    ml_bundle = get_live_ml_bundle()
    model_ready_count = len(ml_bundle)

    assignment_cols = [column for column in working_df.columns if column.endswith("_Assignments")]
    mock_cols = [column for column in working_df.columns if column.endswith("_Mock2")]

    current_attendance = safe_float(row.get("Attendance_Percent"), 75.0)
    current_assignment = average_row_values(row, assignment_cols, safe_float(row.get("Mathematics_Assignments"), 50.0))
    current_mock = average_row_values(row, mock_cols, safe_float(row.get("Mathematics_Mock2"), 50.0))

    st.write("Adjust the intervention levers below to run a live ML forecast. EduPulse now blends notebook-derived machine learning with explicit 30/70 score logic and raw-score tracking.")

    meta_cols = st.columns([1.2, 1, 1])
    with meta_cols[0]:
        scenario_name = st.text_input("Scenario Name", value="Focused improvement plan", key=f"{key_prefix}_scenario_name")
    with meta_cols[1]:
        st.metric("ML Models Ready", model_ready_count, help="One pretrained model is available per BECE subject where the joblib bundle includes it.")
    with meta_cols[2]:
        st.metric("Current Best 6 Raw Total", f"{current_outcome['best_six_raw_total']:.1f}")

    intervention_note = st.text_area(
        "Intervention Notes",
        placeholder="Example: weekly attendance check, Saturday maths clinic, two mock drills before the next revision cycle.",
        key=f"{key_prefix}_intervention_note",
        height=110,
    )

    slider_cols = st.columns(3)
    with slider_cols[0]:
        target_attendance = st.slider(
            "Attendance %",
            min_value=0.0,
            max_value=100.0,
            value=float(round(current_attendance, 1)),
            key=f"{key_prefix}_attendance",
        )
    with slider_cols[1]:
        target_assignment = st.slider(
            "Assignments %",
            min_value=0.0,
            max_value=100.0,
            value=float(round(current_assignment, 1)),
            key=f"{key_prefix}_assignment",
        )
    with slider_cols[2]:
        target_mock = st.slider(
            "Mock Prep %",
            min_value=0.0,
            max_value=100.0,
            value=float(round(current_mock, 1)),
            key=f"{key_prefix}_mock",
        )

    st.markdown("#### Live ML Forecast Across Subjects")
    predicted_rows = []
    predicted_scores = {}
    subject_columns = st.columns(3)

    for index, subject in enumerate(subject_cols):
        # Every subject is predicted independently with its own pretrained model.
        prediction_result = predict_subject_score_ml(
            row,
            subject,
            ml_bundle,
            attendance_value=target_attendance,
            assignment_value=target_assignment,
            mock_value=target_mock,
        )
        predicted_score = prediction_result["predicted_score"]
        predicted_scores[subject] = predicted_score
        score_delta = predicted_score - prediction_result["current_score"]
        if predicted_score < 45:
            risk_flag = "Critical"
        elif predicted_score < 55:
            risk_flag = "Watch"
        else:
            risk_flag = "Stable"

        predicted_rows.append(
            {
                "subject_col": subject,
                "Subject": prediction_result["subject_name"],
                "Current Score": prediction_result["current_score"],
                "Predicted Score": prediction_result["predicted_score"],
                "Predicted Grade": prediction_result["predicted_grade"],
                "Continuous Assessment": prediction_result["continuous_assessment"],
                "CA 30 Component": prediction_result["ca_30_component"],
                "Exam 70 Proxy": prediction_result["exam_70_proxy"],
                "Weighted 30/70 Proxy": prediction_result["weighted_30_70_proxy"],
                "School Bias": prediction_result["school_bias"],
                "Risk Flag": risk_flag,
            }
        )

        with subject_columns[index % 3]:
            st.metric(
                prediction_result["subject_name"],
                f"Grade {prediction_result['predicted_grade']}",
                delta=f"{score_delta:+.1f} pts",
            )

    predicted_outcome = compute_student_outcome_details(row, subject_cols, score_override=predicted_scores)

    st.markdown("---")
    metric_cols = st.columns(4)
    with metric_cols[0]:
        st.metric("Current Aggregate", f"{current_outcome['aggregate']:.0f}")
    with metric_cols[1]:
        st.metric(
            "Predicted Aggregate",
            f"{predicted_outcome['aggregate']:.0f}",
            delta=f"{predicted_outcome['aggregate'] - current_outcome['aggregate']:+.1f}",
            delta_color="inverse",
            help="Lower is better. EduPulse uses the 4 core subjects plus the best 2 electives chosen dynamically per student.",
        )
    with metric_cols[2]:
        st.metric(
            "Predicted Best 6 Raw Total",
            f"{predicted_outcome['best_six_raw_total']:.1f}",
            delta=f"{predicted_outcome['best_six_raw_total'] - current_outcome['best_six_raw_total']:+.1f}",
        )
    with metric_cols[3]:
        st.info(f"**CSSPS Outlook:**\n\n{predicted_outcome['placement_outlook']}")

    st.caption(
        f"Best two electives selected automatically for this scenario: {predicted_outcome['best_two_subject_names']}. "
        "If elective grades tie, higher raw scores are favored for tie-breaking."
    )
    if predicted_outcome["tie_break_band"]:
        st.caption(f"Category A raw-score tie-break signal: {predicted_outcome['tie_break_band']}.")

    recommendations = generate_intervention_recommendations(
        predicted_rows,
        current_attendance,
        target_attendance,
        target_assignment,
        target_mock,
    )
    recommendation_text = "\n".join([f"- {recommendation}" for recommendation in recommendations])

    guidance_cols = st.columns([1.1, 1])
    with guidance_cols[0]:
        st.markdown("#### Suggested Intervention Plan")
        st.markdown(recommendation_text)
        if intervention_note.strip():
            st.caption(f"Saved note: {intervention_note.strip()}")
    with guidance_cols[1]:
        subject_forecast_df = pd.DataFrame(predicted_rows)
        top_risk_subjects = subject_forecast_df.sort_values(["Predicted Score", "Subject"], ascending=[True, True]).head(3)
        st.markdown("#### Priority Subjects")
        st.dataframe(top_risk_subjects[["Subject", "Predicted Score", "Predicted Grade", "Risk Flag"]], use_container_width=True)

    with st.expander("View full subject forecast, 30/70 components, and model source"):
        st.dataframe(pd.DataFrame(predicted_rows), use_container_width=True)

    action_cols = st.columns(3)
    with action_cols[0]:
        if st.button("Save Scenario", type="primary", key=f"{key_prefix}_save_scenario"):
            save_prediction_scenario(
                row,
                scenario_name,
                intervention_note,
                {
                    "current_attendance": current_attendance,
                    "target_attendance": target_attendance,
                    "current_assignment": current_assignment,
                    "target_assignment": target_assignment,
                    "current_mock": current_mock,
                    "target_mock": target_mock,
                },
                current_outcome,
                predicted_outcome,
                predicted_rows,
            )
            st.success("Scenario saved. It will now appear in the history and calibration sections.")
    with action_cols[1]:
        st.download_button(
            "Download Counselling Sheet (PDF)",
            build_student_counselling_sheet_pdf(
                row,
                scenario_name,
                intervention_note,
                current_outcome,
                predicted_outcome,
                predicted_rows,
                recommendations,
            ),
            file_name=f"{slugify_name(student_name).lower()}_counselling_sheet.pdf",
            mime="application/pdf",
            key=f"{key_prefix}_download_counselling",
            use_container_width=True,
        )
    with action_cols[2]:
        st.download_button(
            "Download Scenario CSV",
            pd.DataFrame(predicted_rows).to_csv(index=False).encode("utf-8"),
            file_name=f"{slugify_name(student_name).lower()}_scenario_forecast.csv",
            mime="text/csv",
            key=f"{key_prefix}_download_scenario_csv",
            use_container_width=True,
        )

    scenario_history_df = build_student_scenario_history(row.get("Student_ID", ""))
    st.markdown("---")
    st.markdown("#### Saved Scenarios")
    if scenario_history_df.empty:
        st.info("No saved scenarios for this student yet.")
    else:
        st.dataframe(
            scenario_history_df[
                [
                    "created_at",
                    "scenario_name",
                    "current_aggregate",
                    "predicted_aggregate",
                    "current_placement",
                    "predicted_placement",
                    "best_two_electives",
                ]
            ],
            use_container_width=True,
        )

    calibration_df = build_scenario_calibration_df(working_df, subject_cols)
    student_calibration_df = calibration_df[
        calibration_df["Student_ID"].fillna("").astype(str).str.strip().eq(str(row.get("Student_ID", "")).strip())
    ].copy() if not calibration_df.empty else pd.DataFrame()
    st.markdown("#### Predicted vs Actual Calibration")
    if student_calibration_df.empty:
        st.info("Calibration will appear here after at least one scenario is saved for this student.")
    else:
        st.dataframe(student_calibration_df, use_container_width=True)

    if predicted_outcome["placement_outlook"] != current_outcome["placement_outlook"]:
        st.caption(f"Current placement outlook: {current_outcome['placement_outlook']}. Predicted outlook: {predicted_outcome['placement_outlook']}.")
    render_scroll_to_top()


# ============================================================
# 13. ROLE-BASED PORTAL ROUTING AND APP ENTRYPOINT
# ============================================================
def director_portal(df, subject_cols, data_status):
    scope_label = get_scope_label()
    st.title(f"🌐 Director Dashboard: {scope_label}")
    leaderboard_tab, audit_tab, predictor_tab, reports_tab, data_mgmt_tab = st.tabs(
        [f"🏆 {scope_label} Leaderboard", "📋 Student Audit", "🔍 Search & Predict", "📦 Reports & Alerts", "⚙️ Data Management"]
    )

    with leaderboard_tab:
        render_director_dashboard(df, subject_cols)

    with audit_tab:
        render_audit_table(df, "director_audit", "district_municipal_student_audit.csv")

    with predictor_tab:
        render_student_predictor(df, subject_cols, "director_predictor")

    with reports_tab:
        render_reports_center(df, subject_cols, "Director", key_prefix="director_reports")
        st.markdown("---")
        render_communication_center(df, subject_cols, "director_comms")

    with data_mgmt_tab:
        st.header("⚙️ District System Management")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🗺️ Update Circuit Mapping")
            st.info("Re-upload the circuits CSV to update school-circuit mappings.")
            render_director_data_setup(data_status, standalone=False, key_prefix="mgmt_circuit_update")

        with col2:
            st.subheader("📄 Sync WAEC Result PDFs")
            st.info("Upload official BECE PDF result files to extract and analyze student performance.")

            waec_pdfs = st.file_uploader(
                "Upload WAEC PDF Results",
                type="pdf",
                accept_multiple_files=True,
                key="mgmt_waec_pdf_upload"
            )

            if waec_pdfs:
                st.write(f"**{len(waec_pdfs)}** PDF file(s) selected")

                if st.button("🔄 Process & Sync PDFs", type="primary", key="mgmt_process_pdfs"):
                    progress_bar = st.progress(0)
                    success_count = 0
                    error_list = []

                    for index, pdf_file in enumerate(waec_pdfs):
                        try:
                            pdf_bytes = pdf_file.read()
                            school_name, extracted_rows = extract_waec_pdf_rows(pdf_bytes, fallback_school_name=pdf_file.name)

                            if extracted_rows:
                                # Convert to DataFrame and force string types
                                pdf_df = pd.DataFrame(extracted_rows)
                                # Force all columns to string/object type to prevent float crashes
                                for col in pdf_df.columns:
                                    pdf_df[col] = pdf_df[col].astype(str).replace("nan", "")

                                # Merge with existing data
                                existing_df = read_table_df(DATA_FILE, EXPECTED_DATA_COLUMNS)
                                if not existing_df.empty:
                                    # Remove any existing students with same IDs
                                    existing_ids = pdf_df["Student_ID"].tolist()
                                    existing_df = existing_df[~existing_df["Student_ID"].isin(existing_ids)]
                                    combined_df = pd.concat([existing_df, pdf_df], ignore_index=True)
                                else:
                                    combined_df = pdf_df

                                write_table_df(DATA_FILE, combined_df, EXPECTED_DATA_COLUMNS)
                                success_count += 1

                                # Create notification
                                create_notification(
                                    event_type="waec_pdf_synced",
                                    message=f"PDF synced for {school_name}: {len(extracted_rows)} students extracted by Director.",
                                    target_role="Director",
                                    school=school_name,
                                    created_by=st.session_state.get("current_user", ""),
                                    district=scope_label,
                                )
                        except Exception as exc:
                            error_list.append(f"{pdf_file.name}: {exc}")

                        progress_bar.progress((index + 1) / len(waec_pdfs))

                    if success_count > 0:
                        st.success(f"✅ Successfully processed {success_count} PDF file(s). {len(error_list)} error(s).")
                        st.cache_data.clear()
                    if error_list:
                        with st.expander("⚠️ Processing Errors"):
                            for err in error_list:
                                st.error(err)

                    if success_count > 0:
                        st.rerun()


def render_headteacher_upload_required_screen(school):
    sync_status = get_school_sync_status(school)
    st.title(f"📥 Student Data Required: {school}")
    st.write(
        "This Headteacher account cannot open the full dashboard yet because the school has not synced any student rows. Upload the school CSV below to unlock the portal."
    )
    render_status_message(sync_status)
    render_headteacher_bulk_upload(school, "headteacher_required_upload", redirect_to_login=False)


def headteacher_portal(df, subject_cols, school):
    st.title(f"🏫 Headteacher Portal: {school}")
    show_portal_flash()
    school_df = df[df["School_Name"] == school].copy()

    overview_tab, upload_tab, entry_tab, predictor_tab, reports_tab = st.tabs(
        ["📊 School Performance", "📥 Sync Student Data", "➕ Student Updates", "🔮 Student Predictor", "📦 Reports & Contact"]
    )

    with overview_tab:
        render_school_dashboard(school_df, subject_cols)
        st.markdown("---")
        render_audit_table(school_df, "headteacher_audit", f"{school.replace(' ', '_').lower()}_audit.csv")

    with upload_tab:
        render_headteacher_bulk_upload(school, "headteacher_dashboard_student_upload", redirect_to_login=False)

    with entry_tab:
        manual_entry_form(df, subject_cols, school)

    with predictor_tab:
        render_manual_grade_predictor(school)
        render_student_predictor(school_df, subject_cols, "headteacher_predictor")

    with reports_tab:
        render_reports_center(school_df, subject_cols, "Headteacher", school=school, key_prefix="headteacher_reports")
        st.markdown("---")
        render_headteacher_contact_form(school, "headteacher_contact")


def main():
    init_session_state()

    if not st.session_state["logged_in"]:
        login_ui()
        return

    role = st.session_state["user_role"]
    school = st.session_state["user_school"]
    circuit_status = get_circuit_file_status()
    data_status = get_data_file_status()
    df, subject_cols = load_data(show_errors=False) if data_status["ready"] else (pd.DataFrame(), [])

    st.markdown('<div id="page-top"></div>', unsafe_allow_html=True)
    render_sidebar(df, subject_cols, role, school)

    if role == "Director":
        # Create persistent flag so app remembers if Director is 'Ready'
        if "director_setup_complete" not in st.session_state:
            st.session_state.director_setup_complete = circuit_status["ready"]

        # Only block the view if file is missing AND we haven't cleared setup yet
        if not st.session_state.director_setup_complete and not circuit_status["ready"]:
            render_director_data_setup(data_status, standalone=True, key_prefix="director_first_run_setup")
            return

        # Once we pass the gate, keep flag True for this session
        st.session_state.director_setup_complete = True
        director_portal(df, subject_cols, data_status)
    elif role == "Headteacher":
        if not circuit_status["ready"]:
            render_circuit_waiting_screen(circuit_status)
            return
        school_sync_status = get_school_sync_status(school)
        if not school_sync_status["ready"]:
            render_headteacher_upload_required_screen(school)
            return
        headteacher_portal(df, subject_cols, school)
    else:
        st.error("Unknown account role. Please contact the district/municipal administrator.")


main()
