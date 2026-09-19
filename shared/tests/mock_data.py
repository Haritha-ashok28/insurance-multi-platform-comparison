# Tiny mock rows matching the REAL dataset schemas, just for local smoke-testing
# the generators before Haritha provides the actual Kaggle CSVs.

POLICYHOLDER_COLUMNS = [
    "ID", "POLICY_ID", "KIDSDRIV", "BIRTH", "AGE", "HOMEKIDS", "YOJ", "INCOME", "PARENT1",
    "HOME_VAL", "MSTATUS", "GENDER", "EDUCATION", "OCCUPATION", "TRAVTIME",
    "CAR_USE", "BLUEBOOK", "TIF", "CAR_TYPE", "RED_CAR", "OLDCLAIM", "CLM_FREQ",
    "REVOKED", "MVR_PTS", "CLM_AMT", "CAR_AGE", "CLAIM_FLAG", "URBANICITY",
]

# POLICY_ID is the real row grain (see shared/transforms/bronze.py) - ID (the
# person) can repeat across policies, so POLICY_ID is what's unique per row here
MOCK_POLICYHOLDERS = [
    # ID, POLICY_ID, ..., OLDCLAIM, CLM_FREQ, ..., CLM_AMT, ..., CLAIM_FLAG, ...
    (1, 101, 0, "1/1/1980", 45, 0, 12, 50000, "No", 150000, "Yes", "M", "Bachelors",
     "Manager", 20, "Private", 25000, 5, "SUV", "no", 4500, 2, "No", 1, 3200, 8, 1, "Urban"),
    (2, 102, 1, "1/1/1990", 35, 1, 8, 40000, "No", 0, "No", "F", "Masters",
     "Clerical", 30, "Commercial", 15000, 3, "Sedan", "yes", 0, 0, "No", 0, 0, 5, 0, "Rural"),
    (3, 103, 0, "1/1/1975", 50, 0, 15, 90000, "No", 250000, "Yes", "M", "PhD",
     "Doctor", 10, "Private", 45000, 8, "SUV", "no", 12000, 3, "Yes", 4, 8000, 3, 1, "Urban"),
    # same person (ID=1), a second policy on a different vehicle - the real-world case
    (1, 104, 0, "1/1/1980", 45, 0, 12, 50000, "No", 150000, "Yes", "M", "Bachelors",
     "Manager", 20, "Private", 18000, 5, "Sedan", "no", 900, 1, "No", 0, 0, 4, 0, "Urban"),
]

TELEMATICS_COLUMNS = ["device_id", "timestamp", "PID", "value", "alarm_class"]

MOCK_TELEMATICS = [
    ("DEV-1", 1000, "RPM", 800.0, 0),
    ("DEV-1", 2000, "RPM", 3200.0, 1),
    ("DEV-1", 3000, "RPM", 5200.0, 3),
    ("DEV-2", 1000, "RPM", 900.0, 0),
    ("DEV-2", 2000, "RPM", 4100.0, 2),
    ("DEV-1", 1000, "SPEED", 0.0, 0),
    ("DEV-1", 2000, "SPEED", 60.0, 1),
    ("DEV-2", 1000, "SPEED", 120.0, 3),
    ("DEV-3", 1000, "SPEED", 40.0, 0),
]
