DROP DATABASE IF EXISTS enterprise_health;
CREATE DATABASE enterprise_health;
USE enterprise_health;

SET FOREIGN_KEY_CHECKS = 0;

DROP VIEW IF EXISTS vw_lab_alerts;
DROP VIEW IF EXISTS vw_unpaid_bills;
DROP VIEW IF EXISTS vw_doctor_workload;
DROP VIEW IF EXISTS vw_monthly_revenue;
DROP VIEW IF EXISTS vw_patient_appointment_summary;

DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS lab_tests;
DROP TABLE IF EXISTS billing;
DROP TABLE IF EXISTS prescriptions;
DROP TABLE IF EXISTS diagnoses;
DROP TABLE IF EXISTS appointments;
DROP TABLE IF EXISTS patient_insurance;
DROP TABLE IF EXISTS insurance_providers;
DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS doctors;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS legacy_encounter_data;

SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE legacy_encounter_data (
    legacy_row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    legacy_patient_code VARCHAR(20),
    patient_full_name VARCHAR(120),
    date_of_birth DATE,
    gender VARCHAR(20),
    phone VARCHAR(25),
    email VARCHAR(120),
    address_line VARCHAR(200),
    city VARCHAR(80),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    doctor_full_name VARCHAR(120),
    doctor_specialty VARCHAR(80),
    department_name VARCHAR(80),
    appointment_date DATETIME,
    appointment_status VARCHAR(30),
    visit_reason VARCHAR(150),
    diagnosis_code VARCHAR(20),
    diagnosis_description VARCHAR(150),
    medicine_name VARCHAR(120),
    dosage VARCHAR(80),
    duration_days INT,
    insurance_provider_name VARCHAR(120),
    policy_type VARCHAR(50),
    policy_number VARCHAR(50),
    total_amount DECIMAL(10,2),
    paid_amount DECIMAL(10,2),
    payment_status VARCHAR(30),
    lab_test_name VARCHAR(100),
    result_value VARCHAR(50),
    result_status VARCHAR(30),
    test_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP PROCEDURE IF EXISTS load_legacy_data;
DELIMITER $$

CREATE PROCEDURE load_legacy_data()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE v_patient_code VARCHAR(20);
    DECLARE v_patient_name VARCHAR(120);
    DECLARE v_dob DATE;
    DECLARE v_gender VARCHAR(20);
    DECLARE v_phone VARCHAR(25);
    DECLARE v_email VARCHAR(120);
    DECLARE v_address VARCHAR(200);
    DECLARE v_city VARCHAR(80);
    DECLARE v_state VARCHAR(50);
    DECLARE v_postal VARCHAR(20);
    DECLARE v_doctor_name VARCHAR(120);
    DECLARE v_specialty VARCHAR(80);
    DECLARE v_department VARCHAR(80);
    DECLARE v_appointment_date DATETIME;
    DECLARE v_appointment_status VARCHAR(30);
    DECLARE v_visit_reason VARCHAR(150);
    DECLARE v_diag_code VARCHAR(20);
    DECLARE v_diag_desc VARCHAR(150);
    DECLARE v_medicine VARCHAR(120);
    DECLARE v_dosage VARCHAR(80);
    DECLARE v_duration INT;
    DECLARE v_insurance VARCHAR(120);
    DECLARE v_policy_type VARCHAR(50);
    DECLARE v_policy_number VARCHAR(50);
    DECLARE v_total DECIMAL(10,2);
    DECLARE v_paid DECIMAL(10,2);
    DECLARE v_payment_status VARCHAR(30);
    DECLARE v_lab_test VARCHAR(100);
    DECLARE v_result_value VARCHAR(50);
    DECLARE v_result_status VARCHAR(30);
    DECLARE v_test_date DATE;

    WHILE i <= 1000 DO
        SET v_patient_code = CONCAT('P', LPAD((1 + FLOOR(RAND() * 120)), 5, '0'));

        SET v_patient_name = ELT(1 + FLOOR(RAND() * 20),
            'James Carter','Sophia Miller','Liam Johnson','Olivia Brown','Noah Davis',
            'Emma Wilson','Mason Taylor','Ava Anderson','Lucas Thomas','Mia Jackson',
            'Ethan White','Isabella Harris','Logan Martin','Amelia Thompson','Elijah Garcia',
            'Charlotte Martinez','Benjamin Robinson','Harper Clark','Daniel Rodriguez','Evelyn Lewis'
        );

        SET v_dob = DATE_ADD('1965-01-01', INTERVAL FLOOR(RAND() * 18000) DAY);
        SET v_gender = ELT(1 + FLOOR(RAND() * 3), 'Male','Female','Other');

        IF RAND() < 0.08 THEN
            SET v_phone = NULL;
        ELSE
            SET v_phone = CONCAT(
                '555-',
                LPAD(FLOOR(100 + RAND()*900), 3, '0'),
                '-',
                LPAD(FLOOR(1000 + RAND()*9000), 4, '0')
            );
        END IF;

        SET v_email = CONCAT('patient', i, '@mail.com');
        SET v_address = CONCAT(FLOOR(100 + RAND()*9900), ' Main St');
        SET v_city = ELT(1 + FLOOR(RAND() * 6), 'Dallas','Austin','Houston','Chicago','Phoenix','Atlanta');
        SET v_state = ELT(1 + FLOOR(RAND() * 4), 'TX','IL','AZ','GA');
        SET v_postal = FLOOR(70000 + RAND()*9999);

        SET v_doctor_name = ELT(1 + FLOOR(RAND() * 8),
            'Dr. John Smith','Dr John Smith','Dr. Emily Davis','Dr. Michael Brown',
            'Dr. Sarah Wilson','Dr. Robert Taylor','Dr. David Clark','Dr. Jennifer Martinez'
        );

        SET v_specialty = ELT(1 + FLOOR(RAND() * 8),
            'Cardiology','Neurology','Orthopedics','General Medicine',
            'Pediatrics','Oncology','Dermatology','Radiology'
        );

        SET v_department = ELT(1 + FLOOR(RAND() * 8),
            'Cardiology','Neurology','Orthopedics','General Medicine',
            'Pediatrics','Oncology','Dermatology','Radiology'
        );

        SET v_appointment_date = DATE_ADD(
            DATE_ADD('2025-01-01 08:00:00', INTERVAL FLOOR(RAND()*450) DAY),
            INTERVAL FLOOR(8 + RAND()*9) HOUR
        );

        SET v_appointment_status = ELT(1 + FLOOR(RAND() * 5),
            'Scheduled','Completed','Cancelled','Completed','Completed'
        );

        SET v_visit_reason = ELT(1 + FLOOR(RAND() * 8),
            'Routine Checkup','Follow-up Visit','Chest Pain Evaluation','Headache Assessment',
            'Medication Review','Lab Follow-up','Joint Pain Review','Skin Rash Consultation'
        );

        SET v_diag_code = ELT(1 + FLOOR(RAND() * 8), 'I10','E11','J20','M54','R51','K21','N39','L20');

        SET v_diag_desc = ELT(1 + FLOOR(RAND() * 8),
            'Hypertension','Type 2 Diabetes','Acute Bronchitis','Back Pain',
            'Headache','GERD','Urinary Tract Infection','Dermatitis'
        );

        SET v_medicine = ELT(1 + FLOOR(RAND() * 8),
            'Metformin','Lisinopril','Azithromycin','Ibuprofen',
            'Omeprazole','Amoxicillin','Atorvastatin','Hydrocortisone Cream'
        );

        SET v_dosage = ELT(1 + FLOOR(RAND() * 8),
            '500 mg once daily','10 mg once daily','250 mg twice daily','400 mg as needed',
            '20 mg daily','500 mg three times daily','20 mg at bedtime','Apply twice daily'
        );

        SET v_duration = 3 + FLOOR(RAND() * 27);

        SET v_insurance = ELT(1 + FLOOR(RAND() * 8),
            'BlueCross','Blue Cross','Aetna','Cigna','United Health','UNITEDHEALTH','Medicare','Self Pay'
        );

        SET v_policy_type = ELT(1 + FLOOR(RAND() * 5), 'HMO','PPO','EPO','Government','Self');

        IF RAND() < 0.12 THEN
            SET v_policy_number = NULL;
        ELSE
            SET v_policy_number = CONCAT('POL', FLOOR(100000 + RAND()*900000));
        END IF;

        SET v_total = ROUND(50 + RAND()*950, 2);
        SET v_paid = ROUND(v_total * RAND(), 2);

        IF v_paid = 0 THEN
            SET v_payment_status = 'Pending';
        ELSEIF v_paid >= v_total THEN
            SET v_payment_status = 'Paid';
        ELSE
            SET v_payment_status = 'Partial';
        END IF;

        SET v_lab_test = ELT(1 + FLOOR(RAND() * 8),
            'CBC','Lipid Panel','X-Ray','MRI','CT Scan',
            'Blood Glucose','Urinalysis','Liver Function Test'
        );

        SET v_result_value = ELT(1 + FLOOR(RAND() * 6),
            'Normal','High','Low','Borderline','Positive','Negative'
        );

        SET v_result_status = ELT(1 + FLOOR(RAND() * 3), 'Normal','Abnormal','Critical');
        SET v_test_date = DATE_ADD('2025-01-01', INTERVAL FLOOR(RAND() * 450) DAY);

        INSERT INTO legacy_encounter_data (
            legacy_patient_code,
            patient_full_name,
            date_of_birth,
            gender,
            phone,
            email,
            address_line,
            city,
            state,
            postal_code,
            doctor_full_name,
            doctor_specialty,
            department_name,
            appointment_date,
            appointment_status,
            visit_reason,
            diagnosis_code,
            diagnosis_description,
            medicine_name,
            dosage,
            duration_days,
            insurance_provider_name,
            policy_type,
            policy_number,
            total_amount,
            paid_amount,
            payment_status,
            lab_test_name,
            result_value,
            result_status,
            test_date
        )
        VALUES (
            v_patient_code,
            v_patient_name,
            v_dob,
            v_gender,
            v_phone,
            v_email,
            v_address,
            v_city,
            v_state,
            v_postal,
            v_doctor_name,
            v_specialty,
            v_department,
            v_appointment_date,
            v_appointment_status,
            v_visit_reason,
            v_diag_code,
            v_diag_desc,
            v_medicine,
            v_dosage,
            v_duration,
            v_insurance,
            v_policy_type,
            v_policy_number,
            v_total,
            v_paid,
            v_payment_status,
            v_lab_test,
            v_result_value,
            v_result_status,
            v_test_date
        );

        SET i = i + 1;
    END WHILE;
END $$

DELIMITER ;

CALL load_legacy_data();
DROP PROCEDURE load_legacy_data;

SET SQL_SAFE_UPDATES = 0;

UPDATE legacy_encounter_data
SET insurance_provider_name = 'BLUE CROSS'
WHERE insurance_provider_name = 'BlueCross' AND RAND() < 0.5;

UPDATE legacy_encounter_data
SET doctor_full_name = 'Dr John Smith'
WHERE doctor_full_name = 'Dr. John Smith' AND RAND() < 0.4;

SET SQL_SAFE_UPDATES = 1;

CREATE TABLE departments (
    department_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    department_name VARCHAR(80) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE doctors (
    doctor_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(60) NOT NULL,
    last_name VARCHAR(60) NOT NULL,
    specialty VARCHAR(80) NOT NULL,
    department_id BIGINT NOT NULL,
    full_name VARCHAR(130) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_doctors_department
        FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

CREATE TABLE patients (
    patient_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    legacy_patient_code VARCHAR(20) NOT NULL UNIQUE,
    first_name VARCHAR(60) NOT NULL,
    last_name VARCHAR(60) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender VARCHAR(20) NOT NULL,
    phone VARCHAR(25),
    email VARCHAR(120),
    address_line VARCHAR(200),
    city VARCHAR(80),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_patients_gender
        CHECK (gender IN ('Male','Female','Other'))
);

CREATE TABLE insurance_providers (
    insurance_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    provider_name VARCHAR(120) NOT NULL UNIQUE,
    policy_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE patient_insurance (
    patient_insurance_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    patient_id BIGINT NOT NULL,
    insurance_id BIGINT NOT NULL,
    policy_number VARCHAR(50),
    start_date DATE,
    end_date DATE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE KEY uk_patient_insurance (patient_id, insurance_id, policy_number),
    CONSTRAINT fk_patient_insurance_patient
        FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    CONSTRAINT fk_patient_insurance_insurance
        FOREIGN KEY (insurance_id) REFERENCES insurance_providers(insurance_id)
);

CREATE TABLE appointments (
    appointment_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    patient_id BIGINT NOT NULL,
    doctor_id BIGINT NOT NULL,
    appointment_date DATETIME NOT NULL,
    appointment_status VARCHAR(30) NOT NULL,
    visit_reason VARCHAR(150),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_appointments_patient
        FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    CONSTRAINT fk_appointments_doctor
        FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id),
    CONSTRAINT chk_appointments_status
        CHECK (appointment_status IN ('Scheduled','Completed','Cancelled'))
);

CREATE TABLE diagnoses (
    diagnosis_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    appointment_id BIGINT NOT NULL,
    diagnosis_code VARCHAR(20) NOT NULL,
    diagnosis_description VARCHAR(150) NOT NULL,
    CONSTRAINT fk_diagnoses_appointment
        FOREIGN KEY (appointment_id) REFERENCES appointments(appointment_id)
        ON DELETE CASCADE
);

CREATE TABLE prescriptions (
    prescription_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    appointment_id BIGINT NOT NULL,
    medicine_name VARCHAR(120) NOT NULL,
    dosage VARCHAR(80) NOT NULL,
    duration_days INT NOT NULL,
    CONSTRAINT fk_prescriptions_appointment
        FOREIGN KEY (appointment_id) REFERENCES appointments(appointment_id)
        ON DELETE CASCADE,
    CONSTRAINT chk_duration_days
        CHECK (duration_days > 0)
);

CREATE TABLE billing (
    bill_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    appointment_id BIGINT NOT NULL UNIQUE,
    total_amount DECIMAL(10,2) NOT NULL,
    paid_amount DECIMAL(10,2) NOT NULL DEFAULT 0,
    balance_amount DECIMAL(10,2) NOT NULL DEFAULT 0,
    payment_status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_billing_appointment
        FOREIGN KEY (appointment_id) REFERENCES appointments(appointment_id)
        ON DELETE CASCADE,
    CONSTRAINT chk_total_amount CHECK (total_amount >= 0),
    CONSTRAINT chk_paid_amount CHECK (paid_amount >= 0),
    CONSTRAINT chk_payment_status CHECK (payment_status IN ('Pending','Partial','Paid'))
);

CREATE TABLE lab_tests (
    lab_test_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    appointment_id BIGINT NOT NULL,
    test_name VARCHAR(100) NOT NULL,
    result_value VARCHAR(50),
    result_status VARCHAR(30),
    test_date DATE NOT NULL,
    CONSTRAINT fk_lab_tests_appointment
        FOREIGN KEY (appointment_id) REFERENCES appointments(appointment_id)
        ON DELETE CASCADE,
    CONSTRAINT chk_result_status CHECK (result_status IN ('Normal','Abnormal','Critical'))
);

CREATE TABLE audit_log (
    audit_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    entity_name VARCHAR(50) NOT NULL,
    entity_id BIGINT NOT NULL,
    action_type VARCHAR(20) NOT NULL,
    action_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(100) NOT NULL DEFAULT 'system'
);

DROP TRIGGER IF EXISTS trg_billing_before_insert;
DROP TRIGGER IF EXISTS trg_billing_before_update;
DROP TRIGGER IF EXISTS trg_appointments_after_insert;
DROP PROCEDURE IF EXISTS register_payment;
DROP PROCEDURE IF EXISTS schedule_appointment;

DELIMITER $$

CREATE TRIGGER trg_billing_before_insert
BEFORE INSERT ON billing
FOR EACH ROW
BEGIN
    SET NEW.balance_amount = NEW.total_amount - NEW.paid_amount;

    IF NEW.paid_amount <= 0 THEN
        SET NEW.payment_status = 'Pending';
    ELSEIF NEW.paid_amount >= NEW.total_amount THEN
        SET NEW.payment_status = 'Paid';
        SET NEW.balance_amount = 0;
    ELSE
        SET NEW.payment_status = 'Partial';
    END IF;
END $$

CREATE TRIGGER trg_billing_before_update
BEFORE UPDATE ON billing
FOR EACH ROW
BEGIN
    SET NEW.balance_amount = NEW.total_amount - NEW.paid_amount;

    IF NEW.paid_amount <= 0 THEN
        SET NEW.payment_status = 'Pending';
    ELSEIF NEW.paid_amount >= NEW.total_amount THEN
        SET NEW.payment_status = 'Paid';
        SET NEW.balance_amount = 0;
    ELSE
        SET NEW.payment_status = 'Partial';
    END IF;
END $$

CREATE TRIGGER trg_appointments_after_insert
AFTER INSERT ON appointments
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (entity_name, entity_id, action_type, changed_by)
    VALUES ('appointments', NEW.appointment_id, 'INSERT', CURRENT_USER());
END $$

CREATE PROCEDURE register_payment(IN p_bill_id BIGINT, IN p_amount DECIMAL(10,2))
BEGIN
    UPDATE billing
    SET paid_amount = paid_amount + p_amount
    WHERE bill_id = p_bill_id;
END $$

CREATE PROCEDURE schedule_appointment(
    IN p_patient_id BIGINT,
    IN p_doctor_id BIGINT,
    IN p_appointment_date DATETIME,
    IN p_visit_reason VARCHAR(150)
)
BEGIN
    INSERT INTO appointments (
        patient_id,
        doctor_id,
        appointment_date,
        appointment_status,
        visit_reason
    )
    VALUES (
        p_patient_id,
        p_doctor_id,
        p_appointment_date,
        'Scheduled',
        p_visit_reason
    );
END $$

DELIMITER ;
INSERT INTO departments (department_name)
SELECT DISTINCT TRIM(department_name) AS department_name
FROM legacy_encounter_data
WHERE department_name IS NOT NULL;

INSERT INTO doctors (first_name, last_name, specialty, department_id)
SELECT DISTINCT
    CASE
        WHEN SUBSTRING_INDEX(TRIM(REPLACE(REPLACE(doctor_full_name, 'Dr. ', ''), 'Dr ', '')), ' ', 1) = ''
            THEN 'Unknown'
        ELSE SUBSTRING_INDEX(TRIM(REPLACE(REPLACE(doctor_full_name, 'Dr. ', ''), 'Dr ', '')), ' ', 1)
    END AS first_name,
    CASE
        WHEN SUBSTRING_INDEX(TRIM(REPLACE(REPLACE(doctor_full_name, 'Dr. ', ''), 'Dr ', '')), ' ', -1) = ''
            THEN 'Unknown'
        ELSE SUBSTRING_INDEX(TRIM(REPLACE(REPLACE(doctor_full_name, 'Dr. ', ''), 'Dr ', '')), ' ', -1)
    END AS last_name,
    TRIM(doctor_specialty) AS specialty,
    d.department_id
FROM legacy_encounter_data l
JOIN departments d
    ON TRIM(l.department_name) = d.department_name
WHERE l.doctor_full_name IS NOT NULL;

INSERT INTO patients (
    legacy_patient_code,
    first_name,
    last_name,
    date_of_birth,
    gender,
    phone,
    email,
    address_line,
    city,
    state,
    postal_code
)
SELECT
    x.legacy_patient_code,
    SUBSTRING_INDEX(x.patient_full_name, ' ', 1) AS first_name,
    CASE
        WHEN SUBSTRING_INDEX(x.patient_full_name, ' ', -1) = SUBSTRING_INDEX(x.patient_full_name, ' ', 1)
            THEN 'Unknown'
        ELSE SUBSTRING_INDEX(x.patient_full_name, ' ', -1)
    END AS last_name,
    x.date_of_birth,
    CASE
        WHEN x.gender IN ('Male','Female','Other') THEN x.gender
        ELSE 'Other'
    END AS gender,
    x.phone,
    x.email,
    x.address_line,
    x.city,
    x.state,
    x.postal_code
FROM (
    SELECT l.*
    FROM legacy_encounter_data l
    JOIN (
        SELECT legacy_patient_code, MAX(legacy_row_id) AS max_row_id
        FROM legacy_encounter_data
        WHERE legacy_patient_code IS NOT NULL
        GROUP BY legacy_patient_code
    ) latest
      ON l.legacy_patient_code = latest.legacy_patient_code
     AND l.legacy_row_id = latest.max_row_id
) x;
INSERT IGNORE INTO insurance_providers (provider_name, policy_type)
SELECT
    provider_name,
    MIN(policy_type) AS policy_type
FROM (
    SELECT
        CASE
            WHEN UPPER(REPLACE(insurance_provider_name, ' ', '')) = 'BLUECROSS' THEN 'Blue Cross'
            WHEN UPPER(REPLACE(insurance_provider_name, ' ', '')) = 'UNITEDHEALTH' THEN 'United Health'
            WHEN insurance_provider_name = 'Self Pay' THEN 'Self Pay'
            ELSE CONCAT(
                UPPER(LEFT(LOWER(insurance_provider_name), 1)),
                SUBSTRING(LOWER(insurance_provider_name), 2)
            )
        END AS provider_name,
        COALESCE(policy_type, 'Unknown') AS policy_type
    FROM legacy_encounter_data
    WHERE insurance_provider_name IS NOT NULL
) x
GROUP BY provider_name;

INSERT IGNORE INTO patient_insurance (
    patient_id,
    insurance_id,
    policy_number,
    start_date,
    end_date,
    is_active
)
SELECT DISTINCT
    p.patient_id,
    i.insurance_id,
    COALESCE(l.policy_number, CONCAT('NO-POLICY-', p.patient_id)),
    '2025-01-01',
    '2026-12-31',
    TRUE
FROM legacy_encounter_data l
JOIN patients p
    ON l.legacy_patient_code = p.legacy_patient_code
JOIN insurance_providers i
    ON i.provider_name = CASE
        WHEN UPPER(REPLACE(l.insurance_provider_name, ' ', '')) = 'BLUECROSS' THEN 'Blue Cross'
        WHEN UPPER(REPLACE(l.insurance_provider_name, ' ', '')) = 'UNITEDHEALTH' THEN 'United Health'
        WHEN l.insurance_provider_name = 'Self Pay' THEN 'Self Pay'
        ELSE CONCAT(
            UPPER(LEFT(LOWER(l.insurance_provider_name), 1)),
            SUBSTRING(LOWER(l.insurance_provider_name), 2)
        )
    END;

INSERT INTO appointments (
    patient_id,
    doctor_id,
    appointment_date,
    appointment_status,
    visit_reason
)
SELECT
    p.patient_id,
    d.doctor_id,
    l.appointment_date,
    CASE
        WHEN l.appointment_status IN ('Scheduled','Completed','Cancelled') THEN l.appointment_status
        ELSE 'Scheduled'
    END,
    l.visit_reason
FROM legacy_encounter_data l
JOIN patients p
    ON l.legacy_patient_code = p.legacy_patient_code
JOIN departments dept
    ON TRIM(l.department_name) = dept.department_name
JOIN doctors d
    ON d.department_id = dept.department_id
   AND d.specialty = TRIM(l.doctor_specialty)
   AND d.full_name = CONCAT(
        SUBSTRING_INDEX(TRIM(REPLACE(REPLACE(l.doctor_full_name, 'Dr. ', ''), 'Dr ', '')), ' ', 1),
        ' ',
        SUBSTRING_INDEX(TRIM(REPLACE(REPLACE(l.doctor_full_name, 'Dr. ', ''), 'Dr ', '')), ' ', -1)
   )
ORDER BY l.legacy_row_id;

INSERT INTO diagnoses (appointment_id, diagnosis_code, diagnosis_description)
SELECT
    a.appointment_id,
    l.diagnosis_code,
    l.diagnosis_description
FROM appointments a
JOIN patients p
    ON a.patient_id = p.patient_id
JOIN legacy_encounter_data l
    ON p.legacy_patient_code = l.legacy_patient_code
   AND a.appointment_date = l.appointment_date;

INSERT INTO prescriptions (appointment_id, medicine_name, dosage, duration_days)
SELECT
    a.appointment_id,
    l.medicine_name,
    l.dosage,
    COALESCE(NULLIF(l.duration_days, 0), 7)
FROM appointments a
JOIN patients p
    ON a.patient_id = p.patient_id
JOIN legacy_encounter_data l
    ON p.legacy_patient_code = l.legacy_patient_code
   AND a.appointment_date = l.appointment_date;

INSERT INTO billing (appointment_id, total_amount, paid_amount, payment_status)
SELECT
    a.appointment_id,
    l.total_amount,
    l.paid_amount,
    CASE
        WHEN l.payment_status IN ('Pending','Partial','Paid') THEN l.payment_status
        ELSE 'Pending'
    END
FROM appointments a
JOIN patients p
    ON a.patient_id = p.patient_id
JOIN legacy_encounter_data l
    ON p.legacy_patient_code = l.legacy_patient_code
   AND a.appointment_date = l.appointment_date;

INSERT INTO lab_tests (appointment_id, test_name, result_value, result_status, test_date)
SELECT
    a.appointment_id,
    l.lab_test_name,
    l.result_value,
    CASE
        WHEN l.result_status IN ('Normal','Abnormal','Critical') THEN l.result_status
        ELSE 'Normal'
    END,
    l.test_date
FROM appointments a
JOIN patients p
    ON a.patient_id = p.patient_id
JOIN legacy_encounter_data l
    ON p.legacy_patient_code = l.legacy_patient_code
   AND a.appointment_date = l.appointment_date;

CREATE VIEW vw_patient_appointment_summary AS
SELECT
    a.appointment_id,
    p.legacy_patient_code,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    doc.full_name AS doctor_name,
    d.department_name,
    a.appointment_date,
    a.appointment_status,
    a.visit_reason
FROM appointments a
JOIN patients p ON a.patient_id = p.patient_id
JOIN doctors doc ON a.doctor_id = doc.doctor_id
JOIN departments d ON doc.department_id = d.department_id;

CREATE VIEW vw_monthly_revenue AS
SELECT
    DATE_FORMAT(a.appointment_date, '%Y-%m-01') AS revenue_month,
    SUM(b.total_amount) AS billed_amount,
    SUM(b.paid_amount) AS received_amount,
    SUM(b.balance_amount) AS outstanding_amount
FROM billing b
JOIN appointments a ON b.appointment_id = a.appointment_id
GROUP BY DATE_FORMAT(a.appointment_date, '%Y-%m-01');

CREATE VIEW vw_doctor_workload AS
SELECT
    doc.doctor_id,
    doc.full_name,
    d.department_name,
    COUNT(a.appointment_id) AS total_appointments
FROM doctors doc
LEFT JOIN departments d ON doc.department_id = d.department_id
LEFT JOIN appointments a ON doc.doctor_id = a.doctor_id
GROUP BY doc.doctor_id, doc.full_name, d.department_name;

CREATE VIEW vw_unpaid_bills AS
SELECT
    b.bill_id,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    doc.full_name AS doctor_name,
    b.total_amount,
    b.paid_amount,
    b.balance_amount,
    b.payment_status
FROM billing b
JOIN appointments a ON b.appointment_id = a.appointment_id
JOIN patients p ON a.patient_id = p.patient_id
JOIN doctors doc ON a.doctor_id = doc.doctor_id
WHERE b.payment_status <> 'Paid';

CREATE VIEW vw_lab_alerts AS
SELECT
    lt.lab_test_id,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    lt.test_name,
    lt.result_value,
    lt.result_status,
    lt.test_date
FROM lab_tests lt
JOIN appointments a ON lt.appointment_id = a.appointment_id
JOIN patients p ON a.patient_id = p.patient_id
WHERE lt.result_status IN ('Abnormal','Critical');

CREATE INDEX idx_patients_legacy_code ON patients (legacy_patient_code);
CREATE INDEX idx_appointments_patient ON appointments (patient_id);
CREATE INDEX idx_appointments_doctor ON appointments (doctor_id);
CREATE INDEX idx_appointments_date ON appointments (appointment_date);
CREATE INDEX idx_billing_status ON billing (payment_status);
CREATE INDEX idx_lab_result_status ON lab_tests (result_status);
CREATE INDEX idx_doctors_department ON doctors (department_id);

SELECT 'legacy_rows' AS metric, COUNT(*) AS total FROM legacy_encounter_data
UNION ALL
SELECT 'modern_patients', COUNT(*) FROM patients
UNION ALL
SELECT 'modern_doctors', COUNT(*) FROM doctors
UNION ALL
SELECT 'modern_appointments', COUNT(*) FROM appointments
UNION ALL
SELECT 'modern_bills', COUNT(*) FROM billing
UNION ALL
SELECT 'modern_lab_tests', COUNT(*) FROM lab_tests;

SELECT legacy_patient_code, COUNT(*) AS duplicate_count
FROM patients
GROUP BY legacy_patient_code
HAVING COUNT(*) > 1;

SELECT a.appointment_id
FROM appointments a
LEFT JOIN patients p ON a.patient_id = p.patient_id
LEFT JOIN doctors d ON a.doctor_id = d.doctor_id
WHERE p.patient_id IS NULL OR d.doctor_id IS NULL;

SELECT bill_id, total_amount, paid_amount, balance_amount, payment_status
FROM billing
WHERE balance_amount <> total_amount - paid_amount
   OR (paid_amount >= total_amount AND payment_status <> 'Paid')
   OR (paid_amount = 0 AND payment_status <> 'Pending');

CALL schedule_appointment(1, 1, '2026-05-01 10:30:00', 'Annual physical');

INSERT INTO billing (appointment_id, total_amount, paid_amount, payment_status)
SELECT MAX(appointment_id), 350.00, 100.00, 'Pending'
FROM appointments;

SET @latest_bill_id = (SELECT MAX(bill_id) FROM billing);
CALL register_payment(@latest_bill_id, 250.00);

SELECT * FROM vw_patient_appointment_summary ORDER BY appointment_id DESC LIMIT 5;
SELECT * FROM billing ORDER BY bill_id DESC LIMIT 5;
SELECT * FROM audit_log ORDER BY audit_id DESC LIMIT 10;
SELECT * FROM vw_monthly_revenue;
SELECT * FROM vw_doctor_workload ORDER BY total_appointments DESC LIMIT 10;
SELECT * FROM vw_unpaid_bills LIMIT 10;
SELECT * FROM vw_lab_alerts LIMIT 10;

SELECT gender, COUNT(*) AS total_patients
FROM patients
GROUP BY gender
ORDER BY total_patients DESC;

SELECT
    CASE
        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN 'Under 18'
        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 35 THEN '18-34'
        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 50 THEN '35-49'
        WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 65 THEN '50-64'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS total_patients
FROM patients
GROUP BY age_group
ORDER BY total_patients DESC;

SELECT d.department_name, COUNT(*) AS appointment_count
FROM appointments a
JOIN doctors doc ON a.doctor_id = doc.doctor_id
JOIN departments d ON doc.department_id = d.department_id
GROUP BY d.department_name
ORDER BY appointment_count DESC;

SELECT doc.full_name, COUNT(*) AS total_appointments
FROM appointments a
JOIN doctors doc ON a.doctor_id = doc.doctor_id
GROUP BY doc.full_name
ORDER BY total_appointments DESC
LIMIT 10;

SELECT
    DATE_FORMAT(a.appointment_date, '%Y-%m-01') AS revenue_month,
    ROUND(SUM(b.total_amount), 2) AS billed_amount,
    ROUND(SUM(b.paid_amount), 2) AS received_amount,
    ROUND(SUM(b.balance_amount), 2) AS outstanding_amount
FROM billing b
JOIN appointments a ON b.appointment_id = a.appointment_id
GROUP BY DATE_FORMAT(a.appointment_date, '%Y-%m-01')
ORDER BY revenue_month;

SELECT diagnosis_description, COUNT(*) AS occurrences
FROM diagnoses
GROUP BY diagnosis_description
ORDER BY occurrences DESC
LIMIT 10;

SELECT lt.test_name, lt.result_status, COUNT(*) AS total_cases
FROM lab_tests lt
WHERE lt.result_status IN ('Abnormal','Critical')
GROUP BY lt.test_name, lt.result_status
ORDER BY total_cases DESC;

SELECT
    b.bill_id,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    b.total_amount,
    b.paid_amount,
    b.balance_amount,
    b.payment_status
FROM billing b
JOIN appointments a ON b.appointment_id = a.appointment_id
JOIN patients p ON a.patient_id = p.patient_id
WHERE b.payment_status IN ('Pending','Partial')
ORDER BY b.balance_amount DESC
LIMIT 20;

SELECT
    p.legacy_patient_code,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
    COUNT(*) AS visit_count
FROM appointments a
JOIN patients p ON a.patient_id = p.patient_id
GROUP BY p.legacy_patient_code, patient_name
HAVING COUNT(*) > 3
ORDER BY visit_count DESC, patient_name;

SELECT *
FROM (
    SELECT
        doc.full_name,
        ROUND(SUM(b.total_amount), 2) AS total_revenue,
        RANK() OVER (ORDER BY SUM(b.total_amount) DESC) AS revenue_rank
    FROM billing b
    JOIN appointments a ON b.appointment_id = a.appointment_id
    JOIN doctors doc ON a.doctor_id = doc.doctor_id
    GROUP BY doc.full_name
) x
ORDER BY total_revenue DESC,
LIMIT 10;