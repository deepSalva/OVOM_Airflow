class SqlQueries:
    bloodtest_table_insert = ("""
        INSERT INTO bloodtest (ap_hi, ap_lo, cholesterol, gluc, smoke, alco, active, cardio, lab_id, 
                    patient_id, doc_id, date)
        SELECT
            logs.ap_hi, 
            logs.ap_lo, 
            logs.cholesterol, 
            logs.gluc, 
            logs.smoke, 
            logs.alco, 
            logs.active, 
            logs.cardio,
            logs.lab_id,
            logs.patient_id,
            patient.doc_id,
            logs.date
        FROM staging_logs logs
        INNER JOIN staging_patient patient
        ON logs.patient_id = patient.id
    """)

    patient_table_append = ("""
        INSERT INTO patient (patient_id, first_name, last_name, age, sex, height, weight, address)
        SELECT 
            id, 
            first_name, 
            last_name, 
            age, 
            sex, 
            height, 
            weight, 
            address
        FROM staging_patient
    """)

    patient_table_truncate = ("""
    BEGIN;
    TRUNCATE patient;
        INSERT INTO patient (patient_id, first_name, last_name, age, sex, height, weight, address)
        SELECT 
            id, 
            first_name, 
            last_name, 
            age, 
            sex, 
            height, 
            weight, 
            address
        FROM staging_patient;
        COMMIT;
    """)

    doctor_table_append = ("""
        INSERT INTO doctor (doc_id, doc_name, clinic)
        SELECT 
            distinct doc_id, 
            doc_name, 
            clinic
        FROM staging_patient
    """)

    doctor_table_truncate = ("""
    BEGIN;
    TRUNCATE doctor;
        INSERT INTO doctor (doc_id, doc_name, clinic)
        SELECT 
            distinct doc_id, 
            doc_name, 
            clinic
        FROM staging_patient;
        COMMIT;
    """)

    laboratory_table_append = ("""
        INSERT INTO laboratory (lab_id, lab_name, city)
        SELECT 
            distinct lab_id, 
            lab_name, 
            city
        FROM staging_logs
    """)

    laboratory_table_truncate = ("""
    BEGIN;
    TRUNCATE TABLE laboratory;
        INSERT INTO laboratory (lab_id, lab_name, city)
        SELECT 
            distinct lab_id, 
            lab_name, 
            city
        FROM staging_logs;
        COMMIT;
    """)

    patient_table_insert = [patient_table_append, patient_table_truncate]
    doctor_table_insert = [doctor_table_append, doctor_table_truncate]
    laboratory_table_insert = [laboratory_table_append, laboratory_table_truncate]

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-east-1'
        {}
    """

    quality_null = ("""
        select 
            sum(case when {} is null then 1 else 0 end) as sum_colum
        from {}
    """)

    patient_monitoring = ("""
        select 
            patient_id, sum(cholesterol)   
        from bloodtest 
        group by patient_id
        limit 100;
    """)
