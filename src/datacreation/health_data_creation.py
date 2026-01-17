from faker import Faker
import pandas as pd
import random

fake = Faker()
Faker.seed(42)

def create_patients(num_patients=1000):
    patients = []
    genders = ['M', 'F']
    insurance_types = ['PRIVATE', 'MEDICARE', 'MEDICAID', 'SELF_PAY']

    for i in range(1, num_patients + 1):
        patients.append({
            'patient_id': 1000 + i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'gender': random.choice(genders),
            'date_of_birth': fake.date_of_birth(minimum_age=0, maximum_age=90).strftime('%Y-%m-%d'),
            'email': fake.unique.email(),
            'phone_number': fake.phone_number(),
            'insurance_type': random.choice(insurance_types),
            'registration_date': fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
        })

    return pd.DataFrame(patients)

patients_df = create_patients(1000)
patients_df.to_csv('patients.csv', index=False)




def create_encounters(patients, num_encounters=5000):
    encounters = []
    encounter_types = ['OUTPATIENT', 'INPATIENT', 'EMERGENCY']
    statuses = ['COMPLETED', 'CANCELLED', 'IN_PROGRESS']
    diagnoses = ['Hypertension', 'Diabetes', 'Asthma', 'COVID-19', 'Fracture', 'Migraine']

    for i in range(num_encounters):
        patient = patients.sample(1).iloc[0]

        encounters.append({
            'encounter_id': f'ENC{str(i+1).zfill(5)}',
            'patient_id': patient['patient_id'],
            'encounter_timestamp': fake.date_time_between(start_date='-365d', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
            'encounter_type': random.choice(encounter_types),
            'diagnosis': random.choice(diagnoses),
            'doctor_id': f'DR{random.randint(100,199)}',
            'hospital_unit': random.choice(['CARDIOLOGY', 'NEUROLOGY', 'ORTHOPEDICS', 'GENERAL']),
            'status': random.choice(statuses)
        })

    return pd.DataFrame(encounters)

encounters_df = create_encounters(patients_df, 5000)
encounters_df.to_csv('encounters.csv', index=False)



def create_treatments(encounters, num_treatments=2000):
    treatments = []
    procedures = ['MRI', 'X-RAY', 'CT SCAN', 'BLOOD TEST', 'SURGERY']
    statuses = ['COMPLETED', 'SCHEDULED', 'CANCELLED']

    for i in range(num_treatments):
        enc = encounters.sample(1).iloc[0]

        treatments.append({
            'treatment_id': f'TRT{str(i+1).zfill(4)}',
            'encounter_id': enc['encounter_id'],
            'procedure_name': random.choice(procedures),
            'procedure_date': fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
            'cost': round(random.uniform(200, 20000), 2),
            'status': random.choice(statuses)
        })

    return pd.DataFrame(treatments)

treatments_df = create_treatments(encounters_df, 2000)
treatments_df.to_csv('treatments.csv', index=False)

