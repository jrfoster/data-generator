package com.orionhealth.sample;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.Interval;

import com.datastax.driver.core.Session;

public abstract class AbstractDataStrategy implements EncounterDataStrategy {
    protected final Session session;
    protected final DataGenerator generator;

    private final String keyspaceName;
    private final double percentAbnormal;
    private final Date loadDate;

    protected boolean abnormalResult = false;
    private int throttle = 0;
    private int denominator = 0;
    private int numerator = 0;

    protected AbstractDataStrategy(String keyspace, Session sess,
            DataGenerator gen, Double pctAbnormal, Date loadDate) {
        this.session = sess;
        this.generator = gen;
        this.keyspaceName = keyspace;
        this.percentAbnormal = pctAbnormal;
        this.loadDate = loadDate;
    }

    @Override
    public abstract void generateEncounterData(int patientId,
            int measurementPeriodYear, boolean isMale);

    protected abstract void generateLabs(int patientId, Interval interval);

    protected abstract void generateProcedures(int patientId, Interval interval);

    protected abstract void generateDiagnoses(int patientId, Interval interval);

    protected abstract void generateScreening(int patientId);

    protected abstract void generatePrefills(int patientId, String hicn, boolean isMale, Interval interval, String fullName, Date dob);

    protected final String generateEncounterDML(int patientId, Interval interval, int encId) {
        // One note here is that because we only generate patients who are alive
        // the random discharge disposition generated here only includes the
        // codes indicating that a patient was discharged alive
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
                .append(keyspaceName)
                .append(".patient_encounters (encounter_uid, patient_id,patient_id_src,admit_date,encounter_type,discharge_date,discharge_method,load_date) values (")
                .append(String.format("%s,'%s','%s','%s','%s','%s','%s','%s'",
                        encId, patientId, "OHCP",
                        sdf.format(interval.getStart().toDate()),
                        generator.generateRandomAdmitType(),
                        sdf.format(interval.getEnd().toDate()),
                        generator.generateRandomDischargeDisposition(),
                        sdf.format(loadDate)))
                .append(");");

        return sb.toString();
    }

    protected String generatePatientDiagnosesDML(int patientId,
            Interval interval, CodedValue dxCode, int encId) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
                .append(keyspaceName)
                .append(".patient_diagnoses (diagnosis_uid,encounter_uid,patient_id,patient_id_src,code,codingsystem,diagnosis_date,load_date) values (")
                .append(String.format("%s,%s,'%s','%s','%s','%s','%s','%s'", 
                    generator.generateNextDiagnosisSequence(), encId, patientId, "OHCP", 
                    dxCode.getIdentifier(), dxCode.getCodingSystem(), sdf.format(
                        generator.generateRandomTimestamp(interval.getStart().toDate(), 
                            interval.getEnd().toDate())),
                    sdf.format(loadDate)))
                .append(");");

        return sb.toString();
    }

    protected String generatePatientProcedureDML(int patientId,
            Interval interval, CodedValue procCode) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
                .append(keyspaceName)
                .append(".patient_procedures (procedure_uid,patient_id,patient_id_src,code,codingsystem,procedure_date,load_date) values (")
                .append(String.format("%s,'%s','%s','%s','%s','%s','%s'", 
                    generator.generateNextProcedureSequence(), patientId, "OHCP", 
                    procCode.getIdentifier(), procCode.getCodingSystem(), 
                    sdf.format(generator.generateRandomTimestamp(interval.getStart().toDate(), 
                        interval.getEnd().toDate())),
                    sdf.format(loadDate)))
                .append(");");

        return sb.toString();
    }

    protected String generatePrefillElementDML(String hicn, boolean isMale, 
            Interval interval, String fullName, Date dob) {
        StringBuffer sb = new StringBuffer(256);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        String[] nameParts = fullName.split(",");
        
        sb.append("insert into ")
            .append(keyspaceName)
            .append(".cms_prefilled_elements (hicno, pat_first_name, pat_last_name, gender, dob, dm_hba1c_date, dm_ldlc_date, pcflushot, pcpneumoshot) values (")
            .append(String.format("'%s','%s','%s','%s','%s','%s','%s',%s,%s", 
                hicn, nameParts[1].trim(), nameParts[0].trim(), isMale ? "M" : "F", 
                sdf.format(dob), 
                sdf.format(generator.generateRandomTimestamp(
                    interval.getStart().toDate(),interval.getEnd().toDate())),
                sdf.format(generator.generateRandomTimestamp(
                    interval.getStart().toDate(),interval.getEnd().toDate())),
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0))
            .append(");");

        return sb.toString();
    }
    
    protected String generateScreeningDML(int patientId) {
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
            .append(keyspaceName)
            .append(".patient_screening(patient_id,patient_id_src,bmi_screening,bmi_followup_plan,tobacco_screening,tobacco_cessation,bp_screening,bp_followup_plan,clinical_depression,clinical_depression_followup_plan,breast_cancer_screening) values (")
            .append(String.format("'%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s", patientId, "OHCP",
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0, 
                generator.generateRandomBoolean() ? 1 : 0))
            .append(");");

        return sb.toString();
    }

    protected String generateHemoglobinResultDML(int patientId,
            Interval interval) {
        Date date = generator.generateRandomTimestamp(interval.getStart()
                .toDate(), interval.getEnd().toDate());
        return generateLabResultDML(patientId, date, "HBA1C",
                generator.generateRandomHemoglobinResult(abnormalResult));
    }

    protected String generateDiastolicResultDML(int patientId, Date resultDate) {
        return generateLabResultDML(patientId, resultDate, "BPD",
                generator.generateRandomDiastolicBP(abnormalResult));
    }

    protected String generateSystolicResultDML(int patientId, Date resultDate) {
        return generateLabResultDML(patientId, resultDate, "BPS",
                generator.generateRandomSystolicBP(abnormalResult));
    }

    protected String generateTobaccoScreeningDML(int patientId,
            Interval interval) {
        Date date = generator.generateRandomTimestamp(interval.getStart()
                .toDate(), interval.getEnd().toDate());
        return generateLabResultDML(patientId, date, "TOBACCO_NONUSE",
                generator.generateRandomBoolean() ? 1.0D : 0.0D);
    }

    protected String generateHDLCResultDML(int patientId,
            LipidPanelResult lipids) {
        return generateLabResultDML(patientId, lipids.getLipdsDate(), "HDLC",
                lipids.getHDLC());
    }

    protected String generateLDLCResultDML(int patientId,
            LipidPanelResult lipids) {
        return generateLabResultDML(patientId, lipids.getLipdsDate(), "LDLC",
                lipids.getLDLC());
    }

    protected String generateTRIGResultDML(int patientId,
            LipidPanelResult lipids) {
        return generateLabResultDML(patientId, lipids.getLipdsDate(), "TRIG",
                lipids.getTRIG());
    }

    protected String generateCHOLResultDML(int patientId,
            LipidPanelResult lipids) {
        return generateLabResultDML(patientId, lipids.getLipdsDate(), "CHOL",
                lipids.getCHOL());
    }
    
    protected String generateFallRiskDML(int patientId,
            Interval interval) {
        Date date = generator.generateRandomTimestamp(interval.getStart()
                .toDate(), interval.getEnd().toDate());
        return generateLabResultDML(patientId, date, "FALL_RISK_SCREENED",
                generator.generateRandomBoolean() ? 1.0D : 0.0D);
        
    }

    protected void maintainAbnormalPercentage() {
        // Increment our counters
        ++denominator;
        if (abnormalResult)
            ++numerator;

        // We only change every 1000 results
        if (++throttle > 1000)
            throttle = 0;

        // Calculate the current percentage and adjust abnormal flag if
        // necessary
        abnormalResult = ((double) numerator / (double) denominator) < percentAbnormal;
    }

    private String generateLabResultDML(int patientId, Date resultDate,
            String name, double value) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
            .append(keyspaceName)
            .append(".patient_results (result_uid,patient_id,patient_id_src,result_name,result_date,result_value,result_source,load_date) values (")
            .append(String.format("%s,'%s','%s','%s','%s',%s,'%s','%s'",
                "uuid()", patientId, "OHCP", name, sdf.format(resultDate), value,
                "ADSLOADER", sdf.format(loadDate)))
            .append(");");

        return sb.toString();
    }

    @SuppressWarnings("unused")
    private String generateSodiumResultDDL(int patientId, Date admitDate,
            Date dischargeDate, boolean abnormal) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
            .append(keyspaceName)
            .append(".pat_results (patient_id, test_name, test_date, test_value) values (")
            .append(String.format("%s,'%s','%s',%s", patientId, "NA", sdf
                .format(generator.generateRandomTimestamp(admitDate,
                    dischargeDate)), generator
                .generateRandomSodiumResult(abnormal))).append(");");

        return sb.toString();
    }
}
