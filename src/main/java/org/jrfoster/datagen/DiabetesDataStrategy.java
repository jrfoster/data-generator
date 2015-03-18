package org.jrfoster.datagen;

import java.util.Date;

import org.joda.time.Interval;

import com.datastax.driver.core.Session;

public class DiabetesDataStrategy extends AbstractDataStrategy {

    /**
     * Generate a new instance with the given Session, DataGenerator and
     * percent abnormal results
     * 
     * @param sess
     *            Cassandra session to use when executing DML
     * @param gen
     *            DataGenerator instance to use to generate data
     * @param pctAbnormal
     *            Desired percentage of abnormal results to create
     */
    public DiabetesDataStrategy(String keyspace, Session sess, 
            DataGenerator gen, Double pctAbnormal, Date loadDate) {
        super(keyspace, sess, gen, pctAbnormal, loadDate);
    }

    @Override
    public void generateEncounterData(int patientId, int measurementPeriodYear, boolean isMale) {
        // For the diabetes measures, we need to see that they have had at least
        // one visit in the measurement period and in the year prior to the
        // measurement period. So, while we don't really care too much about the
        // encounter data itself for the measures, we do use the admit/discharge
        // dates for other things and since almost everything we care about is
        // related to an encounter, we have to generate random encounter data
        // for some times in that two year period.
        Interval currMP = generator.generateMeasurementPeriod(measurementPeriodYear);
        Interval prevMP = new Interval(currMP.getStart().minusYears(1), currMP.getEnd());
        int numEncounters = generator.generateRandomCount(7);
        for (int j = 0; j < numEncounters; j++) {
            Date admitDate = generator.generateRandomTimestamp(
                    prevMP.getStart().toDate(), prevMP.getEnd().toDate());
            Date dschgDate = generator.generateEndOfDuration(admitDate,
                    generator.generateRandomCount(8));
            Interval encIntv = new Interval(admitDate.getTime(), dschgDate.getTime());
            
            // We need to generate an encounter id, so diagnoses can be tied to
            // an encounter
            int encId = generator.generateNextEncounterSequence();
        
            // Encounters
            session.execute(this.generateEncounterDML(patientId, encIntv, encId));
            
            // Diagnoses
            generateDiagnoses(patientId, encIntv, isMale, encId);
            
            // Labs
            generateLabs(patientId, encIntv);
            
            // Procedures
            generateProcedures(patientId, encIntv);
        }        
    }

    @Override
    public void generateLabs(int patientId, Interval interval) {
        // Generating lab tests related to the diabetes measures are limited
        // to HBA1C, LDLC. In addition, we include some vitals in this which
        // are Diastolic and Systolic blood pressure. We also fudge a little
        // and include tobacco nonuse and aspirin use for the composite
        // meausres.
        for (int k = 0; k < generator.generateRandomCount(5); k++) {
            // Determine if we should generate normal or abnormal ranges
            maintainAbnormalPercentage();

            // Hemoglobin Result
            session.execute(this.generateHemoglobinResultDML(patientId, interval));
            
            // LDL Cholesterol Result.  We go ahead and generate results for a
            // full lipid panel, but we are really only using the LDLC value
            // from it for diabetes work.
            Date lipidDate = generator.generateRandomTimestamp(
                    interval.getStart().toDate(), interval.getEnd().toDate());
            LipidPanelResult lipids = generator.generateRandomLipidPanelResult(
                    lipidDate, abnormalResult);
            session.execute(this.generateLDLCResultDML(patientId, lipids));
            
            // Diastolic and Systolic blood pressure (vitals taken same time)
            Date bpDate = generator.generateRandomTimestamp(
                    interval.getStart().toDate(), interval.getEnd().toDate());
            session.execute(this.generateSystolicResultDML(patientId, bpDate));
            session.execute(this.generateDiastolicResultDML(patientId, bpDate));
            
            // Tobacco Screening Result
            session.execute(this.generateTobaccoScreeningDML(patientId, interval));
        }
    }

    @Override
    public void generateProcedures(int patientId, Interval interval) {
        // No-Op
    }

    @Override
    public void generateDiagnoses(int patientId, Interval interval) {
        throw new UnsupportedOperationException();
    }
    
    private void generateDiagnoses(int patientId, Interval interval, boolean isMale, int encId) {
        // Some of the females will be pregnant, and some of those pregnant females
        // will have to be diagnosed with gestational diabetes.  All the rest will
        // get a random diabetes diagnosis
        // The DM measures use a very large list of diagnoses codes, but still
        // only contains the ICD-9 codes for simplicity's sake.  We only need
        // a single row for a diagnoses for diabetes.
        CodedValue dxCode = null;
        if (isMale) {
            dxCode = this.generator.generateRandomDMDiagnosis();
            session.execute(this.generatePatientDiagnosesDML(patientId, interval, dxCode, encId));
        } else {
            dxCode = this.generator.generateRandomPregnancyDiagnosis();
            if (dxCode != null) {
                session.execute(this.generatePatientDiagnosesDML(patientId, interval, dxCode, encId));
                dxCode = this.generator.generateRandomDMDiagnosis(false);
                session.execute(this.generatePatientDiagnosesDML(patientId, interval, dxCode, encId));
            } else {
                dxCode = this.generator.generateRandomDMDiagnosis();
                session.execute(this.generatePatientDiagnosesDML(patientId, interval, dxCode, encId));
            }
        }
    }

    @Override
    protected void generateScreening(int patientId) {
        // no-op
    }

    @Override
    protected void generatePrefills(int patientId, String hicn, boolean isMale, Interval interval, String fullName, Date dob) {
        // no-op
    }
}
