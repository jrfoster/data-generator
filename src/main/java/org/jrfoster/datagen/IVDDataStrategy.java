package org.jrfoster.datagen;

import java.util.Date;

import org.joda.time.Interval;

import com.datastax.driver.core.Session;

public class IVDDataStrategy extends AbstractDataStrategy {

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
    public IVDDataStrategy(String keyspace, Session sess, 
            DataGenerator gen, Double pctAbnormal, Date loadDate) {
        super(keyspace, sess, gen, pctAbnormal, loadDate);
    }

    @Override
    public void generateEncounterData(int patientId, int measurementPeriodYear, boolean isMale) {
        // For the IVD measures, we need to have a couple of different types
        // of patients in the population.  The first, would be those that have
        // a diagnosis of IVD within the measurement period, and the second is
        // a person discharged alive with AMI who also had one or more specific
        // procedures performed in the year prior to the measurement period.
        // Also note that we ignore the gender flag for IVD, it has no bearing.
        // We also don't generate any pregnant females for the IVD population.
        int numEncounters = generator.generateRandomCount(7);
        Interval currMP = generator.generateMeasurementPeriod(measurementPeriodYear);
        Interval prevMP = new Interval(currMP.getStart().minusYears(1), currMP.getEnd());
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
            
            // Labs
            generateLabs(patientId, encIntv);

            // We use the discharge date as the determining factor as to which
            // population the patient will be in.  If the discharge date is before
            // the measurement period, we go with AMI/Procedure route, if its after
            // we go with just the plain IVD diagnosis route.
            if (dschgDate.before(currMP.getStart().toDate())) {
                // Diagnoses
                generateDiagnoses(patientId, encIntv, true, encId);
                
                // Procedures
                generateProcedures(patientId, encIntv);
            } else {
                // Diagnoses
                generateDiagnoses(patientId, encIntv, false, encId);
            }
        }    
    }

    @Override
    protected void generateLabs(int patientId, Interval interval) {
        // Generating lab tests related to the IVD measures are limited
        // to a full lipid panel.
        for (int k = 0; k < generator.generateRandomCount(5); k++) {
            // Determine if we should generate normal or abnormal ranges
            maintainAbnormalPercentage();

            // Full lipid panel results
            Date lipidDate = generator.generateRandomTimestamp(
                    interval.getStart().toDate(), interval.getEnd().toDate());
            LipidPanelResult lipids = generator.generateRandomLipidPanelResult(
                    lipidDate, abnormalResult);
            session.execute(this.generateHDLCResultDML(patientId, lipids));
            session.execute(this.generateLDLCResultDML(patientId, lipids));
            session.execute(this.generateTRIGResultDML(patientId, lipids));
            session.execute(this.generateCHOLResultDML(patientId, lipids));
        }
    }

    @Override
    protected void generateProcedures(int patientId, Interval interval) {
        CodedValue procCode = generator.generateRandomIVDProcCode();
        session.execute(this.generatePatientProcedureDML(patientId, interval, procCode));
    }

    @Override
    protected void generateDiagnoses(int patientId, Interval interval) {
        // We don't support this signature in this implementation.
        throw new UnsupportedOperationException();
    }
    
    private void generateDiagnoses(int patientId, Interval interval, boolean isAMI, int encId) {
        // The IVD measures use a very large list of diagnoses codes, but still
        // only contains the ICD-9 codes for simplicity's sake.  We only need
        // a single row for a diagnoses for diabetes.
        CodedValue dxCode = generator.generateRandomIVDDiagnosis(isAMI);
        session.execute(this.generatePatientDiagnosesDML(patientId, interval, dxCode, encId));
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
