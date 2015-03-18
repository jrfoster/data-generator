package org.jrfoster.datagen;

import java.util.Date;

import org.joda.time.Interval;

import com.datastax.driver.core.Session;

public class CareDataStrategy extends AbstractDataStrategy {

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
    public CareDataStrategy(String keyspace, Session sess, 
            DataGenerator gen, Double pctAbnormal, Date loadDate) {
        super(keyspace, sess, gen, pctAbnormal, loadDate);
    }

    @Override
    public void generateEncounterData(int patientId, int measurementPeriodYear, boolean isMale) {
        // For this measure we need to create encounters in the current 
        // measurement period. Once we have an encounter, we simply generate 
        // data in the patient_results (labs) table.
        int numEncounters = generator.generateRandomCount(7);
        Interval currMP = generator.generateMeasurementPeriod(measurementPeriodYear);
        
        for (int j = 0; j < numEncounters; j++) {
            Date admitDate = generator.generateRandomTimestamp(
                    currMP.getStart().toDate(), currMP.getEnd().toDate());
            Date dschgDate = generator.generateEndOfDuration(admitDate,
                    generator.generateRandomCount(8));
            Interval encIntv = new Interval(admitDate.getTime(), dschgDate.getTime());
            
            // Encounters
            session.execute(this.generateEncounterDML(patientId, encIntv, 
                    generator.generateNextEncounterSequence()));
            
            // Labs
            generateLabs(patientId, encIntv);
        }
    }

    @Override
    protected void generateLabs(int patientId, Interval interval) {
        generateFallRiskDML(patientId, interval);
    }

    @Override
    protected void generateProcedures(int patientId, Interval interval) {
        // no-op
    }

    @Override
    protected void generateDiagnoses(int patientId, Interval interval) {
        // no-op
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
