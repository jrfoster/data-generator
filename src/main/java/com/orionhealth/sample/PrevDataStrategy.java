package com.orionhealth.sample;

import java.util.Date;

import org.joda.time.Interval;

import com.datastax.driver.core.Session;

public class PrevDataStrategy extends AbstractDataStrategy {

    public PrevDataStrategy(String keyspace, Session sess,
            DataGenerator gen, Double pctAbnormal, Date loadDate) {
        super(keyspace, sess, gen, pctAbnormal, loadDate);
    }

    public void generateEncounterData(int patientId, String hicn, int measurementPeriodYear, boolean isMale,
            String fullName, Date dob) {
        // For this measure we need to create encounters in three different
        // periods, the current measurement period, the "flu season" period and
        // the year prior to the measurement period.  We generate a random
        // encounter within the 2 year period and hopefully we will get some in
        // all the periods.  Once we have an encounter, we simply generate data
        // in the cms prefilled elements table and the screening (forms) table.
        int numEncounters = generator.generateRandomCount(7);
        Interval currMP = generator.generateMeasurementPeriod(measurementPeriodYear);
        Interval prevMP = new Interval(currMP.getStart().minusYears(1), currMP.getEnd());
        
        for (int j = 0; j < numEncounters; j++) {
            Date admitDate = generator.generateRandomTimestamp(
                    prevMP.getStart().toDate(), prevMP.getEnd().toDate());
            Date dschgDate = generator.generateEndOfDuration(admitDate,
                    generator.generateRandomCount(8));
            Interval encIntv = new Interval(admitDate.getTime(), dschgDate.getTime());
            
            // Encounters
            session.execute(this.generateEncounterDML(patientId, encIntv, 
                    generator.generateNextEncounterSequence()));
            
            // Prefills
            this.generatePrefills(patientId, hicn, isMale, encIntv, fullName, dob);
            
            // Screenings
            this.generateScreening(patientId);
        }
    }
        
    @Override
    public void generateEncounterData(int patientId, int measurementPeriodYear, boolean isMale) {
        // We don't support this signature in this implementation.
        throw new UnsupportedOperationException();
    }

    @Override
    protected void generateLabs(int patientId, Interval interval) {
        // no-op
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
        session.execute(this.generateScreeningDML(patientId));
    }

    @Override
    protected void generatePrefills(int patientId, String hicn, boolean isMale, Interval interval, String fullName, Date dob) {
        session.execute(this.generatePrefillElementDML(hicn, isMale, interval, fullName, dob));
    }
}
