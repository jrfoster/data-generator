package org.jrfoster.datagen;


public interface EncounterDataStrategy {
    
    void generateEncounterData(int patientId, int measurementPeriodYear, boolean isMale);
}
