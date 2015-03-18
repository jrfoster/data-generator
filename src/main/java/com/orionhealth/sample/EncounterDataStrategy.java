package com.orionhealth.sample;


public interface EncounterDataStrategy {
    
    void generateEncounterData(int patientId, int measurementPeriodYear, boolean isMale);
}
