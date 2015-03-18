package com.orionhealth.sample;

import java.util.Date;

/**
 * This class holds results related to a Lipid panel, including HDLC, LDLC,
 * TRIG and TC.
 * 
 * @author jasonf
 *
 */
public class LipidPanelResult {
    private Date lipidsDate;
    private double hdlcValue;
    private double ldlcValue;
    private double trigValue;
    
    public LipidPanelResult(Date date, double hdlcValue, double ldlcValue, double trigValue) {
        this.lipidsDate = date;
        this.hdlcValue = hdlcValue;
        this.ldlcValue = ldlcValue;
        this.trigValue = trigValue;
    }
    
    public Date getLipdsDate() {
        return lipidsDate;
    }

    public double getHDLC() {
        return hdlcValue;
    }

    public double getLDLC() {
        return ldlcValue;
    }

    public double getTRIG() {
        return trigValue;
    }

    public double getCHOL() {
        if (ldlcValue == 0 || hdlcValue == 0)
            return 0.0;
        
        return ldlcValue + hdlcValue;
    }
}
