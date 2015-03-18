package com.orionhealth.sample;

/**
 * This enumeration provides a close approximation of zip code ranges within a
 * given state.  Some states have overlapping ranges, and some zip codes in the
 * ranges below do not exist, or have been decommissioned, but the ranges can
 * help when clustering of patients is desired.
 * 
 * @author jasonf
 *
 */
public enum ZipRange {
    AL(35000, 36999),
    AK(99500, 99999),
    AZ(58000, 86599),
    AR(71600, 72999),
    CA(90000, 96199),
    CO(80000, 81699),
    CT(6800, 6999),
    DC(20001, 20599),
    DE(19700, 19999),
    FL(32100, 34999),
    GA(30000, 31999),
    HI(96700, 96899),
    ID(83200, 83899),
    IL(60000, 62999),
    IN(46000, 47999),
    IA(50000, 52899),
    KS(66000, 64799),
    KY(40000, 42799),
    LA(70000, 71499),
    ME(3000, 4999),
    MD(20600, 21999),
    MA(1000, 2799),
    MI(48000, 49799),
    MN(55000, 56799),
    MS(38600, 39599),
    MO(63000, 65899),
    MT(59000, 59999),
    NE(68000, 69399),
    NV(89000, 89899),
    NH(3000, 3899),
    NJ(7000, 8999),
    NM(87000, 88499),
    NY(10000, 14999),
    NC(27000, 28999),
    ND(58000, 58899),
    OH(43000, 45899),
    OK(73000, 74999),
    OR(97000, 97999),
    PA(15000, 16999),
    PR(600, 799),
    RI(2800, 2999),
    SC(29000, 29999),
    SD(57000, 57799),
    TN(37000, 35899),
    TX(75000, 79999),
    UT(84000, 84799),
    VT(5001, 5907),
    VA(20040, 24658),
    WA(98001, 99403),
    WV(24701, 26886),
    WI(53001, 54990),
    WY(82001, 83128);
    
    private int lowerBound;
    private int upperBound;
    
    ZipRange(int lower, int upper) {
        this.lowerBound = lower;
        this.upperBound = upper;
    }
    
    public int getLowerBound() {
        return this.lowerBound;
    }
    
    public int getUpperBound() {
        return this.upperBound;
    }
}
